import unittest
from datetime import date, datetime, time, timedelta, timezone
import httpx
from fastapi import HTTPException

from chart_history import save_chart_snapshot, snapshot_values
from collection_worker import (
    _process_pending_history_jobs,
    _release_schedule,
    _start_run,
    next_schedule_time,
    normalize_chart_types,
    run_collection_tick,
)
from spotify_charts import parse_retry_after_delay


class RecordingConnection:
    def __init__(self):
        self.query = ""
        self.rows = []
        self.executed = []

    def transaction(self):
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def execute(self, query, *args):
        self.executed.append((query, args))

    async def executemany(self, query, rows):
        self.query = query
        self.rows = rows


class RecordingPool:
    def __init__(self, job, entity_uris=None):
        self.job = job
        self.entity_uris = entity_uris or ["spotify:track:0123456789abcdef"]
        self.connection = RecordingConnection()
        self.executed = []
        self.fetchrow_queries = []

    async def fetch(self, query, *args):
        return [self.job]

    async def fetchrow(self, query, *args):
        self.fetchrow_queries.append(query)
        return {"attempt_count": 1}

    async def fetchval(self, query, *args):
        return self.entity_uris

    async def execute(self, query, *args):
        self.executed.append((query, args))

    def acquire(self):
        return self

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, *args):
        return False


class RunLifecycleConnection:
    def __init__(self, schedule=None):
        self.schedule = schedule
        self.job_claim = {"attempt_count": 1}
        self.existing_status = None
        self.executed = []
        self.executemany_calls = []
        self.fetchval_calls = []

    def transaction(self):
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def fetchrow(self, query, *args):
        if "spotify_collection_schedule" in query:
            return self.schedule
        if "UPDATE public.spotify_chart_refresh_jobs" in query:
            return self.job_claim
        return None

    async def fetchval(self, query, *args):
        self.fetchval_calls.append((query, args))
        if "INSERT INTO public.spotify_collection_runs" in query:
            return 91
        if "SELECT status FROM public.spotify_chart_refresh_jobs" in query:
            return self.existing_status
        return None

    async def execute(self, query, *args):
        self.executed.append((query, args))

    async def executemany(self, query, rows):
        self.executemany_calls.append((query, rows))


class RunLifecyclePool:
    def __init__(self, fetch_responses=None, schedule=None):
        self.fetch_responses = list(fetch_responses or [])
        self.connection = RunLifecycleConnection(schedule)
        self.executed = []
        self.fetch_queries = []
        self.fetch_arguments = []

    async def fetch(self, query, *args):
        self.fetch_queries.append(query)
        self.fetch_arguments.append(args)
        return self.fetch_responses.pop(0) if self.fetch_responses else []

    async def fetchrow(self, query, *args):
        return None

    async def fetchval(self, query, *args):
        return None

    async def execute(self, query, *args):
        self.executed.append((query, args))

    def acquire(self):
        return self

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, *args):
        return False


class CollectionWorkerTests(unittest.IsolatedAsyncioTestCase):
    def test_empty_chart_selection_defaults_to_daily_top_songs(self):
        self.assertEqual(normalize_chart_types(None), ["daily_top_songs"])
        self.assertEqual(normalize_chart_types([]), ["daily_top_songs"])

    async def test_empty_workflow_selection_fetches_full_daily_song_chart_for_every_market(self):
        payload = {
            "displayChart": {"date": "2026-11-19"},
            "entries": [{
                "trackMetadata": {
                    "trackUri": "spotify:track:0123456789abcdef",
                    "trackName": "Example song",
                },
                "chartEntryData": {
                    "currentRank": 1,
                    "rankingMetric": {"type": "STREAMS", "value": "1234"},
                },
            }],
        }

        class Scraper:
            async def daily_top_songs(self, country, requested_date):
                return payload

        pool = RunLifecyclePool(fetch_responses=[[{
            "chart_type": "daily_top_songs",
            "country_code": "GLOBAL",
            "entity_uris": None,
        }]])

        async def scraper_factory():
            return Scraper()

        result = await run_collection_tick(
            pool,
            scraper_factory,
            include_history=False,
            max_chart_keys=1,
            all_markets_for_daily_songs=True,
        )

        self.assertEqual(result["requestedChartKeys"], 1)
        query = pool.fetch_queries[0]
        self.assertIn("unnest($5::text[])", query)
        self.assertNotIn("spotify_chart_tracking_configs", query)
        self.assertEqual(pool.fetch_arguments[0][0], ["daily_top_songs"])
        self.assertIn("GLOBAL", pool.fetch_arguments[0][4])
        self.assertIn("US", pool.fetch_arguments[0][4])
        self.assertEqual(len(pool.connection.executemany_calls[0][1]), 1)

    def test_chart_selection_filters_and_deduplicates_supported_charts(self):
        self.assertEqual(
            normalize_chart_types(["weekly_top_albums", "daily_top_songs", "daily_top_songs"]),
            ["daily_top_songs", "weekly_top_albums"],
        )

    def test_chart_selection_rejects_unsupported_types(self):
        with self.assertRaises(HTTPException) as raised:
            normalize_chart_types(["not_a_chart"])
        self.assertEqual(raised.exception.status_code, 400)

    def test_next_run_respects_local_timezone_and_advances_to_next_day(self):
        now = datetime(2026, 11, 14, 10, 0, tzinfo=timezone.utc)

        result = next_schedule_time(now, time(9, 0), "Africa/Algiers")

        self.assertEqual(result, datetime(2026, 11, 15, 8, 0, tzinfo=timezone.utc))

    def test_next_run_uses_same_local_day_when_schedule_is_still_ahead(self):
        now = datetime(2026, 11, 14, 7, 0, tzinfo=timezone.utc)

        result = next_schedule_time(now, time(9, 0), "Africa/Algiers")

        self.assertEqual(result, datetime(2026, 11, 14, 8, 0, tzinfo=timezone.utc))

    async def test_queued_date_job_fetches_once_and_saves_immutable_snapshot(self):
        job = {
            "chart_type": "daily_top_songs",
            "country_code": "GLOBAL",
            "requested_date": "2026-11-14",
        }

        class Scraper:
            async def daily_top_songs(self, country, requested_date):
                return {
                    "displayChart": {"date": requested_date},
                    "entries": [{
                        "trackMetadata": {
                            "trackUri": "spotify:track:0123456789abcdef",
                            "trackName": "Example song",
                            "artists": [{"name": "Example artist"}],
                        },
                        "chartEntryData": {
                            "rankingMetric": {"type": "STREAMS", "value": "1234"},
                            "currentRank": 1,
                        },
                    }],
                }

        pool = RecordingPool(job)

        result = await _process_pending_history_jobs(pool, Scraper(), "worker-1", 1)

        self.assertEqual(result, (1, 0, []))
        self.assertEqual(len(pool.connection.rows), 1)
        self.assertIn("DO NOTHING", pool.connection.query)
        self.assertIn("status = 'succeeded'", pool.connection.executed[-1][0])

    async def test_expired_history_job_lease_is_reclaimed(self):
        job = {
            "chart_type": "daily_top_songs",
            "country_code": "GLOBAL",
            "requested_date": "2026-11-14",
        }

        class Scraper:
            async def daily_top_songs(self, country, requested_date):
                return {"displayChart": {"date": requested_date}, "entries": []}

        pool = RecordingPool(job)

        result = await _process_pending_history_jobs(pool, Scraper(), "worker-2", 1)

        self.assertEqual(result, (1, 0, []))
        self.assertIn("status = 'running' AND lease_expires_at < now()", pool.fetchrow_queries[0])
        self.assertIn("lease_owner = $4", pool.fetchrow_queries[0])

    async def test_stale_spotify_authorization_keeps_date_job_retryable(self):
        from fastapi import HTTPException

        job = {
            "chart_type": "daily_top_songs",
            "country_code": "GLOBAL",
            "requested_date": "2026-11-14",
        }

        class Scraper:
            async def daily_top_songs(self, country, requested_date):
                raise HTTPException(status_code=401, detail="Reconnect Spotify.")

        pool = RecordingPool(job)

        result = await _process_pending_history_jobs(pool, Scraper(), "worker-1", 1)

        self.assertEqual(result[0:2], (0, 1))
        self.assertIn("status = 'retry'", pool.executed[0][0])
        self.assertIn("History retry failed (HTTPException)", pool.executed[0][1][4])
        self.assertEqual(pool.connection.rows, [])

    async def test_history_job_retries_when_spotify_returns_a_different_date(self):
        job = {
            "chart_type": "daily_top_songs",
            "country_code": "GLOBAL",
            "requested_date": "2026-11-14",
        }

        class Scraper:
            async def daily_top_songs(self, country, requested_date):
                return {"displayChart": {"date": "2026-11-13"}, "entries": []}

        pool = RecordingPool(job)

        result = await _process_pending_history_jobs(pool, Scraper(), "worker-1", 1)

        self.assertEqual(result[0:2], (0, 1))
        self.assertIn("status = 'retry'", pool.executed[0][0])
        self.assertIn("ValueError", pool.executed[0][1][4])
        self.assertEqual(pool.connection.rows, [])

    async def test_unverified_404_remains_retryable_not_unavailable(self):
        job = {
            "chart_type": "daily_top_songs",
            "country_code": "GLOBAL",
            "requested_date": "2026-11-14",
        }

        class Scraper:
            async def daily_top_songs(self, country, requested_date):
                response = httpx.Response(
                    404,
                    request=httpx.Request("GET", "https://api.spotify.com/charts"),
                )
                raise httpx.HTTPStatusError("not found", request=response.request, response=response)

        pool = RecordingPool(job)

        await _process_pending_history_jobs(pool, Scraper(), "worker-1", 1)

        self.assertIn("status = 'retry'", pool.executed[0][0])
        self.assertNotIn("status = 'unavailable'", pool.executed[0][0])
        self.assertEqual(pool.executed[0][1][5], 404)

    async def test_history_retry_persists_retry_after_from_spotify(self):
        job = {
            "chart_type": "daily_top_songs",
            "country_code": "GLOBAL",
            "requested_date": "2026-11-14",
        }

        class Scraper:
            async def daily_top_songs(self, country, requested_date):
                response = httpx.Response(
                    429,
                    headers={"Retry-After": "73"},
                    request=httpx.Request("GET", "https://api.spotify.com/charts"),
                )
                raise httpx.HTTPStatusError("rate limited", request=response.request, response=response)

        pool = RecordingPool(job)

        await _process_pending_history_jobs(pool, Scraper(), "worker-1", 1)

        self.assertEqual(pool.executed[0][1][6], 73)

    async def test_pending_history_saves_each_tracked_entity_and_deduplicates(self):
        job = {
            "chart_type": "daily_top_songs",
            "country_code": "GLOBAL",
            "requested_date": "2026-11-14",
        }
        first_uri = "spotify:track:0123456789abcdef"
        second_uri = "spotify:track:fedcba9876543210"
        first_entry = {
            "trackMetadata": {"trackUri": first_uri, "trackName": "First"},
            "chartEntryData": {"rankingMetric": {"type": "STREAMS", "value": "100"}},
        }
        duplicate_entry = {
            "trackMetadata": {"trackUri": first_uri, "trackName": "First"},
            "chartEntryData": {"rankingMetric": {"type": "STREAMS", "value": "125"}},
        }
        second_entry = {
            "trackMetadata": {"trackUri": second_uri, "trackName": "Second"},
            "chartEntryData": {"rankingMetric": {"type": "STREAMS", "value": "200"}},
        }

        class Scraper:
            async def daily_top_songs(self, country, requested_date):
                return {
                    "displayChart": {"date": requested_date},
                    "entries": [first_entry, duplicate_entry, second_entry],
                }

        pool = RecordingPool(job, [first_uri, second_uri])

        result = await _process_pending_history_jobs(pool, Scraper(), "worker-1", 1)

        self.assertEqual(result[0:2], (1, 0))
        self.assertEqual(len(pool.connection.rows), 2)
        self.assertEqual({row[6] for row in pool.connection.rows}, {125, 200})

    def test_retry_after_supports_seconds_and_http_dates(self):
        now = datetime(2026, 11, 14, 12, 0, tzinfo=timezone.utc)

        self.assertEqual(parse_retry_after_delay("12.5", now), 12.5)
        self.assertEqual(
            parse_retry_after_delay("Sat, 14 Nov 2026 12:01:00 GMT", now),
            60,
        )
        self.assertIsNone(parse_retry_after_delay("not a date", now))

    def test_local_daily_schedule_tracks_daylight_saving_time(self):
        spring_now = datetime(2026, 3, 7, 15, 0, tzinfo=timezone.utc)
        fall_now = datetime(2026, 10, 31, 14, 0, tzinfo=timezone.utc)

        self.assertEqual(
            next_schedule_time(spring_now, time(9, 0), "America/New_York"),
            datetime(2026, 3, 8, 13, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(
            next_schedule_time(fall_now, time(9, 0), "America/New_York"),
            datetime(2026, 11, 1, 14, 0, tzinfo=timezone.utc),
        )

    async def test_snapshot_with_missing_optional_metrics_preserves_rank(self):
        connection = RecordingConnection()
        entry = {
            "trackMetadata": {
                "trackUri": "spotify:track:0123456789abcdef",
                "trackName": "Ranked song",
            },
            "chartEntryData": {"currentRank": 3},
        }

        count = await save_chart_snapshot(
            None, "daily_top_songs", "GLOBAL", date(2026, 11, 14), [entry],
            connection=connection,
        )

        self.assertEqual(count, 1)
        self.assertIsNone(connection.rows[0][6])
        self.assertEqual(connection.rows[0][8], 3)
        self.assertIsNone(connection.rows[0][9])
        self.assertIsNotNone(snapshot_values(entry))

    async def test_snapshot_matches_selected_artist_by_artist_uri(self):
        connection = RecordingConnection()
        entry = {
            "trackMetadata": {
                "trackUri": "spotify:track:0123456789abcdef",
                "trackName": "Artist song",
                "artists": [{"spotifyUri": "spotify:artist:0123456789abcdef", "name": "Artist"}],
            },
            "chartEntryData": {"currentRank": 4},
        }

        count = await save_chart_snapshot(
            None, "daily_top_songs", "US", date(2026, 11, 14), [entry],
            allowed_entity_uris=set(), connection=connection,
            allowed_artist_uris={"spotify:artist:0123456789abcdef"},
        )

        self.assertEqual(count, 1)
        self.assertEqual(connection.rows[0][3], "spotify:track:0123456789abcdef")

    async def test_snapshot_conflicts_never_overwrite_saved_history(self):
        connection = RecordingConnection()
        entry = {
            "trackMetadata": {
                "trackUri": "spotify:track:0123456789abcdef",
                "trackName": "Example song",
                "artists": [{"name": "Example artist"}],
            },
            "chartEntryData": {
                "rankingMetric": {"type": "STREAMS", "value": "1234"},
                "currentRank": 1,
            },
        }

        count = await save_chart_snapshot(
            None,
            "daily_top_songs",
            "GLOBAL",
            date(2026, 11, 14),
            [entry],
            connection=connection,
        )

        self.assertEqual(count, 1)
        self.assertEqual(len(connection.rows), 1)
        self.assertIn("ON CONFLICT (chart_type, country_code, chart_date, track_uri) DO NOTHING", connection.query)
        self.assertNotIn("DO UPDATE", connection.query)

    async def test_repeated_latest_refresh_persists_same_date_payload_and_snapshots(self):
        track_uri = "spotify:track:0123456789abcdef"
        payload = {
            "displayChart": {"date": "2026-11-18"},
            "entries": [{
                "trackMetadata": {"trackUri": track_uri, "trackName": "Example song"},
                "chartEntryData": {"currentRank": 4},
            }],
        }

        class Scraper:
            async def daily_top_songs(self, country, requested_date):
                return payload

        pool = RunLifecyclePool(fetch_responses=[[{
            "chart_type": "daily_top_songs",
            "country_code": "US",
            "entity_uris": [track_uri],
        }]])
        pool.connection.job_claim = None
        pool.connection.existing_status = "succeeded"

        async def scraper_factory():
            return Scraper()

        result = await run_collection_tick(
            pool,
            scraper_factory,
            include_history=False,
            chart_types=["daily_top_songs"],
        )

        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(result["completedChartKeys"], 1)
        persisted_cache = [
            (query, args) for query, args in pool.connection.executed
            if "spotify_chart_payload_cache" in query
        ]
        self.assertEqual(len(persisted_cache), 2)
        self.assertTrue(all("DO UPDATE SET" in query for query, _ in persisted_cache))
        self.assertEqual({args[2] for _, args in persisted_cache}, {"2026-11-18", "latest"})
        self.assertEqual(len(pool.connection.executemany_calls), 1)
        self.assertEqual(pool.connection.executemany_calls[0][1][0][3], track_uri)
        updated_job = [query for query, _ in pool.connection.executed if "SET status = 'succeeded'" in query]
        self.assertEqual(len(updated_job), 1)

    async def test_scheduled_run_claims_due_schedule_and_creates_a_run(self):
        now = datetime.now(timezone.utc)
        pool = RunLifecyclePool(schedule={
            "enabled": True,
            "local_time": time(9, 0),
            "timezone": "UTC",
            "next_run_at": now - timedelta(seconds=1),
            "lease_expires_at": now - timedelta(minutes=1),
        })

        result = await _start_run(pool, "scheduled")

        self.assertIsNotNone(result)
        self.assertEqual(result[0], 91)
        self.assertTrue(result[1])
        self.assertEqual(len(pool.connection.executed), 1)
        self.assertIn("lease_expires_at", pool.connection.executed[0][0])
        self.assertIn("INSERT INTO public.spotify_collection_runs", pool.connection.fetchval_calls[0][0])

    async def test_scheduled_run_skips_when_another_worker_holds_the_lease(self):
        now = datetime.now(timezone.utc)
        pool = RunLifecyclePool(schedule={
            "enabled": True,
            "local_time": time(9, 0),
            "timezone": "UTC",
            "next_run_at": now - timedelta(minutes=1),
            "lease_expires_at": now + timedelta(minutes=2),
        })

        result = await _start_run(pool, "scheduled")

        self.assertIsNone(result)
        self.assertEqual(pool.connection.executed, [])
        self.assertEqual(pool.connection.fetchval_calls, [])

    async def test_schedule_release_calculates_next_run_and_clears_lease(self):
        pool = RunLifecyclePool(schedule={
            "enabled": True,
            "local_time": time(9, 0),
            "timezone": "Africa/Algiers",
        })

        await _release_schedule(pool, "worker-1")

        query, args = pool.connection.executed[0]
        self.assertIn("lease_owner = NULL", query)
        self.assertGreater(args[0], datetime.now(timezone.utc))
        self.assertEqual(args[1], "worker-1")

    async def test_latest_only_collection_skips_history_queue(self):
        uri = "spotify:track:0123456789abcdef"
        config = {
            "chart_type": "daily_top_songs",
            "country_code": "US",
            "entity_uris": [uri],
        }
        pool = RunLifecyclePool(fetch_responses=[[config]])

        class Scraper:
            requested_dates = []

            async def daily_top_songs(self, country, requested_date):
                self.requested_dates.append(requested_date)
                return {
                    "displayChart": {"date": "2026-11-19"},
                    "entries": [{
                        "trackMetadata": {"trackUri": uri, "trackName": "Example song"},
                        "chartEntryData": {
                            "rankingMetric": {"type": "STREAMS", "value": "12"},
                            "currentRank": 1,
                        },
                    }],
                }

        scraper = Scraper()

        async def scraper_factory():
            return scraper

        result = await run_collection_tick(
            pool, scraper_factory, source="manual", max_chart_keys=1, include_history=False
        )

        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(result["requestedChartKeys"], 1)
        self.assertEqual(result["completedChartKeys"], 1)
        self.assertEqual(scraper.requested_dates, ["latest"])
        self.assertEqual(len(pool.fetch_queries), 1)
        self.assertNotIn("requested_date <> 'latest'", pool.fetch_queries[0])
        self.assertIn("NOT $3::boolean", pool.fetch_queries[0])

    async def test_workflow_refresh_cutoff_is_passed_to_config_selection(self):
        uri = "spotify:track:0123456789abcdef"
        config = {
            "chart_type": "daily_top_songs",
            "country_code": "US",
            "entity_uris": [uri],
        }
        pool = RunLifecyclePool(fetch_responses=[[config]])
        workflow_started_at = datetime(2026, 11, 19, 20, 0, tzinfo=timezone.utc)

        class Scraper:
            async def daily_top_songs(self, country, requested_date):
                return {
                    "displayChart": {"date": "2026-11-18"},
                    "entries": [{
                        "trackMetadata": {"trackUri": uri, "trackName": "Example song"},
                        "chartEntryData": {"currentRank": 1},
                    }],
                }

        async def scraper_factory():
            return Scraper()

        result = await run_collection_tick(
            pool,
            scraper_factory,
            source="manual",
            max_chart_keys=1,
            include_history=False,
            refresh_after=workflow_started_at,
        )

        self.assertEqual(result["completedChartKeys"], 1)
        self.assertEqual(pool.fetch_arguments[0][3], workflow_started_at)
        self.assertIn("j.fetched_at >= $4::timestamptz", pool.fetch_queries[0])

    async def test_latest_refresh_resaves_existing_chart_date(self):
        config = {
            "chart_type": "weekly_top_albums",
            "country_code": "US",
            "entity_uris": ["spotify:album:0123456789abcdef"],
        }
        pool = RunLifecyclePool(fetch_responses=[[config]])
        pool.connection.job_claim = None
        pool.connection.existing_status = "succeeded"

        class Scraper:
            requested = []

            async def weekly_top_albums(self, country, requested_date):
                self.requested.append((country, requested_date))
                return {"displayChart": {"date": "2026-11-15"}, "entries": []}

        scraper = Scraper()

        async def scraper_factory():
            return scraper

        result = await run_collection_tick(
            pool, scraper_factory, source="manual", max_chart_keys=1, include_history=False
        )

        self.assertEqual(result["requestedChartKeys"], 1)
        self.assertEqual(result["completedChartKeys"], 1)
        self.assertEqual(scraper.requested, [("US", "latest")])
        cache_refresh = next(
            query for query, _ in pool.connection.executed
            if "spotify_chart_payload_cache" in query
        )
        self.assertIn("DO UPDATE SET", cache_refresh)
        job_refresh = next(
            (query, args) for query, args in pool.connection.executed
            if "SET status = 'succeeded'" in query
        )
        self.assertIn("status IN ('succeeded', 'unavailable', 'complete')", job_refresh[0])
        self.assertEqual(job_refresh[1], ("weekly_top_albums", "US", "2026-11-15", date(2026, 11, 15)))

    async def test_latest_404_is_marked_unavailable_for_the_day(self):
        config = {
            "chart_type": "weekly_top_albums",
            "country_code": "AE",
            "entity_uris": ["spotify:album:0123456789abcdef"],
        }
        pool = RunLifecyclePool(fetch_responses=[[], [config]])

        class Scraper:
            async def weekly_top_albums(self, country, requested_date):
                response = httpx.Response(
                    404,
                    request=httpx.Request("GET", "https://charts.example.test"),
                )
                raise httpx.HTTPStatusError("not found", request=response.request, response=response)

        async def scraper_factory():
            return Scraper()

        result = await run_collection_tick(pool, scraper_factory, source="manual", max_chart_keys=1)

        self.assertEqual(result["status"], "partial")
        self.assertEqual(result["requestedChartKeys"], 1)
        self.assertEqual(result["unavailableChartKeys"], 1)
        self.assertEqual(result["failedChartKeys"], 0)
        self.assertIn("status IN ('succeeded', 'complete', 'unavailable')", pool.fetch_queries[1])
        unavailable_write = next(
            (query, args) for query, args in pool.executed
            if "INSERT INTO public.spotify_chart_refresh_jobs" in query
        )
        self.assertIn("'latest', 'unavailable'", unavailable_write[0])
        self.assertEqual(unavailable_write[1], ("weekly_top_albums", "AE"))

    async def test_collection_run_records_partial_success_and_failure(self):
        first_uri = "spotify:track:0123456789abcdef"
        configs = [
            {"chart_type": "daily_top_songs", "country_code": "GLOBAL", "entity_uris": [first_uri]},
            {"chart_type": "daily_top_songs", "country_code": "US", "entity_uris": [first_uri]},
        ]
        pool = RunLifecyclePool(fetch_responses=[[], configs])

        class Scraper:
            async def daily_top_songs(self, country, requested_date):
                if country == "US":
                    response = httpx.Response(
                        503,
                        request=httpx.Request("GET", "https://charts.example.test"),
                    )
                    raise httpx.HTTPStatusError("temporary failure", request=response.request, response=response)
                return {
                    "displayChart": {"date": "2026-11-15"},
                    "entries": [{
                        "trackMetadata": {"trackUri": first_uri, "trackName": "Example song"},
                        "chartEntryData": {
                            "rankingMetric": {"type": "STREAMS", "value": "12"},
                            "currentRank": 1,
                        },
                    }],
                }

        async def scraper_factory():
            return Scraper()

        result = await run_collection_tick(pool, scraper_factory, source="manual", max_chart_keys=2)

        self.assertEqual(result["status"], "partial")
        self.assertEqual(result["requestedChartKeys"], 2)
        self.assertEqual(result["completedChartKeys"], 1)
        self.assertEqual(result["unavailableChartKeys"], 0)
        self.assertEqual(result["failedChartKeys"], 1)
        run_update = next(item for item in pool.executed if "UPDATE public.spotify_collection_runs" in item[0])
        self.assertEqual(run_update[1][1:6], ("partial", 2, 1, 0, 1))
        self.assertEqual(len(pool.connection.executemany_calls[0][1]), 1)


if __name__ == "__main__":
    unittest.main()
