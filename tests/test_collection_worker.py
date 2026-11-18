import unittest
from datetime import date, datetime, time, timedelta, timezone
import httpx

from chart_history import save_chart_snapshot, snapshot_values
from collection_worker import (
    _process_pending_history_jobs,
    _release_schedule,
    _start_run,
    next_schedule_time,
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
            return {"attempt_count": 1}
        return None

    async def fetchval(self, query, *args):
        self.fetchval_calls.append((query, args))
        if "INSERT INTO public.spotify_collection_runs" in query:
            return 91
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

    async def fetch(self, query, *args):
        self.fetch_queries.append(query)
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
