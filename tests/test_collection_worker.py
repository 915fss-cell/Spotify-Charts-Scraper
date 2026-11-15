import unittest
from datetime import date, datetime, time, timezone
import httpx

from chart_history import save_chart_snapshot, snapshot_values
from collection_worker import _process_pending_history_jobs, next_schedule_time
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

    async def fetch(self, query, *args):
        return [self.job]

    async def fetchrow(self, query, *args):
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


if __name__ == "__main__":
    unittest.main()
