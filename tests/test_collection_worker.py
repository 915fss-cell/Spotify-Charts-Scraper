import unittest
from datetime import date, datetime, time, timezone

from chart_history import save_chart_snapshot
from collection_worker import _process_pending_history_jobs, next_schedule_time


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
    def __init__(self, job):
        self.job = job
        self.connection = RecordingConnection()
        self.executed = []

    async def fetch(self, query, *args):
        return [self.job]

    async def fetchrow(self, query, *args):
        return {"attempt_count": 1}

    async def fetchval(self, query, *args):
        return ["spotify:track:0123456789abcdef"]

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
