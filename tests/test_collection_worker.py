import unittest
from datetime import date, datetime, time, timezone

from chart_history import save_chart_snapshot
from collection_worker import next_schedule_time


class RecordingConnection:
    def __init__(self):
        self.query = ""
        self.rows = []

    async def executemany(self, query, rows):
        self.query = query
        self.rows = rows


class CollectionWorkerTests(unittest.IsolatedAsyncioTestCase):
    def test_next_run_respects_local_timezone_and_advances_to_next_day(self):
        now = datetime(2026, 11, 14, 10, 0, tzinfo=timezone.utc)

        result = next_schedule_time(now, time(9, 0), "Africa/Algiers")

        self.assertEqual(result, datetime(2026, 11, 15, 8, 0, tzinfo=timezone.utc))

    def test_next_run_uses_same_local_day_when_schedule_is_still_ahead(self):
        now = datetime(2026, 11, 14, 7, 0, tzinfo=timezone.utc)

        result = next_schedule_time(now, time(9, 0), "Africa/Algiers")

        self.assertEqual(result, datetime(2026, 11, 14, 8, 0, tzinfo=timezone.utc))

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
