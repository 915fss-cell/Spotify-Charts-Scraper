import asyncio
import unittest
from datetime import date
from unittest.mock import AsyncMock, patch

import app


class HistoryPoolStub:
    def __init__(self):
        self.fetchval_result = 42
        self.fetch_rows = [
            {
                "entity_uri": "spotify:track:0123456789abcdef",
                "entity_type": "track",
                "entity_name": "Example song",
                "artist_names": ["Example artist"],
                "appearance_count": 4,
                "latest_appearance": date(2026, 11, 14),
                "peak_rank": 1,
                "active_configs": 1,
            }
        ]
        self.fetchrow_results = []

    async def fetchval(self, *_args):
        return self.fetchval_result

    async def fetch(self, *_args):
        return self.fetch_rows

    async def fetchrow(self, *_args):
        return self.fetchrow_results.pop(0)


class HistoryDashboardTests(unittest.TestCase):
    def test_entity_list_is_paginated_and_serializes_dates(self):
        async def run():
            pool = HistoryPoolStub()
            with patch.object(app, "get_history_pool", new=AsyncMock(return_value=pool)), patch.object(
                app, "require_spotify_connection"
            ):
                result = await app.history_entities("Example", "track", "latest", 20, 20)
            self.assertEqual(result["total"], 42)
            self.assertTrue(result["hasMore"])
            self.assertEqual(result["items"][0]["latest_appearance"], "2026-11-14")

        asyncio.run(run())

    def test_entity_detail_returns_summary_and_bounded_points(self):
        async def run():
            pool = HistoryPoolStub()
            pool.fetchrow_results = [
                {
                    "entity_uri": "spotify:track:0123456789abcdef",
                    "entity_type": "track",
                    "entity_name": "Example song",
                    "artist_names": ["Example artist"],
                    "is_archived": False,
                },
                {
                    "appearances": 1,
                    "peak_rank": 2,
                    "first_appearance": date(2026, 11, 14),
                    "latest_appearance": date(2026, 11, 14),
                    "measured_stream_appearances": 1,
                },
            ]
            pool.fetch_rows = [
                {
                    "chart_type": "daily_top_songs",
                    "country_code": "GLOBAL",
                    "chart_date": date(2026, 11, 14),
                    "chart_rank": 2,
                    "stream_count": 1000,
                }
            ]
            with patch.object(app, "get_history_pool", new=AsyncMock(return_value=pool)), patch.object(
                app, "require_spotify_connection"
            ):
                result = await app.history_entity_detail(
                    "spotify:track:0123456789abcdef", None, None, None, None, 120, 0
                )
            self.assertEqual(result["summary"]["peak_rank"], 2)
            self.assertEqual(result["items"][0]["chart_date"], "2026-11-14")
            self.assertFalse(result["hasMore"])

        asyncio.run(run())

    def test_entity_detail_rejects_invalid_date_range(self):
        async def run():
            with patch.object(app, "require_spotify_connection"):
                with self.assertRaises(app.HTTPException) as raised:
                    await app.history_entity_detail(
                        "spotify:track:0123456789abcdef",
                        None,
                        None,
                        date(2026, 11, 15),
                        date(2026, 11, 14),
                        120,
                        0,
                    )
            self.assertEqual(raised.exception.status_code, 400)

        asyncio.run(run())

    def test_collection_jobs_are_filtered_and_paginated(self):
        async def run():
            pool = HistoryPoolStub()
            pool.fetchval_result = 12
            pool.fetch_rows = [{
                "chart_type": "daily_top_songs",
                "country_code": "GLOBAL",
                "requested_date": "2026-11-14",
                "chart_date": date(2026, 11, 14),
                "status": "retry",
                "attempt_count": 2,
                "next_attempt_at": None,
                "last_error": "timeout",
                "last_http_status": None,
                "updated_at": None,
            }]
            with patch.object(app, "get_history_pool", new=AsyncMock(return_value=pool)), patch.object(
                app, "require_spotify_connection"
            ):
                result = await app.collection_jobs("retry", "daily_top_songs", "global", None, None, None, 10, 10)
            self.assertEqual(result["total"], 12)
            self.assertTrue(result["hasMore"])
            self.assertEqual(result["items"][0]["chart_date"], "2026-11-14")

        asyncio.run(run())

    def test_retry_requeues_retryable_or_unavailable_job(self):
        async def run():
            pool = HistoryPoolStub()
            pool.fetchrow = AsyncMock(return_value={
                "chart_type": "daily_top_songs",
                "country_code": "GLOBAL",
                "requested_date": "2026-11-14",
                "status": "pending",
            })
            with patch.object(app, "get_history_pool", new=AsyncMock(return_value=pool)), patch.object(
                app, "require_spotify_connection"
            ):
                result = await app.retry_collection_job(app.ChartJobRetryInput(
                    chart_type="daily_top_songs", country_code="global", requested_date="2026-11-14"
                ))
            self.assertEqual(result["status"], "pending")
            self.assertEqual(result["country_code"], "GLOBAL")

        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
