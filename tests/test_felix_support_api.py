import os
import unittest
from datetime import date, datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException

from app import felix_support_charts


class FelixSupportApiTests(unittest.IsolatedAsyncioTestCase):
    async def test_rejects_missing_or_invalid_bearer_token(self):
        request = SimpleNamespace(headers={"authorization": "Bearer wrong"})
        with patch.dict(os.environ, {"FELIX_SUPPORT_API_TOKEN": "correct"}, clear=False):
            with self.assertRaises(HTTPException) as raised:
                await felix_support_charts(
                    request, "live", None, None, None, None, 100, 0
                )
        self.assertEqual(raised.exception.status_code, 401)

    async def test_live_view_returns_saved_chart_payloads(self):
        fetched_at = datetime(2026, 11, 20, tzinfo=timezone.utc)
        pool = SimpleNamespace(
            fetch=AsyncMock(return_value=[{
                "chart_type": "daily_top_songs",
                "country_code": "GLOBAL",
                "chart_date": date(2026, 11, 20),
                "payload": {"entries": [{"id": "track-1"}]},
                "fetched_at": fetched_at,
            }]),
            fetchval=AsyncMock(return_value=1),
        )
        request = SimpleNamespace(headers={"authorization": "Bearer correct"})
        with (
            patch.dict(os.environ, {"FELIX_SUPPORT_API_TOKEN": "correct"}, clear=False),
            patch("app.get_history_pool", AsyncMock(return_value=pool)),
        ):
            result = await felix_support_charts(
                request, "live", None, None, None, None, 100, 0
            )

        self.assertEqual(result["view"], "live")
        self.assertEqual(result["total"], 1)
        self.assertEqual(result["charts"][0]["chartType"], "daily_top_songs")
        self.assertEqual(result["charts"][0]["data"]["entries"][0]["id"], "track-1")

    async def test_history_view_returns_paginated_snapshot_records(self):
        fetched_at = datetime(2026, 11, 20, tzinfo=timezone.utc)
        pool = SimpleNamespace(
            fetch=AsyncMock(return_value=[{
                "chart_type": "daily_top_songs",
                "country_code": "GLOBAL",
                "chart_date": date(2026, 11, 20),
                "track_uri": "spotify:track:123",
                "entity_type": "track",
                "track_name": "Song",
                "artist_names": ["Artist"],
                "chart_rank": 1,
                "previous_rank": 2,
                "peak_rank": 1,
                "consecutive_appearances": 3,
                "appearances": 5,
                "entry_date": date(2026, 11, 16),
                "stream_count": 12345,
                "fetched_at": fetched_at,
            }]),
            fetchval=AsyncMock(return_value=3),
        )
        request = SimpleNamespace(headers={"authorization": "Bearer correct"})
        with (
            patch.dict(os.environ, {"FELIX_SUPPORT_API_TOKEN": "correct"}, clear=False),
            patch("app.get_history_pool", AsyncMock(return_value=pool)),
        ):
            result = await felix_support_charts(
                request, "history", "daily_top_songs", "GLOBAL", None, None, 1, 1
            )

        self.assertEqual(result["total"], 3)
        self.assertTrue(result["hasMore"])
        self.assertEqual(result["charts"][0]["entityName"], "Song")
        self.assertEqual(result["charts"][0]["streamCount"], 12345)


if __name__ == "__main__":
    unittest.main()
