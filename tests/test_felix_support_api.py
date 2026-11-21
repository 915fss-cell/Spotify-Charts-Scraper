import os
import unittest
from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException

from app import felix_support_charts


class FelixSupportApiTests(unittest.IsolatedAsyncioTestCase):
    async def test_rejects_invalid_bearer_token(self):
        request = SimpleNamespace(headers={"authorization": "Bearer wrong"})
        with patch.dict(os.environ, {"SPOTIFY_CHARTS_API_TOKEN": "correct"}, clear=False):
            with self.assertRaises(HTTPException) as raised:
                await felix_support_charts(
                    request, "live", None, "global", None, None, None, 50, 0
                )
        self.assertEqual(raised.exception.status_code, 401)

    async def test_live_view_uses_selected_entity_live_chart_data(self):
        request = SimpleNamespace(headers={"authorization": "Bearer correct"})
        focused_result = {"releases": [{"entityName": "Song"}], "regions": []}
        with (
            patch.dict(os.environ, {"SPOTIFY_CHARTS_API_TOKEN": "correct"}, clear=False),
            patch("app.focused_live", AsyncMock(return_value=focused_result)) as focused_live,
        ):
            result = await felix_support_charts(
                request, "live", "daily_top_songs", "global", None, None, None, 50, 0
            )

        focused_live.assert_awaited_once_with(
            "daily", "track", "global", False, False, "daily_top_songs"
        )
        self.assertEqual(result["view"], "live")
        self.assertEqual(result["data"], focused_result)

    async def test_history_calendar_uses_history_page_data(self):
        request = SimpleNamespace(headers={"authorization": "Bearer correct"})
        calendar_result = {"year": 2026, "month": 11, "days": []}
        with (
            patch.dict(os.environ, {"SPOTIFY_CHARTS_API_TOKEN": "correct"}, clear=False),
            patch("app.history_calendar", AsyncMock(return_value=calendar_result)) as history_calendar,
        ):
            result = await felix_support_charts(
                request, "history", "daily_top_songs", "global", 2026, 11, None, 50, 0
            )

        history_calendar.assert_awaited_once_with(2026, 11, "daily_top_songs")
        self.assertEqual(result["view"], "history")
        self.assertEqual(result["data"], calendar_result)

    async def test_history_date_uses_selected_day_results(self):
        request = SimpleNamespace(headers={"authorization": "Bearer correct"})
        day_result = {"chart_date": "2026-11-19", "items": [{"track_name": "Song"}]}
        with (
            patch.dict(os.environ, {"SPOTIFY_CHARTS_API_TOKEN": "correct"}, clear=False),
            patch("app.history_date_results", AsyncMock(return_value=day_result)) as history_date,
        ):
            result = await felix_support_charts(
                request, "history", "daily_top_songs", "global", None, None,
                date(2026, 11, 19), 25, 10,
            )

        history_date.assert_awaited_once_with(date(2026, 11, 19), 25, 10, "daily_top_songs")
        self.assertEqual(result["data"], day_result)


if __name__ == "__main__":
    unittest.main()
