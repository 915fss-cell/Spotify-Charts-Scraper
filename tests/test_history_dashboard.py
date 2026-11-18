import asyncio
import unittest
from datetime import date
from types import SimpleNamespace
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
    def test_history_calendar_serializes_saved_dates(self):
        async def run():
            pool = HistoryPoolStub()
            pool.fetch_rows = [{
                "chart_date": date(2026, 11, 14),
                "appearance_count": 87,
                "chart_count": 2,
                "market_count": 3,
                "charts": [{"chart_type": "daily_top_songs", "appearance_count": 50}, {"chart_type": "weekly_top_songs", "appearance_count": 37}],
            }]
            with patch.object(app, "get_history_pool", new=AsyncMock(return_value=pool)), patch.object(
                app, "require_spotify_connection"
            ):
                result = await app.history_calendar(2026, 11, None)
            self.assertEqual((result["year"], result["month"]), (2026, 11))
            self.assertEqual(result["days"][0]["chart_date"], "2026-11-14")
            self.assertEqual(result["days"][0]["appearance_count"], 87)
            self.assertEqual(result["days"][0]["charts"][0]["chart_type"], "daily_top_songs")

        asyncio.run(run())

    def test_history_date_returns_paginated_saved_chart_entries(self):
        async def run():
            pool = HistoryPoolStub()
            pool.fetchval_result = 3
            pool.fetch_rows = [{
                "chart_type": "daily_top_songs",
                "country_code": "GLOBAL",
                "chart_date": date(2026, 11, 14),
                "track_uri": "spotify:track:0123456789abcdef",
                "track_name": "Example song",
                "artist_names": ["Example artist"],
                "entity_type": "track",
                "chart_rank": 1,
                "previous_rank": 2,
                "peak_rank": 1,
                "consecutive_appearances": 3,
                "appearances": 7,
                "entry_date": date(2026, 11, 10),
                "stream_count": 1234,
            }]
            with patch.object(app, "get_history_pool", new=AsyncMock(return_value=pool)), patch.object(
                app, "require_spotify_connection"
            ):
                result = await app.history_date_results(date(2026, 11, 14), 1, 1, None)
            self.assertEqual(result["total"], 3)
            self.assertTrue(result["hasMore"])
            self.assertEqual(result["items"][0]["chart_date"], "2026-11-14")
            self.assertEqual(result["items"][0]["track_name"], "Example song")

        asyncio.run(run())

    def test_focused_history_date_filters_chart_snapshots(self):
        async def run():
            selected_date = date(2026, 11, 14)
            row = {
                "track_uri": "spotify:track:0123456789abcdef",
                "entity_type": "track",
                "track_name": "Example song",
                "artist_names": "Example artist",
                "country_code": "GLOBAL",
                "chart_date": selected_date,
                "chart_rank": 3,
                "previous_rank": 4,
                "peak_rank": 1,
                "consecutive_appearances": 3,
                "appearances": 7,
                "entry_date": selected_date,
                "stream_count": 1234,
                "total_rows": 1,
            }

            class FocusedPool:
                def __init__(self):
                    self.fetch_calls = []

                async def fetch(self, query, *args):
                    self.fetch_calls.append((query, args))
                    return [row] if len(self.fetch_calls) == 1 else []

            pool = FocusedPool()
            with patch.object(app, "get_history_pool", new=AsyncMock(return_value=pool)), patch.object(
                app, "read_selected_entities", new=AsyncMock(return_value=[])
            ):
                result = await app.focused_history(
                    cadence="daily",
                    entity_type="track",
                    chart_type="daily_top_songs",
                    chart_date=selected_date,
                    range="all",
                    limit=50,
                    offset=0,
                )
            self.assertEqual(pool.fetch_calls[0][1][4], selected_date)
            self.assertEqual(result["chartDate"], "2026-11-14")
            self.assertEqual(result["releases"][0]["regions"][0]["chartDate"], "2026-11-14")

        asyncio.run(run())

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
                "chart_date": "2026-11-14",
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

    def test_manual_collection_delegates_to_shared_worker(self):
        async def run():
            pool = HistoryPoolStub()
            worker = AsyncMock(return_value={"id": 7, "source": "manual", "status": "succeeded"})
            with patch.object(app, "get_history_pool", new=AsyncMock(return_value=pool)), patch.object(
                app, "require_spotify_connection"
            ), patch.object(app, "run_collection_tick", new=worker):
                result = await app.start_manual_collection()
            self.assertEqual(result["id"], 7)
            worker.assert_awaited_once_with(pool, app.get_spotify_scraper, source="manual")

        asyncio.run(run())

    def test_repository_workflow_rejects_invalid_cron_secret(self):
        async def run():
            request = SimpleNamespace(headers={"authorization": "Bearer wrong-secret"})
            with patch.dict(app.os.environ, {"CRON_SECRET": "expected-secret"}):
                with self.assertRaises(app.HTTPException) as raised:
                    await app.repository_workflow_collection(request)
            self.assertEqual(raised.exception.status_code, 401)

        asyncio.run(run())

    def test_repository_workflow_runs_collection_anytime_after_authentication(self):
        async def run():
            pool = HistoryPoolStub()
            worker = AsyncMock(return_value={"id": 7, "source": "manual", "status": "succeeded"})
            request = SimpleNamespace(headers={"authorization": "Bearer expected-secret"})
            with patch.dict(app.os.environ, {"CRON_SECRET": "expected-secret"}), patch.object(
                app, "get_history_pool", new=AsyncMock(return_value=pool)
            ), patch.object(app, "run_collection_tick", new=worker):
                result = await app.repository_workflow_collection(request)
            self.assertEqual(result["status"], "succeeded")
            worker.assert_awaited_once_with(pool, app.get_spotify_scraper, source="manual")

        asyncio.run(run())

    def test_retry_returns_conflict_for_non_retryable_job(self):
        async def run():
            pool = HistoryPoolStub()
            pool.fetchrow = AsyncMock(return_value=None)
            pool.fetchval = AsyncMock(return_value="succeeded")
            with patch.object(app, "get_history_pool", new=AsyncMock(return_value=pool)), patch.object(
                app, "require_spotify_connection"
            ):
                with self.assertRaises(app.HTTPException) as raised:
                    await app.retry_collection_job(app.ChartJobRetryInput(
                        chart_type="daily_top_songs", country_code="GLOBAL", requested_date="2026-11-14"
                    ))
            self.assertEqual(raised.exception.status_code, 409)

        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
