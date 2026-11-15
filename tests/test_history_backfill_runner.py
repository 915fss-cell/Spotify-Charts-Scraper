import asyncio
import unittest
from datetime import date
from unittest.mock import AsyncMock, patch

import app


class BackfillQueuePoolStub:
    def __init__(self, rows):
        self.rows = list(rows)
        self.calls = []

    async def fetchrow(self, *args):
        self.calls.append(args)
        return self.rows.pop(0)


class HistoryBackfillRunnerTests(unittest.TestCase):
    def test_queue_step_runs_one_bounded_batch(self):
        async def run():
            pool = BackfillQueuePoolStub([
                {
                    "chart_type": "daily_top_songs",
                    "country_code": "GLOBAL",
                    "start_date": date(2024, 1, 1),
                    "target_date": date(2026, 11, 14),
                    "next_date": date(2024, 1, 1),
                },
                {"total_jobs": 12, "complete_jobs": 3, "active_jobs": 9, "remaining_dates": 5100},
            ])
            progress = {
                "supported": True,
                "complete": False,
                "status": "pending",
                "processedDates": 6,
                "totalDates": 1000,
                "retryDates": 0,
                "error": None,
            }
            with patch.object(app, "require_spotify_connection"), patch.object(
                app, "get_history_pool", new=AsyncMock(return_value=pool)
            ), patch.object(app, "run_durable_history_backfill_batch", new=AsyncMock(return_value=progress)) as batch:
                result = await app.chart_history_backfill_queue_step()

            self.assertEqual(result["state"], "batch")
            self.assertEqual(result["queue"]["remainingDates"], 5100)
            self.assertEqual(result["batch"]["chartType"], "daily_top_songs")
            self.assertEqual(batch.await_args.args[:5], (
                "daily_top_songs", "GLOBAL", date(2026, 11, 14), date(2024, 1, 1), None
            ))

        asyncio.run(run())

    def test_queue_step_waits_after_a_retryable_failure(self):
        async def run():
            pool = BackfillQueuePoolStub([
                {
                    "chart_type": "daily_top_songs",
                    "country_code": "GLOBAL",
                    "start_date": date(2024, 1, 1),
                    "target_date": date(2026, 11, 14),
                    "next_date": date(2024, 1, 1),
                },
                {"total_jobs": 12, "complete_jobs": 3, "active_jobs": 9, "remaining_dates": 5100},
                {"retry_after_seconds": 45},
            ])
            progress = {
                "supported": True,
                "complete": False,
                "status": "pending",
                "processedDates": 5,
                "totalDates": 1000,
                "retryDates": 1,
                "nextDate": "2024-01-06",
                "error": "Spotify returned HTTP 429",
            }
            with patch.object(app, "require_spotify_connection"), patch.object(
                app, "get_history_pool", new=AsyncMock(return_value=pool)
            ), patch.object(app, "run_durable_history_backfill_batch", new=AsyncMock(return_value=progress)):
                result = await app.chart_history_backfill_queue_step()

            self.assertEqual(result["state"], "waiting")
            self.assertEqual(result["retryAfterSeconds"], 45)
            self.assertEqual(result["queue"]["remainingDates"], 5100)
            self.assertEqual(pool.calls[-1][3], "2024-01-06")

        asyncio.run(run())
