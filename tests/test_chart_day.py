import unittest
from datetime import datetime, timezone

from chart_day import chart_day_start, current_chart_day


class ChartDayTests(unittest.TestCase):
    def test_chart_day_rolls_over_at_1300_utc(self):
        before_rollover = datetime(2026, 11, 21, 12, 59, tzinfo=timezone.utc)
        at_rollover = datetime(2026, 11, 21, 13, 0, tzinfo=timezone.utc)

        self.assertEqual(chart_day_start(before_rollover), datetime(2026, 11, 20, 13, 0, tzinfo=timezone.utc))
        self.assertEqual(current_chart_day(before_rollover).isoformat(), "2026-11-20")
        self.assertEqual(chart_day_start(at_rollover), at_rollover)
        self.assertEqual(current_chart_day(at_rollover).isoformat(), "2026-11-21")

    def test_chart_day_converts_non_utc_timezones(self):
        local_time = datetime.fromisoformat("2026-11-21T14:00:00+01:00")

        self.assertEqual(chart_day_start(local_time), datetime(2026, 11, 21, 13, 0, tzinfo=timezone.utc))


if __name__ == "__main__":
    unittest.main()
