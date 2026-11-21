from datetime import date, datetime, time, timedelta, timezone


CHART_DAY_START_UTC = time(hour=13)


def chart_day_start(now: datetime | None = None) -> datetime:
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    current = current.astimezone(timezone.utc)
    start = current.replace(
        hour=CHART_DAY_START_UTC.hour,
        minute=CHART_DAY_START_UTC.minute,
        second=0,
        microsecond=0,
    )
    return start if current >= start else start - timedelta(days=1)


def current_chart_day(now: datetime | None = None) -> date:
    return chart_day_start(now).date()
