from __future__ import annotations

import asyncio
import hashlib
import os
from datetime import date, datetime, timedelta
from typing import Any, Awaitable, Callable

import asyncpg
import httpx
from fastapi import HTTPException

from spotify_charts import SpotifyCharts


CHART_STEPS = {"daily_top_songs": 1, "weekly_top_songs": 7, "daily_top_artists": 1, "weekly_top_artists": 7}
BACKFILL_BATCH_SIZE = 6
BACKFILL_CONCURRENCY = 3
LEASE_DURATION = timedelta(minutes=3)

_pool: asyncpg.Pool | None = None
_pool_lock = asyncio.Lock()


class ChartHistoryStorageError(RuntimeError):
    pass


async def get_history_pool() -> asyncpg.Pool:
    global _pool

    if _pool is not None:
        return _pool

    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise ChartHistoryStorageError("DATABASE_URL is not configured.")

    async with _pool_lock:
        if _pool is None:
            try:
                _pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=5)
            except Exception as error:
                print(f"[v0] Neon history connection unavailable: {type(error).__name__}")
                raise ChartHistoryStorageError("Historical storage is unavailable.") from error
    return _pool


async def close_history_pool() -> None:
    global _pool
    async with _pool_lock:
        if _pool is not None:
            pool = _pool
            _pool = None
            await pool.close()


def parse_chart_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if value is None:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def chart_date_from_payload(payload: dict[str, Any], requested_date: str) -> date | None:
    display_chart = payload.get("displayChart") or {}
    if not isinstance(display_chart, dict):
        display_chart = {}
    candidates = (display_chart.get("date"), payload.get("date"), requested_date)
    for candidate in candidates:
        parsed = parse_chart_date(candidate)
        if parsed is not None:
            return parsed
    return None


def entry_entity_uri(entry: dict[str, Any]) -> str | None:
    track_metadata = entry.get("trackMetadata") or {}
    artist_metadata = entry.get("artistMetadata") or {}
    uri = (
        track_metadata.get("trackUri")
        or artist_metadata.get("artistUri")
        or entry.get("trackUri")
        or entry.get("artistUri")
    )
    if isinstance(uri, str) and uri.startswith(("spotify:track:", "spotify:artist:")):
        return uri
    return None


def snapshot_values(entry: dict[str, Any]) -> tuple[str, str, str, int | None, int | None] | None:
    uri = entry_entity_uri(entry)
    if uri is None:
        return None
    entity_type = "artist" if uri.startswith("spotify:artist:") else "track"
    metadata = entry.get("artistMetadata" if entity_type == "artist" else "trackMetadata") or {}
    stats = entry.get("chartEntryData") or {}
    metric = stats.get("rankingMetric") or {}
    streams: int | None = None
    if str(metric.get("type", "")).upper() == "STREAMS":
        try:
            streams = int(str(metric.get("value")).replace(",", ""))
        except (TypeError, ValueError):
            streams = None
        if streams is not None and streams < 0:
            streams = None

    try:
        rank = int(stats.get("currentRank"))
    except (TypeError, ValueError):
        rank = None
    if rank is not None and rank <= 0:
        rank = None
    if streams is None and rank is None:
        return None

    artists = metadata.get("artists") or []
    if not isinstance(artists, list):
        artists = []
    artist_names = []
    for artist in artists:
        name = artist.get("name") if isinstance(artist, dict) else artist
        if isinstance(name, str) and name.strip():
            artist_names.append(name.strip())

    entity_name = metadata.get("trackName" if entity_type == "track" else "artistName")
    if not isinstance(entity_name, str) or not entity_name.strip():
        entity_name = "Untitled"
    return entity_type, entity_name.strip(), ", ".join(artist_names), streams, rank


def first_entry_date(entries: list[dict[str, Any]]) -> date | None:
    entry_dates = []
    for entry in entries:
        stats = entry.get("chartEntryData") or {}
        parsed = parse_chart_date(stats.get("entryDate"))
        if parsed is not None:
            entry_dates.append(parsed)
    return min(entry_dates) if entry_dates else None


def _date_count(start_date: date, end_date: date, step: int) -> int:
    if end_date < start_date:
        return 0
    return (end_date - start_date).days // step + 1


async def save_chart_snapshot(
    pool: asyncpg.Pool,
    chart_type: str,
    country: str,
    chart_date: date,
    entries: list[dict[str, Any]],
    allowed_entity_uris: set[str] | None = None,
) -> int:
    deduplicated: dict[str, tuple[Any, ...]] = {}
    for entry in entries:
        uri = entry_entity_uri(entry)
        values = snapshot_values(entry)
        if uri and values and (allowed_entity_uris is None or uri in allowed_entity_uris):
            entity_type, entity_name, artist_names, streams, rank = values
            deduplicated[uri] = (
                chart_type, country, chart_date, uri, entity_name, artist_names, streams, entity_type, rank
            )

    if not deduplicated:
        return 0

    async with pool.acquire() as connection:
        await connection.executemany(
            """
            INSERT INTO public.spotify_chart_snapshots
                (chart_type, country_code, chart_date, track_uri, track_name, artist_names,
                 stream_count, entity_type, chart_rank)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (chart_type, country_code, chart_date, track_uri)
            DO UPDATE SET
                track_name = EXCLUDED.track_name,
                artist_names = EXCLUDED.artist_names,
                stream_count = EXCLUDED.stream_count,
                entity_type = EXCLUDED.entity_type,
                chart_rank = EXCLUDED.chart_rank,
                fetched_at = now()
            """,
            list(deduplicated.values()),
        )
    return len(deduplicated)


async def ensure_backfill_job(
    pool: asyncpg.Pool,
    chart_type: str,
    country: str,
    start_date: date,
    target_date: date,
    selection_fingerprint: str,
) -> asyncpg.Record:
    step = CHART_STEPS[chart_type]
    if start_date > target_date:
        start_date = target_date

    async with pool.acquire() as connection:
        async with connection.transaction():
            existing = await connection.fetchrow(
                """
                SELECT start_date, target_date, next_date, processed_dates, total_dates,
                       status, lease_expires_at, last_error, selection_fingerprint
                FROM public.spotify_chart_backfills
                WHERE chart_type = $1 AND country_code = $2
                FOR UPDATE
                """,
                chart_type,
                country,
            )

            if existing is None:
                await connection.execute(
                    """
                    INSERT INTO public.spotify_chart_backfills
                        (chart_type, country_code, start_date, target_date, next_date,
                         processed_dates, total_dates, status, selection_fingerprint)
                    VALUES ($1, $2, $3, $4, $3, 0, $5, 'pending', $6)
                    """,
                    chart_type,
                    country,
                    start_date,
                    target_date,
                    _date_count(start_date, target_date, step),
                    selection_fingerprint,
                )
            else:
                old_start = existing["start_date"]
                old_target = existing["target_date"]
                old_next = existing["next_date"]
                new_start = min(start_date, old_start)
                new_target = max(target_date, old_target)
                reset = new_start < old_start or existing["selection_fingerprint"] != selection_fingerprint
                next_date = new_start if reset else old_next
                processed_dates = 0 if reset else existing["processed_dates"]
                status = existing["status"]
                lease_expires_at = existing["lease_expires_at"]
                last_error = existing["last_error"]

                if not reset and new_target > old_target and old_next > old_target:
                    next_date = old_target + timedelta(days=step)
                if reset or status == "paused":
                    status = "pending"
                    lease_expires_at = None
                    last_error = None
                if next_date > new_target:
                    status = "complete"
                    lease_expires_at = None
                elif status == "complete":
                    status = "pending"

                await connection.execute(
                    """
                    UPDATE public.spotify_chart_backfills
                    SET start_date = $1, target_date = $2, next_date = $3,
                        processed_dates = $4, total_dates = $5, status = $6,
                        lease_expires_at = $7, last_error = $8, selection_fingerprint = $9,
                        updated_at = now()
                    WHERE chart_type = $10 AND country_code = $11
                    """,
                    new_start,
                    new_target,
                    next_date,
                    processed_dates,
                    _date_count(new_start, new_target, step),
                    status,
                    lease_expires_at,
                    last_error,
                    selection_fingerprint,
                    chart_type,
                    country,
                )

            return await connection.fetchrow(
                """
                SELECT start_date, target_date, next_date, processed_dates, total_dates,
                       status, lease_expires_at, last_error, selection_fingerprint
                FROM public.spotify_chart_backfills
                WHERE chart_type = $1 AND country_code = $2
                """,
                chart_type,
                country,
            )


async def get_backfill_progress(
    pool: asyncpg.Pool,
    chart_type: str,
    country: str,
    target_date: date,
) -> dict[str, Any]:
    job = await pool.fetchrow(
        """
        SELECT start_date, target_date, next_date, processed_dates, total_dates,
               status, last_error
        FROM public.spotify_chart_backfills
        WHERE chart_type = $1 AND country_code = $2
        """,
        chart_type,
        country,
    )
    if job is None:
        return {"supported": True, "complete": False, "status": "pending", "processedDates": 0, "totalDates": 0}

    step = CHART_STEPS[chart_type]
    requested_end = min(target_date, job["target_date"])
    total_dates = _date_count(job["start_date"], requested_end, step)
    processed_dates = min(job["processed_dates"], total_dates)
    complete = job["next_date"] > target_date
    status = "complete" if complete else job["status"]
    return {
        "supported": True,
        "complete": complete,
        "status": status,
        "startDate": job["start_date"].isoformat(),
        "targetDate": target_date.isoformat(),
        "nextDate": job["next_date"].isoformat(),
        "processedDates": processed_dates,
        "totalDates": total_dates,
        "error": job["last_error"],
    }


async def get_stream_totals(
    pool: asyncpg.Pool,
    chart_type: str,
    country: str,
    target_date: date,
    entity_uris: list[str],
) -> dict[str, int]:
    unique_uris = list(dict.fromkeys(entity_uris))
    if not unique_uris:
        return {}

    rows = await pool.fetch(
        """
        SELECT track_uri, SUM(stream_count)::bigint AS total_streams
        FROM public.spotify_chart_snapshots
        WHERE chart_type = $1 AND country_code = $2 AND chart_date <= $3
          AND stream_count IS NOT NULL AND track_uri = ANY($4::text[])
        GROUP BY track_uri
        """,
        chart_type,
        country,
        target_date,
        unique_uris,
    )
    return {row["track_uri"]: int(row["total_streams"]) for row in rows}


async def prepare_chart_history(
    chart_type: str,
    country: str,
    requested_date: str,
    payload: dict[str, Any],
    allowed_entity_uris: set[str] | None = None,
) -> dict[str, Any]:
    if chart_type not in CHART_STEPS:
        return {
            "supported": False,
            "complete": False,
            "status": "unsupported",
            "reason": "Historical snapshots are available for daily and weekly Top Songs and Top Artists charts.",
        }

    entries = [
        entry
        for entry in payload.get("entries") or []
        if allowed_entity_uris is None or entry_entity_uri(entry) in allowed_entity_uris
    ]
    if not entries:
        return {
            "supported": False,
            "complete": False,
            "status": "unavailable",
            "reason": "No chart entries are available to save.",
        }
    target_date = chart_date_from_payload(payload, requested_date)
    start_date = target_date if allowed_entity_uris is None else first_entry_date(entries)
    if target_date is None:
        return {
            "supported": False,
            "complete": False,
            "status": "unavailable",
            "reason": "Spotify did not provide the chart date needed to save a snapshot.",
        }
    if start_date is None:
        start_date = target_date

    pool = await get_history_pool()
    try:
        saved_count = await save_chart_snapshot(
            pool,
            chart_type,
            country,
            target_date,
            entries,
            allowed_entity_uris=allowed_entity_uris,
        )
        if saved_count == 0:
            return {
                "supported": False,
                "complete": False,
                "status": "unavailable",
                "reason": "Spotify did not provide a rank or stream count for this chart.",
            }
        selection_fingerprint = (
            "all-chart-entries"
            if allowed_entity_uris is None
            else hashlib.sha256("\n".join(sorted(allowed_entity_uris)).encode("utf-8")).hexdigest()
        )
        await ensure_backfill_job(
            pool,
            chart_type,
            country,
            start_date,
            target_date,
            selection_fingerprint,
        )
        progress = await get_backfill_progress(pool, chart_type, country, target_date)
        totals: dict[str, int] = {}
        if progress["complete"]:
            uris = [uri for entry in entries if (uri := entry_entity_uri(entry))]
            totals = await get_stream_totals(pool, chart_type, country, target_date, uris)

        for entry in entries:
            stats = entry.setdefault("chartEntryData", {})
            uri = entry_entity_uri(entry)
            stats["cumulativeChartStreams"] = totals.get(uri, 0) if progress["complete"] and uri else None
        return progress
    except ChartHistoryStorageError:
        raise
    except Exception as error:
        raise ChartHistoryStorageError("Historical storage is unavailable.") from error


async def _pause_job(pool: asyncpg.Pool, chart_type: str, country: str, message: str) -> None:
    await pool.execute(
        """
        UPDATE public.spotify_chart_backfills
        SET status = 'paused', lease_expires_at = NULL, last_error = $1, updated_at = now()
        WHERE chart_type = $2 AND country_code = $3
        """,
        message,
        chart_type,
        country,
    )


async def run_history_backfill_batch(
    chart_type: str,
    country: str,
    target_date: date,
    start_date: date,
    total_entity_uris: list[str] | None,
    scraper_factory: Callable[[], Awaitable[SpotifyCharts]],
) -> dict[str, Any]:
    if chart_type not in CHART_STEPS:
        raise HTTPException(status_code=400, detail="Choose a supported songs or artists chart to backfill history.")

    pool = await get_history_pool()
    step = CHART_STEPS[chart_type]
    if start_date > target_date:
        start_date = target_date
    if step == 7 and start_date < target_date:
        start_date += timedelta(days=(target_date.weekday() - start_date.weekday()) % step)
        if start_date > target_date:
            start_date = target_date

    await ensure_backfill_job(pool, chart_type, country, start_date, target_date, "all-chart-entries")
    job = await pool.fetchrow(
        """
        SELECT start_date, target_date, next_date, processed_dates, total_dates, status
        FROM public.spotify_chart_backfills
        WHERE chart_type = $1 AND country_code = $2
        """,
        chart_type,
        country,
    )
    if target_date > job["target_date"]:
        raise HTTPException(status_code=400, detail="The selected date is newer than the saved chart history target.")

    claim = await pool.fetchrow(
        """
        UPDATE public.spotify_chart_backfills
        SET status = 'running', lease_expires_at = now() + $4::interval,
            last_error = NULL, updated_at = now()
        WHERE chart_type = $1 AND country_code = $2 AND next_date <= $3
          AND (status <> 'running' OR lease_expires_at IS NULL OR lease_expires_at < now())
        RETURNING start_date, target_date, next_date, processed_dates, total_dates
        """,
        chart_type,
        country,
        target_date,
        LEASE_DURATION,
    )

    if claim is None:
        progress = await get_backfill_progress(pool, chart_type, country, target_date)
        totals = await get_stream_totals(pool, chart_type, country, target_date, total_entity_uris) if progress["complete"] and total_entity_uris else {}
        return {**progress, "totals": totals}

    try:
        scraper = await scraper_factory()
    except HTTPException:
        await _pause_job(pool, chart_type, country, "Spotify authorization needs attention. Reconnect and reload the chart to resume.")
        raise

    cursor = claim["next_date"]
    batch_end = min(target_date, claim["target_date"])
    dates = [cursor + timedelta(days=step * index) for index in range(BACKFILL_BATCH_SIZE)]
    dates = [chart_day for chart_day in dates if chart_day <= batch_end]
    semaphore = asyncio.Semaphore(BACKFILL_CONCURRENCY)

    async def fetch_day(chart_day: date) -> dict[str, Any]:
        async with semaphore:
            method = getattr(scraper, chart_type)
            return await method(country, chart_day.isoformat())

    results = await asyncio.gather(*(fetch_day(chart_day) for chart_day in dates), return_exceptions=True)
    for chart_day, result in zip(dates, results):
        if isinstance(result, BaseException):
            if isinstance(result, httpx.HTTPStatusError) and result.response.status_code == 404:
                message = f"Spotify history is unavailable for {chart_day.isoformat()}; totals are incomplete."
            elif isinstance(result, httpx.HTTPStatusError) and result.response.status_code == 429:
                message = "Spotify rate-limited the history backfill. Reload the chart later to resume."
            else:
                message = "A Spotify history request failed. Reload the chart later to resume."
            await _pause_job(pool, chart_type, country, message)
            break

        historical_entries = result.get("entries") if isinstance(result, dict) else None
        if not isinstance(historical_entries, list):
            await _pause_job(pool, chart_type, country, "Spotify returned an unsupported historical chart response.")
            break
        if await save_chart_snapshot(pool, chart_type, country, chart_day, historical_entries) == 0:
            await _pause_job(pool, chart_type, country, "Spotify did not include a rank or stream count in a historical chart response.")
            break

        next_date = chart_day + timedelta(days=step)
        await pool.execute(
            """
            UPDATE public.spotify_chart_backfills
            SET next_date = $1, processed_dates = processed_dates + 1, updated_at = now()
            WHERE chart_type = $2 AND country_code = $3 AND next_date = $4 AND status = 'running'
            """,
            next_date,
            chart_type,
            country,
            chart_day,
        )

    await pool.execute(
        """
        UPDATE public.spotify_chart_backfills
        SET status = CASE WHEN next_date > target_date THEN 'complete' ELSE 'pending' END,
            lease_expires_at = NULL, updated_at = now()
        WHERE chart_type = $1 AND country_code = $2 AND status = 'running'
        """,
        chart_type,
        country,
    )

    progress = await get_backfill_progress(pool, chart_type, country, target_date)
    totals = await get_stream_totals(pool, chart_type, country, target_date, total_entity_uris) if progress["complete"] and total_entity_uris else {}
    return {**progress, "totals": totals}
