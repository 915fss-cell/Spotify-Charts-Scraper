from __future__ import annotations

import json
import secrets
from datetime import date, datetime, time as clock_time, timedelta, timezone
from typing import Any, Awaitable, Callable
from zoneinfo import ZoneInfo

import asyncpg
import httpx
from fastapi import HTTPException
from Countries import Countries
from chart_day import chart_day_start

from chart_history import CHART_STEPS, parse_chart_date, save_chart_snapshot
from spotify_charts import SpotifyCharts, parse_retry_after_delay

MAX_CHART_KEYS_PER_TICK = 8
COLLECTION_LEASE = timedelta(minutes=15)


def normalize_chart_types(chart_types: list[str] | None) -> list[str]:
    if not chart_types:
        return ["daily_top_songs"]
    selected = set(chart_types)
    if selected.difference(CHART_STEPS):
        raise HTTPException(status_code=400, detail="Choose only supported Spotify chart types.")
    return [chart_type for chart_type in CHART_STEPS if chart_type in selected]


def next_schedule_time(now: datetime, local_time: clock_time, timezone_name: str) -> datetime:
    zone = ZoneInfo(timezone_name)
    local_now = now.astimezone(zone)
    candidate = local_now.replace(
        hour=local_time.hour,
        minute=local_time.minute,
        second=0,
        microsecond=0,
    )
    if candidate <= local_now:
        candidate += timedelta(days=1)
    return candidate.astimezone(timezone.utc)


async def _start_run(
    pool: asyncpg.Pool,
    source: str,
) -> tuple[int, str] | None:
    owner = secrets.token_urlsafe(18)
    async with pool.acquire() as connection:
        async with connection.transaction():
            if source == "scheduled":
                schedule = await connection.fetchrow(
                    """SELECT enabled, local_time, timezone, next_run_at, lease_expires_at
                       FROM public.spotify_collection_schedule WHERE singleton = true FOR UPDATE"""
                )
                if (
                    schedule is None
                    or not schedule["enabled"]
                    or schedule["next_run_at"] is None
                    or schedule["next_run_at"] > datetime.now(timezone.utc)
                    or (schedule["lease_expires_at"] is not None and schedule["lease_expires_at"] > datetime.now(timezone.utc))
                ):
                    return None
                await connection.execute(
                    """UPDATE public.spotify_collection_schedule
                       SET lease_owner = $1, lease_expires_at = now() + $2::interval, updated_at = now()
                       WHERE singleton = true""",
                    owner,
                    COLLECTION_LEASE,
                )
            run_id = await connection.fetchval(
                """INSERT INTO public.spotify_collection_runs (source, status, started_at)
                   VALUES ($1, 'running', now()) RETURNING id""",
                source,
            )
            return int(run_id), owner


async def _retry_claimed_job(
    pool: asyncpg.Pool,
    chart_type: str,
    country: str,
    requested_date: str | None,
    owner: str,
    message: str,
    http_status: int | None,
    retry_after_seconds: float | None = None,
) -> None:
    if requested_date is None:
        return
    await pool.execute(
        """UPDATE public.spotify_chart_refresh_jobs SET status = 'retry',
             next_attempt_at = now() + LEAST(3600, GREATEST(
               0, COALESCE($7::double precision,
                 30 * power(2, LEAST(attempt_count - 1, 7)))
             )) * interval '1 second',
             last_error = $5, last_http_status = $6, lease_owner = NULL,
             lease_expires_at = NULL, completed_at = NULL, updated_at = now()
           WHERE chart_type = $1 AND country_code = $2 AND requested_date = $3
             AND lease_owner = $4 AND status = 'running'""",
        chart_type, country, requested_date, owner, message, http_status, retry_after_seconds,
    )


async def _mark_latest_chart_unavailable(
    pool: asyncpg.Pool,
    chart_type: str,
    country: str,
) -> None:
    await pool.execute(
        """INSERT INTO public.spotify_chart_refresh_jobs
             (chart_type, country_code, requested_date, status, next_attempt_at,
              last_error, last_http_status, completed_at, fetched_at)
           VALUES ($1, $2, 'latest', 'unavailable', now() + interval '1 day',
                   'Latest chart is unavailable in this market.', 404, now(), now())
           ON CONFLICT (chart_type, country_code, requested_date) DO UPDATE SET
             status = 'unavailable', next_attempt_at = now() + interval '1 day',
             last_error = EXCLUDED.last_error, last_http_status = 404,
             lease_owner = NULL, lease_expires_at = NULL,
             completed_at = now(), fetched_at = now(), updated_at = now()""",
        chart_type,
        country,
    )


async def _process_pending_history_jobs(
    pool: asyncpg.Pool,
    scraper: SpotifyCharts,
    owner: str,
    limit: int,
) -> tuple[int, int, list[str]]:
    if limit <= 0:
        return 0, 0, []
    candidates = await pool.fetch(
        """SELECT chart_type, country_code, requested_date
           FROM public.spotify_chart_refresh_jobs
           WHERE requested_date <> 'latest'
             AND ((status IN ('pending', 'retry', 'failed') AND next_attempt_at <= now())
               OR (status = 'running' AND lease_expires_at < now()))
           ORDER BY next_attempt_at, updated_at
           LIMIT $1""",
        limit,
    )
    completed = 0
    failed = 0
    errors: list[str] = []
    for candidate in candidates:
        chart_type = candidate["chart_type"]
        country = candidate["country_code"]
        requested_date = candidate["requested_date"]
        claimed = await pool.fetchrow(
            """UPDATE public.spotify_chart_refresh_jobs
               SET status = 'running', lease_owner = $4,
                   lease_expires_at = now() + $5::interval,
                   attempt_count = attempt_count + 1, updated_at = now()
               WHERE chart_type = $1 AND country_code = $2 AND requested_date = $3
                 AND ((status IN ('pending', 'retry', 'failed') AND next_attempt_at <= now())
                   OR (status = 'running' AND lease_expires_at < now()))
               RETURNING attempt_count""",
            chart_type, country, requested_date, owner, COLLECTION_LEASE,
        )
        if claimed is None:
            continue
        try:
            payload = await getattr(scraper, chart_type)(country, requested_date)
            entries = payload.get("entries") if isinstance(payload, dict) else None
            display_chart = payload.get("displayChart") if isinstance(payload, dict) else None
            response_date = parse_chart_date(
                display_chart.get("date") if isinstance(display_chart, dict) else payload.get("date")
            )
            if not isinstance(entries, list) or response_date is None or response_date.isoformat() != requested_date:
                raise ValueError("The returned chart did not match its requested date.")
            entity_uris = await pool.fetchval(
                """SELECT array_agg(entity_uri ORDER BY entity_uri)
                   FROM public.spotify_chart_tracking_configs
                   WHERE enabled = true AND chart_type = $1 AND country_code = $2""",
                chart_type, country,
            ) or []
            async with pool.acquire() as connection:
                async with connection.transaction():
                    await connection.execute(
                        """INSERT INTO public.spotify_chart_payload_cache
                             (chart_type, country_code, requested_date, payload, chart_date, fetched_at)
                           VALUES ($1, $2, $3, $4::jsonb, $5, now())
                           ON CONFLICT (chart_type, country_code, requested_date) DO UPDATE SET
                             payload = EXCLUDED.payload, chart_date = EXCLUDED.chart_date,
                             fetched_at = EXCLUDED.fetched_at""",
                        chart_type, country, requested_date,
                        json.dumps(payload, separators=(",", ":")), response_date,
                    )
                    await save_chart_snapshot(
                        pool, chart_type, country, response_date, entries,
                        allowed_entity_uris=set(entity_uris), connection=connection,
                        allowed_artist_uris={uri for uri in entity_uris if uri.startswith("spotify:artist:")},
                    )
                    await connection.execute(
                        """UPDATE public.spotify_chart_refresh_jobs SET status = 'succeeded',
                             chart_date = $5, response_chart_date = $5, completed_at = now(),
                             fetched_at = now(), last_http_status = 200, last_error = NULL,
                             lease_owner = NULL, lease_expires_at = NULL, updated_at = now()
                           WHERE chart_type = $1 AND country_code = $2 AND requested_date = $3
                             AND lease_owner = $4""",
                        chart_type, country, requested_date, owner, response_date,
                    )
            completed += 1
        except Exception as error:
            failed += 1
            message = f"History retry failed ({type(error).__name__})."
            await _retry_claimed_job(
                pool, chart_type, country, requested_date, owner, message,
                error.response.status_code if isinstance(error, httpx.HTTPStatusError) else None,
                parse_retry_after_delay(error.response.headers.get("Retry-After"))
                if isinstance(error, httpx.HTTPStatusError) else None,
            )
            errors.append(f"{chart_type}/{country}/{requested_date}: {type(error).__name__}")
    return completed, failed, errors


async def _persist_chart_response(
    pool: asyncpg.Pool,
    connection: asyncpg.Connection,
    chart_type: str,
    country: str,
    requested_date: str,
    response_date: date,
    payload: dict[str, Any],
    entries: list[dict[str, Any]],
    entity_uris: set[str] | None,
    cache_as_latest: bool = False,
) -> None:
    cache_keys = [requested_date]
    if cache_as_latest and requested_date != "latest":
        cache_keys.append("latest")
    serialized_payload = json.dumps(payload, separators=(",", ":"))
    for cache_key in cache_keys:
        await connection.execute(
            """INSERT INTO public.spotify_chart_payload_cache
                 (chart_type, country_code, requested_date, payload, chart_date, fetched_at)
               VALUES ($1, $2, $3, $4::jsonb, $5, now())
               ON CONFLICT (chart_type, country_code, requested_date) DO UPDATE SET
                 payload = EXCLUDED.payload, chart_date = EXCLUDED.chart_date, fetched_at = now()""",
            chart_type,
            country,
            cache_key,
            serialized_payload,
            response_date,
        )
    await save_chart_snapshot(
        pool,
        chart_type,
        country,
        response_date,
        entries,
        allowed_entity_uris=entity_uris,
        connection=connection,
        allowed_artist_uris=(
            {uri for uri in entity_uris if uri.startswith("spotify:artist:")}
            if entity_uris is not None else None
        ),
    )


async def run_collection_tick(
    pool: asyncpg.Pool,
    scraper_factory: Callable[[], Awaitable[SpotifyCharts]],
    source: str = "manual",
    max_chart_keys: int = MAX_CHART_KEYS_PER_TICK,
    include_history: bool = True,
    chart_types: list[str] | None = None,
    refresh_after: datetime | None = None,
    all_markets_for_daily_songs: bool = False,
) -> dict[str, Any] | None:
    if source not in {"scheduled", "manual"}:
        raise ValueError("Collection source must be 'scheduled' or 'manual'.")
    selected_chart_types = normalize_chart_types(chart_types)
    started = await _start_run(pool, source)

    if started is None:
        return None
    run_id, owner = started
    completed = 0
    unavailable = 0
    failed = 0
    error_messages: list[str] = []

    try:
        scraper = await scraper_factory()
    except Exception as error:
        await pool.execute(
            """UPDATE public.spotify_collection_runs SET status = 'failed', completed_at = now(),
                 summary_error = $2 WHERE id = $1""",
            run_id,
            f"Spotify authorization unavailable ({type(error).__name__}).",
        )
        if source == "scheduled":
            await _release_schedule(pool, owner)
        raise HTTPException(status_code=502, detail="Spotify authorization is unavailable for collection.") from error

    history_completed = 0
    history_failed = 0
    history_errors: list[str] = []
    if include_history:
        history_completed, history_failed, history_errors = await _process_pending_history_jobs(
            pool, scraper, owner, max_chart_keys
        )
        completed += history_completed
        failed += history_failed
        error_messages.extend(history_errors)
    remaining_chart_keys = max(0, max_chart_keys - history_completed - history_failed)
    if all_markets_for_daily_songs:
        config_rows = await pool.fetch(
            """SELECT 'daily_top_songs'::text AS chart_type, markets.country_code,
                      (SELECT array_agg(selection.entity_uri ORDER BY selection.entity_uri)
                       FROM public.spotify_chart_selections selection
                       WHERE selection.is_archived = false
                         AND selection.entity_type IN ('track', 'artist')) AS entity_uris
               FROM unnest($5::text[]) AS markets(country_code)
               WHERE 'daily_top_songs' = ANY($1::text[])
                 AND NOT EXISTS (
                 SELECT 1 FROM public.spotify_chart_refresh_jobs j
                 WHERE j.chart_type = 'daily_top_songs' AND j.country_code = markets.country_code
                   AND j.status IN ('succeeded', 'complete', 'unavailable')
                   AND (($4::timestamptz IS NULL AND j.fetched_at >= $6::timestamptz)
                     OR ($4::timestamptz IS NOT NULL AND j.fetched_at >= $4::timestamptz))
               )
                 AND (NOT $3::boolean OR NOT EXISTS (
                   SELECT 1 FROM public.spotify_chart_refresh_jobs j
                   WHERE j.chart_type = 'daily_top_songs' AND j.country_code = markets.country_code
                     AND j.status = 'retry' AND j.next_attempt_at > now()
                 ))
               ORDER BY (SELECT max(j.fetched_at) FROM public.spotify_chart_refresh_jobs j
                         WHERE j.chart_type = 'daily_top_songs' AND j.country_code = markets.country_code),
                        markets.country_code
               LIMIT $2""",
            selected_chart_types,
            remaining_chart_keys,
            include_history,
            refresh_after,
            [country.value for country in Countries],
            chart_day_start(),
        )
    else:
        config_rows = await pool.fetch(
            """SELECT c.chart_type, c.country_code, array_agg(c.entity_uri ORDER BY c.entity_uri) AS entity_uris
               FROM public.spotify_chart_tracking_configs c
               WHERE c.enabled = true AND c.chart_type = ANY($1::text[])
                 AND NOT EXISTS (
                   SELECT 1 FROM public.spotify_chart_refresh_jobs j
                   WHERE j.chart_type = c.chart_type AND j.country_code = c.country_code
                     AND j.status IN ('succeeded', 'complete', 'unavailable')
                     AND (($4::timestamptz IS NULL AND j.fetched_at >= $5::timestamptz)
                       OR ($4::timestamptz IS NOT NULL AND j.fetched_at >= $4::timestamptz))
                 )
                 AND (NOT $3::boolean OR NOT EXISTS (
                   SELECT 1 FROM public.spotify_chart_refresh_jobs j
                   WHERE j.chart_type = c.chart_type AND j.country_code = c.country_code
                     AND j.status = 'retry' AND j.next_attempt_at > now()
                 ))
               GROUP BY c.chart_type, c.country_code
               ORDER BY (SELECT max(j.fetched_at) FROM public.spotify_chart_refresh_jobs j
                         WHERE j.chart_type = c.chart_type AND j.country_code = c.country_code),
                        c.chart_type, c.country_code
               LIMIT $2""",
            selected_chart_types,
            remaining_chart_keys,
            include_history,
            refresh_after,
            chart_day_start(),
        )
    requested = history_completed + history_failed + len(config_rows)

    for config in config_rows:
        chart_type = config["chart_type"]
        country = config["country_code"]
        requested_date: str | None = None
        http_status: int | None = None
        try:
            payload = await getattr(scraper, chart_type)(country, "latest")
            entries = payload.get("entries") if isinstance(payload, dict) else None
            display_chart = payload.get("displayChart") if isinstance(payload, dict) else None
            raw_date = display_chart.get("date") if isinstance(display_chart, dict) else None
            response_date = parse_chart_date(raw_date or (payload.get("date") if isinstance(payload, dict) else None))
            if not isinstance(entries, list) or response_date is None:
                raise ValueError("Spotify returned a chart without a valid date and entries.")

            requested_date = response_date.isoformat()
            entity_uris = (
                set(config["entity_uris"])
                if config["entity_uris"] is not None else None
            )
            async with pool.acquire() as connection:
                async with connection.transaction():
                    await connection.execute(
                        """INSERT INTO public.spotify_chart_refresh_jobs
                             (chart_type, country_code, requested_date, chart_date, status)
                           VALUES ($1, $2, $3, $4, 'pending') ON CONFLICT DO NOTHING""",
                        chart_type, country, requested_date, response_date,
                    )
                    job = await connection.fetchrow(
                        """UPDATE public.spotify_chart_refresh_jobs
                           SET status = 'running', lease_owner = $4,
                               lease_expires_at = now() + $5::interval,
                               attempt_count = attempt_count + 1, updated_at = now()
                           WHERE chart_type = $1 AND country_code = $2 AND requested_date = $3
                             AND status NOT IN ('succeeded', 'unavailable', 'complete')
                             AND (NOT $6::boolean OR next_attempt_at <= now())
                             AND (status <> 'running' OR lease_expires_at IS NULL OR lease_expires_at < now())
                           RETURNING attempt_count""",
                        chart_type, country, requested_date, owner, COLLECTION_LEASE, include_history,
                    )
                    if job is None:
                        existing_status = await connection.fetchval(
                            """SELECT status FROM public.spotify_chart_refresh_jobs
                               WHERE chart_type = $1 AND country_code = $2 AND requested_date = $3""",
                            chart_type, country, requested_date,
                        )
                        if existing_status in {"succeeded", "unavailable", "complete"}:
                            await _persist_chart_response(
                                pool, connection, chart_type, country, requested_date,
                                response_date, payload, entries, entity_uris,
                                cache_as_latest=True,
                            )
                            await connection.execute(
                                """UPDATE public.spotify_chart_refresh_jobs
                                   SET status = 'succeeded', response_chart_date = $4,
                                       completed_at = now(), fetched_at = now(), last_http_status = 200,
                                       last_error = NULL, lease_owner = NULL, lease_expires_at = NULL,
                                       updated_at = now()
                                   WHERE chart_type = $1 AND country_code = $2 AND requested_date = $3
                                     AND status IN ('succeeded', 'unavailable', 'complete')""",
                                chart_type, country, requested_date, response_date,
                            )
                            completed += 1
                            continue
                        raise RuntimeError("Chart date is already leased or waiting for retry.")

                    await _persist_chart_response(
                        pool, connection, chart_type, country, requested_date,
                        response_date, payload, entries, entity_uris,
                        cache_as_latest=True,
                    )
                    await connection.execute(
                        """UPDATE public.spotify_chart_refresh_jobs SET status = 'succeeded',
                             response_chart_date = $5, completed_at = now(), fetched_at = now(),
                             last_http_status = 200, last_error = NULL,
                             lease_owner = NULL, lease_expires_at = NULL, updated_at = now()
                           WHERE chart_type = $1 AND country_code = $2 AND requested_date = $3 AND lease_owner = $4""",
                        chart_type, country, requested_date, owner, response_date,
                    )
            completed += 1
        except httpx.HTTPStatusError as error:
            http_status = error.response.status_code
            if requested_date is None and http_status == 404:
                unavailable += 1
                await _mark_latest_chart_unavailable(pool, chart_type, country)
                continue

            failed += 1
            message = f"Spotify returned HTTP {http_status}; no date was classified unavailable."
            await _retry_claimed_job(
                pool, chart_type, country, requested_date, owner, message, http_status,
                parse_retry_after_delay(error.response.headers.get("Retry-After")),
            )
            error_messages.append(f"{chart_type}/{country}: HTTP {http_status}; date not classified unavailable")
        except Exception as error:
            failed += 1
            message = f"Collection processing failed ({type(error).__name__})."
            await _retry_claimed_job(pool, chart_type, country, requested_date, owner, message, http_status)
            error_messages.append(f"{chart_type}/{country}: {type(error).__name__}")

    status = "succeeded" if failed == 0 and unavailable == 0 else "partial" if completed or unavailable else "failed"
    summary = "; ".join(error_messages[:8]) or None
    await pool.execute(
        """UPDATE public.spotify_collection_runs SET status = $2, completed_at = now(),
             requested_chart_keys = $3, completed_chart_keys = $4, unavailable_chart_keys = $5,
             failed_chart_keys = $6, summary_error = $7 WHERE id = $1""",
        run_id, status, requested, completed, unavailable, failed, summary,
    )
    if source == "scheduled":
        await _release_schedule(pool, owner)
    return {
        "id": run_id,
        "source": source,
        "status": status,
        "requestedChartKeys": requested,
        "completedChartKeys": completed,
        "unavailableChartKeys": unavailable,
        "failedChartKeys": failed,
        "summaryError": summary,
    }


async def _release_schedule(pool: asyncpg.Pool, owner: str) -> None:
    async with pool.acquire() as connection:
        async with connection.transaction():
            schedule = await connection.fetchrow(
                """SELECT local_time, timezone, enabled FROM public.spotify_collection_schedule
                   WHERE singleton = true AND lease_owner = $1 FOR UPDATE""",
                owner,
            )
            if schedule is None:
                return
            next_run = (
                next_schedule_time(datetime.now(timezone.utc), schedule["local_time"], schedule["timezone"])
                if schedule["enabled"]
                else None
            )
            await connection.execute(
                """UPDATE public.spotify_collection_schedule SET next_run_at = $1,
                     lease_owner = NULL, lease_expires_at = NULL, updated_at = now()
                   WHERE singleton = true AND lease_owner = $2""",
                next_run,
                owner,
            )
