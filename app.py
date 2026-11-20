from __future__ import annotations

import asyncio
import json
import os
import secrets
import time
from contextlib import asynccontextmanager
from datetime import date as calendar_date, datetime, timedelta
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlencode, urlsplit

import httpx
from fastapi import Cookie, FastAPI, HTTPException, Query, Request
from pydantic import BaseModel, Field
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse

from chart_history import (
    CHART_STEPS,
    ChartHistoryStorageError,
    close_history_pool,
    entry_entity_uri,
    get_history_pool,
    parse_chart_date,
    run_durable_history_backfill_batch,
    save_chart_snapshot,
    snapshot_values,
)
from collection_worker import run_collection_tick
from Countries import Countries
from Settings import SpotifySettings
from spotify_charts import SpotifyCharts


@asynccontextmanager
async def lifespan(_: FastAPI):
    global refresh_token
    try:
        pool = await get_history_pool()
        row = await pool.fetchrow(
            "SELECT refresh_token FROM public.spotify_oauth_tokens WHERE singleton = true"
        )
        refresh_token = row["refresh_token"] if row else None
    except Exception as error:
        print(f"[v0] Spotify authorization could not be restored: {type(error).__name__}")
    yield
    await close_history_pool()


app = FastAPI(title="Spotify Charts", docs_url=None, redoc_url=None, lifespan=lifespan)
settings = SpotifySettings()
refresh_token: str | None = None
access_token: str | None = None
access_token_expires_at = 0.0
token_refresh_lock = asyncio.Lock()


async def store_spotify_refresh_token(token: str) -> None:
    pool = await get_history_pool()
    await pool.execute(
        """
        INSERT INTO public.spotify_oauth_tokens (singleton, refresh_token, updated_at)
        VALUES (true, $1, now())
        ON CONFLICT (singleton) DO UPDATE
        SET refresh_token = EXCLUDED.refresh_token, updated_at = now()
        """,
        token,
    )


async def remove_spotify_refresh_token() -> None:
    pool = await get_history_pool()
    await pool.execute("DELETE FROM public.spotify_oauth_tokens WHERE singleton = true")

HISTORY_CHART_TYPES = {
    "daily_top_songs": "track",
    "daily_viral_songs": "track",
    "weekly_top_songs": "track",
    "daily_top_artists": "artist",
    "weekly_top_artists": "artist",
    "weekly_top_albums": "album",
}
FOCUSED_CHART_CACHE_TTL = 180
CHART_PAYLOAD_TTL_SECONDS = 300
CHART_REFRESH_LEASE_SECONDS = 120
focused_chart_cache: dict[tuple[str, ...], tuple[float, dict[str, Any]]] = {}
focused_chart_cache_lock = asyncio.Lock()
COUNTRY_NAMES = {country.value: country.name.replace("_", " ").title() for country in Countries}
FOCUSED_HISTORY_CHART_TYPES = set(HISTORY_CHART_TYPES) - {"daily_viral_songs"}


def focused_result_cache_key(scope: str, chart_type: str, selection_fingerprint: str) -> str:
    value = f"{scope}\n{chart_type}\n{selection_fingerprint}"
    return __import__("hashlib").sha256(value.encode()).hexdigest()


def decode_json_value(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value


async def read_focused_result_snapshot(
    cadence: str, entity_type: str, scope: str, chart_type: str, selection_fingerprint: str
) -> dict[str, Any] | None:
    pool = await get_history_pool()
    row = await pool.fetchrow(
        """SELECT payload, fetched_at FROM public.spotify_focused_chart_snapshots
           WHERE cadence=$1 AND entity_type=$2 AND market_scope='all' AND snapshot_date=CURRENT_DATE""",
        cadence, entity_type,
    )
    if not row:
        return None
    payload = decode_json_value(row["payload"])
    results = payload.get("results", []) if isinstance(payload, dict) else []
    if not isinstance(results, list):
        results = []
    cache_key = focused_result_cache_key(scope, chart_type, selection_fingerprint)
    match = next((item for item in reversed(results) if isinstance(item, dict) and item.get("cacheKey") == cache_key), None)
    if not match:
        return None
    response = dict(match["response"])
    response.update({
        "snapshotReused": True,
        "historyStorageAvailable": True,
        "historySavedRegions": len(response.get("regions", [])),
        "snapshotFetchedAt": row["fetched_at"].isoformat() if hasattr(row["fetched_at"], "isoformat") else str(row["fetched_at"]),
    })
    return response


async def read_legacy_focused_scan(cadence: str, entity_type: str, chart_type: str) -> dict[str, Any] | None:
    pool = await get_history_pool()
    row = await pool.fetchrow(
        """SELECT partial_payload, updated_at,
                  (SELECT count(*) FROM public.spotify_chart_selections WHERE is_archived = true) AS archived_selections,
                  (SELECT max(created_at) FROM public.spotify_chart_selections) AS latest_selection_at
           FROM public.spotify_focused_scan_jobs
           WHERE cadence=$1 AND entity_type=$2 AND status IN ('complete','partial')
             AND updated_at::date=CURRENT_DATE""",
        cadence, entity_type,
    )
    if not row or row["archived_selections"] or (
        row["latest_selection_at"] is not None and row["latest_selection_at"] > row["updated_at"]
    ):
        return None
    response = decode_json_value(row["partial_payload"])
    if not isinstance(response, dict) or response.get("chartType") != chart_type or response.get("marketScope") != "all":
        return None
    return response


def focused_response_for_scope(response: dict[str, Any], scope: str) -> dict[str, Any]:
    result = dict(response)
    if scope == "all":
        return result
    regions = [region for region in response.get("regions", []) if region.get("regionCode") == "GLOBAL"]
    diagnostics = [item for item in response.get("marketDiagnostics", []) if item.get("countryCode") == "GLOBAL"]
    releases = []
    for release in response.get("releases", []):
        matching_regions = [region for region in release.get("regions", []) if region.get("regionCode") == "GLOBAL"]
        if matching_regions:
            releases.append({**release, "regions": matching_regions})
    failed = [item for item in diagnostics if item.get("status") == "failed"]
    result.update({
        "marketScope": "global",
        "marketsTotal": 1,
        "marketsScanned": sum(item.get("status") == "succeeded" for item in diagnostics),
        "matchingRegions": len(regions),
        "failedRegions": [item.get("countryName", "Global") for item in failed],
        "marketDiagnostics": diagnostics,
        "regions": regions,
        "releases": releases,
    })
    return result


async def save_focused_result_snapshot(
    pool: Any,
    cadence: str,
    entity_type: str,
    scope: str,
    chart_type: str,
    selection_fingerprint: str,
    response: dict[str, Any],
) -> None:
    cache_key = focused_result_cache_key(scope, chart_type, selection_fingerprint)
    snapshot = {
        "cacheKey": cache_key,
        "scope": scope,
        "chartType": chart_type,
        "selectionFingerprint": selection_fingerprint,
        "response": response,
    }
    await pool.execute(
        """INSERT INTO public.spotify_focused_chart_snapshots
               (cadence, entity_type, market_scope, snapshot_date, payload, fetched_at)
           VALUES ($1, $2, 'all', CURRENT_DATE, jsonb_build_object('results', $3::jsonb), now())
           ON CONFLICT (cadence, entity_type, market_scope, snapshot_date) DO UPDATE
           SET payload = jsonb_build_object(
                   'results',
                   COALESCE((
                       SELECT jsonb_agg(saved.result)
                       FROM jsonb_array_elements(
                           CASE WHEN jsonb_typeof(spotify_focused_chart_snapshots.payload->'results') = 'array'
                                THEN spotify_focused_chart_snapshots.payload->'results'
                                ELSE '[]'::jsonb END
                       ) AS saved(result)
                       WHERE saved.result->>'cacheKey' <> $4
                   ), '[]'::jsonb) || (EXCLUDED.payload->'results')
               ),
               fetched_at = now()""",
        cadence, entity_type, json.dumps([snapshot], separators=(",", ":")), cache_key,
    )


async def save_focused_results_to_history(pool: Any, chart_type: str, response: dict[str, Any]) -> int:
    if chart_type not in FOCUSED_HISTORY_CHART_TYPES:
        return len(response.get("regions", [])) if chart_type == "daily_viral_songs" else 0
    saved_regions = set()
    for region in response.get("regions", []):
        country_code = region.get("regionCode")
        if not country_code:
            continue
        entries = []
        for item in region.get("entries", []):
            uri = item.get("entityUri")
            entity_type = item.get("entityType")
            metadata_key, uri_key, name_key = {
                "track": ("trackMetadata", "trackUri", "trackName"),
                "artist": ("artistMetadata", "artistUri", "artistName"),
                "album": ("albumMetadata", "albumUri", "albumName"),
            }.get(entity_type, (None, None, None))
            chart_date = parse_chart_date(item.get("chartDate"))
            if not uri or not metadata_key or chart_date is None:
                continue
            metadata = {uri_key: uri, name_key: item.get("entityName") or "Untitled"}
            artist_names = [name.strip() for name in (item.get("artistNames") or "").split(",") if name.strip()]
            if artist_names:
                metadata["artists"] = [{"name": name} for name in artist_names]
            entries.append({
                metadata_key: metadata,
                "chartEntryData": {
                    "currentRank": item.get("chartRank"),
                    "previousRank": item.get("previousRank"),
                    "peakRank": item.get("peakRank"),
                    "consecutiveAppearancesOnChart": item.get("consecutiveAppearancesOnChart"),
                    "appearancesOnChart": item.get("appearancesOnChart"),
                    "entryDate": item.get("entryDate"),
                    "rankingMetric": {"type": item.get("metricType"), "value": item.get("metricValue")},
                },
            })
        if entries:
            await save_chart_snapshot(pool, chart_type, country_code, chart_date, entries)
            saved_regions.add(country_code)
    return len(saved_regions)


def chart_delivery(payload: dict[str, Any], fetched_at: Any, status: str) -> dict[str, Any]:
    delivered = dict(payload)
    age = max(0, int(time.time() - fetched_at.timestamp())) if hasattr(fetched_at, "timestamp") else 0
    delivered["delivery"] = {
        "fetchedAt": fetched_at.isoformat() if hasattr(fetched_at, "isoformat") else str(fetched_at),
        "ageSeconds": age,
        "isStale": age >= CHART_PAYLOAD_TTL_SECONDS,
        "refreshStatus": status,
    }
    return delivered


async def read_chart_cache(chart_type: str, country: str, requested_date: str):
    pool = await get_history_pool()
    return await pool.fetchrow(
        """SELECT payload, chart_date, fetched_at FROM public.spotify_chart_payload_cache
           WHERE chart_type = $1 AND country_code = $2 AND requested_date = $3""",
        chart_type, country, requested_date,
    )


async def save_chart_cache(chart_type: str, country: str, requested_date: str, payload: dict[str, Any]) -> Any:
    chart_date_value = (payload.get("displayChart") or {}).get("date")
    try:
        chart_date_value = calendar_date.fromisoformat(str(chart_date_value)[:10]) if chart_date_value else None
    except ValueError:
        chart_date_value = None
    pool = await get_history_pool()
    return await pool.fetchval(
        """INSERT INTO public.spotify_chart_payload_cache
             (chart_type, country_code, requested_date, payload, chart_date, fetched_at)
           VALUES ($1, $2, $3, $4::jsonb, $5, now())
           ON CONFLICT (chart_type, country_code, requested_date) DO UPDATE
             SET payload = EXCLUDED.payload, chart_date = EXCLUDED.chart_date, fetched_at = EXCLUDED.fetched_at
           RETURNING fetched_at""",
        chart_type, country, requested_date, json.dumps(payload, separators=(",", ":")), chart_date_value,
    )


async def enqueue_chart_refresh(chart_type: str, country: str, requested_date: str) -> None:
    pool = await get_history_pool()
    await pool.execute(
        """INSERT INTO public.spotify_chart_refresh_jobs (chart_type, country_code, requested_date)
           VALUES ($1, $2, $3) ON CONFLICT (chart_type, country_code, requested_date) DO UPDATE
           SET status = CASE WHEN public.spotify_chart_refresh_jobs.status = 'complete'
                                  OR (public.spotify_chart_refresh_jobs.status = 'failed' AND public.spotify_chart_refresh_jobs.next_attempt_at <= now())
                             THEN 'pending' ELSE public.spotify_chart_refresh_jobs.status END,
               next_attempt_at = CASE WHEN public.spotify_chart_refresh_jobs.status = 'complete'
                                  OR (public.spotify_chart_refresh_jobs.status = 'failed' AND public.spotify_chart_refresh_jobs.next_attempt_at <= now())
                                      THEN now() ELSE public.spotify_chart_refresh_jobs.next_attempt_at END,
               updated_at = now()""",
        chart_type, country, requested_date,
    )


class ChartHistoryBackfillRequest(BaseModel):
    chart_type: str
    country: str
    target_date: calendar_date
    start_date: calendar_date
    entity_uris: list[str] = Field(default_factory=list, max_length=200)
    target_entity_uris: list[str] | None = Field(default=None, max_length=200)


class SelectionInput(BaseModel):
    entity_type: Literal["track", "artist"]
    entity_uri: str = Field(min_length=16, max_length=128)
    entity_name: str = Field(min_length=1, max_length=300)
    artist_names: str = Field(default="", max_length=500)
    image_url: str | None = Field(default=None, max_length=2048)


class TrackingConfigInput(BaseModel):
    entity_uri: str = Field(min_length=16, max_length=128)
    chart_type: Literal[
        "daily_top_songs", "daily_viral_songs", "weekly_top_songs", "daily_top_artists",
        "weekly_top_artists", "weekly_top_albums",
    ]
    country_code: str = Field(min_length=2, max_length=6)
    enabled: bool = True
    backfill_start_date: calendar_date | None = None


class ChartJobRetryInput(BaseModel):
    chart_type: Literal[
        "daily_top_songs", "daily_viral_songs", "weekly_top_songs", "daily_top_artists",
        "weekly_top_artists", "weekly_top_albums",
    ]
    country_code: str = Field(min_length=2, max_length=6)
    requested_date: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}$")


class CollectionWorkflowInput(BaseModel):
    chart_types: list[str] = Field(default_factory=list, max_length=len(CHART_STEPS), alias="chartTypes")
    workflow_started_at: datetime | None = Field(default=None, alias="workflowStartedAt")


CHART_METHODS = {
    "daily_top_songs": "daily_top_songs",
    "daily_top_artists": "daily_top_artists",
    "daily_viral_songs": "daily_viral_songs",
    "weekly_top_songs": "weekly_top_songs",
    "weekly_top_artists": "weekly_top_artists",
    "weekly_top_albums": "weekly_top_albums",
}


@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
    response.headers["X-Frame-Options"] = "SAMEORIGIN"
    if request.url.scheme == "https":
        response.headers["Strict-Transport-Security"] = "max-age=63072000"
    return response


def callback_url(request: Request) -> str:
    return settings.redirect_url or str(request.url_for("oauth_callback"))


def missing_configuration() -> list[str]:
    missing = []
    if not settings.client_id:
        missing.append("SPOTIFY_CLIENT_ID")
    if not settings.client_secret:
        missing.append("SPOTIFY_CLIENT_SECRET")
    return missing


def require_spotify_connection() -> None:
    if not refresh_token:
        raise HTTPException(status_code=401, detail="Connect Spotify before loading charts and history.")


async def get_spotify_scraper() -> SpotifyCharts:
    global refresh_token, access_token, access_token_expires_at

    async with token_refresh_lock:
        if access_token and time.monotonic() < access_token_expires_at - 60:
            return SpotifyCharts(access_token=access_token)
        try:
            pool = await get_history_pool()
            async with pool.acquire() as connection:
                async with connection.transaction():
                    row = await connection.fetchrow(
                        "SELECT refresh_token FROM public.spotify_oauth_tokens WHERE singleton = true FOR UPDATE"
                    )
                    if not row:
                        refresh_token = None
                        raise HTTPException(status_code=401, detail="Connect Spotify before loading charts.")
                    current_refresh_token = row["refresh_token"]
                    async with httpx.AsyncClient(timeout=15) as client:
                        response = await client.post(
                            "https://accounts.spotify.com/api/token",
                            data={"grant_type": "refresh_token", "refresh_token": current_refresh_token},
                            auth=(settings.client_id or "", settings.client_secret or ""),
                        )
                        response.raise_for_status()
                        token_data = response.json()
                    next_refresh_token = token_data.get("refresh_token", current_refresh_token)
                    if not isinstance(next_refresh_token, str) or not next_refresh_token:
                        raise ValueError("Spotify returned an invalid refresh token.")
                    if next_refresh_token != current_refresh_token:
                        await connection.execute(
                            """
                            UPDATE public.spotify_oauth_tokens
                            SET refresh_token = $1, updated_at = now()
                            WHERE singleton = true
                            """,
                            next_refresh_token,
                        )
                    access_token = token_data["access_token"]
                    refresh_token = next_refresh_token
                    access_token_expires_at = time.monotonic() + max(60, int(token_data.get("expires_in", 3600)))
        except HTTPException:
            raise
        except (KeyError, TypeError, ValueError) as error:
            raise HTTPException(status_code=502, detail="Spotify returned an unexpected token response.") from error
        except httpx.HTTPError as error:
            raise HTTPException(status_code=502, detail="Spotify could not refresh authorization. Reconnect and try again.") from error
        except Exception as error:
            raise HTTPException(status_code=503, detail="Spotify authorization storage is temporarily unavailable.") from error

    return SpotifyCharts(access_token=access_token)


@app.get("/", response_class=HTMLResponse)
async def index() -> HTMLResponse:
    page = Path(__file__).with_name("index.html").read_text(encoding="utf-8")
    return HTMLResponse(page)


@app.get("/selected-pages.js", include_in_schema=False)
async def selected_pages_script() -> FileResponse:
    return FileResponse(Path(__file__).with_name("selected_pages.js"), media_type="application/javascript")


@app.get("/api/status")
async def status(request: Request) -> dict[str, Any]:
    missing = missing_configuration()
    return {
        "configured": not missing,
        "missing": missing,
        "connected": refresh_token is not None,
        "callbackUrl": callback_url(request),
    }


@app.get("/api/countries")
async def countries() -> list[dict[str, str]]:
    return [
        {"code": country.value, "name": country.name.replace("_", " ").title()}
        for country in Countries
    ]


async def read_selected_entities(entity_type: str | None = None) -> list[dict[str, Any]]:
    try:
        pool = await get_history_pool()
        if entity_type:
            rows = await pool.fetch(
                """
                SELECT entity_uri, entity_type, entity_name, artist_names, image_url
                FROM public.spotify_chart_selections
                WHERE entity_type = $1 AND is_archived = false
                ORDER BY entity_name ASC
                """,
                entity_type,
            )
        else:
            rows = await pool.fetch(
                """
                SELECT entity_uri, entity_type, entity_name, artist_names, image_url
                FROM public.spotify_chart_selections
                WHERE is_archived = false
                ORDER BY entity_type ASC, entity_name ASC
                """
            )
        return [dict(row) for row in rows]
    except Exception as error:
        raise HTTPException(status_code=503, detail="Selected entities are temporarily unavailable.") from error


def is_valid_entity_uri(uri: str, entity_type: str | None = None) -> bool:
    parts = uri.split(":")
    return (
        len(parts) == 3
        and parts[0] == "spotify"
        and parts[1] in {"track", "artist", "album"}
        and (entity_type is None or parts[1] == entity_type)
        and bool(parts[2])
        and all(character.isascii() and character.isalnum() for character in parts[2])
    )


def selection_response(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "entityUri": row["entity_uri"],
        "entityType": row["entity_type"],
        "entityName": row["entity_name"],
        "artistNames": row["artist_names"],
        "imageUrl": row["image_url"],
    }


@app.get("/api/selections")
async def list_selections() -> list[dict[str, Any]]:
    require_spotify_connection()
    return [selection_response(row) for row in await read_selected_entities()]


@app.post("/api/selections")
async def save_selection(selection: SelectionInput) -> dict[str, Any]:
    require_spotify_connection()
    if not is_valid_entity_uri(selection.entity_uri, selection.entity_type):
        raise HTTPException(status_code=400, detail="Choose a valid Spotify song or artist.")
    if selection.image_url:
        try:
            image = urlsplit(selection.image_url)
        except ValueError as error:
            raise HTTPException(status_code=400, detail="Spotify artwork URL is invalid.") from error
        if image.scheme != "https" or image.hostname != "i.scdn.co":
            raise HTTPException(status_code=400, detail="Spotify artwork must use a trusted Spotify image URL.")

    try:
        pool = await get_history_pool()
        row = await pool.fetchrow(
            """
            INSERT INTO public.spotify_chart_selections
                (entity_uri, entity_type, entity_name, artist_names, image_url)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (entity_uri) DO UPDATE SET
                entity_type = EXCLUDED.entity_type,
                entity_name = EXCLUDED.entity_name,
                artist_names = EXCLUDED.artist_names,
                image_url = EXCLUDED.image_url,
                is_archived = false
            RETURNING entity_uri, entity_type, entity_name, artist_names, image_url
            """,
            selection.entity_uri,
            selection.entity_type,
            selection.entity_name.strip(),
            selection.artist_names.strip(),
            selection.image_url,
        )
        return selection_response(dict(row))
    except Exception as error:
        raise HTTPException(status_code=503, detail="The selection could not be saved right now.") from error


@app.delete("/api/selections/{entity_uri:path}")
async def remove_selection(entity_uri: str) -> dict[str, bool]:
    require_spotify_connection()
    if not is_valid_entity_uri(entity_uri):
        raise HTTPException(status_code=400, detail="Choose a valid Spotify song or artist.")
    try:
        pool = await get_history_pool()
        async with pool.acquire() as connection:
            async with connection.transaction():
                result = await connection.execute(
                    "UPDATE public.spotify_chart_selections SET is_archived = true WHERE entity_uri = $1 AND is_archived = false",
                    entity_uri,
                )
                await connection.execute(
                    "UPDATE public.spotify_chart_tracking_configs SET enabled = false, updated_at = now() WHERE entity_uri = $1",
                    entity_uri,
                )
        return {"deleted": result.endswith("1")}
    except Exception as error:
        raise HTTPException(status_code=503, detail="The selection could not be removed right now.") from error


@app.get("/api/tracking/configs")
async def list_tracking_configs() -> list[dict[str, Any]]:
    require_spotify_connection()
    pool = await get_history_pool()
    rows = await pool.fetch(
        """SELECT c.entity_uri, s.entity_type, s.entity_name, c.chart_type,
                  c.country_code, c.enabled, c.backfill_start_date, c.updated_at
           FROM public.spotify_chart_tracking_configs c
           JOIN public.spotify_chart_selections s ON s.entity_uri = c.entity_uri
           WHERE s.is_archived = false
           ORDER BY s.entity_name, c.chart_type, c.country_code"""
    )
    return [
        {**dict(row), "backfill_start_date": row["backfill_start_date"].isoformat() if row["backfill_start_date"] else None,
         "updated_at": row["updated_at"].isoformat()}
        for row in rows
    ]


@app.post("/api/tracking/configs")
async def save_tracking_config(config: TrackingConfigInput) -> dict[str, Any]:
    require_spotify_connection()
    if not is_valid_entity_uri(config.entity_uri):
        raise HTTPException(status_code=400, detail="Choose a valid Spotify entity.")
    country = config.country_code.upper()
    if country != "ALL" and country not in {item.value for item in Countries}:
        raise HTTPException(status_code=400, detail="Choose a country from the list.")
    countries_to_save = [item.value for item in Countries] if country == "ALL" else [country]
    selected_type = config.entity_uri.split(":")[1]
    if config.chart_type in {"daily_top_artists", "weekly_top_artists"}:
        valid_type = selected_type == "artist"
    elif config.chart_type == "weekly_top_albums":
        valid_type = selected_type == "artist"
    else:
        valid_type = selected_type in {"artist", "track"}
    if not valid_type:
        raise HTTPException(status_code=400, detail="Track charts accept saved songs or artists; artist and album charts accept saved artists.")
    pool = await get_history_pool()
    saved = []
    async with pool.acquire() as connection:
        async with connection.transaction():
            for country_code in countries_to_save:
                row = await connection.fetchrow(
                    """INSERT INTO public.spotify_chart_tracking_configs
                         (entity_uri, chart_type, country_code, enabled, backfill_start_date)
                       SELECT $1, $2, $3, $4, $5
                       WHERE EXISTS (SELECT 1 FROM public.spotify_chart_selections
                                     WHERE entity_uri = $1 AND is_archived = false)
                       ON CONFLICT (entity_uri, chart_type, country_code) DO UPDATE SET
                         enabled = EXCLUDED.enabled,
                         backfill_start_date = COALESCE(EXCLUDED.backfill_start_date,
                                                         public.spotify_chart_tracking_configs.backfill_start_date),
                         updated_at = now()
                       RETURNING entity_uri, chart_type, country_code, enabled, backfill_start_date, updated_at""",
                    config.entity_uri, config.chart_type, country_code, config.enabled, config.backfill_start_date,
                )
                if row is None:
                    raise HTTPException(status_code=404, detail="Save the entity to your tracked list before configuring its charts.")
                saved.append({**dict(row), "backfill_start_date": row["backfill_start_date"].isoformat() if row["backfill_start_date"] else None,
                              "updated_at": row["updated_at"].isoformat()})
    return {"saved": saved, "country_count": len(saved)} if country == "ALL" else saved[0]


@app.get("/api/history/calendar")
async def history_calendar(
    year: int | None = Query(None, ge=1900, le=2100),
    month: int | None = Query(None, ge=1, le=12),
    chart_type: str | None = Query(None),
) -> dict[str, Any]:
    require_spotify_connection()
    if (year is None) != (month is None):
        raise HTTPException(status_code=400, detail="Choose both a year and month.")
    if chart_type is not None and chart_type not in HISTORY_CHART_TYPES:
        raise HTTPException(status_code=400, detail="Choose a supported chart type.")
    pool = await get_history_pool()
    if year is None or month is None:
        latest_date = await pool.fetchval(
            "SELECT max(chart_date) FROM public.spotify_chart_snapshots WHERE ($1::text IS NULL OR chart_type = $1)",
            chart_type,
        )
        anchor = latest_date or calendar_date.today()
        year, month = anchor.year, anchor.month
    available_years = await pool.fetchval(
        """SELECT COALESCE(array_agg(available.year ORDER BY available.year DESC), ARRAY[]::integer[])
           FROM (
             SELECT DISTINCT EXTRACT(YEAR FROM chart_date)::int AS year
             FROM public.spotify_chart_snapshots
             WHERE ($1::text IS NULL OR chart_type = $1)
           ) AS available""",
        chart_type,
    )
    if year not in available_years:
        available_years = [*available_years, year]
    start_date = calendar_date(year, month, 1)
    end_date = calendar_date(year + 1, 1, 1) if month == 12 else calendar_date(year, month + 1, 1)
    rows = await pool.fetch(
        """WITH typed_days AS (
             SELECT chart_date, chart_type, count(*)::int AS appearance_count
             FROM public.spotify_chart_snapshots
             WHERE chart_date >= $1 AND chart_date < $2
               AND ($3::text IS NULL OR chart_type = $3)
             GROUP BY chart_date, chart_type
           )
           SELECT typed_days.chart_date, sum(typed_days.appearance_count)::int AS appearance_count,
                  count(*)::int AS chart_count,
                  (SELECT count(DISTINCT country_code)::int
                   FROM public.spotify_chart_snapshots snapshot
                   WHERE snapshot.chart_date = typed_days.chart_date
                     AND ($3::text IS NULL OR snapshot.chart_type = $3)) AS market_count,
                  jsonb_agg(jsonb_build_object(
                    'chart_type', typed_days.chart_type,
                    'appearance_count', typed_days.appearance_count
                  ) ORDER BY typed_days.chart_type) AS charts
           FROM typed_days
           GROUP BY typed_days.chart_date ORDER BY typed_days.chart_date""",
        start_date, end_date, chart_type,
    )
    return {
        "year": year,
        "month": month,
        "available_years": available_years,
        "chart_type": chart_type,
        "days": [
            {
                **dict(row),
                "chart_date": row["chart_date"].isoformat(),
                "charts": decode_json_value(row["charts"]),
            }
            for row in rows
        ],
    }


@app.get("/api/history/date")
async def history_date_results(
    chart_date: calendar_date = Query(...),
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0, le=100000),
    chart_type: str | None = Query(None),
) -> dict[str, Any]:
    require_spotify_connection()
    if chart_type is not None and chart_type not in HISTORY_CHART_TYPES:
        raise HTTPException(status_code=400, detail="Choose a supported chart type.")
    pool = await get_history_pool()
    total = int(await pool.fetchval(
        """SELECT count(*) FROM public.spotify_chart_snapshots
           WHERE chart_date = $1 AND ($2::text IS NULL OR chart_type = $2)""",
        chart_date, chart_type,
    ) or 0)
    rows = await pool.fetch(
        """SELECT snapshot.chart_type, snapshot.country_code, snapshot.chart_date,
                  snapshot.track_uri, snapshot.track_name, snapshot.artist_names,
                  snapshot.entity_type, snapshot.chart_rank, snapshot.previous_rank,
                  snapshot.peak_rank, snapshot.consecutive_appearances, snapshot.appearances,
                  snapshot.entry_date, snapshot.stream_count,
                  COALESCE(art.image_url, selection.image_url) AS image_url
           FROM public.spotify_chart_snapshots snapshot
           LEFT JOIN LATERAL (
               SELECT COALESCE(
                   chart_entry->'trackMetadata'->>'displayImageUri',
                   chart_entry->'albumMetadata'->>'displayImageUri',
                   chart_entry->'artistMetadata'->>'displayImageUri'
               ) AS image_url
               FROM public.spotify_chart_payload_cache pc
               CROSS JOIN LATERAL jsonb_array_elements(COALESCE(pc.payload->'entries', '[]'::jsonb)) chart_entry
               WHERE pc.chart_type = snapshot.chart_type
                 AND pc.country_code = snapshot.country_code
                 AND pc.chart_date = snapshot.chart_date
                 AND COALESCE(
                     chart_entry->'trackMetadata'->>'trackUri',
                     chart_entry->'albumMetadata'->>'albumUri',
                     chart_entry->'artistMetadata'->>'artistUri',
                     chart_entry->>'trackUri', chart_entry->>'albumUri', chart_entry->>'artistUri'
                 ) = snapshot.track_uri
               ORDER BY pc.fetched_at DESC
               LIMIT 1
           ) art ON true
           LEFT JOIN public.spotify_chart_selections selection
             ON selection.entity_uri = snapshot.track_uri
           WHERE snapshot.chart_date = $1 AND ($2::text IS NULL OR snapshot.chart_type = $2)
           ORDER BY snapshot.country_code, snapshot.chart_rank NULLS LAST, snapshot.track_name, snapshot.track_uri
           LIMIT $3 OFFSET $4""",
        chart_date, chart_type, limit, offset,
    )
    return {
        "chart_date": chart_date.isoformat(),
        "chart_type": chart_type,
        "items": [
            {
                **dict(row),
                "imageUrl": row.get("image_url"),
                "chart_date": row["chart_date"].isoformat(),
                "entry_date": row["entry_date"].isoformat() if row["entry_date"] else None,
                "country_name": COUNTRY_NAMES.get(row["country_code"], row["country_code"]),
            }
            for row in rows
        ],
        "total": total,
        "limit": limit,
        "offset": offset,
        "hasMore": offset + len(rows) < total,
    }


@app.get("/api/history/entities")
async def history_entities(
    q: str = Query("", max_length=80),
    entity_type: Literal["all", "track", "artist"] = Query("all"),
    sort: Literal["name", "appearances", "latest"] = Query("latest"),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0, le=100000),
) -> dict[str, Any]:
    require_spotify_connection()
    pool = await get_history_pool()
    search = q.strip()
    total = await pool.fetchval(
        """SELECT count(*) FROM public.spotify_chart_selections s
           WHERE s.is_archived = false
             AND ($1 = '' OR s.entity_name ILIKE '%' || $1 || '%' OR s.entity_uri ILIKE '%' || $1 || '%')
             AND ($2 = 'all' OR s.entity_type = $2)""",
        search, entity_type,
    )
    sort_column = {
        "name": "s.entity_name ASC",
        "appearances": "appearance_count DESC NULLS LAST, s.entity_name ASC",
        "latest": "latest_appearance DESC NULLS LAST, s.entity_name ASC",
    }[sort]
    rows = await pool.fetch(
        f"""SELECT s.entity_uri, s.entity_type, s.entity_name, s.artist_names,
                   COALESCE(h.appearance_count, 0)::int AS appearance_count,
                   h.latest_appearance, h.peak_rank,
                   (SELECT count(*)::int FROM public.spotify_chart_tracking_configs c
                    WHERE c.entity_uri = s.entity_uri AND c.enabled = true) AS active_configs
            FROM public.spotify_chart_selections s
            LEFT JOIN LATERAL (
              SELECT count(*)::int AS appearance_count, max(chart_date) AS latest_appearance,
                     min(chart_rank) AS peak_rank
              FROM public.spotify_chart_snapshots
              WHERE track_uri = s.entity_uri
            ) h ON true
            WHERE s.is_archived = false
              AND ($1 = '' OR s.entity_name ILIKE '%' || $1 || '%' OR s.entity_uri ILIKE '%' || $1 || '%')
              AND ($2 = 'all' OR s.entity_type = $2)
            ORDER BY {sort_column}
            LIMIT $3 OFFSET $4""",
        search, entity_type, limit, offset,
    )
    return {
        "items": [
            {**dict(row), "latest_appearance": row["latest_appearance"].isoformat() if row["latest_appearance"] else None}
            for row in rows
        ],
        "total": int(total or 0),
        "limit": limit,
        "offset": offset,
        "hasMore": offset + len(rows) < int(total or 0),
    }


@app.get("/api/history/entity")
async def history_entity_detail(
    entity_uri: str = Query(..., min_length=16, max_length=128),
    country: str | None = Query(None, min_length=2, max_length=6),
    chart_type: str | None = Query(None),
    date_from: calendar_date | None = Query(None),
    date_to: calendar_date | None = Query(None),
    limit: int = Query(120, ge=1, le=365),
    offset: int = Query(0, ge=0, le=100000),
) -> dict[str, Any]:
    require_spotify_connection()
    if not is_valid_entity_uri(entity_uri):
        raise HTTPException(status_code=400, detail="Choose a valid tracked Spotify entity.")
    if chart_type is not None and chart_type not in HISTORY_CHART_TYPES:
        raise HTTPException(status_code=400, detail="Choose a supported history chart.")
    if country is not None:
        country = country.upper()
        if country not in {item.value for item in Countries}:
            raise HTTPException(status_code=400, detail="Choose a country from the list.")
    if date_from and date_to and date_from > date_to:
        raise HTTPException(status_code=400, detail="The start date must be before the end date.")

    pool = await get_history_pool()
    entity = await pool.fetchrow(
        """SELECT entity_uri, entity_type, entity_name, artist_names, is_archived
           FROM public.spotify_chart_selections WHERE entity_uri = $1""",
        entity_uri,
    )
    if entity is None:
        raise HTTPException(status_code=404, detail="Tracked entity was not found.")
    filters = (entity_uri, country, chart_type, date_from, date_to)
    summary = await pool.fetchrow(
        """SELECT count(*)::int AS appearances, min(chart_rank) AS peak_rank,
                  min(chart_date) AS first_appearance, max(chart_date) AS latest_appearance,
                  count(stream_count)::int AS measured_stream_appearances
           FROM public.spotify_chart_snapshots
           WHERE track_uri = $1 AND ($2::text IS NULL OR country_code = $2)
             AND ($3::text IS NULL OR chart_type = $3)
             AND ($4::date IS NULL OR chart_date >= $4)
             AND ($5::date IS NULL OR chart_date <= $5)""",
        *filters,
    )
    total = int(summary["appearances"] or 0)
    rows = await pool.fetch(
        """WITH history AS (
             SELECT chart_type, country_code, chart_date, chart_rank, previous_rank,
                    peak_rank, consecutive_appearances, entry_date, stream_count,
                    lag(stream_count) OVER (PARTITION BY country_code, chart_type ORDER BY chart_date) AS prior_stream_count,
                    first_value(chart_rank) OVER (PARTITION BY country_code, chart_type ORDER BY chart_date) AS first_entry_position,
                    min(chart_rank) OVER (PARTITION BY country_code, chart_type ORDER BY chart_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS peak_to_date,
                    count(*) OVER (PARTITION BY country_code, chart_type ORDER BY chart_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS total_days
             FROM public.spotify_chart_snapshots
             WHERE track_uri = $1 AND ($2::text IS NULL OR country_code = $2)
               AND ($3::text IS NULL OR chart_type = $3)
           ), enriched AS (
             SELECT *, greatest(coalesce(stream_count, 0) - coalesce(prior_stream_count, 0), 0) AS daily_streams
             FROM history
           ), totals AS (
             SELECT *, sum(daily_streams) OVER (PARTITION BY country_code, chart_type ORDER BY chart_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS total_chart_streams
             FROM enriched
           )
           SELECT chart_type, country_code, chart_date, chart_rank, previous_rank,
                  peak_to_date AS peak_rank, consecutive_appearances, entry_date,
                  first_entry_position, total_days, stream_count, daily_streams,
                  total_chart_streams
           FROM totals
           WHERE ($4::date IS NULL OR chart_date >= $4)
             AND ($5::date IS NULL OR chart_date <= $5)
           ORDER BY country_code, chart_date DESC, chart_type
           LIMIT $6 OFFSET $7""",
        *filters, limit, offset,
    )
    release_date = None
    if entity["entity_type"] == "track":
        try:
            scraper = await get_spotify_scraper()
            track_id = entity_uri.rsplit(":", 1)[-1]
            async with httpx.AsyncClient(timeout=8) as client:
                response = await client.get(
                    f"https://api.spotify.com/v1/tracks/{track_id}",
                    headers=scraper.headers,
                )
            if response.is_success:
                release_date = (response.json().get("album") or {}).get("release_date")
        except (HTTPException, httpx.HTTPError, KeyError, TypeError, ValueError, AttributeError, IndexError):
            release_date = None
    return {
        "entity": {**dict(entity), "release_date": release_date},
        "summary": {
            **dict(summary),
            "first_appearance": summary["first_appearance"].isoformat() if summary["first_appearance"] else None,
            "latest_appearance": summary["latest_appearance"].isoformat() if summary["latest_appearance"] else None,
        },
        "items": [
            {**dict(row), "chart_date": row["chart_date"].isoformat(), "entry_date": row["entry_date"].isoformat() if row.get("entry_date") else None}
            for row in rows
        ],
        "total": total,
        "limit": limit,
        "offset": offset,
        "hasMore": offset + len(rows) < total,
    }


@app.get("/api/history/overview")
async def history_overview() -> dict[str, Any]:
    require_spotify_connection()
    pool = await get_history_pool()
    counts = await pool.fetchrow(
        """SELECT
             (SELECT count(*) FROM public.spotify_chart_selections WHERE is_archived = false) AS tracked_entities,
             (SELECT count(*) FROM public.spotify_chart_tracking_configs WHERE enabled = true) AS active_configs,
             (SELECT count(*) FROM public.spotify_chart_snapshots) AS snapshot_rows,
             (SELECT count(*) FROM public.spotify_chart_refresh_jobs WHERE status IN ('retry','failed')) AS retry_jobs"""
    )
    last_run = await pool.fetchrow(
        """SELECT id, source, status, started_at, completed_at, requested_chart_keys,
                  completed_chart_keys, unavailable_chart_keys, failed_chart_keys, summary_error
           FROM public.spotify_collection_runs ORDER BY id DESC LIMIT 1"""
    )
    return {
        **dict(counts),
        "lastRun": dict(last_run) if last_run else None,
    }


@app.post("/api/collection/run")
async def start_manual_collection() -> dict[str, Any]:
    require_spotify_connection()
    pool = await get_history_pool()
    result = await run_collection_tick(pool, get_spotify_scraper, source="manual")
    return result or {"status": "not_started"}


@app.get("/api/collection/tick")
async def scheduled_collection_tick(request: Request) -> dict[str, Any]:
    secret = os.environ.get("CRON_SECRET") or os.environ.get("SPOTIFY_COLLECTION_CRON_SECRET")
    authorization = request.headers.get("authorization", "")
    supplied = authorization.removeprefix("Bearer ")
    if not secret or not secrets.compare_digest(supplied, secret):
        raise HTTPException(status_code=401, detail="Collection cron authorization is invalid.")
    pool = await get_history_pool()
    result = await run_collection_tick(pool, get_spotify_scraper, source="scheduled")
    return result or {"status": "not_started"}


@app.post("/api/collection/workflow")
async def repository_workflow_collection(
    request: Request, selection: CollectionWorkflowInput | None = None
) -> dict[str, Any]:
    secret = os.environ.get("CRON_SECRET") or os.environ.get("SPOTIFY_COLLECTION_CRON_SECRET")
    authorization = request.headers.get("authorization", "")
    supplied = authorization.removeprefix("Bearer ")
    if not secret or not secrets.compare_digest(supplied, secret):
        raise HTTPException(status_code=401, detail="Collection workflow authorization is invalid.")
    pool = await get_history_pool()
    result = await run_collection_tick(
        pool,
        get_spotify_scraper,
        source="manual",
        include_history=False,
        chart_types=selection.chart_types if selection else None,
        refresh_after=selection.workflow_started_at if selection else None,
        all_markets_for_daily_songs=not selection or not selection.chart_types,
    )
    return result or {"status": "not_started"}


@app.get("/api/collection/runs")
async def collection_runs(
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0, le=100000),
) -> dict[str, Any]:
    require_spotify_connection()
    pool = await get_history_pool()
    total = int(await pool.fetchval("SELECT count(*) FROM public.spotify_collection_runs") or 0)
    rows = await pool.fetch(
        """SELECT id, source, status, started_at, completed_at, requested_chart_keys,
                  completed_chart_keys, unavailable_chart_keys, failed_chart_keys, summary_error, created_at
           FROM public.spotify_collection_runs ORDER BY id DESC LIMIT $1 OFFSET $2""",
        limit, offset,
    )
    return {
        "items": [dict(row) for row in rows],
        "limit": limit,
        "offset": offset,
        "total": total,
        "hasMore": offset + len(rows) < total,
    }


@app.get("/api/collection/jobs")
async def collection_jobs(
    status: Literal["all", "pending", "running", "succeeded", "complete", "unavailable", "retry", "failed"] = Query("all"),
    chart_type: str | None = Query(None),
    country: str | None = Query(None, min_length=2, max_length=6),
    date_from: calendar_date | None = Query(None),
    date_to: calendar_date | None = Query(None),
    entity_uri: str | None = Query(None, min_length=16, max_length=128),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0, le=100000),
) -> dict[str, Any]:
    require_spotify_connection()
    if chart_type is not None and chart_type not in HISTORY_CHART_TYPES:
        raise HTTPException(status_code=400, detail="Choose a supported chart type.")
    if country is not None:
        country = country.upper()
        if country not in {item.value for item in Countries}:
            raise HTTPException(status_code=400, detail="Choose a country from the list.")
    if entity_uri is not None and not is_valid_entity_uri(entity_uri):
        raise HTTPException(status_code=400, detail="Choose a valid Spotify entity.")
    if date_from and date_to and date_from > date_to:
        raise HTTPException(status_code=400, detail="The start date must be before the end date.")

    pool = await get_history_pool()
    predicate = """($1 = 'all' OR j.status = $1)
      AND ($2::text IS NULL OR j.chart_type = $2)
      AND ($3::text IS NULL OR j.country_code = $3)
      AND ($4::date IS NULL OR j.requested_date >= ($4::date)::text)
      AND ($5::date IS NULL OR j.requested_date <= ($5::date)::text)
      AND ($6::text IS NULL OR EXISTS (
        SELECT 1 FROM public.spotify_chart_tracking_configs c
        WHERE c.chart_type = j.chart_type AND c.country_code = j.country_code
          AND c.entity_uri = $6
      ))"""
    total = int(await pool.fetchval(
        f"SELECT count(*) FROM public.spotify_chart_refresh_jobs j WHERE {predicate}",
        status, chart_type, country, date_from, date_to, entity_uri,
    ) or 0)
    rows = await pool.fetch(
        f"""SELECT j.chart_type, j.country_code, j.requested_date,
                   COALESCE(j.chart_date::text, CASE WHEN j.requested_date ~ '^\\d{4}-\\d{2}-\\d{2}$' THEN j.requested_date END) AS chart_date,
                   j.status, j.attempt_count, j.next_attempt_at, j.last_error,
                   j.last_http_status, j.updated_at
            FROM public.spotify_chart_refresh_jobs j WHERE {predicate}
            ORDER BY j.updated_at DESC, j.chart_type, j.country_code, j.requested_date
            LIMIT $7 OFFSET $8""",
        status, chart_type, country, date_from, date_to, entity_uri, limit, offset,
    )
    items = []
    for row in rows:
        item = dict(row)
        for field in ("chart_date", "next_attempt_at", "updated_at"):
            if hasattr(item.get(field), "isoformat"):
                item[field] = item[field].isoformat()
        items.append(item)
    return {"items": items, "total": total, "limit": limit, "offset": offset, "hasMore": offset + len(items) < total}


@app.post("/api/collection/jobs/retry")
async def retry_collection_job(retry: ChartJobRetryInput) -> dict[str, Any]:
    require_spotify_connection()
    country = retry.country_code.upper()
    if country not in {item.value for item in Countries}:
        raise HTTPException(status_code=400, detail="Choose a country from the list.")
    try:
        requested_date = calendar_date.fromisoformat(retry.requested_date)
        if requested_date.isoformat() != retry.requested_date:
            raise ValueError
    except ValueError as error:
        raise HTTPException(status_code=400, detail="Date must be a valid YYYY-MM-DD date.") from error

    pool = await get_history_pool()
    row = await pool.fetchrow(
        """UPDATE public.spotify_chart_refresh_jobs
           SET status = 'pending', next_attempt_at = now(), lease_owner = NULL,
               lease_expires_at = NULL, completed_at = NULL, last_error = NULL,
               updated_at = now()
           WHERE chart_type = $1 AND country_code = $2 AND requested_date = $3
             AND status IN ('retry', 'failed', 'unavailable')
           RETURNING chart_type, country_code, requested_date, status""",
        retry.chart_type, country, requested_date.isoformat(),
    )
    if row is None:
        existing = await pool.fetchval(
            """SELECT status FROM public.spotify_chart_refresh_jobs
               WHERE chart_type = $1 AND country_code = $2 AND requested_date = $3""",
            retry.chart_type, country, requested_date.isoformat(),
        )
        if existing is None:
            raise HTTPException(status_code=404, detail="Chart date job was not found.")
        raise HTTPException(status_code=409, detail=f"This chart date cannot be retried while its status is {existing}.")
    return {"chart_type": row["chart_type"], "country_code": row["country_code"],
            "requested_date": row["requested_date"], "status": row["status"]}


@app.get("/api/search")
async def search_entities(
    query: str = Query(..., min_length=2, max_length=80),
    entity_type: Literal["all", "track", "artist"] = Query("all"),
    limit: int = Query(10, ge=1, le=10),
) -> dict[str, Any]:
    scraper = await get_spotify_scraper()
    try:
        items = await scraper.search_entities(query.strip(), entity_type, limit)
        return {"items": items}
    except httpx.HTTPStatusError as error:
        if error.response.status_code == 429:
            raise HTTPException(status_code=429, detail="Spotify search is rate-limited. Try again shortly.") from error
        raise HTTPException(status_code=502, detail="Spotify could not complete this search. Try again shortly.") from error
    except (httpx.HTTPError, ValueError) as error:
        raise HTTPException(status_code=502, detail="Spotify could not complete this search. Try again shortly.") from error


@app.get("/api/selected/live")
async def selected_live(
    country: str = Query("GLOBAL", min_length=2, max_length=6),
    cadence: Literal["daily", "weekly"] = Query("daily"),
) -> dict[str, Any]:
    require_spotify_connection()
    country = country.upper()
    if country not in {item.value for item in Countries}:
        raise HTTPException(status_code=400, detail="Choose a country from the list.")

    selections = await read_selected_entities()
    selected_artist_uris = {row["entity_uri"] for row in selections if row["entity_type"] == "artist"}
    selected_track_uris = {row["entity_uri"] for row in selections if row["entity_type"] == "track"}
    scraper = await get_spotify_scraper()
    response: dict[str, Any] = {"country": country, "cadence": cadence, "songs": [], "artists": [], "history": {}}
    for entity_type in ("track", "artist"):
        chart_type = f"{cadence}_top_{'songs' if entity_type == 'track' else 'artists'}"
        try:
            payload = await getattr(scraper, CHART_METHODS[chart_type])(country, "latest")
        except httpx.HTTPStatusError as error:
            if error.response.status_code == 429:
                raise HTTPException(status_code=429, detail="Spotify rate-limited this chart. Try again shortly.") from error
            if error.response.status_code in {401, 403}:
                raise HTTPException(status_code=502, detail="Spotify rejected chart access. Reconnect Spotify and try again.") from error
            raise HTTPException(
                status_code=502,
                detail=f"Spotify could not return this chart (HTTP {error.response.status_code}). Try again shortly.",
            ) from error
        except (httpx.HTTPError, ValueError) as error:
            raise HTTPException(status_code=502, detail="Spotify could not return this chart. Try again shortly.") from error

        entries = payload.get("entries") if isinstance(payload, dict) else None
        if not isinstance(entries, list):
            raise HTTPException(status_code=502, detail="Spotify returned an unsupported chart format.")
        output_key = "songs" if entity_type == "track" else "artists"
        chart_date = (payload.get("displayChart") or {}).get("date", "Latest")
        for entry in entries:
            if not isinstance(entry, dict) or not live_entry_matches(
                entry, chart_type, selected_artist_uris, selected_track_uris
            ):
                continue
            uri = entry_entity_uri(entry)
            values = snapshot_values(entry)
            if not uri or not values:
                continue
            entry_type, name, artists, streams, rank = values
            stats = entry.get("chartEntryData") or {}
            metric = stats.get("rankingMetric") or {}
            response[output_key].append({
                "entityUri": uri,
                "entityType": entry_type,
                "entityName": name,
                "artistNames": artists,
                "imageUrl": None,
                "inChart": True,
                "chartRank": stats.get("currentRank") or rank,
                "previousRank": stats.get("previousRank"),
                "peakRank": stats.get("peakRank"),
                "metricType": metric.get("type"),
                "metricValue": metric.get("value") if metric.get("value") is not None else streams,
                "chartDate": chart_date,
            })
    return response


@app.get("/api/selected/history")
async def selected_history(
    chart_type: Literal["daily_top_songs", "weekly_top_songs", "daily_top_artists", "weekly_top_artists"],
    country: str = Query("GLOBAL", min_length=2, max_length=6),
    entity_uri: str | None = Query(None, min_length=16, max_length=128),
    range: str = Query("90", min_length=1, max_length=10),
) -> dict[str, Any]:
    require_spotify_connection()
    country = country.upper()
    if country not in {item.value for item in Countries}:
        raise HTTPException(status_code=400, detail="Choose a country from the list.")
    if range == "all":
        start_date = None
    else:
        try:
            days = int(range)
        except ValueError as error:
            raise HTTPException(status_code=400, detail="Choose a valid history range.") from error
        if days < 1 or days > 3650:
            raise HTTPException(status_code=400, detail="Choose a valid history range.")
        start_date = calendar_date.today() - timedelta(days=days)

    entity_type = HISTORY_CHART_TYPES[chart_type]
    if entity_uri and not is_valid_entity_uri(entity_uri, entity_type):
        raise HTTPException(status_code=400, detail="Choose a valid song or artist from this chart.")

    try:
        pool = await get_history_pool()
        if entity_uri:
            rows = await pool.fetch(
                """
                SELECT track_uri, entity_type, track_name, artist_names, chart_date, chart_rank, stream_count
                FROM public.spotify_chart_snapshots
                WHERE chart_type = $1 AND country_code = $2 AND entity_type = $3
                  AND track_uri = $4 AND ($5::date IS NULL OR chart_date >= $5::date)
                ORDER BY chart_date ASC
                """,
                chart_type,
                country,
                entity_type,
                entity_uri,
                start_date,
            )
        else:
            rows = await pool.fetch(
                """
                SELECT track_uri, entity_type, track_name, artist_names, chart_date, chart_rank, stream_count
                FROM (
                    SELECT DISTINCT ON (track_uri)
                        track_uri, entity_type, track_name, artist_names, chart_date, chart_rank, stream_count
                    FROM public.spotify_chart_snapshots
                    WHERE chart_type = $1 AND country_code = $2 AND entity_type = $3
                      AND ($4::date IS NULL OR chart_date >= $4::date)
                    ORDER BY track_uri, chart_date DESC
                ) latest
                ORDER BY chart_rank ASC NULLS LAST, track_name ASC
                """,
                chart_type,
                country,
                entity_type,
                start_date,
            )
    except Exception as error:
        raise HTTPException(status_code=503, detail="Chart history is temporarily unavailable.") from error

    points = [
        {
            "entityUri": row["track_uri"],
            "entityType": row["entity_type"],
            "entityName": row["track_name"],
            "artistNames": row["artist_names"],
            "chartDate": row["chart_date"].isoformat(),
            "chartRank": row["chart_rank"],
            "streamCount": row["stream_count"],
        }
        for row in rows
    ]
    entity = None
    if entity_uri and rows:
        row = rows[0]
        entity = {
            "entityUri": row["track_uri"],
            "entityType": row["entity_type"],
            "entityName": row["track_name"],
            "artistNames": row["artist_names"],
            "imageUrl": None,
        }
    return {"chartType": chart_type, "country": country, "entity": entity, "points": points}


def metadata_artist_uris(metadata: dict[str, Any]) -> set[str]:
    artists = metadata.get("artists") or []
    if not isinstance(artists, list):
        return set()
    uris: set[str] = set()
    for artist in artists:
        if not isinstance(artist, dict):
            continue
        uri = artist.get("spotifyUri") or artist.get("artistUri") or artist.get("uri")
        if isinstance(uri, str) and uri.startswith("spotify:artist:"):
            uris.add(uri)
    return uris


def live_entry_matches(
    entry: dict[str, Any],
    chart_type: str,
    selected_artist_uris: set[str],
    selected_track_uris: set[str],
) -> bool:
    if chart_type in {"daily_top_artists", "weekly_top_artists"}:
        return entry_entity_uri(entry) in selected_artist_uris

    if chart_type == "weekly_top_albums":
        metadata = entry.get("albumMetadata") or {}
        return bool(metadata_artist_uris(metadata) & selected_artist_uris)

    track_uri = entry_entity_uri(entry)
    if track_uri in selected_track_uris:
        return True
    metadata = entry.get("trackMetadata") or {}
    return bool(metadata_artist_uris(metadata) & selected_artist_uris)


def focused_chart_item(entry: dict[str, Any], country_code: str, chart_date: str) -> dict[str, Any] | None:
    values = snapshot_values(entry)
    if not values:
        return None

    entity_type, name, artists, streams, rank = values[:5]
    metadata = entry.get({"track": "trackMetadata", "artist": "artistMetadata", "album": "albumMetadata"}[entity_type]) or {}
    stats = entry.get("chartEntryData") or {}
    metric = stats.get("rankingMetric") or {}
    return {
        "entityUri": entry_entity_uri(entry),
        "entityType": entity_type,
        "entityName": name,
        "artistNames": artists,
        "imageUrl": metadata.get("displayImageUri"),
        "regionCode": country_code,
        "regionName": COUNTRY_NAMES.get(country_code, country_code),
        "chartRank": stats.get("currentRank") or rank,
        "previousRank": stats.get("previousRank"),
        "peakRank": stats.get("peakRank"),
        "consecutiveAppearancesOnChart": stats.get("consecutiveAppearancesOnChart"),
        "appearancesOnChart": stats.get("appearancesOnChart"),
        "entryDate": stats.get("entryDate"),
        "metricType": metric.get("type"),
        "metricValue": metric.get("value") if metric.get("value") is not None else streams,
        "chartDate": chart_date,
    }


focused_chart_scan_tasks: dict[tuple[str, ...], asyncio.Task] = {}


def pending_focused_scan(cadence: str, entity_type: str, chart_type: str | None = None) -> dict[str, Any]:
    chart_type = chart_type or f"{cadence}_top_{ {'track': 'songs', 'artist': 'artists', 'album': 'albums'}[entity_type] }"
    return {
        "artist": {"entityName": "Saved selections"},
        "cadence": cadence,
        "entityType": entity_type,
        "chartType": chart_type,
        "marketScope": "all",
        "marketsTotal": len(Countries),
        "marketsScanned": 0,
        "matchingRegions": 0,
        "failedRegions": [],
        "marketDiagnostics": [],
        "historySavedRegions": 0,
        "historyStorageAvailable": None,
        "releases": [],
        "regions": [],
        "scanInProgress": True,
    }


async def run_focused_scan_and_cache(
    cache_key: tuple[str, ...], cadence: str, entity_type: str, scope: str, chart_type: str
) -> None:
    response: dict[str, Any] | None = None
    try:
        response = await scan_focused_live(cadence, entity_type, scope, chart_type)
        pool = await get_history_pool()
        await save_focused_result_snapshot(pool, cadence, entity_type, scope, chart_type, cache_key[4], response)
        response["snapshotSaved"] = True
        response["historyStorageAvailable"] = True
        response["historySavedRegions"] = await save_focused_results_to_history(pool, chart_type, response)
        if scope == "all":
            failed_markets = [item for item in response["marketDiagnostics"] if item["status"] == "failed"]
            completed_markets = [item["countryCode"] for item in response["marketDiagnostics"] if item["status"] == "succeeded"]
            status = "partial" if failed_markets else "complete"
            await pool.execute(
                """UPDATE public.spotify_focused_scan_jobs
                   SET status=$3, completed_markets=$4, failed_markets=$5::jsonb,
                       partial_payload=$6::jsonb, lease_owner=NULL, lease_expires_at=NULL,
                       attempt_count=attempt_count+1, last_error=$7, updated_at=now()
                   WHERE cadence=$1 AND entity_type=$2""",
                cadence, entity_type, status, completed_markets, json.dumps(failed_markets),
                json.dumps(response), "one_or_more_markets_failed" if failed_markets else None,
            )
    except Exception as error:
        if response is None:
            response = pending_focused_scan(cadence, entity_type, chart_type)
            response.update({
                "scanInProgress": False,
                "scanError": f"The regional scan stopped before market results were available ({type(error).__name__}).",
                "failedRegions": [],
                "marketDiagnostics": [],
            })
        else:
            response.update({
                "scanInProgress": False,
                "scanError": "The regional scan finished, but its results could not be saved for recovery.",
            })
        if scope == "all":
            try:
                pool = await get_history_pool()
                await pool.execute("UPDATE public.spotify_focused_scan_jobs SET status='failed', lease_owner=NULL, lease_expires_at=NULL, last_error=$3, updated_at=now() WHERE cadence=$1 AND entity_type=$2", cadence, entity_type, type(error).__name__[:80])
            except Exception:
                pass
    async with focused_chart_cache_lock:
        focused_chart_cache[cache_key] = (time.monotonic(), response)
        focused_chart_scan_tasks.pop(cache_key, None)


@app.get("/api/focused/live")
async def focused_live(
    cadence: Literal["daily", "weekly"] = Query("daily"),
    entity_type: Literal["track", "artist", "album"] = Query("track"),
    scope: Literal["global", "all"] = Query("global"),
    refresh: bool = Query(False),
    retry_failed: bool = False,
    chart_type: str | None = Query(None),
) -> dict[str, Any]:
    default_chart_type = f"{cadence}_top_{ {'track': 'songs', 'artist': 'artists', 'album': 'albums'}[entity_type] }"
    selected_chart_type = chart_type or default_chart_type
    supported = {
        "daily_top_songs": ("daily", "track"), "daily_viral_songs": ("daily", "track"),
        "weekly_top_songs": ("weekly", "track"), "daily_top_artists": ("daily", "artist"),
        "weekly_top_artists": ("weekly", "artist"), "weekly_top_albums": ("weekly", "album"),
    }
    if selected_chart_type not in supported or supported[selected_chart_type] != (cadence, entity_type):
        raise HTTPException(status_code=400, detail="Choose a supported chart for this Live view.")
    require_spotify_connection()

    selections = await read_selected_entities()
    selected_uris = sorted(row["entity_uri"] for row in selections)
    if not selected_uris:
        return {
            "artist": {"entityName": "Saved selections"}, "cadence": cadence, "entityType": entity_type,
            "chartType": selected_chart_type, "marketScope": scope, "marketsTotal": 0,
            "marketsScanned": 0, "matchingRegions": 0, "failedRegions": [], "marketDiagnostics": [],
            "historySavedRegions": 0, "historyStorageAvailable": None, "releases": [], "regions": [],
            "scanInProgress": False,
        }
    selection_fingerprint = __import__("hashlib").sha256("\n".join(selected_uris).encode()).hexdigest()
    cache_key = (cadence, entity_type, scope, selected_chart_type, selection_fingerprint)
    if retry_failed:
        if scope != "all":
            raise HTTPException(status_code=400, detail="Failed-region retry is available for all-region scans.")
        previous = await read_focused_result_snapshot(
            cadence, entity_type, scope, selected_chart_type, selection_fingerprint
        )
        failed_codes = {
            item.get("countryCode") for item in (previous or {}).get("marketDiagnostics", [])
            if item.get("status") == "failed" and item.get("countryCode")
        }
        if not failed_codes:
            raise HTTPException(status_code=409, detail="There are no failed regions to retry.")
        retried = await scan_focused_live(cadence, entity_type, scope, selected_chart_type, failed_codes)
        previous = previous or {}
        regions_by_code = {item.get("regionCode"): item for item in previous.get("regions", [])}
        regions_by_code.update({item.get("regionCode"): item for item in retried.get("regions", [])})
        diagnostics_by_code = {item.get("countryCode"): item for item in previous.get("marketDiagnostics", [])}
        diagnostics_by_code.update({item.get("countryCode"): item for item in retried.get("marketDiagnostics", [])})
        combined_regions = sorted(regions_by_code.values(), key=lambda item: (item.get("regionCode") != "GLOBAL", item.get("regionName", "")))
        combined_releases: dict[str, dict[str, Any]] = {}
        for region in combined_regions:
            for entry in region.get("entries", []):
                release = combined_releases.setdefault(entry["entityUri"], {
                    "entityUri": entry["entityUri"], "entityType": entry["entityType"],
                    "entityName": entry["entityName"], "artistNames": entry["artistNames"],
                    "imageUrl": entry.get("imageUrl"), "regions": [],
                })
                release["regions"].append({key: value for key, value in entry.items()
                                           if key not in {"entityUri", "entityType", "entityName", "artistNames", "imageUrl"}})
        diagnostics = list(diagnostics_by_code.values())
        failed = [item for item in diagnostics if item.get("status") == "failed"]
        combined = {**previous, **retried, "regions": combined_regions,
                    "marketDiagnostics": diagnostics, "failedRegions": [item.get("countryName", item.get("countryCode")) for item in failed],
                    "marketsTotal": len(diagnostics), "marketsScanned": len(diagnostics) - len(failed),
                    "matchingRegions": len(combined_regions), "releases": sorted(combined_releases.values(), key=lambda item: (-len(item["regions"]), item["entityName"].casefold())),
                    "snapshotReused": False}
        pool = await get_history_pool()
        await save_focused_result_snapshot(pool, cadence, entity_type, scope, selected_chart_type, selection_fingerprint, combined)
        combined["snapshotSaved"] = True
        combined["historyStorageAvailable"] = True
        combined["historySavedRegions"] = await save_focused_results_to_history(pool, selected_chart_type, combined)
        async with focused_chart_cache_lock:
            focused_chart_cache[cache_key] = (time.monotonic(), combined)
        return combined

    async with focused_chart_cache_lock:
        cached = focused_chart_cache.get(cache_key)
        if not refresh and cached and time.monotonic() - cached[0] < FOCUSED_CHART_CACHE_TTL:
            return cached[1]
        if refresh:
            focused_chart_cache.pop(cache_key, None)

    try:
        snapshot = await read_focused_result_snapshot(
            cadence, entity_type, scope, selected_chart_type, selection_fingerprint
        )
    except Exception as error:
        print(f"[v0] Focused Live snapshot lookup failed: {type(error).__name__}")
        snapshot = None
    if snapshot is not None:
        async with focused_chart_cache_lock:
            focused_chart_cache[cache_key] = (time.monotonic(), snapshot)
        return snapshot

    try:
        previous_scan = await read_legacy_focused_scan(cadence, entity_type, selected_chart_type)
    except Exception as error:
        print(f"[v0] Previous focused scan lookup failed: {type(error).__name__}")
        previous_scan = None
    if previous_scan is not None:
        response = focused_response_for_scope(previous_scan, scope)
        response["snapshotReused"] = True
        try:
            pool = await get_history_pool()
            await save_focused_result_snapshot(
                pool, cadence, entity_type, scope, selected_chart_type, selection_fingerprint, response
            )
            response["snapshotSaved"] = True
            response["historyStorageAvailable"] = True
            response["historySavedRegions"] = await save_focused_results_to_history(pool, selected_chart_type, response)
        except Exception as error:
            print(f"[v0] Previous focused scan could not be archived: {type(error).__name__}")
            response["snapshotSaved"] = False
            response["historyStorageAvailable"] = False
            response["historySavedRegions"] = 0
        async with focused_chart_cache_lock:
            focused_chart_cache[cache_key] = (time.monotonic(), response)
        return response

    if scope == "all":
        async with focused_chart_cache_lock:
            task = focused_chart_scan_tasks.get(cache_key)
            if task is None or task.done():
                pool = await get_history_pool()
                await pool.execute(
                    """INSERT INTO public.spotify_focused_scan_jobs (cadence, entity_type, status)
                       VALUES ($1, $2, 'pending') ON CONFLICT (cadence, entity_type) DO UPDATE
                       SET status='pending', updated_at=now()""",
                    cadence, entity_type,
                )
                task = asyncio.create_task(
                    run_focused_scan_and_cache(cache_key, cadence, entity_type, scope, selected_chart_type)
                )
                focused_chart_scan_tasks[cache_key] = task
            return pending_focused_scan(cadence, entity_type, selected_chart_type)

    response = await scan_focused_live(cadence, entity_type, scope, selected_chart_type)
    try:
        pool = await get_history_pool()
        await save_focused_result_snapshot(
            pool, cadence, entity_type, scope, selected_chart_type, selection_fingerprint, response
        )
        response["snapshotSaved"] = True
        response["historyStorageAvailable"] = True
        response["historySavedRegions"] = await save_focused_results_to_history(pool, selected_chart_type, response)
    except Exception as error:
        print(f"[v0] Focused Live snapshot save failed: {type(error).__name__}")
        response["snapshotSaved"] = False
        response["historyStorageAvailable"] = False
        response["historySavedRegions"] = 0
    async with focused_chart_cache_lock:
        focused_chart_cache[cache_key] = (time.monotonic(), response)
    return response


async def scan_focused_live(
    cadence: str,
    entity_type: str,
    scope: str,
    chart_type: str | None = None,
    country_codes: set[str] | None = None,
) -> dict[str, Any]:
    chart_type = chart_type or f"{cadence}_top_{ {'track': 'songs', 'artist': 'artists', 'album': 'albums'}[entity_type] }"
    selections = await read_selected_entities()
    selected_artist_uris = {row["entity_uri"] for row in selections if row["entity_type"] == "artist"}
    selected_track_uris = {row["entity_uri"] for row in selections if row["entity_type"] == "track"}
    if chart_type == "weekly_top_albums":
        selected_track_uris = set()
    scraper = await get_spotify_scraper()
    method = getattr(scraper, CHART_METHODS[chart_type])
    markets = [Countries.GLOBAL] if scope == "global" else list(Countries)
    if country_codes is not None:
        markets = [country for country in markets if country.value in country_codes]
    semaphore = asyncio.Semaphore(2)

    async def fetch_market(country) -> tuple[Any, dict[str, Any] | None, dict[str, Any]]:
        async with semaphore:
            try:
                payload = await method(country.value, "latest")
                if not isinstance(payload, dict) or not isinstance(payload.get("entries"), list):
                    raise ValueError("Spotify returned an unsupported chart payload.")
                diagnostic = scraper.get_chart_diagnostics(chart_type, country.value, "latest")
                diagnostic.update({"countryCode": country.value, "countryName": COUNTRY_NAMES.get(country.value, country.value),
                                   "chartType": chart_type, "status": "succeeded"})
                return country, payload, diagnostic
            except Exception as error:
                diagnostic = scraper.get_chart_diagnostics(chart_type, country.value, "latest")
                response = getattr(error, "response", None)
                diagnostic.update({
                    "countryCode": country.value,
                    "countryName": COUNTRY_NAMES.get(country.value, country.value),
                    "chartType": chart_type,
                    "status": "failed",
                    "httpStatus": response.status_code if response is not None else diagnostic.get("httpStatus"),
                    "error": type(error).__name__,
                    "errorResponse": response.text[:500] if response is not None else diagnostic.get("errorResponse"),
                    "timeout": isinstance(error, httpx.TimeoutException) or bool(diagnostic.get("timeout")),
                    "rateLimited": bool(diagnostic.get("rateLimited")) or (response is not None and response.status_code == 429),
                })
                return country, None, diagnostic

    results = await asyncio.gather(*(fetch_market(country) for country in markets))
    releases: dict[str, dict[str, Any]] = {}
    regions: list[dict[str, Any]] = []
    diagnostics: list[dict[str, Any]] = []
    for country, payload, diagnostic in results:
        diagnostics.append(diagnostic)
        if payload is None:
            continue
        chart_date = str((payload.get("displayChart") or {}).get("date") or "Latest")
        region_items = []
        for entry in payload["entries"]:
            if not isinstance(entry, dict) or not live_entry_matches(
                entry, chart_type, selected_artist_uris, selected_track_uris
            ):
                continue
            item = focused_chart_item(entry, country.value, chart_date)
            if not item or not item["entityUri"]:
                continue
            region_items.append(item)
            release = releases.setdefault(item["entityUri"], {
                "entityUri": item["entityUri"], "entityType": item["entityType"],
                "entityName": item["entityName"], "artistNames": item["artistNames"],
                "imageUrl": item["imageUrl"], "regions": [],
            })
            release["regions"].append({key: value for key, value in item.items()
                                        if key not in {"entityUri", "entityType", "entityName", "artistNames", "imageUrl"}})
        if region_items:
            region_items.sort(key=lambda item: item["chartRank"] or 10_000)
            regions.append({"regionCode": country.value, "regionName": COUNTRY_NAMES.get(country.value, country.value),
                            "chartDate": chart_date, "entries": region_items})

    failed = [item for item in diagnostics if item["status"] == "failed"]
    if not regions and failed and scope == "global":
        raise HTTPException(status_code=502, detail={"message": "Spotify could not load the Global chart.", "marketDiagnostics": diagnostics})
    regions.sort(key=lambda region: (region["regionCode"] != "GLOBAL", region["regionName"]))
    release_items = list(releases.values())
    release_items.sort(key=lambda release: (-len(release["regions"]), release["entityName"].casefold()))
    album_metadata_missing = chart_type == "weekly_top_albums" and bool(selected_artist_uris) and any(
        not metadata_artist_uris(entry.get("albumMetadata") or {})
        for _, payload, _ in results if payload
        for entry in payload.get("entries", []) if isinstance(entry, dict)
    )
    return {
        "artist": {"entityName": "Saved selections"},
        "cadence": cadence,
        "entityType": entity_type,
        "chartType": chart_type,
        "marketScope": scope,
        "marketsTotal": len(markets),
        "marketsScanned": len(markets) - len(failed),
        "matchingRegions": len(regions),
        "failedRegions": [item["countryName"] for item in failed],
        "marketDiagnostics": diagnostics,
        "historySavedRegions": 0,
        "historyStorageAvailable": None,
        "metadataLimitation": "Spotify did not include artist IDs for one or more album chart entries; those entries were not matched." if album_metadata_missing else None,
        "releases": release_items,
        "regions": regions,
        "scanInProgress": False,
    }


def history_start_date(range_value: str) -> calendar_date | None:
    if range_value == "all":
        return None
    try:
        days = int(range_value)
    except ValueError as error:
        raise HTTPException(status_code=400, detail="Choose a valid history range.") from error
    if days < 1 or days > 3650:
        raise HTTPException(status_code=400, detail="Choose a valid history range.")
    return calendar_date.today() - timedelta(days=days)


def history_point(row) -> dict[str, Any]:
    country_code = row["country_code"]
    return {
        "entityUri": row["track_uri"],
        "entityType": row["entity_type"],
        "entityName": row["track_name"],
        "artistNames": row["artist_names"],
        "imageUrl": row.get("image_url"),
        "regionCode": country_code,
        "regionName": COUNTRY_NAMES.get(country_code, country_code),
        "chartDate": row["chart_date"].isoformat(),
        "chartRank": row["chart_rank"],
        "previousRank": row["previous_rank"],
        "peakRank": row["peak_rank"],
        "consecutiveAppearancesOnChart": row["consecutive_appearances"],
        "appearancesOnChart": row["appearances"],
        "entryDate": row["entry_date"].isoformat() if row["entry_date"] else None,
        "streamCount": row["stream_count"],
    }


def group_history_points(points: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    releases: dict[str, dict[str, Any]] = {}
    regions: dict[str, dict[str, Any]] = {}
    for point in points:
        release = releases.setdefault(point["entityUri"], {
            "entityUri": point["entityUri"],
            "entityType": point["entityType"],
            "entityName": point["entityName"],
            "artistNames": point["artistNames"],
            "imageUrl": point.get("imageUrl"),
            "regions": [],
        })
        if not release["imageUrl"] and point.get("imageUrl"):
            release["imageUrl"] = point["imageUrl"]
        release["regions"].append(point)
        region = regions.setdefault(point["regionCode"], {
            "regionCode": point["regionCode"],
            "regionName": point["regionName"],
            "entries": [],
        })
        region["entries"].append(point)

    release_items = list(releases.values())
    for release in release_items:
        release["regions"].sort(key=lambda point: (point["regionCode"] != "GLOBAL", point["regionName"]))
    release_items.sort(key=lambda release: (-len(release["regions"]), release["entityName"].casefold()))
    region_items = list(regions.values())
    for region in region_items:
        region["entries"].sort(key=lambda point: point["chartRank"] if point["chartRank"] is not None else 10_000)
    region_items.sort(key=lambda region: (region["regionCode"] != "GLOBAL", region["regionName"]))
    return release_items, region_items


def focused_snapshot_history_points(
    rows: list[Any], chart_type: str, selection_fingerprint: str
) -> list[dict[str, Any]]:
    points_by_key: dict[tuple[str, str, str], tuple[int, dict[str, Any]]] = {}
    for row in rows:
        snapshot_date = row["snapshot_date"]
        snapshot_date_text = snapshot_date.isoformat() if hasattr(snapshot_date, "isoformat") else str(snapshot_date)
        payload = decode_json_value(row["payload"])
        results = payload.get("results", []) if isinstance(payload, dict) else []
        if not isinstance(results, list):
            continue
        for saved in results:
            if not isinstance(saved, dict) or saved.get("chartType") != chart_type or saved.get("selectionFingerprint") != selection_fingerprint:
                continue
            scope_priority = 1 if saved.get("scope") == "all" else 0
            response = saved.get("response") or {}
            for region in response.get("regions", []):
                region_code = region.get("regionCode")
                for item in region.get("entries", []):
                    entity_uri = item.get("entityUri")
                    if not region_code or not entity_uri:
                        continue
                    chart_day = parse_chart_date(item.get("chartDate")) or parse_chart_date(snapshot_date_text)
                    if chart_day is None:
                        continue
                    point = {
                        "entityUri": entity_uri,
                        "entityType": item.get("entityType"),
                        "entityName": item.get("entityName") or "Untitled",
                        "artistNames": item.get("artistNames") or "",
                        "imageUrl": item.get("imageUrl"),
                        "regionCode": region_code,
                        "regionName": item.get("regionName") or region.get("regionName") or COUNTRY_NAMES.get(region_code, region_code),
                        "chartDate": chart_day.isoformat(),
                        "chartRank": item.get("chartRank"),
                        "previousRank": item.get("previousRank"),
                        "peakRank": item.get("peakRank"),
                        "consecutiveAppearancesOnChart": item.get("consecutiveAppearancesOnChart"),
                        "appearancesOnChart": item.get("appearancesOnChart"),
                        "entryDate": item.get("entryDate"),
                        "streamCount": item.get("metricValue") if str(item.get("metricType", "")).upper() == "STREAMS" else None,
                    }
                    key = (region_code, entity_uri, chart_day.isoformat())
                    previous = points_by_key.get(key)
                    if previous is None or scope_priority > previous[0]:
                        points_by_key[key] = (scope_priority, point)
    return [value[1] for value in points_by_key.values()]


@app.get("/api/focused/history")
async def focused_history(
    cadence: Literal["daily", "weekly"] = Query("daily"),
    entity_type: Literal["track", "artist", "album"] = Query("track"),
    chart_type: str | None = Query(None),
    chart_date: calendar_date | None = Query(None),
    range: str = Query("90", min_length=1, max_length=10),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0, le=1000000),
) -> dict[str, Any]:
    default_chart_type = f"{cadence}_top_{ {'track': 'songs', 'artist': 'artists', 'album': 'albums'}[entity_type] }"
    chart_type = chart_type or default_chart_type
    if chart_type not in HISTORY_CHART_TYPES or HISTORY_CHART_TYPES[chart_type] != entity_type:
        raise HTTPException(status_code=400, detail="Choose a supported chart for this History view.")
    start_date = history_start_date(range) if chart_date is None else None
    market_codes = [country.value for country in Countries]
    try:
        pool = await get_history_pool()
        rows = await pool.fetch(
            f"""
            WITH latest_per_market AS (
                SELECT DISTINCT ON (country_code, track_uri)
                    track_uri, entity_type, track_name, artist_names, country_code, chart_date,
                    chart_rank, previous_rank, peak_rank, consecutive_appearances, appearances,
                    entry_date, stream_count
                FROM public.spotify_chart_snapshots
                WHERE chart_type = $1 AND entity_type = $2 AND country_code = ANY($3::text[])
                  AND ($4::date IS NULL OR chart_date >= $4::date)
                  AND ($5::date IS NULL OR chart_date = $5::date)
                  AND (
                      EXISTS (
                          SELECT 1 FROM public.spotify_chart_selections s
                          WHERE s.entity_uri = spotify_chart_snapshots.track_uri
                            AND s.is_archived = false
                      )
                      OR EXISTS (
                          SELECT 1
                          FROM public.spotify_chart_payload_cache pc
                          CROSS JOIN LATERAL jsonb_array_elements(COALESCE(pc.payload->'entries', '[]'::jsonb)) chart_entry
                          CROSS JOIN LATERAL jsonb_array_elements(COALESCE(
                              chart_entry->(CASE WHEN $2 = 'album' THEN 'albumMetadata' ELSE 'trackMetadata' END)->'artists',
                              '[]'::jsonb
                          )) chart_artist
                          JOIN public.spotify_chart_selections s
                            ON s.entity_uri = COALESCE(chart_artist->>'spotifyUri', chart_artist->>'artistUri', chart_artist->>'uri')
                           AND s.entity_type = 'artist' AND s.is_archived = false
                          WHERE pc.chart_type = spotify_chart_snapshots.chart_type
                            AND pc.country_code = spotify_chart_snapshots.country_code
                            AND pc.chart_date = spotify_chart_snapshots.chart_date
                            AND COALESCE(
                                chart_entry->'trackMetadata'->>'trackUri', chart_entry->'albumMetadata'->>'albumUri',
                                chart_entry->'artistMetadata'->>'artistUri', chart_entry->>'trackUri',
                                chart_entry->>'albumUri', chart_entry->>'artistUri'
                            ) = spotify_chart_snapshots.track_uri
                      )
                  )
                ORDER BY country_code, track_uri, chart_date DESC, fetched_at DESC
            )
            SELECT history.*,
                   COALESCE(art.image_url, selection.image_url) AS image_url,
                   count(*) OVER() AS total_rows
            FROM latest_per_market history
            LEFT JOIN LATERAL (
                SELECT COALESCE(
                    chart_entry->'trackMetadata'->>'displayImageUri',
                    chart_entry->'albumMetadata'->>'displayImageUri',
                    chart_entry->'artistMetadata'->>'displayImageUri'
                ) AS image_url
                FROM public.spotify_chart_payload_cache pc
                CROSS JOIN LATERAL jsonb_array_elements(COALESCE(pc.payload->'entries', '[]'::jsonb)) chart_entry
                WHERE pc.chart_type = $1
                  AND pc.country_code = history.country_code
                  AND pc.chart_date = history.chart_date
                  AND COALESCE(
                      chart_entry->'trackMetadata'->>'trackUri',
                      chart_entry->'albumMetadata'->>'albumUri',
                      chart_entry->'artistMetadata'->>'artistUri',
                      chart_entry->>'trackUri', chart_entry->>'albumUri', chart_entry->>'artistUri'
                  ) = history.track_uri
                ORDER BY pc.fetched_at DESC
                LIMIT 1
            ) art ON true
            LEFT JOIN public.spotify_chart_selections selection
              ON selection.entity_uri = history.track_uri
            ORDER BY history.country_code, history.track_uri LIMIT $6 OFFSET $7
            """,
            chart_type,
            entity_type,
            market_codes,
            start_date,
            chart_date,
            limit,
            offset,
        )
    except Exception as error:
        raise HTTPException(status_code=503, detail="Focused chart history is temporarily unavailable.") from error

    points = [history_point(row) for row in rows]
    total_rows = int(rows[0]["total_rows"]) if rows else 0
    try:
        selections = await read_selected_entities()
        selected_uris = sorted(row["entity_uri"] for row in selections)
        selection_fingerprint = __import__("hashlib").sha256("\n".join(selected_uris).encode()).hexdigest()
        focused_rows = await pool.fetch(
            """SELECT snapshot_date, payload FROM public.spotify_focused_chart_snapshots
               WHERE cadence=$1 AND entity_type=$2 AND market_scope='all'
                 AND ($3::date IS NULL OR snapshot_date >= $3::date)
               ORDER BY snapshot_date""",
            cadence, entity_type, start_date,
        )
    except Exception as error:
        raise HTTPException(status_code=503, detail="Focused chart history is temporarily unavailable.") from error
    live_points = focused_snapshot_history_points(focused_rows, chart_type, selection_fingerprint)
    if chart_date is not None:
        selected_day = chart_date.isoformat()
        live_points = [point for point in live_points if point["chartDate"] == selected_day]
    page_live_points = live_points[offset:offset + limit]
    merged = {
        (point["regionCode"], point["entityUri"], point["chartDate"]): point
        for point in page_live_points
    }
    for point in points:
        merged.setdefault((point["regionCode"], point["entityUri"], point["chartDate"]), point)
    points = sorted(
        merged.values(),
        key=lambda point: (point["regionCode"], point["entityUri"], point["chartDate"]),
    )[:limit]
    total_rows = max(total_rows, len(live_points), offset + len(points))
    releases, regions = group_history_points(points)
    return {
        "artist": {"entityName": "Tracked entities"},
        "cadence": cadence,
        "entityType": entity_type,
        "chartType": chart_type,
        "range": range,
        "chartDate": chart_date.isoformat() if chart_date else None,
        "snapshotRows": len(points),
        "totalRows": total_rows,
        "limit": limit,
        "offset": offset,
        "hasMore": offset + len(points) < total_rows,
        "releases": releases,
        "regions": regions,
    }


@app.get("/api/focused/history/trend")
async def focused_history_trend(
    entity_uri: str = Query(..., min_length=16, max_length=128),
    cadence: Literal["daily", "weekly"] = Query("daily"),
    entity_type: Literal["track", "artist", "album"] = Query("track"),
    chart_type: str | None = Query(None),
    range: str = Query("90", min_length=1, max_length=10),
) -> dict[str, Any]:
    if not is_valid_entity_uri(entity_uri, entity_type):
        raise HTTPException(status_code=400, detail="Choose a valid chart release.")
    default_chart_type = f"{cadence}_top_{ {'track': 'songs', 'artist': 'artists', 'album': 'albums'}[entity_type] }"
    chart_type = chart_type or default_chart_type
    if chart_type not in HISTORY_CHART_TYPES or HISTORY_CHART_TYPES[chart_type] != entity_type:
        raise HTTPException(status_code=400, detail="Choose a supported chart for this History trend.")
    start_date = history_start_date(range)
    market_codes = [country.value for country in Countries]
    try:
        pool = await get_history_pool()
        rows = await pool.fetch(
            """
            SELECT track_uri, entity_type, track_name, artist_names, country_code, chart_date, chart_rank, stream_count
            FROM public.spotify_chart_snapshots
            WHERE chart_type = $1 AND entity_type = $2 AND track_uri = $3
              AND country_code = ANY($4::text[]) AND ($5::date IS NULL OR chart_date >= $5::date)
              AND (
                  EXISTS (
                      SELECT 1 FROM public.spotify_chart_selections s
                      WHERE s.entity_uri = spotify_chart_snapshots.track_uri AND s.is_archived = false
                  )
                  OR EXISTS (
                      SELECT 1
                      FROM public.spotify_chart_payload_cache pc
                      CROSS JOIN LATERAL jsonb_array_elements(COALESCE(pc.payload->'entries', '[]'::jsonb)) chart_entry
                      CROSS JOIN LATERAL jsonb_array_elements(COALESCE(
                          chart_entry->(CASE WHEN $2 = 'album' THEN 'albumMetadata' ELSE 'trackMetadata' END)->'artists',
                          '[]'::jsonb
                      )) chart_artist
                      JOIN public.spotify_chart_selections s
                        ON s.entity_uri = COALESCE(chart_artist->>'spotifyUri', chart_artist->>'artistUri', chart_artist->>'uri')
                       AND s.entity_type = 'artist' AND s.is_archived = false
                      WHERE pc.chart_type = spotify_chart_snapshots.chart_type
                        AND pc.country_code = spotify_chart_snapshots.country_code
                        AND pc.chart_date = spotify_chart_snapshots.chart_date
                        AND COALESCE(chart_entry->'trackMetadata'->>'trackUri', chart_entry->'albumMetadata'->>'albumUri') = spotify_chart_snapshots.track_uri
                  )
              )
            ORDER BY country_code ASC, chart_date ASC
            """,
            chart_type,
            entity_type,
            entity_uri,
            market_codes,
            start_date,
        )
    except Exception as error:
        raise HTTPException(status_code=503, detail="Focused chart history is temporarily unavailable.") from error

    points = [history_point(row) for row in rows]
    entity = None if not points else {key: points[0][key] for key in ("entityUri", "entityType", "entityName", "artistNames")}
    return {
        "artist": {"entityName": "Tracked entities"},
        "entity": entity,
        "cadence": cadence,
        "entityType": entity_type,
        "chartType": chart_type,
        "range": range,
        "points": points,
    }


@app.get("/auth/login")
async def login(request: Request):
    if missing_configuration():
        raise HTTPException(status_code=503, detail="Spotify app credentials are not configured.")

    state = secrets.token_urlsafe(32)
    query = urlencode(
        {
            "response_type": "code",
            "client_id": settings.client_id,
            "redirect_uri": callback_url(request),
            "state": state,
        }
    )
    response = RedirectResponse(f"https://accounts.spotify.com/authorize?{query}", status_code=302)
    response.set_cookie(
        "spotify_oauth_state",
        state,
        max_age=600,
        httponly=True,
        secure=request.url.scheme == "https",
        samesite="lax",
        path="/oauth/callback",
    )
    return response


@app.get("/oauth/callback", name="oauth_callback")
async def oauth_callback(
    request: Request,
    code: str | None = None,
    state: str | None = None,
    error: str | None = None,
    oauth_state: str | None = Cookie(default=None, alias="spotify_oauth_state"),
):
    global refresh_token, access_token, access_token_expires_at

    if error or not code or not state or not oauth_state or not secrets.compare_digest(state, oauth_state):
        response = RedirectResponse("/?auth=error", status_code=303)
        response.delete_cookie("spotify_oauth_state", path="/oauth/callback")
        return response

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.post(
                "https://accounts.spotify.com/api/token",
                data={
                    "code": code,
                    "redirect_uri": callback_url(request),
                    "grant_type": "authorization_code",
                },
                auth=(settings.client_id or "", settings.client_secret or ""),
            )
            response.raise_for_status()
            token_data = response.json()
        new_refresh_token = token_data.get("refresh_token")
        if not isinstance(new_refresh_token, str) or not new_refresh_token:
            response = RedirectResponse("/?auth=error", status_code=303)
            response.delete_cookie("spotify_oauth_state", path="/oauth/callback")
            return response
        await store_spotify_refresh_token(new_refresh_token)
        refresh_token = new_refresh_token
        access_token = None
        access_token_expires_at = 0.0
    except Exception:
        response = RedirectResponse("/?auth=error", status_code=303)
        response.delete_cookie("spotify_oauth_state", path="/oauth/callback")
        return response

    response = RedirectResponse("/?auth=connected", status_code=303)
    response.delete_cookie("spotify_oauth_state", path="/oauth/callback")
    return response


@app.post("/api/disconnect")
async def disconnect() -> dict[str, bool]:
    global refresh_token, access_token, access_token_expires_at
    try:
        await remove_spotify_refresh_token()
    except Exception as error:
        raise HTTPException(status_code=503, detail="Spotify authorization could not be removed from storage.") from error
    refresh_token = None
    access_token = None
    access_token_expires_at = 0.0
    return {"connected": False}


async def decorate_chart(raw_payload: dict[str, Any], chart_type: str, country: str, requested_date: str) -> dict[str, Any]:
    result = json.loads(json.dumps(raw_payload))
    result["chartHistory"] = {
        "supported": False,
        "complete": False,
        "status": "browsing_only",
        "metric": "rank" if HISTORY_CHART_TYPES.get(chart_type) == "artist" else "streams",
        "reason": "Charts Explorer is browse-only. History is collected by the tracked-entity collector.",
    }
    return result


@app.get("/api/charts")
async def chart(
    chart_type: str = Query(..., alias="type"),
    country: str = Query("GLOBAL", min_length=2, max_length=6),
    date: str = Query("latest", min_length=1, max_length=10),
) -> dict[str, Any]:
    if chart_type not in CHART_METHODS:
        raise HTTPException(status_code=400, detail="Choose a supported chart type.")
    normalized_country = country.upper()
    if normalized_country not in {item.value for item in Countries}:
        raise HTTPException(status_code=400, detail="Choose a country from the list.")
    if date != "latest":
        try:
            if calendar_date.fromisoformat(date).isoformat() != date:
                raise ValueError
        except ValueError as error:
            raise HTTPException(status_code=400, detail="Date must be latest or YYYY-MM-DD.") from error

    require_spotify_connection()
    cached_row = None
    try:
        cached_row = await read_chart_cache(chart_type, normalized_country, date)
    except Exception as error:
        print(f"[v0] Chart cache read unavailable: {type(error).__name__}")
    if cached_row is not None:
        raw = cached_row["payload"]
        try:
            if isinstance(raw, str):
                raw = json.loads(raw)
        except (TypeError, ValueError):
            raw = None
        if isinstance(raw, dict) and isinstance(raw.get("entries"), list):
            age_seconds = max(0, int(time.time() - cached_row["fetched_at"].timestamp()))
            stale = age_seconds >= CHART_PAYLOAD_TTL_SECONDS
            job_status = "fresh"
            if stale:
                try:
                    await enqueue_chart_refresh(chart_type, normalized_country, date)
                    job_status = "pending"
                except Exception as error:
                    print(f"[v0] Chart refresh queue unavailable: {type(error).__name__}")
                    job_status = "unavailable"
            decorated = await decorate_chart(raw, chart_type, normalized_country, date)
            decorated["delivery"] = {
                "fetchedAt": cached_row["fetched_at"].isoformat(), "ageSeconds": age_seconds,
                "isStale": stale, "refreshStatus": job_status,
            }
            return decorated

    scraper = await get_spotify_scraper()
    try:
        raw_result = await getattr(scraper, CHART_METHODS[chart_type])(normalized_country, date)
    except httpx.HTTPStatusError as error:
        if error.response.status_code == 429:
            raise HTTPException(status_code=429, detail="Spotify rate-limited this chart. Try again shortly.") from error
        if error.response.status_code in {401, 403}:
            raise HTTPException(status_code=502, detail="Spotify rejected chart access. Reconnect Spotify and try again.") from error
        raise HTTPException(status_code=502, detail=f"Spotify could not return this chart (HTTP {error.response.status_code}). Try again shortly.") from error
    except (httpx.HTTPError, ValueError) as error:
        raise HTTPException(status_code=502, detail="Spotify could not return this chart. Try again shortly.") from error
    if not isinstance(raw_result, dict) or not isinstance(raw_result.get("entries"), list):
        raise HTTPException(status_code=502, detail="Spotify returned an unsupported chart format.")

    fetched_at = None
    try:
        fetched_at = await save_chart_cache(chart_type, normalized_country, date, raw_result)
    except Exception as error:
        print(f"[v0] Chart cache write unavailable: {type(error).__name__}")
    result = await decorate_chart(raw_result, chart_type, normalized_country, date)
    result["delivery"] = {
        "fetchedAt": fetched_at.isoformat() if fetched_at else None,
        "ageSeconds": 0, "isStale": False, "refreshStatus": "fresh" if fetched_at else "uncached",
    }
    return result


@app.post("/api/charts/refresh")
async def refresh_chart(
    chart_type: str = Query(..., alias="type"),
    country: str = Query("GLOBAL", min_length=2, max_length=6),
    date: str = Query("latest", min_length=1, max_length=10),
) -> dict[str, Any]:
    if chart_type not in CHART_METHODS or country.upper() not in {item.value for item in Countries}:
        raise HTTPException(status_code=400, detail="Choose a supported chart and country.")
    normalized_country = country.upper()
    if date != "latest":
        try:
            if calendar_date.fromisoformat(date).isoformat() != date:
                raise ValueError
        except ValueError as error:
            raise HTTPException(status_code=400, detail="Date must be latest or YYYY-MM-DD.") from error
    require_spotify_connection()
    pool = await get_history_pool()
    owner = secrets.token_urlsafe(18)
    job = await pool.fetchrow(
        """UPDATE public.spotify_chart_refresh_jobs SET status='running', lease_owner=$4,
             lease_expires_at=now()+($5 * interval '1 second'), attempt_count=attempt_count+1,
             updated_at=now() WHERE chart_type=$1 AND country_code=$2 AND requested_date=$3
             AND next_attempt_at <= now() AND (status IN ('pending','failed') OR
                 (status='running' AND lease_expires_at < now())) RETURNING attempt_count""",
        chart_type, normalized_country, date, owner, CHART_REFRESH_LEASE_SECONDS,
    )
    if not job:
        current = await pool.fetchrow(
            "SELECT status, next_attempt_at FROM public.spotify_chart_refresh_jobs WHERE chart_type=$1 AND country_code=$2 AND requested_date=$3",
            chart_type, normalized_country, date,
        )
        return {"status": current["status"] if current else "pending"}
    try:
        scraper = await get_spotify_scraper()
        payload = await getattr(scraper, CHART_METHODS[chart_type])(normalized_country, date)
        if not isinstance(payload, dict) or not isinstance(payload.get("entries"), list):
            raise ValueError("Spotify returned an unsupported chart format.")
        fetched_at = await save_chart_cache(chart_type, normalized_country, date, payload)
        await pool.execute(
            """UPDATE public.spotify_chart_refresh_jobs SET status='complete', lease_owner=NULL,
                 lease_expires_at=NULL, last_error=NULL, updated_at=now()
               WHERE chart_type=$1 AND country_code=$2 AND requested_date=$3 AND lease_owner=$4""",
            chart_type, normalized_country, date, owner,
        )
        return {"status": "complete", "fetchedAt": fetched_at.isoformat()}
    except Exception as error:
        delay = min(300, 5 * (2 ** min(int(job["attempt_count"]) - 1, 6)))
        await pool.execute(
            """UPDATE public.spotify_chart_refresh_jobs SET status='failed', lease_owner=NULL,
                 lease_expires_at=NULL, next_attempt_at=now()+($5 * interval '1 second'),
                 last_error=$6, updated_at=now()
               WHERE chart_type=$1 AND country_code=$2 AND requested_date=$3 AND lease_owner=$4""",
            chart_type, normalized_country, date, owner, delay, type(error).__name__[:80],
        )
        return {"status": "failed", "retryAfterSeconds": delay, "error": "Refresh failed; the saved chart is unchanged."}


async def chart_history_backfill_queue_summary(pool: Any) -> dict[str, int]:
    row = await pool.fetchrow(
        """SELECT count(*) AS total_jobs,
                  count(*) FILTER (WHERE status = 'complete') AS complete_jobs,
                  count(*) FILTER (WHERE status IN ('pending', 'paused', 'running')) AS active_jobs,
                  coalesce(sum(greatest(total_dates - processed_dates, 0)), 0) AS remaining_dates
           FROM public.spotify_chart_backfills"""
    )
    return {
        "totalJobs": int(row["total_jobs"] or 0),
        "completeJobs": int(row["complete_jobs"] or 0),
        "activeJobs": int(row["active_jobs"] or 0),
        "remainingDates": int(row["remaining_dates"] or 0),
    }


@app.get("/api/chart-history/backfill/queue")
async def chart_history_backfill_queue() -> dict[str, int]:
    require_spotify_connection()
    return await chart_history_backfill_queue_summary(await get_history_pool())


@app.post("/api/chart-history/backfill/queue/step")
async def chart_history_backfill_queue_step() -> dict[str, Any]:
    require_spotify_connection()
    pool = await get_history_pool()
    job = await pool.fetchrow(
        """SELECT chart_type, country_code, start_date, target_date, next_date
           FROM public.spotify_chart_backfills
           WHERE status IN ('pending', 'paused')
              OR (status = 'running' AND (lease_expires_at IS NULL OR lease_expires_at < now()))
           ORDER BY updated_at, chart_type, country_code
           LIMIT 1"""
    )
    if job is None:
        queue = await chart_history_backfill_queue_summary(pool)
        if queue["remainingDates"] == 0:
            return {"state": "complete", "queue": queue}
        wait = await pool.fetchrow(
            """SELECT ceil(extract(epoch FROM (wait_until - now()))) AS retry_after_seconds
               FROM (
                 SELECT min(lease_expires_at) AS wait_until
                 FROM public.spotify_chart_backfills
                 WHERE status = 'running' AND lease_expires_at > now()
                 UNION ALL
                 SELECT min(next_attempt_at) AS wait_until
                 FROM public.spotify_chart_refresh_jobs
                 WHERE status = 'retry' AND next_attempt_at > now()
               ) waits
               WHERE wait_until IS NOT NULL
               ORDER BY wait_until
               LIMIT 1"""
        )
        return {
            "state": "waiting",
            "queue": queue,
            "retryAfterSeconds": max(1, int(wait["retry_after_seconds"] or 30)) if wait else 30,
        }

    if job["chart_type"] not in HISTORY_CHART_TYPES:
        raise HTTPException(status_code=409, detail="The backfill queue contains an unsupported chart type.")
    try:
        progress = await run_durable_history_backfill_batch(
            job["chart_type"], job["country_code"], job["target_date"], job["start_date"],
            None, get_spotify_scraper,
        )
    except ChartHistoryStorageError as error:
        raise HTTPException(status_code=503, detail="Historical storage is unavailable.") from error

    queue = await chart_history_backfill_queue_summary(pool)
    if progress.get("error"):
        retry = await pool.fetchrow(
            """SELECT ceil(extract(epoch FROM (next_attempt_at - now()))) AS retry_after_seconds
               FROM public.spotify_chart_refresh_jobs
               WHERE chart_type = $1 AND country_code = $2 AND requested_date = $3
                 AND status = 'retry' AND next_attempt_at > now()""",
            job["chart_type"], job["country_code"], progress.get("nextDate", job["next_date"].isoformat()),
        )
        return {
            "state": "waiting",
            "batch": {**progress, "chartType": job["chart_type"], "country": job["country_code"]},
            "queue": queue,
            "retryAfterSeconds": max(1, int(retry["retry_after_seconds"] or 30)) if retry else 30,
        }
    return {
        "state": "complete" if queue["remainingDates"] == 0 else "batch",
        "batch": {**progress, "chartType": job["chart_type"], "country": job["country_code"]},
        "queue": queue,
    }


@app.post("/api/chart-history/backfill")
async def chart_history_backfill(request: ChartHistoryBackfillRequest) -> dict[str, Any]:
    chart_type = request.chart_type
    country = request.country.upper()
    require_spotify_connection()
    if chart_type not in HISTORY_CHART_TYPES:
        raise HTTPException(status_code=400, detail="Choose a supported songs or artists chart to backfill history.")
    if country not in {item.value for item in Countries}:
        raise HTTPException(status_code=400, detail="Choose a country from the list.")

    entity_uris = list(dict.fromkeys(request.entity_uris))
    target_entity_uris = list(dict.fromkeys(request.target_entity_uris or [])) or None
    allowed_selection_types = {"artist"} if chart_type in {"daily_top_artists", "weekly_top_artists", "weekly_top_albums"} else {"artist", "track"}
    if any(not is_valid_entity_uri(uri) or uri.split(":")[1] not in allowed_selection_types
           for uri in entity_uris + (target_entity_uris or [])):
        raise HTTPException(status_code=400, detail="The chart selection list is invalid.")

    try:
        return await run_durable_history_backfill_batch(
            chart_type,
            country,
            request.target_date,
            request.start_date,
            entity_uris,
            get_spotify_scraper,
            target_entity_uris=target_entity_uris,
        )
    except ChartHistoryStorageError as error:
        raise HTTPException(status_code=503, detail="Historical storage is unavailable.") from error
