from __future__ import annotations

import asyncio
import secrets
import time
from contextlib import asynccontextmanager
from datetime import date as calendar_date, timedelta
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlencode, urlsplit

import httpx
from fastapi import Cookie, FastAPI, HTTPException, Query, Request
from pydantic import BaseModel, Field
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse

from chart_history import (
    ChartHistoryStorageError,
    close_history_pool,
    entry_entity_uri,
    get_history_pool,
    prepare_chart_history,
    run_history_backfill_batch,
    snapshot_values,
)
from Countries import Countries
from Settings import SpotifySettings
from spotify_charts import SpotifyCharts


@asynccontextmanager
async def lifespan(_: FastAPI):
    yield
    await close_history_pool()


app = FastAPI(title="Spotify Charts", docs_url=None, redoc_url=None, lifespan=lifespan)
settings = SpotifySettings()
refresh_token: str | None = None
access_token: str | None = None
access_token_expires_at = 0.0
token_refresh_lock = asyncio.Lock()

HISTORY_CHART_TYPES = {
    "daily_top_songs": "track",
    "weekly_top_songs": "track",
    "daily_top_artists": "artist",
    "weekly_top_artists": "artist",
    "daily_top_albums": "album",
    "weekly_top_albums": "album",
}
FOCUSED_ARTIST_URI = "spotify:artist:06HL4z0CvFAxyc27GXpf02"
FOCUSED_ARTIST_NAME = "Taylor Swift"
FOCUSED_CHART_CACHE_TTL = 180
focused_chart_cache: dict[tuple[str, str], tuple[float, dict[str, Any]]] = {}
focused_chart_cache_lock = asyncio.Lock()
COUNTRY_NAMES = {country.value: country.name.replace("_", " ").title() for country in Countries}


class ChartHistoryBackfillRequest(BaseModel):
    chart_type: str
    country: str
    target_date: calendar_date
    start_date: calendar_date
    entity_uris: list[str] = Field(default_factory=list, max_length=200)


class SelectionInput(BaseModel):
    entity_type: Literal["track", "artist"]
    entity_uri: str = Field(min_length=16, max_length=128)
    entity_name: str = Field(min_length=1, max_length=300)
    artist_names: str = Field(default="", max_length=500)
    image_url: str | None = Field(default=None, max_length=2048)


CHART_METHODS = {
    "daily_top_songs": "daily_top_songs",
    "daily_top_artists": "daily_top_artists",
    "daily_viral_songs": "daily_viral_songs",
    "daily_top_albums": "daily_top_albums",
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

    if not refresh_token:
        raise HTTPException(status_code=401, detail="Connect Spotify before loading charts.")

    async with token_refresh_lock:
        if access_token and time.monotonic() < access_token_expires_at - 60:
            return SpotifyCharts(access_token=access_token)
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                response = await client.post(
                    "https://accounts.spotify.com/api/token",
                    data={"grant_type": "refresh_token", "refresh_token": refresh_token},
                    auth=(settings.client_id or "", settings.client_secret or ""),
                )
                response.raise_for_status()
                token_data = response.json()
            access_token = token_data["access_token"]
            refresh_token = token_data.get("refresh_token", refresh_token)
            access_token_expires_at = time.monotonic() + max(60, int(token_data.get("expires_in", 3600)))
        except (KeyError, TypeError, ValueError) as error:
            raise HTTPException(status_code=502, detail="Spotify returned an unexpected token response.") from error
        except httpx.HTTPError as error:
            raise HTTPException(status_code=502, detail="Spotify could not refresh authorization. Reconnect and try again.") from error

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
                WHERE entity_type = $1
                ORDER BY entity_name ASC
                """,
                entity_type,
            )
        else:
            rows = await pool.fetch(
                """
                SELECT entity_uri, entity_type, entity_name, artist_names, image_url
                FROM public.spotify_chart_selections
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
                image_url = EXCLUDED.image_url
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
        result = await pool.execute(
            "DELETE FROM public.spotify_chart_selections WHERE entity_uri = $1",
            entity_uri,
        )
        return {"deleted": result.endswith("1")}
    except Exception as error:
        raise HTTPException(status_code=503, detail="The selection could not be removed right now.") from error


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
        try:
            response["history"][entity_type] = await prepare_chart_history(
                chart_type, country, "latest", payload, None
            )
        except ChartHistoryStorageError:
            response["history"][entity_type] = {"status": "unavailable", "reason": "History storage is unavailable."}

        output_key = "songs" if entity_type == "track" else "artists"
        chart_date = (payload.get("displayChart") or {}).get("date", "Latest")
        for entry in entries:
            if not isinstance(entry, dict):
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


def focused_artist_matches(entry: dict[str, Any], entity_type: str) -> bool:
    metadata = entry.get({"track": "trackMetadata", "artist": "artistMetadata", "album": "albumMetadata"}[entity_type]) or {}
    if entity_type == "artist":
        artist_uri = metadata.get("artistUri") or entry_entity_uri(entry)
        artist_name = metadata.get("artistName") or ""
        return artist_uri == FOCUSED_ARTIST_URI or artist_name.casefold() == FOCUSED_ARTIST_NAME.casefold()

    artists = metadata.get("artists") or []
    return any(
        isinstance(artist, dict)
        and (
            artist.get("spotifyUri") == FOCUSED_ARTIST_URI
            or artist.get("uri") == FOCUSED_ARTIST_URI
            or str(artist.get("name", "")).casefold() == FOCUSED_ARTIST_NAME.casefold()
        )
        for artist in artists
    )


def focused_chart_item(entry: dict[str, Any], country_code: str, chart_date: str) -> dict[str, Any] | None:
    values = snapshot_values(entry)
    if not values:
        return None

    entity_type, name, artists, streams, rank = values
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


@app.get("/api/focused/live")
async def focused_live(
    cadence: Literal["daily", "weekly"] = Query("daily"),
    entity_type: Literal["track", "artist", "album"] = Query("track"),
    refresh: bool = Query(False),
) -> dict[str, Any]:
    chart_type = f"{cadence}_top_{ {'track': 'songs', 'artist': 'artists', 'album': 'albums'}[entity_type] }"
    cache_key = (cadence, entity_type)
    async with focused_chart_cache_lock:
        cached = focused_chart_cache.get(cache_key)
        if not refresh and cached and time.monotonic() - cached[0] < FOCUSED_CHART_CACHE_TTL:
            return cached[1]

    scraper = await get_spotify_scraper()
    method = getattr(scraper, CHART_METHODS[chart_type])
    markets = list(Countries)
    semaphore = asyncio.Semaphore(2)
    history_saved_regions = 0
    history_storage_available = True

    async def fetch_market(country) -> tuple[Any, dict[str, Any] | None, bool]:
        nonlocal history_storage_available
        async with semaphore:
            try:
                payload = await method(country.value, "latest")
            except (httpx.HTTPError, ValueError):
                return country, None, False

            entries = payload.get("entries") if isinstance(payload, dict) else None
            if not isinstance(entries, list):
                return country, None, False

            saved = False
            try:
                history = await prepare_chart_history(chart_type, country.value, "latest", payload, None)
                saved = bool(history.get("supported"))
            except (ChartHistoryStorageError, HTTPException):
                history_storage_available = False
            return country, payload, saved

    results = await asyncio.gather(*(fetch_market(country) for country in markets))
    releases: dict[str, dict[str, Any]] = {}
    regions: list[dict[str, Any]] = []
    failed_regions: list[str] = []

    for country, payload, saved in results:
        if payload is None:
            failed_regions.append(COUNTRY_NAMES.get(country.value, country.value))
            continue
        if saved:
            history_saved_regions += 1

        display_chart = payload.get("displayChart") or {}
        chart_date = str(display_chart.get("date") or "Latest")
        region_items = []
        for entry in payload.get("entries") or []:
            if not isinstance(entry, dict) or not focused_artist_matches(entry, entity_type):
                continue
            item = focused_chart_item(entry, country.value, chart_date)
            if not item or not item["entityUri"]:
                continue
            region_items.append(item)
            release = releases.setdefault(item["entityUri"], {
                "entityUri": item["entityUri"],
                "entityType": item["entityType"],
                "entityName": item["entityName"],
                "artistNames": item["artistNames"],
                "imageUrl": item["imageUrl"],
                "regions": [],
            })
            release["regions"].append({key: value for key, value in item.items() if key not in {"entityUri", "entityType", "entityName", "artistNames", "imageUrl"}})

        if region_items:
            region_items.sort(key=lambda item: item["chartRank"] or 10_000)
            regions.append({
                "regionCode": country.value,
                "regionName": COUNTRY_NAMES.get(country.value, country.value),
                "chartDate": chart_date,
                "entries": region_items,
            })

    if not regions and failed_regions:
        raise HTTPException(status_code=502, detail="Spotify could not load any regional charts. Try again shortly.")

    regions.sort(key=lambda region: (region["regionCode"] != "GLOBAL", region["regionName"]))
    release_items = list(releases.values())
    release_items.sort(key=lambda release: (-len(release["regions"]), release["entityName"].casefold()))
    response = {
        "artist": {"entityUri": FOCUSED_ARTIST_URI, "entityName": FOCUSED_ARTIST_NAME},
        "cadence": cadence,
        "entityType": entity_type,
        "chartType": chart_type,
        "marketsScanned": len(markets) - len(failed_regions),
        "matchingRegions": len(regions),
        "failedRegions": failed_regions,
        "historySavedRegions": history_saved_regions,
        "historyStorageAvailable": history_storage_available,
        "releases": release_items,
        "regions": regions,
    }
    async with focused_chart_cache_lock:
        focused_chart_cache[cache_key] = (time.monotonic(), response)
    return response


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


def focused_history_filter_sql() -> str:
    return """(($2 = 'artist' AND track_uri = $5) OR ($2 IN ('track', 'album') AND $6 = ANY(string_to_array(COALESCE(artist_names, ''), ', '))))"""


def history_point(row) -> dict[str, Any]:
    country_code = row["country_code"]
    return {
        "entityUri": row["track_uri"],
        "entityType": row["entity_type"],
        "entityName": row["track_name"],
        "artistNames": row["artist_names"],
        "regionCode": country_code,
        "regionName": COUNTRY_NAMES.get(country_code, country_code),
        "chartDate": row["chart_date"].isoformat(),
        "chartRank": row["chart_rank"],
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
            "imageUrl": None,
            "regions": [],
        })
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


@app.get("/api/focused/history")
async def focused_history(
    cadence: Literal["daily", "weekly"] = Query("daily"),
    entity_type: Literal["track", "artist", "album"] = Query("track"),
    range: str = Query("90", min_length=1, max_length=10),
) -> dict[str, Any]:
    chart_type = f"{cadence}_top_{ {'track': 'songs', 'artist': 'artists', 'album': 'albums'}[entity_type] }"
    start_date = history_start_date(range)
    market_codes = [country.value for country in Countries]
    try:
        pool = await get_history_pool()
        rows = await pool.fetch(
            f"""
            SELECT DISTINCT ON (country_code, track_uri)
                track_uri, entity_type, track_name, artist_names, country_code, chart_date, chart_rank, stream_count
            FROM public.spotify_chart_snapshots
            WHERE chart_type = $1 AND entity_type = $2 AND country_code = ANY($3::text[])
              AND ($4::date IS NULL OR chart_date >= $4::date)
              AND {focused_history_filter_sql()}
            ORDER BY country_code, track_uri, chart_date DESC, fetched_at DESC
            """,
            chart_type,
            entity_type,
            market_codes,
            start_date,
            FOCUSED_ARTIST_URI,
            FOCUSED_ARTIST_NAME,
        )
    except Exception as error:
        raise HTTPException(status_code=503, detail="Taylor Swift chart history is temporarily unavailable.") from error

    points = [history_point(row) for row in rows]
    releases, regions = group_history_points(points)
    return {
        "artist": {"entityUri": FOCUSED_ARTIST_URI, "entityName": FOCUSED_ARTIST_NAME},
        "cadence": cadence,
        "entityType": entity_type,
        "chartType": chart_type,
        "range": range,
        "snapshotRows": len(points),
        "releases": releases,
        "regions": regions,
    }


@app.get("/api/focused/history/trend")
async def focused_history_trend(
    entity_uri: str = Query(..., min_length=16, max_length=128),
    cadence: Literal["daily", "weekly"] = Query("daily"),
    entity_type: Literal["track", "artist", "album"] = Query("track"),
    range: str = Query("90", min_length=1, max_length=10),
) -> dict[str, Any]:
    if not is_valid_entity_uri(entity_uri, entity_type):
        raise HTTPException(status_code=400, detail="Choose a valid chart release.")
    if entity_type == "artist" and entity_uri != FOCUSED_ARTIST_URI:
        raise HTTPException(status_code=404, detail="That artist is outside the focused chart selection.")

    chart_type = f"{cadence}_top_{ {'track': 'songs', 'artist': 'artists', 'album': 'albums'}[entity_type] }"
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
              AND (($2 = 'artist' AND $3 = $6) OR ($2 IN ('track', 'album') AND $7 = ANY(string_to_array(COALESCE(artist_names, ''), ', '))))
            ORDER BY country_code ASC, chart_date ASC
            """,
            chart_type,
            entity_type,
            entity_uri,
            market_codes,
            start_date,
            FOCUSED_ARTIST_URI,
            FOCUSED_ARTIST_NAME,
        )
    except Exception as error:
        raise HTTPException(status_code=503, detail="Taylor Swift chart history is temporarily unavailable.") from error

    points = [history_point(row) for row in rows]
    entity = None if not points else {key: points[0][key] for key in ("entityUri", "entityType", "entityName", "artistNames")}
    return {
        "artist": {"entityUri": FOCUSED_ARTIST_URI, "entityName": FOCUSED_ARTIST_NAME},
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
        refresh_token = token_data.get("refresh_token")
        access_token = None
        access_token_expires_at = 0.0
        if not refresh_token:
            response = RedirectResponse("/?auth=error", status_code=303)
            response.delete_cookie("spotify_oauth_state", path="/oauth/callback")
            return response
    except (httpx.HTTPError, ValueError):
        response = RedirectResponse("/?auth=error", status_code=303)
        response.delete_cookie("spotify_oauth_state", path="/oauth/callback")
        return response

    response = RedirectResponse("/?auth=connected", status_code=303)
    response.delete_cookie("spotify_oauth_state", path="/oauth/callback")
    return response


@app.post("/api/disconnect")
async def disconnect() -> dict[str, bool]:
    global refresh_token, access_token, access_token_expires_at
    refresh_token = None
    access_token = None
    access_token_expires_at = 0.0
    return {"connected": False}


@app.get("/api/charts")
async def chart(
    chart_type: str = Query(..., alias="type"),
    country: str = Query("GLOBAL", min_length=2, max_length=6),
    date: str = Query("latest", min_length=1, max_length=10),
) -> dict[str, Any]:
    if chart_type not in CHART_METHODS:
        raise HTTPException(status_code=400, detail="Choose a supported chart type.")
    if country.upper() not in {item.value for item in Countries}:
        raise HTTPException(status_code=400, detail="Choose a country from the list.")
    if date != "latest":
        try:
            calendar_date.fromisoformat(date)
        except ValueError as error:
            raise HTTPException(status_code=400, detail="Date must be latest or YYYY-MM-DD.") from error

    scraper = await get_spotify_scraper()
    try:
        result = await getattr(scraper, CHART_METHODS[chart_type])(country.upper(), date)
    except KeyError as error:
        raise HTTPException(status_code=502, detail="Spotify returned an unexpected chart response.") from error
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

    if not isinstance(result, dict) or not isinstance(result.get("entries"), list):
        raise HTTPException(status_code=502, detail="Spotify returned an unsupported chart format.")

    if chart_type not in HISTORY_CHART_TYPES:
        result["chartHistory"] = {
            "supported": False,
            "complete": False,
            "status": "unsupported",
            "reason": "History is available for daily and weekly Top Songs and Top Artists charts.",
        }
        return result

    try:
        history_info = await prepare_chart_history(chart_type, country.upper(), date, result, None)
        history_info["metric"] = "rank" if HISTORY_CHART_TYPES[chart_type] == "artist" else "streams"
        result["chartHistory"] = history_info
    except ChartHistoryStorageError:
        result["chartHistory"] = {
            "supported": False,
            "complete": False,
            "status": "unavailable",
            "reason": "Historical storage is unavailable; snapshots could not be saved.",
        }
    except HTTPException as error:
        if error.status_code != 503:
            raise
        result["chartHistory"] = {
            "supported": False,
            "complete": False,
            "status": "unavailable",
            "reason": "Historical storage is unavailable; snapshots could not be saved.",
        }
    return result


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
    if any(not is_valid_entity_uri(uri, HISTORY_CHART_TYPES[chart_type]) for uri in entity_uris):
        raise HTTPException(status_code=400, detail="The chart entity list is invalid.")

    try:
        return await run_history_backfill_batch(
            chart_type,
            country,
            request.target_date,
            request.start_date,
            entity_uris,
            get_spotify_scraper,
        )
    except ChartHistoryStorageError as error:
        raise HTTPException(status_code=503, detail="Historical storage is unavailable.") from error
