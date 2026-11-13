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
}


class ChartHistoryBackfillRequest(BaseModel):
    chart_type: str
    country: str
    target_date: calendar_date
    entity_uris: list[str] = Field(max_length=200)


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
        raise HTTPException(status_code=401, detail="Connect Spotify before using your saved selection.")


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
        and parts[1] in {"track", "artist"}
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
    limit: int = Query(12, ge=1, le=20),
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

    selected = await read_selected_entities()
    grouped = {
        entity_type: [row for row in selected if row["entity_type"] == entity_type]
        for entity_type in ("track", "artist")
    }
    if not selected:
        return {"country": country, "cadence": cadence, "songs": [], "artists": [], "history": {}}

    scraper = await get_spotify_scraper()
    response: dict[str, Any] = {"country": country, "cadence": cadence, "songs": [], "artists": [], "history": {}}
    for entity_type, entities in grouped.items():
        if not entities:
            continue
        chart_type = f"{cadence}_top_{'songs' if entity_type == 'track' else 'artists'}"
        try:
            payload = await getattr(scraper, CHART_METHODS[chart_type])(country, "latest")
        except httpx.HTTPStatusError as error:
            if error.response.status_code == 429:
                raise HTTPException(status_code=429, detail="Spotify rate-limited this chart. Try again shortly.") from error
            raise HTTPException(status_code=502, detail="Spotify could not return this chart. Try again shortly.") from error
        except (httpx.HTTPError, ValueError) as error:
            raise HTTPException(status_code=502, detail="Spotify could not return this chart. Try again shortly.") from error

        entries = payload.get("entries") if isinstance(payload, dict) else None
        if not isinstance(entries, list):
            raise HTTPException(status_code=502, detail="Spotify returned an unsupported chart format.")
        entry_by_uri = {
            uri: entry
            for entry in entries
            if isinstance(entry, dict) and (uri := entry_entity_uri(entry)) is not None
        }
        present_entries = [entry_by_uri[row["entity_uri"]] for row in entities if row["entity_uri"] in entry_by_uri]
        response["history"][entity_type] = {"status": "not_started"}
        if present_entries:
            try:
                response["history"][entity_type] = await prepare_chart_history(
                    chart_type,
                    country,
                    "latest",
                    {**payload, "entries": present_entries},
                    {row["entity_uri"] for row in entities},
                )
            except ChartHistoryStorageError:
                response["history"][entity_type] = {"status": "unavailable", "reason": "History storage is unavailable."}

        output_key = "songs" if entity_type == "track" else "artists"
        for selection in entities:
            entry = entry_by_uri.get(selection["entity_uri"])
            stats = (entry or {}).get("chartEntryData") or {}
            metric = stats.get("rankingMetric") or {}
            response[output_key].append({
                **selection_response(selection),
                "inChart": entry is not None,
                "chartRank": stats.get("currentRank"),
                "previousRank": stats.get("previousRank"),
                "peakRank": stats.get("peakRank"),
                "metricType": metric.get("type"),
                "metricValue": metric.get("value"),
                "chartDate": (payload.get("displayChart") or {}).get("date", "Latest"),
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
    selected = await read_selected_entities(entity_type)
    available = {row["entity_uri"]: row for row in selected}
    if entity_uri and entity_uri not in available:
        raise HTTPException(status_code=404, detail="Select an entity from your saved watchlist.")
    current_uri = entity_uri or (selected[0]["entity_uri"] if selected else None)
    current = available.get(current_uri) if current_uri else None
    if current is None:
        return {"chartType": chart_type, "country": country, "entity": None, "points": []}

    try:
        pool = await get_history_pool()
        rows = await pool.fetch(
            """
            SELECT chart_date, chart_rank, stream_count
            FROM public.spotify_chart_snapshots
            WHERE chart_type = $1 AND country_code = $2 AND entity_type = $3
              AND track_uri = $4 AND ($5::date IS NULL OR chart_date >= $5::date)
            ORDER BY chart_date ASC
            """,
            chart_type,
            country,
            entity_type,
            current_uri,
            start_date,
        )
    except Exception as error:
        raise HTTPException(status_code=503, detail="Chart history is temporarily unavailable.") from error

    return {
        "chartType": chart_type,
        "country": country,
        "entity": selection_response(current),
        "points": [
            {
                "chartDate": row["chart_date"].isoformat(),
                "chartRank": row["chart_rank"],
                "streamCount": row["stream_count"],
            }
            for row in rows
        ],
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
    except (httpx.HTTPError, ValueError) as error:
        raise HTTPException(status_code=502, detail="Spotify could not return this chart. Try again shortly.") from error

    if not isinstance(result, dict) or not isinstance(result.get("entries"), list):
        raise HTTPException(status_code=502, detail="Spotify returned an unsupported chart format.")

    if chart_type not in HISTORY_CHART_TYPES:
        result["chartHistory"] = {
            "supported": False,
            "complete": False,
            "status": "unsupported",
            "reason": "History is available for selected Top Songs and Top Artists charts.",
        }
        return result

    try:
        selected_entities = await read_selected_entities(HISTORY_CHART_TYPES[chart_type])
        selected_uris = {row["entity_uri"] for row in selected_entities}
        if not selected_uris:
            result["chartHistory"] = {
                "supported": False,
                "complete": False,
                "status": "unconfigured",
                "reason": "Add songs or artists to your selection to collect chart history.",
            }
            return result

        selected_entries = [entry for entry in result["entries"] if entry_entity_uri(entry) in selected_uris]
        if not selected_entries:
            result["chartHistory"] = {
                "supported": False,
                "complete": False,
                "status": "unavailable",
                "reason": "None of your selected entities appears in this chart.",
            }
        else:
            history_info = await prepare_chart_history(
                chart_type,
                country.upper(),
                date,
                {**result, "entries": selected_entries},
                selected_uris,
            )
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

    selected_entities = await read_selected_entities(HISTORY_CHART_TYPES[chart_type])
    selected_uris = {row["entity_uri"] for row in selected_entities}
    if not selected_uris:
        raise HTTPException(status_code=409, detail="Add songs or artists to your selection before backfilling.")

    entity_uris = list(dict.fromkeys(request.entity_uris))
    if len(entity_uris) > 200 or any(
        not is_valid_entity_uri(uri, HISTORY_CHART_TYPES[chart_type]) or uri not in selected_uris
        for uri in entity_uris
    ):
        raise HTTPException(status_code=400, detail="The selected entity list is invalid.")

    try:
        return await run_history_backfill_batch(
            chart_type,
            country,
            request.target_date,
            entity_uris,
            get_spotify_scraper,
        )
    except ChartHistoryStorageError as error:
        raise HTTPException(status_code=503, detail="Historical storage is unavailable.") from error
