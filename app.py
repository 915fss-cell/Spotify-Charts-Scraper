from __future__ import annotations

import asyncio
import secrets
import time
from contextlib import asynccontextmanager
from datetime import date as calendar_date
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import httpx
from fastapi import Cookie, FastAPI, HTTPException, Query, Request
from pydantic import BaseModel, Field
from fastapi.responses import HTMLResponse, RedirectResponse

from chart_history import (
    ChartHistoryStorageError,
    close_history_pool,
    entry_track_uri,
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

SONG_CHART_STEPS = {"daily_top_songs": 1, "weekly_top_songs": 7}
# Add Spotify track URIs here only after the user identifies songs to track.
TRACK_HISTORY_TRACK_URIS: frozenset[str] = frozenset()


class ChartHistoryBackfillRequest(BaseModel):
    chart_type: str
    country: str
    target_date: calendar_date
    track_uris: list[str] = Field(max_length=200)


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

    if chart_type not in SONG_CHART_STEPS:
        result["chartHistory"] = {
            "supported": False,
            "complete": False,
            "status": "unsupported",
            "reason": "Cumulative chart streams are available for Top Songs charts only.",
        }
    elif not TRACK_HISTORY_TRACK_URIS:
        result["chartHistory"] = {
            "supported": False,
            "complete": False,
            "status": "unconfigured",
            "reason": "Cumulative totals are opt-in; no songs are configured for tracking yet.",
        }
    else:
        selected_entries = [
            entry for entry in result["entries"] if entry_track_uri(entry) in TRACK_HISTORY_TRACK_URIS
        ]
        if not selected_entries:
            result["chartHistory"] = {
                "supported": False,
                "complete": False,
                "status": "unavailable",
                "reason": "None of the configured songs appears in this chart selection.",
            }
        else:
            try:
                selected_payload = {**result, "entries": selected_entries}
                result["chartHistory"] = await prepare_chart_history(
                    chart_type,
                    country.upper(),
                    date,
                    selected_payload,
                    set(TRACK_HISTORY_TRACK_URIS),
                )
            except ChartHistoryStorageError:
                result["chartHistory"] = {
                    "supported": False,
                    "complete": False,
                    "status": "unavailable",
                    "reason": "Historical storage is unavailable; cumulative totals cannot be calculated right now.",
                }
    return result


@app.post("/api/chart-history/backfill")
async def chart_history_backfill(request: ChartHistoryBackfillRequest) -> dict[str, Any]:
    chart_type = request.chart_type
    country = request.country.upper()
    if chart_type not in SONG_CHART_STEPS:
        raise HTTPException(status_code=400, detail="Choose a Top Songs chart to backfill stream history.")
    if country not in {item.value for item in Countries}:
        raise HTTPException(status_code=400, detail="Choose a country from the list.")

    if not TRACK_HISTORY_TRACK_URIS:
        raise HTTPException(status_code=409, detail="No songs are configured for history tracking yet.")

    track_uris = list(dict.fromkeys(request.track_uris))
    if len(track_uris) > 200 or any(
        not uri.startswith("spotify:track:") or len(uri) > 128 or uri not in TRACK_HISTORY_TRACK_URIS
        for uri in track_uris
    ):
        raise HTTPException(status_code=400, detail="The chart track list is invalid.")

    try:
        return await run_history_backfill_batch(
            chart_type,
            country,
            request.target_date,
            track_uris,
            get_spotify_scraper,
        )
    except ChartHistoryStorageError as error:
        raise HTTPException(status_code=503, detail="Historical storage is unavailable.") from error
