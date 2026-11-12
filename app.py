from __future__ import annotations

import secrets
from datetime import date as calendar_date
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import httpx
from fastapi import Cookie, FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from Countries import Countries
from Settings import SpotifySettings
from spotify_charts import SpotifyCharts


app = FastAPI(title="Spotify Charts", docs_url=None, redoc_url=None)
settings = SpotifySettings()
refresh_token: str | None = None

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
    global refresh_token

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
    global refresh_token
    refresh_token = None
    return {"connected": False}


@app.get("/api/charts")
async def chart(
    chart_type: str = Query(..., alias="type"),
    country: str = Query("GLOBAL", min_length=2, max_length=6),
    date: str = Query("latest", min_length=1, max_length=10),
) -> dict[str, Any]:
    global refresh_token

    if chart_type not in CHART_METHODS:
        raise HTTPException(status_code=400, detail="Choose a supported chart type.")
    if country.upper() not in {item.value for item in Countries}:
        raise HTTPException(status_code=400, detail="Choose a country from the list.")
    if date != "latest":
        try:
            calendar_date.fromisoformat(date)
        except ValueError as error:
            raise HTTPException(status_code=400, detail="Date must be latest or YYYY-MM-DD.") from error
    if not refresh_token:
        raise HTTPException(status_code=401, detail="Connect Spotify before loading charts.")

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.post(
                "https://accounts.spotify.com/api/token",
                data={"grant_type": "refresh_token", "refresh_token": refresh_token},
                auth=(settings.client_id or "", settings.client_secret or ""),
            )
            response.raise_for_status()
            token_data = response.json()
        refresh_token = token_data.get("refresh_token", refresh_token)
        access_token = token_data["access_token"]
        scraper = SpotifyCharts(access_token=access_token)
        result = await getattr(scraper, CHART_METHODS[chart_type])(country.upper(), date)
    except KeyError as error:
        raise HTTPException(status_code=502, detail="Spotify returned an unexpected chart response.") from error
    except (httpx.HTTPError, ValueError) as error:
        raise HTTPException(status_code=502, detail="Spotify could not return this chart. Try again shortly.") from error

    if not isinstance(result, dict) or not isinstance(result.get("entries"), list):
        raise HTTPException(status_code=502, detail="Spotify returned an unsupported chart format.")
    return result
