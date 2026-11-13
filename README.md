# Spotify Charts Scraper

A Python client and browser dashboard for exploring Spotify Charts by country, chart type, and date.

## Requirements

- Python 3.10 or newer
- A Spotify Developer app with a client ID and client secret

## Run the dashboard

1. Create an isolated environment and install dependencies with `python -m venv .venv` followed by `.venv/bin/python -m pip install -r requirements.txt`.
2. Copy `example.env` to `.env` and set `SPOTIFY_CLIENT_ID`, `SPOTIFY_CLIENT_SECRET`, and `SPOTIFY_REDIRECT_URI`.
3. In your Spotify Developer app, register the exact callback URI shown by the dashboard at `/oauth/callback`.
4. Start the browser app with `.venv/bin/python -m uvicorn app:app --host 0.0.0.0 --port 8000` and open it in a browser.
5. Connect Spotify, then choose a market and chart to load the live rankings.

Keep client secrets in environment variables or an untracked `.env` file. Do not commit credentials. The dashboard stores the Spotify refresh token in the connected Neon database and refreshes short-lived access tokens automatically, so normal app restarts do not require reconnecting. The database must be available for the dashboard to restore and refresh authorization; reconnect only if Spotify authorization is revoked or the stored token becomes invalid.

## Python client

The existing asynchronous client remains available for notebooks and scripts:

```python
from spotify_charts import SpotifyCharts

charts = SpotifyCharts()
```

The local client uses the legacy `CLIENT_ID`, `CLIENT_SECRET`, `REDIRECT_URL`, and `REDIRECT_PORT` environment names and `.refresh_token` file. For server-side integrations, pass a current access token with `SpotifyCharts(access_token=...)`.

## Available charts

- Daily top songs
- Daily top artists
- Daily viral songs
- Weekly top songs
- Weekly top artists
- Weekly top albums

## Historical chart streams

Cumulative chart streams are opt-in. By default, no songs are configured and the dashboard does not save chart snapshots. After specific Spotify track URIs are added to `TRACK_HISTORY_TRACK_URIS` in `app.py`, the app stores deduplicated snapshots and resumable backfill progress in Neon through `DATABASE_URL` for those songs only. It sums `STREAMS` values only on dates each song appeared in the selected market and chart; these are chart-period streams, not lifetime Spotify plays. The Neon schema must include `spotify_chart_snapshots` and `spotify_chart_backfills`.

The chart service URLs used by this project are not part of Spotify's public Web API contract and may change. The notebook in `Testing.ipynb` contains an older example response; it is not live data.

Copyright © [Henry Richard J](https://github.com/henry-richard7)
