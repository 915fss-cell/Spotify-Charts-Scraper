import asyncio
import unittest
from unittest.mock import AsyncMock, patch

import app


TRACK_URI = "spotify:track:0123456789abcdef"
ARTIST_URI = "spotify:artist:0123456789abcdef"


def track_entry(country):
    return {
        "trackMetadata": {
            "trackUri": TRACK_URI,
            "trackName": f"Song in {country}",
            "artists": [{"spotifyUri": ARTIST_URI, "name": "Saved artist"}],
        },
        "chartEntryData": {
            "currentRank": 1,
            "rankingMetric": {"type": "STREAMS", "value": "123"},
        },
    }


class FakeMarket:
    def __init__(self, name, value):
        self.name = name
        self.value = value


class FakeMarketsMeta(type):
    def __iter__(cls):
        return iter([cls.GLOBAL, cls.USA, cls.CANADA])


class FakeMarkets(metaclass=FakeMarketsMeta):
    GLOBAL = FakeMarket("GLOBAL", "GLOBAL")
    USA = FakeMarket("USA", "US")
    CANADA = FakeMarket("CANADA", "CA")


class LiveIncrementalTests(unittest.IsolatedAsyncioTestCase):
    async def test_cached_markets_return_without_waiting_for_missing_markets(self):
        selections = [{"entity_uri": TRACK_URI, "entity_type": "track"}]
        cached = {
            "payload": {"displayChart": {"date": "2026-11-21"}, "entries": [track_entry("GLOBAL")]},
            "fetchedAt": None,
        }
        with patch.object(app, "Countries", FakeMarkets), \
             patch.object(app, "read_selected_entities", new=AsyncMock(return_value=selections)), \
             patch.object(app, "get_spotify_scraper", new=AsyncMock(side_effect=AssertionError("Cached-only reads must not call Spotify"))):
            result = await app.scan_focused_live(
                "daily", "track", "all", "daily_top_songs",
                cached_payloads={"GLOBAL": cached}, fetch_missing=False,
            )

        self.assertEqual(result["marketsScanned"], 1)
        self.assertEqual(result["marketsTotal"], 3)
        self.assertEqual(result["pendingMarkets"], ["US", "CA"])
        self.assertTrue(result["scanInProgress"])
        self.assertEqual(result["regions"][0]["regionCode"], "GLOBAL")

    async def test_scanner_emits_partial_results_as_markets_finish(self):
        selections = [{"entity_uri": TRACK_URI, "entity_type": "track"}]
        cached = {
            "payload": {"displayChart": {"date": "2026-11-21"}, "entries": [track_entry("GLOBAL")]},
            "fetchedAt": None,
        }
        callbacks = []

        class Scraper:
            async def daily_top_songs(self, country, _date):
                await asyncio.sleep(0.04 if country == "US" else 0.08)
                return {"displayChart": {"date": "2026-11-21"}, "entries": [track_entry(country)]}

            def get_chart_diagnostics(self, _chart_type, _country, _date):
                return {"attemptCount": 1, "httpStatus": 200, "timeout": False, "rateLimited": False}

        async def on_market_complete(payload):
            callbacks.append(payload)

        with patch.object(app, "Countries", FakeMarkets), \
             patch.object(app, "read_selected_entities", new=AsyncMock(return_value=selections)), \
             patch.object(app, "get_spotify_scraper", new=AsyncMock(return_value=Scraper())):
            result = await app.scan_focused_live(
                "daily", "track", "all", "daily_top_songs",
                cached_payloads={"GLOBAL": cached}, on_market_complete=on_market_complete,
            )

        self.assertTrue(callbacks)
        self.assertEqual(callbacks[0]["marketsScanned"], 1)
        self.assertTrue(callbacks[0]["scanInProgress"])
        self.assertEqual(callbacks[0]["regions"][0]["regionCode"], "GLOBAL")
        self.assertEqual(result["marketsScanned"], 3)
        self.assertFalse(result["scanInProgress"])

    async def test_resumed_scan_fetches_only_missing_markets_and_keeps_saved_results(self):
        fingerprint = "selection-fingerprint"
        cache_key = ("daily", "track", "all", "daily_top_songs", fingerprint)
        initial = {
            "marketScope": "all",
            "marketsTotal": 3,
            "marketsScanned": 2,
            "marketDiagnostics": [
                {"countryCode": "GLOBAL", "countryName": "Global", "status": "succeeded"},
                {"countryCode": "US", "countryName": "Usa", "status": "succeeded"},
            ],
            "regions": [
                {"regionCode": "GLOBAL", "regionName": "Global", "entries": [
                    {"entityUri": TRACK_URI, "entityType": "track", "entityName": "Song", "artistNames": "Artist", "chartRank": 1}
                ]},
                {"regionCode": "US", "regionName": "Usa", "entries": [
                    {"entityUri": TRACK_URI, "entityType": "track", "entityName": "Song", "artistNames": "Artist", "chartRank": 2}
                ]},
            ],
            "pendingMarkets": ["CA"],
            "scanInProgress": True,
        }
        scanned = {
            "marketScope": "all",
            "marketsTotal": 3,
            "marketsScanned": 1,
            "marketDiagnostics": [{"countryCode": "CA", "countryName": "Canada", "status": "succeeded"}],
            "regions": [{"regionCode": "CA", "regionName": "Canada", "entries": [
                {"entityUri": TRACK_URI, "entityType": "track", "entityName": "Song", "artistNames": "Artist", "chartRank": 3}
            ]}],
            "pendingMarkets": [],
            "scanInProgress": False,
        }
        pool = type("FakePool", (), {"execute": AsyncMock()})()
        async def persist(_cadence, _entity_type, _scope, _chart_type, _fingerprint, response):
            return response

        app.focused_chart_cache.pop(cache_key, None)
        with patch.object(app, "scan_focused_live", new=AsyncMock(return_value=scanned)) as scan, \
             patch.object(app, "get_history_pool", new=AsyncMock(return_value=pool)), \
             patch.object(app, "persist_focused_live_response", new=AsyncMock(side_effect=persist)):
            await app.run_focused_scan_and_cache(
                cache_key, "daily", "track", "all", "daily_top_songs", initial
            )

        self.assertEqual(scan.await_args.kwargs["country_codes"], {"CA"})
        result = app.focused_chart_cache[cache_key][1]
        self.assertEqual({region["regionCode"] for region in result["regions"]}, {"GLOBAL", "US", "CA"})
        self.assertEqual(result["marketsScanned"], 3)
        self.assertFalse(result["scanInProgress"])
        app.focused_chart_cache.pop(cache_key, None)

    def test_new_cached_markets_merge_without_dropping_saved_regions(self):
        saved = {
            "marketScope": "all",
            "marketsTotal": 3,
            "marketDiagnostics": [{"countryCode": "US", "countryName": "Usa", "status": "succeeded"}],
            "regions": [{"regionCode": "US", "regionName": "Usa", "entries": [
                {"entityUri": TRACK_URI, "entityType": "track", "entityName": "Song", "artistNames": "Artist", "chartRank": 3}
            ]}],
            "releases": [],
            "pendingMarkets": ["GLOBAL", "CA"],
            "scanInProgress": True,
        }
        current = {
            "marketScope": "all",
            "marketsTotal": 3,
            "marketDiagnostics": [
                {"countryCode": "GLOBAL", "countryName": "Global", "status": "succeeded"},
                {"countryCode": "US", "countryName": "Usa", "status": "pending"},
                {"countryCode": "CA", "countryName": "Canada", "status": "pending"},
            ],
            "regions": [{"regionCode": "GLOBAL", "regionName": "Global", "entries": [
                {"entityUri": TRACK_URI, "entityType": "track", "entityName": "Song", "artistNames": "Artist", "chartRank": 1}
            ]}],
            "pendingMarkets": ["US", "CA"],
            "scanInProgress": True,
        }

        result = app.merge_focused_live_responses(saved, current)

        self.assertEqual({region["regionCode"] for region in result["regions"]}, {"GLOBAL", "US"})
        self.assertEqual(result["marketsScanned"], 2)
        self.assertEqual(result["pendingMarkets"], ["CA"])
        self.assertTrue(result["scanInProgress"])


if __name__ == "__main__":
    unittest.main()
