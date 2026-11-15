import json
import unittest
from unittest.mock import AsyncMock, patch

import httpx

import app


ARTIST_URI = "spotify:artist:0123456789abcdef"
TRACK_URI = "spotify:track:0123456789abcdef"
ALBUM_URI = "spotify:album:0123456789abcdef"


def track_entry(track_uri=TRACK_URI, artist_uri=ARTIST_URI):
    return {
        "trackMetadata": {
            "trackUri": track_uri,
            "trackName": "Example song",
            "artists": [{"spotifyUri": artist_uri, "name": "Saved artist"}],
        },
        "chartEntryData": {"currentRank": 1, "rankingMetric": {"type": "STREAMS", "value": "100"}},
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


class LiveSelectionTests(unittest.TestCase):
    def test_songs_match_selected_artist_ids_and_individual_track_ids(self):
        entry = track_entry()
        self.assertTrue(app.live_entry_matches(entry, "daily_top_songs", {ARTIST_URI}, set()))
        self.assertTrue(app.live_entry_matches(entry, "daily_top_songs", set(), {TRACK_URI}))
        self.assertFalse(app.live_entry_matches(entry, "daily_top_songs", set(), {"spotify:track:fedcba9876543210"}))

    def test_artist_chart_matches_saved_artist_uri_only(self):
        entry = {"artistMetadata": {"artistUri": ARTIST_URI, "artistName": "Any name"}}
        self.assertTrue(app.live_entry_matches(entry, "weekly_top_artists", {ARTIST_URI}, set()))
        self.assertFalse(app.live_entry_matches(entry, "weekly_top_artists", set(), {ARTIST_URI}))

    def test_album_match_requires_artist_ids_not_name_similarity(self):
        selected = {ARTIST_URI}
        matched = {"albumMetadata": {"albumUri": ALBUM_URI, "artists": [{"spotifyUri": ARTIST_URI, "name": "Different spelling"}]}}
        same_name_only = {"albumMetadata": {"albumUri": ALBUM_URI, "artists": [{"name": "Saved artist"}]}}
        self.assertTrue(app.live_entry_matches(matched, "weekly_top_albums", selected, set()))
        self.assertFalse(app.live_entry_matches(same_name_only, "weekly_top_albums", selected, set()))

    def test_market_failures_are_isolated_and_diagnostics_are_returned(self):
        async def run():
            class Scraper:
                async def daily_top_songs(self, country, _date):
                    if country == "US":
                        response = httpx.Response(503, text="regional upstream unavailable", request=httpx.Request("GET", "https://charts.example/US"))
                        raise httpx.HTTPStatusError("service unavailable", request=response.request, response=response)
                    if country == "GLOBAL":
                        return {"displayChart": {"date": "2026-11-14"}, "entries": [track_entry()]}
                    return {"displayChart": {"date": "2026-11-14"}, "entries": []}

                def get_chart_diagnostics(self, _chart_type, country, _date):
                    return {"attemptCount": 3 if country == "US" else 1, "httpStatus": 200, "timeout": False, "rateLimited": False}

            selections = [{"entity_uri": ARTIST_URI, "entity_type": "artist"}]
            with patch.object(app, "Countries", FakeMarkets), \
                 patch.object(app, "read_selected_entities", new=AsyncMock(return_value=selections)), \
                 patch.object(app, "get_spotify_scraper", new=AsyncMock(return_value=Scraper())), \
                 patch.object(app, "get_history_pool", new=AsyncMock(side_effect=AssertionError("Live must not access History storage"))) as history_pool:
                result = await app.scan_focused_live("daily", "track", "all", "daily_top_songs")
                history_pool.assert_not_awaited()

            self.assertEqual(result["marketsTotal"], 3)
            self.assertEqual(result["marketsScanned"], 2)
            self.assertEqual(result["failedRegions"], ["Usa"])
            self.assertEqual(result["releases"][0]["entityUri"], TRACK_URI)
            failure = next(item for item in result["marketDiagnostics"] if item["countryCode"] == "US")
            self.assertEqual(failure["chartType"], "daily_top_songs")
            self.assertEqual(failure["httpStatus"], 503)
            self.assertEqual(failure["attemptCount"], 3)
            self.assertIn("regional upstream unavailable", failure["errorResponse"])

        import asyncio
        asyncio.run(run())

    def test_partial_scan_persists_diagnostics_for_recovery(self):
        async def run():
            response = {
                "marketScope": "all",
                "marketsTotal": 2,
                "marketsScanned": 1,
                "matchingRegions": 1,
                "failedRegions": ["Usa"],
                "marketDiagnostics": [
                    {"countryCode": "CA", "status": "succeeded"},
                    {"countryCode": "US", "status": "failed", "httpStatus": 429},
                ],
                "regions": [{"regionCode": "CA", "entries": []}],
                "releases": [],
                "scanInProgress": False,
            }
            class FakePool:
                execute = AsyncMock()

            pool = FakePool()
            key = ("weekly", "album", "all", "weekly_top_albums", "test-fingerprint")
            with patch.object(app, "scan_focused_live", new=AsyncMock(return_value=response)), \
                 patch.object(app, "get_history_pool", new=AsyncMock(return_value=pool)):
                await app.run_focused_scan_and_cache(key, "weekly", "album", "all", "weekly_top_albums")

            sql, cadence, entity_type, status, completed, failed, payload, last_error = pool.execute.await_args.args
            self.assertIn("completed_markets=$4", sql)
            self.assertEqual((cadence, entity_type, status), ("weekly", "album", "partial"))
            self.assertEqual(completed, ["CA"])
            self.assertEqual(json.loads(failed), [response["marketDiagnostics"][1]])
            self.assertEqual(json.loads(payload), response)
            self.assertEqual(last_error, "one_or_more_markets_failed")
            self.assertEqual(app.focused_chart_cache[key][1], response)
            app.focused_chart_cache.pop(key, None)

        import asyncio
        asyncio.run(run())

    def test_chart_explorer_is_browse_only(self):
        async def run():
            payload = {"entries": [{"trackMetadata": {"trackUri": TRACK_URI}}, {"trackMetadata": {"trackUri": "spotify:track:fedcba9876543210"}}]}
            with patch.object(app, "get_history_pool", new=AsyncMock(side_effect=AssertionError("Explorer must not access History storage"))) as history_pool:
                result = await app.decorate_chart(payload, "daily_top_songs", "GLOBAL", "latest")
                history_pool.assert_not_awaited()
            self.assertEqual(len(result["entries"]), 2)
            self.assertFalse(result["chartHistory"]["supported"])
            self.assertEqual(result["chartHistory"]["status"], "browsing_only")

        import asyncio
        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
