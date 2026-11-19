import unittest
from unittest.mock import AsyncMock, patch

import httpx

import spotify_charts


class FakeAsyncClient:
    def __init__(self, responses):
        self.responses = iter(responses)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    async def get(self, *_args, **_kwargs):
        return next(self.responses)


class SpotifyChartRateLimitTests(unittest.IsolatedAsyncioTestCase):
    async def test_chart_request_slots_are_two_seconds_apart(self):
        previous_slot = spotify_charts._next_chart_request_at
        spotify_charts._next_chart_request_at = 0.0
        sleep = AsyncMock()

        try:
            with patch.object(spotify_charts, "monotonic", side_effect=[100.0, 100.25]), \
                 patch.object(spotify_charts.asyncio, "sleep", new=sleep):
                await spotify_charts._wait_for_chart_request_slot()
                await spotify_charts._wait_for_chart_request_slot()

            self.assertEqual(spotify_charts.CHART_REQUEST_INTERVAL, 2.0)
            sleep.assert_awaited_once_with(1.75)
        finally:
            spotify_charts._next_chart_request_at = previous_slot

    async def test_429_without_retry_after_applies_shared_cooldown(self):
        url = "https://charts-spotify-com-service.spotify.com/test-rate-limit-fallback"
        request = httpx.Request("GET", url)
        responses = [
            httpx.Response(429, text="too many requests", request=request),
            httpx.Response(200, json={"entries": []}, request=request),
        ]
        client = spotify_charts.SpotifyCharts(access_token="test-token")

        with patch.object(spotify_charts, "AsyncClient", return_value=FakeAsyncClient(responses)), \
             patch.object(spotify_charts, "_wait_for_chart_request_slot", new=AsyncMock()), \
             patch.object(spotify_charts, "_defer_chart_requests", new=AsyncMock()) as defer_requests, \
             patch.object(spotify_charts.asyncio, "sleep", new=AsyncMock()):
            payload = await client._fetch_chart_uncached(url)

        self.assertEqual(payload, {"entries": []})
        self.assertEqual(defer_requests.await_args.args, (spotify_charts.CHART_RATE_LIMIT_DEFAULT_COOLDOWN,))
        self.assertEqual(client._request_diagnostics[url]["attemptCount"], 2)
        self.assertEqual(client._request_diagnostics[url]["httpStatus"], 200)
        self.assertTrue(client._request_diagnostics[url]["rateLimited"])
        self.assertEqual(client._request_diagnostics[url]["retryDelaySeconds"], spotify_charts.CHART_RATE_LIMIT_DEFAULT_COOLDOWN)


if __name__ == "__main__":
    unittest.main()
