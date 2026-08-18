"""Tests for the SSE price streaming endpoint."""

import asyncio
import json

from fastapi import APIRouter

from app.market.cache import PriceCache
from app.market.stream import _generate_events, create_stream_router


class FakeClient:
    def __init__(self, host: str = "127.0.0.1"):
        self.host = host


class FakeRequest:
    """Minimal stand-in for fastapi.Request, driving disconnect timing."""

    def __init__(self, disconnect_after: int | None = 0, client: FakeClient | None = None):
        self.client = client if client is not None else FakeClient()
        self._disconnect_after = disconnect_after
        self._checks = 0

    async def is_disconnected(self) -> bool:
        if self._disconnect_after is None:
            return False
        result = self._checks >= self._disconnect_after
        self._checks += 1
        return result


class TestCreateStreamRouter:
    """Tests for the router factory."""

    def test_returns_api_router(self):
        cache = PriceCache()
        router = create_stream_router(cache)
        assert isinstance(router, APIRouter)

    def test_registers_prices_route(self):
        cache = PriceCache()
        router = create_stream_router(cache)
        paths = {route.path for route in router.routes}
        assert "/api/stream/prices" in paths

    def test_repeated_calls_do_not_share_router(self):
        """Each call must return an independent router with exactly one route.

        Regression test: create_stream_router() used to close over a
        module-level router, so calling it more than once (e.g. once per
        test-created FastAPI app) silently duplicated the /prices route
        registration on a shared object.
        """
        cache = PriceCache()
        router_a = create_stream_router(cache)
        router_b = create_stream_router(cache)

        assert router_a is not router_b
        assert len(router_a.routes) == 1
        assert len(router_b.routes) == 1


class TestGenerateEvents:
    """Tests for the underlying SSE event generator."""

    async def test_yields_retry_directive_first(self):
        cache = PriceCache()
        request = FakeRequest(disconnect_after=0)
        events = [event async for event in _generate_events(cache, request, interval=0.01)]
        assert events[0] == "retry: 1000\n\n"

    async def test_stops_immediately_on_disconnect(self):
        cache = PriceCache()
        request = FakeRequest(disconnect_after=0)
        events = [event async for event in _generate_events(cache, request, interval=0.01)]
        # Only the retry preamble; disconnect is checked before any data is sent.
        assert events == ["retry: 1000\n\n"]

    async def test_sends_data_before_disconnect(self):
        cache = PriceCache()
        cache.update("AAPL", 190.50)
        request = FakeRequest(disconnect_after=1)
        events = [event async for event in _generate_events(cache, request, interval=0.01)]
        data_events = [e for e in events if e.startswith("data: ")]
        assert len(data_events) == 1
        payload = json.loads(data_events[0][len("data: ") : -2])
        assert payload["AAPL"]["price"] == 190.50

    async def test_skips_send_when_cache_empty(self):
        cache = PriceCache()
        request = FakeRequest(disconnect_after=1)
        events = [event async for event in _generate_events(cache, request, interval=0.01)]
        assert all(not e.startswith("data: ") for e in events)

    async def test_does_not_resend_unchanged_version(self):
        cache = PriceCache()
        cache.update("AAPL", 190.50)
        request = FakeRequest(disconnect_after=2)
        events = [event async for event in _generate_events(cache, request, interval=0.01)]
        data_events = [e for e in events if e.startswith("data: ")]
        # Version unchanged between the two loop iterations, so only one send.
        assert len(data_events) == 1

    async def test_resends_on_version_change(self):
        """A cache mutation between loop iterations triggers a second send."""
        cache = PriceCache()
        cache.update("AAPL", 190.50)

        class BumpingRequest(FakeRequest):
            """Bumps the cache on the 2nd disconnect check, disconnects on the 3rd."""

            def __init__(self):
                super().__init__(disconnect_after=None)
                self.call_count = 0

            async def is_disconnected(self) -> bool:
                self.call_count += 1
                if self.call_count == 2:
                    cache.update("AAPL", 191.00)
                return self.call_count >= 3

        request = BumpingRequest()
        events = [event async for event in _generate_events(cache, request, interval=0.01)]
        data_events = [e for e in events if e.startswith("data: ")]
        assert len(data_events) == 2
        prices = [json.loads(e[len("data: ") : -2])["AAPL"]["price"] for e in data_events]
        assert prices == [190.50, 191.00]

    async def test_handles_cancelled_error_gracefully(self):
        cache = PriceCache()
        cache.update("AAPL", 190.50)

        class CancellingRequest(FakeRequest):
            async def is_disconnected(self) -> bool:
                raise asyncio.CancelledError()

        request = CancellingRequest()
        # Generator should catch CancelledError internally and stop cleanly
        # rather than propagating it out of the async for loop.
        events = [event async for event in _generate_events(cache, request, interval=0.01)]
        assert events == ["retry: 1000\n\n"]

    async def test_multiple_tickers_serialized(self):
        cache = PriceCache()
        cache.update("AAPL", 190.50)
        cache.update("GOOGL", 175.25)
        request = FakeRequest(disconnect_after=1)
        events = [event async for event in _generate_events(cache, request, interval=0.01)]
        data_events = [e for e in events if e.startswith("data: ")]
        payload = json.loads(data_events[0][len("data: ") : -2])
        assert set(payload.keys()) == {"AAPL", "GOOGL"}
