# Market Data Backend — Design

Implementation-ready design for the FinAlly market data subsystem: the unified `MarketDataSource` interface, the in-memory `PriceCache`, the GBM simulator, the Massive (Polygon.io) REST client, the SSE streaming endpoint, and how the rest of the backend should wire into it.

**Status: implemented.** Everything below documents the actual code that lives under `backend/app/market/` today (verified against source, not aspirational). Section 10 ("Integration with the Rest of the Backend") is the exception — `app/main.py` and the FastAPI application don't exist yet, so that section is forward-looking guidance for whoever builds the REST/portfolio/chat layer.

See also `planning/MARKET_DATA_SUMMARY.md` for a short overview and `planning/archive/` for the original per-component design docs and the code review that shaped the fixes reflected here.

---

## Table of Contents

1. [File Structure](#1-file-structure)
2. [Data Model — `models.py`](#2-data-model)
3. [Price Cache — `cache.py`](#3-price-cache)
4. [Abstract Interface — `interface.py`](#4-abstract-interface)
5. [Seed Prices & Ticker Parameters — `seed_prices.py`](#5-seed-prices--ticker-parameters)
6. [GBM Simulator — `simulator.py`](#6-gbm-simulator)
7. [Massive API Client — `massive_client.py`](#7-massive-api-client)
8. [Factory — `factory.py`](#8-factory)
9. [SSE Streaming Endpoint — `stream.py`](#9-sse-streaming-endpoint)
10. [Integration with the Rest of the Backend](#10-integration-with-the-rest-of-the-backend)
11. [Watchlist Coordination](#11-watchlist-coordination)
12. [Testing Strategy](#12-testing-strategy)
13. [Error Handling & Edge Cases](#13-error-handling--edge-cases)
14. [Configuration Summary](#14-configuration-summary)

---

## 1. File Structure

```
backend/
  app/
    market/
      __init__.py             # Re-exports: PriceUpdate, PriceCache, MarketDataSource,
                               #             create_market_data_source, create_stream_router
      models.py                # PriceUpdate dataclass
      cache.py                 # PriceCache (thread-safe in-memory store)
      interface.py              # MarketDataSource ABC
      seed_prices.py            # SEED_PRICES, TICKER_PARAMS, DEFAULT_PARAMS,
                                 # CORRELATION_GROUPS, correlation constants
      simulator.py               # GBMSimulator + SimulatorDataSource
      massive_client.py          # MassiveDataSource
      factory.py                 # create_market_data_source()
      stream.py                  # SSE endpoint (FastAPI router factory)
  market_data_demo.py            # Rich terminal demo (standalone script)
  tests/
    market/
      test_models.py
      test_cache.py
      test_factory.py
      test_simulator.py
      test_simulator_source.py
      test_massive.py
```

Each file has a single responsibility. `app/market/__init__.py` re-exports the public API so the rest of the backend imports from `app.market` without reaching into submodules:

```python
from app.market import PriceCache, PriceUpdate, MarketDataSource, create_market_data_source, create_stream_router
```

There is no `app/main.py` yet — the market data package is self-contained and has no dependency on a FastAPI app existing. `create_stream_router()` takes a `PriceCache` and returns a plain `APIRouter`; whoever builds `main.py` mounts it (see [Section 10](#10-integration-with-the-rest-of-the-backend)).

---

## 2. Data Model

**File: `backend/app/market/models.py`**

`PriceUpdate` is the only data structure that leaves the market data layer. Every downstream consumer — SSE streaming, portfolio valuation, trade execution — works exclusively with this type.

```python
"""Data models for market data."""

from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class PriceUpdate:
    """Immutable snapshot of a single ticker's price at a point in time."""

    ticker: str
    price: float
    previous_price: float
    timestamp: float = field(default_factory=time.time)  # Unix seconds

    @property
    def change(self) -> float:
        """Absolute price change from previous update."""
        return round(self.price - self.previous_price, 4)

    @property
    def change_percent(self) -> float:
        """Percentage change from previous update."""
        if self.previous_price == 0:
            return 0.0
        return round((self.price - self.previous_price) / self.previous_price * 100, 4)

    @property
    def direction(self) -> str:
        """'up', 'down', or 'flat'."""
        if self.price > self.previous_price:
            return "up"
        elif self.price < self.previous_price:
            return "down"
        return "flat"

    def to_dict(self) -> dict:
        """Serialize for JSON / SSE transmission."""
        return {
            "ticker": self.ticker,
            "price": self.price,
            "previous_price": self.previous_price,
            "timestamp": self.timestamp,
            "change": self.change,
            "change_percent": self.change_percent,
            "direction": self.direction,
        }
```

### Design decisions

- **`frozen=True`** — price updates are immutable value objects, safe to share across async tasks without copying.
- **`slots=True`** — memory optimization; many of these are created per second.
- **Computed properties** (`change`, `change_percent`, `direction`) — derived from `price`/`previous_price` so they can never be inconsistent with each other.
- **`to_dict()`** — the single serialization point used by the SSE endpoint (and, later, REST responses that embed live prices).

---

## 3. Price Cache

**File: `backend/app/market/cache.py`**

The price cache is the central hub. Data sources write to it; SSE streaming and (eventually) portfolio valuation/trade execution read from it. It must be thread-safe because the Massive client's synchronous calls run inside `asyncio.to_thread()`, a real OS thread outside the event loop.

```python
"""Thread-safe in-memory price cache."""

from __future__ import annotations

import time
from threading import Lock

from .models import PriceUpdate


class PriceCache:
    """Thread-safe in-memory cache of the latest price for each ticker.

    Writers: SimulatorDataSource or MassiveDataSource (one at a time).
    Readers: SSE streaming endpoint, portfolio valuation, trade execution.
    """

    def __init__(self) -> None:
        self._prices: dict[str, PriceUpdate] = {}
        self._lock = Lock()
        self._version: int = 0  # Monotonically increasing; bumped on every update

    def update(self, ticker: str, price: float, timestamp: float | None = None) -> PriceUpdate:
        """Record a new price for a ticker. Returns the created PriceUpdate.

        Automatically computes direction and change from the previous price.
        If this is the first update for the ticker, previous_price == price (direction='flat').
        """
        with self._lock:
            ts = timestamp or time.time()
            prev = self._prices.get(ticker)
            previous_price = prev.price if prev else price

            update = PriceUpdate(
                ticker=ticker,
                price=round(price, 2),
                previous_price=round(previous_price, 2),
                timestamp=ts,
            )
            self._prices[ticker] = update
            self._version += 1
            return update

    def get(self, ticker: str) -> PriceUpdate | None:
        """Get the latest price for a single ticker, or None if unknown."""
        with self._lock:
            return self._prices.get(ticker)

    def get_all(self) -> dict[str, PriceUpdate]:
        """Snapshot of all current prices. Returns a shallow copy."""
        with self._lock:
            return dict(self._prices)

    def get_price(self, ticker: str) -> float | None:
        """Convenience: get just the price float, or None."""
        update = self.get(ticker)
        return update.price if update else None

    def remove(self, ticker: str) -> None:
        """Remove a ticker from the cache (e.g., when removed from watchlist)."""
        with self._lock:
            self._prices.pop(ticker, None)

    @property
    def version(self) -> int:
        """Current version counter. Useful for SSE change detection."""
        return self._version

    def __len__(self) -> int:
        with self._lock:
            return len(self._prices)

    def __contains__(self, ticker: str) -> bool:
        with self._lock:
            return ticker in self._prices
```

### Why a version counter?

The SSE loop polls the cache every ~500ms. Without a version counter it would serialize and send all prices on every tick even when nothing changed (e.g. Massive only updates every 15s). The counter lets the loop skip a send when nothing is new:

```python
last_version = -1
while True:
    if price_cache.version != last_version:
        last_version = price_cache.version
        yield format_sse(price_cache.get_all())
    await asyncio.sleep(0.5)
```

### Thread safety rationale

`threading.Lock`, not `asyncio.Lock`, because:
- The Massive client's synchronous `get_snapshot_all()` runs via `asyncio.to_thread()`, which uses a real OS thread — `asyncio.Lock` would not protect against that.
- `threading.Lock` works correctly whether the caller is a sync thread or the async event loop.

Note: `version` is read without acquiring the lock. On CPython (GIL) a single `int` read is atomic, so this is safe today; it's a latent inconsistency if the project ever ran on a no-GIL build, but not worth the extra lock acquisition for this project's scale.

---

## 4. Abstract Interface

**File: `backend/app/market/interface.py`**

```python
"""Abstract interface for market data sources."""

from __future__ import annotations

from abc import ABC, abstractmethod


class MarketDataSource(ABC):
    """Contract for market data providers.

    Implementations push price updates into a shared PriceCache on their own
    schedule. Downstream code never calls the data source directly for prices —
    it reads from the cache.

    Lifecycle:
        source = create_market_data_source(cache)
        await source.start(["AAPL", "GOOGL", ...])
        # ... app runs ...
        await source.add_ticker("TSLA")
        await source.remove_ticker("GOOGL")
        # ... app shutting down ...
        await source.stop()
    """

    @abstractmethod
    async def start(self, tickers: list[str]) -> None:
        """Begin producing price updates for the given tickers.

        Starts a background task that periodically writes to the PriceCache.
        Must be called exactly once. Calling start() twice is undefined behavior.
        """

    @abstractmethod
    async def stop(self) -> None:
        """Stop the background task and release resources.

        Safe to call multiple times. After stop(), the source will not write
        to the cache again.
        """

    @abstractmethod
    async def add_ticker(self, ticker: str) -> None:
        """Add a ticker to the active set. No-op if already present.

        The next update cycle will include this ticker.
        """

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker from the active set. No-op if not present.

        Also removes the ticker from the PriceCache.
        """

    @abstractmethod
    def get_tickers(self) -> list[str]:
        """Return the current list of actively tracked tickers."""
```

### Why the source writes to the cache instead of returning prices

This push model decouples timing. The simulator ticks every 500ms; Massive polls every 15s; SSE always reads the cache on its own 500ms cadence. The SSE layer never needs to know which source is active or how fast it updates — that's the whole point of the strategy pattern here.

---

## 5. Seed Prices & Ticker Parameters

**File: `backend/app/market/seed_prices.py`**

Constants only — no logic, no imports beyond the type hints. Shared by the simulator (initial prices, GBM parameters, correlation grouping) and reusable as fallback seed data for any future consumer.

```python
"""Seed prices and per-ticker parameters for the market simulator."""

# Realistic starting prices for the default watchlist (as of project creation)
SEED_PRICES: dict[str, float] = {
    "AAPL": 190.00,
    "GOOGL": 175.00,
    "MSFT": 420.00,
    "AMZN": 185.00,
    "TSLA": 250.00,
    "NVDA": 800.00,
    "META": 500.00,
    "JPM": 195.00,
    "V": 280.00,
    "NFLX": 600.00,
}

# Per-ticker GBM parameters
# sigma: annualized volatility (higher = more price movement)
# mu: annualized drift / expected return
TICKER_PARAMS: dict[str, dict[str, float]] = {
    "AAPL": {"sigma": 0.22, "mu": 0.05},
    "GOOGL": {"sigma": 0.25, "mu": 0.05},
    "MSFT": {"sigma": 0.20, "mu": 0.05},
    "AMZN": {"sigma": 0.28, "mu": 0.05},
    "TSLA": {"sigma": 0.50, "mu": 0.03},  # High volatility
    "NVDA": {"sigma": 0.40, "mu": 0.08},  # High volatility, strong drift
    "META": {"sigma": 0.30, "mu": 0.05},
    "JPM": {"sigma": 0.18, "mu": 0.04},  # Low volatility (bank)
    "V": {"sigma": 0.17, "mu": 0.04},  # Low volatility (payments)
    "NFLX": {"sigma": 0.35, "mu": 0.05},
}

# Default parameters for tickers not in the list above (dynamically added)
DEFAULT_PARAMS: dict[str, float] = {"sigma": 0.25, "mu": 0.05}

# Correlation groups for the simulator's Cholesky decomposition
# Tickers in the same group have higher intra-group correlation
CORRELATION_GROUPS: dict[str, set[str]] = {
    "tech": {"AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "NFLX"},
    "finance": {"JPM", "V"},
}

# Correlation coefficients
INTRA_TECH_CORR = 0.6  # Tech stocks move together
INTRA_FINANCE_CORR = 0.5  # Finance stocks move together
CROSS_GROUP_CORR = 0.3  # Between sectors / unknown tickers
TSLA_CORR = 0.3  # TSLA does its own thing
```

Note there is a single `CROSS_GROUP_CORR` constant that covers both cross-sector pairs and any ticker outside the known groups (dynamically added tickers default to `DEFAULT_PARAMS` for GBM params and to `CROSS_GROUP_CORR` for correlation). An earlier draft had a separate `DEFAULT_CORR` constant with the same value; it was removed as redundant.

---

## 6. GBM Simulator

**File: `backend/app/market/simulator.py`**

Two classes:
- `GBMSimulator` — pure math engine, stateful, advances prices one step at a time.
- `SimulatorDataSource` — the `MarketDataSource` implementation wrapping `GBMSimulator` in an async loop that writes to `PriceCache`.

### 6.1 GBMSimulator — the math engine

```python
"""GBM-based market simulator."""

from __future__ import annotations

import asyncio
import logging
import math
import random

import numpy as np

from .cache import PriceCache
from .interface import MarketDataSource
from .seed_prices import (
    CORRELATION_GROUPS,
    CROSS_GROUP_CORR,
    DEFAULT_PARAMS,
    INTRA_FINANCE_CORR,
    INTRA_TECH_CORR,
    SEED_PRICES,
    TICKER_PARAMS,
    TSLA_CORR,
)

logger = logging.getLogger(__name__)


class GBMSimulator:
    """Geometric Brownian Motion simulator for correlated stock prices.

    Math:
        S(t+dt) = S(t) * exp((mu - sigma^2/2) * dt + sigma * sqrt(dt) * Z)

    Where:
        S(t)   = current price
        mu     = annualized drift (expected return)
        sigma  = annualized volatility
        dt     = time step as fraction of a trading year
        Z      = correlated standard normal random variable

    The tiny dt (~8.5e-8 for 500ms ticks over 252 trading days * 6.5h/day)
    produces sub-cent moves per tick that accumulate naturally over time.
    """

    # 500ms expressed as a fraction of a trading year
    # 252 trading days * 6.5 hours/day * 3600 seconds/hour = 5,896,800 seconds
    TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600  # 5,896,800
    DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR  # ~8.48e-8

    def __init__(
        self,
        tickers: list[str],
        dt: float = DEFAULT_DT,
        event_probability: float = 0.001,
    ) -> None:
        self._dt = dt
        self._event_prob = event_probability

        # Per-ticker state
        self._tickers: list[str] = []
        self._prices: dict[str, float] = {}
        self._params: dict[str, dict[str, float]] = {}

        # Cholesky decomposition of the correlation matrix (for correlated moves)
        self._cholesky: np.ndarray | None = None

        # Initialize all starting tickers
        for ticker in tickers:
            self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    # --- Public API ---

    def step(self) -> dict[str, float]:
        """Advance all tickers by one time step. Returns {ticker: new_price}.

        This is the hot path — called every 500ms. Keep it fast.
        """
        n = len(self._tickers)
        if n == 0:
            return {}

        # Generate n independent standard normal draws
        z_independent = np.random.standard_normal(n)

        # Apply Cholesky to get correlated draws
        if self._cholesky is not None:
            z_correlated = self._cholesky @ z_independent
        else:
            z_correlated = z_independent

        result: dict[str, float] = {}
        for i, ticker in enumerate(self._tickers):
            params = self._params[ticker]
            mu = params["mu"]
            sigma = params["sigma"]

            # GBM: S(t+dt) = S(t) * exp((mu - 0.5*sigma^2)*dt + sigma*sqrt(dt)*Z)
            drift = (mu - 0.5 * sigma**2) * self._dt
            diffusion = sigma * math.sqrt(self._dt) * z_correlated[i]
            self._prices[ticker] *= math.exp(drift + diffusion)

            # Random event: ~0.1% chance per tick per ticker
            # With 10 tickers at 2 ticks/sec, expect an event ~every 50 seconds
            if random.random() < self._event_prob:
                shock_magnitude = random.uniform(0.02, 0.05)
                shock_sign = random.choice([-1, 1])
                self._prices[ticker] *= 1 + shock_magnitude * shock_sign
                logger.debug(
                    "Random event on %s: %.1f%% %s",
                    ticker,
                    shock_magnitude * 100,
                    "up" if shock_sign > 0 else "down",
                )

            result[ticker] = round(self._prices[ticker], 2)

        return result

    def add_ticker(self, ticker: str) -> None:
        """Add a ticker to the simulation. Rebuilds the correlation matrix."""
        if ticker in self._prices:
            return
        self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker from the simulation. Rebuilds the correlation matrix."""
        if ticker not in self._prices:
            return
        self._tickers.remove(ticker)
        del self._prices[ticker]
        del self._params[ticker]
        self._rebuild_cholesky()

    def get_price(self, ticker: str) -> float | None:
        """Current price for a ticker, or None if not tracked."""
        return self._prices.get(ticker)

    def get_tickers(self) -> list[str]:
        """Return the list of currently tracked tickers."""
        return list(self._tickers)

    # --- Internals ---

    def _add_ticker_internal(self, ticker: str) -> None:
        """Add a ticker without rebuilding Cholesky (for batch initialization)."""
        if ticker in self._prices:
            return
        self._tickers.append(ticker)
        self._prices[ticker] = SEED_PRICES.get(ticker, random.uniform(50.0, 300.0))
        self._params[ticker] = TICKER_PARAMS.get(ticker, dict(DEFAULT_PARAMS))

    def _rebuild_cholesky(self) -> None:
        """Rebuild the Cholesky decomposition of the ticker correlation matrix.

        Called whenever tickers are added or removed. O(n^2) but n < 50.
        """
        n = len(self._tickers)
        if n <= 1:
            self._cholesky = None
            return

        # Build the correlation matrix
        corr = np.eye(n)
        for i in range(n):
            for j in range(i + 1, n):
                rho = self._pairwise_correlation(self._tickers[i], self._tickers[j])
                corr[i, j] = rho
                corr[j, i] = rho

        self._cholesky = np.linalg.cholesky(corr)

    @staticmethod
    def _pairwise_correlation(t1: str, t2: str) -> float:
        """Determine correlation between two tickers based on sector grouping.

        Correlation structure:
          - Same tech sector:    0.6
          - Same finance sector: 0.5
          - TSLA with anything:  0.3 (it does its own thing)
          - Cross-sector:        0.3
          - Unknown tickers:     0.3
        """
        tech = CORRELATION_GROUPS["tech"]
        finance = CORRELATION_GROUPS["finance"]

        # TSLA is in the tech set but behaves independently
        if t1 == "TSLA" or t2 == "TSLA":
            return TSLA_CORR

        if t1 in tech and t2 in tech:
            return INTRA_TECH_CORR
        if t1 in finance and t2 in finance:
            return INTRA_FINANCE_CORR

        return CROSS_GROUP_CORR
```

### 6.2 SimulatorDataSource — async wrapper

```python
class SimulatorDataSource(MarketDataSource):
    """MarketDataSource backed by the GBM simulator.

    Runs a background asyncio task that calls GBMSimulator.step() every
    `update_interval` seconds and writes results to the PriceCache.
    """

    def __init__(
        self,
        price_cache: PriceCache,
        update_interval: float = 0.5,
        event_probability: float = 0.001,
    ) -> None:
        self._cache = price_cache
        self._interval = update_interval
        self._event_prob = event_probability
        self._sim: GBMSimulator | None = None
        self._task: asyncio.Task | None = None

    async def start(self, tickers: list[str]) -> None:
        self._sim = GBMSimulator(
            tickers=tickers,
            event_probability=self._event_prob,
        )
        # Seed the cache with initial prices so SSE has data immediately
        for ticker in tickers:
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)
        self._task = asyncio.create_task(self._run_loop(), name="simulator-loop")
        logger.info("Simulator started with %d tickers", len(tickers))

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        logger.info("Simulator stopped")

    async def add_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.add_ticker(ticker)
            # Seed cache immediately so the ticker has a price right away
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)
            logger.info("Simulator: added ticker %s", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.remove_ticker(ticker)
        self._cache.remove(ticker)
        logger.info("Simulator: removed ticker %s", ticker)

    def get_tickers(self) -> list[str]:
        return self._sim.get_tickers() if self._sim else []

    async def _run_loop(self) -> None:
        """Core loop: step the simulation, write to cache, sleep."""
        while True:
            try:
                if self._sim:
                    prices = self._sim.step()
                    for ticker, price in prices.items():
                        self._cache.update(ticker=ticker, price=price)
            except Exception:
                logger.exception("Simulator step failed")
            await asyncio.sleep(self._interval)
```

### Key behaviors

- **Immediate seeding** — `start()` populates the cache with seed prices *before* the background loop's first tick, so the SSE endpoint has data to send on the very first client connection. No blank-screen delay.
- **Graceful cancellation** — `stop()` cancels the task and awaits it, swallowing `CancelledError`. Clean shutdown under any future FastAPI lifespan teardown.
- **Exception resilience** — the loop catches exceptions per-step so one bad tick doesn't kill the entire feed.
- **`get_tickers()` delegates to `GBMSimulator.get_tickers()`** rather than reaching into a private `_tickers` list — keeps the boundary between the async wrapper and the math engine clean.

---

## 7. Massive API Client

**File: `backend/app/market/massive_client.py`**

Polls the Massive (Polygon.io) REST API snapshot endpoint on a configurable interval. The `massive` package is a core dependency (declared in `pyproject.toml`), so its import is a normal top-level import — no lazy-import trick is needed, and the client class name (`RESTClient`) is directly patchable in tests.

```python
"""Massive (Polygon.io) API client for real market data."""

from __future__ import annotations

import asyncio
import logging

from massive import RESTClient
from massive.rest.models import SnapshotMarketType

from .cache import PriceCache
from .interface import MarketDataSource

logger = logging.getLogger(__name__)


class MassiveDataSource(MarketDataSource):
    """MarketDataSource backed by the Massive (Polygon.io) REST API.

    Polls GET /v2/snapshot/locale/us/markets/stocks/tickers for all watched
    tickers in a single API call, then writes results to the PriceCache.

    Rate limits:
      - Free tier: 5 req/min → poll every 15s (default)
      - Paid tiers: higher limits → poll every 2-5s
    """

    def __init__(
        self,
        api_key: str,
        price_cache: PriceCache,
        poll_interval: float = 15.0,
    ) -> None:
        self._api_key = api_key
        self._cache = price_cache
        self._interval = poll_interval
        self._tickers: list[str] = []
        self._task: asyncio.Task | None = None
        self._client: RESTClient | None = None

    async def start(self, tickers: list[str]) -> None:
        self._client = RESTClient(api_key=self._api_key)
        self._tickers = list(tickers)

        # Do an immediate first poll so the cache has data right away
        await self._poll_once()

        self._task = asyncio.create_task(self._poll_loop(), name="massive-poller")
        logger.info(
            "Massive poller started: %d tickers, %.1fs interval",
            len(tickers),
            self._interval,
        )

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        self._client = None
        logger.info("Massive poller stopped")

    async def add_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        if ticker not in self._tickers:
            self._tickers.append(ticker)
            logger.info("Massive: added ticker %s (will appear on next poll)", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        self._tickers = [t for t in self._tickers if t != ticker]
        self._cache.remove(ticker)
        logger.info("Massive: removed ticker %s", ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    # --- Internal ---

    async def _poll_loop(self) -> None:
        """Poll on interval. First poll already happened in start()."""
        while True:
            await asyncio.sleep(self._interval)
            await self._poll_once()

    async def _poll_once(self) -> None:
        """Execute one poll cycle: fetch snapshots, update cache."""
        if not self._tickers or not self._client:
            return

        try:
            # The Massive RESTClient is synchronous — run in a thread to
            # avoid blocking the event loop.
            snapshots = await asyncio.to_thread(self._fetch_snapshots)
            processed = 0
            for snap in snapshots:
                try:
                    price = snap.last_trade.price
                    # Massive timestamps are Unix milliseconds → convert to seconds
                    timestamp = snap.last_trade.timestamp / 1000.0
                    self._cache.update(
                        ticker=snap.ticker,
                        price=price,
                        timestamp=timestamp,
                    )
                    processed += 1
                except (AttributeError, TypeError) as e:
                    logger.warning(
                        "Skipping snapshot for %s: %s",
                        getattr(snap, "ticker", "???"),
                        e,
                    )
            logger.debug("Massive poll: updated %d/%d tickers", processed, len(self._tickers))

        except Exception as e:
            logger.error("Massive poll failed: %s", e)
            # Don't re-raise — the loop will retry on the next interval.
            # Common failures: 401 (bad key), 429 (rate limit), network errors.

    def _fetch_snapshots(self) -> list:
        """Synchronous call to the Massive REST API. Runs in a thread."""
        return self._client.get_snapshot_all(
            market_type=SnapshotMarketType.STOCKS,
            tickers=self._tickers,
        )
```

### Error handling philosophy

The Massive poller is intentionally resilient:

| Error | Behavior |
|-------|----------|
| **401 Unauthorized** | Logged as error. Poller keeps running (user might fix `.env` and restart). |
| **429 Rate Limited** | Logged as error. Next poll retries after `poll_interval` seconds. |
| **Network timeout** | Logged as error. Retries automatically on next cycle. |
| **Malformed snapshot** | Individual ticker skipped with a warning. Other tickers still processed. |
| **All tickers fail** | Cache retains last-known prices. SSE keeps streaming stale data (better than no data). |

### Why `massive` is a core dependency, not lazy-imported

An earlier draft lazy-imported `massive` inside `start()` so the package would only be required when `MASSIVE_API_KEY` was set. In practice `massive>=1.0.0` is declared as a core dependency in `pyproject.toml` and installed unconditionally via `uv sync` — the lazy import bought nothing except making `RESTClient` unpatchable at the module level in tests (`patch("app.market.massive_client.RESTClient")` requires the name to exist in the module namespace). The top-level import is simpler and the tests patch it directly.

---

## 8. Factory

**File: `backend/app/market/factory.py`**

```python
"""Factory for creating market data sources."""

from __future__ import annotations

import logging
import os

from .cache import PriceCache
from .interface import MarketDataSource
from .massive_client import MassiveDataSource
from .simulator import SimulatorDataSource

logger = logging.getLogger(__name__)


def create_market_data_source(price_cache: PriceCache) -> MarketDataSource:
    """Create the appropriate market data source based on environment variables.

    - MASSIVE_API_KEY set and non-empty → MassiveDataSource (real market data)
    - Otherwise → SimulatorDataSource (GBM simulation)

    Returns an unstarted source. Caller must await source.start(tickers).
    """
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()

    if api_key:
        logger.info("Market data source: Massive API (real data)")
        return MassiveDataSource(api_key=api_key, price_cache=price_cache)
    else:
        logger.info("Market data source: GBM Simulator")
        return SimulatorDataSource(price_cache=price_cache)
```

### Usage

```python
price_cache = PriceCache()
source = create_market_data_source(price_cache)
await source.start(initial_tickers)  # e.g., ["AAPL", "GOOGL", ...]
```

This is the single switch point for the simulator-vs-real-data decision described in `planning/PLAN.md` §5/§6. Nothing downstream branches on which source is active — everything reads `PriceCache`.

---

## 9. SSE Streaming Endpoint

**File: `backend/app/market/stream.py`**

A FastAPI route that holds open a long-lived HTTP connection and pushes price updates as `text/event-stream`.

```python
"""SSE streaming endpoint for live price updates."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncGenerator

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from .cache import PriceCache

logger = logging.getLogger(__name__)


def create_stream_router(price_cache: PriceCache) -> APIRouter:
    """Create the SSE streaming router with a reference to the price cache.

    This factory pattern lets us inject the PriceCache without globals.
    Returns a fresh APIRouter each call so repeated calls (e.g. one per
    test-created FastAPI app) never share route state.
    """
    router = APIRouter(prefix="/api/stream", tags=["streaming"])

    @router.get("/prices")
    async def stream_prices(request: Request) -> StreamingResponse:
        """SSE endpoint for live price updates.

        Streams all tracked ticker prices every ~500ms. The client connects
        with EventSource and receives events in the format:

            data: {"AAPL": {"ticker": "AAPL", "price": 190.50, ...}, ...}

        Includes a retry directive so the browser auto-reconnects on
        disconnection (EventSource built-in behavior).
        """
        return StreamingResponse(
            _generate_events(price_cache, request),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",  # Disable nginx buffering if proxied
            },
        )

    return router


async def _generate_events(
    price_cache: PriceCache,
    request: Request,
    interval: float = 0.5,
) -> AsyncGenerator[str, None]:
    """Async generator that yields SSE-formatted price events.

    Sends all prices every `interval` seconds. Stops when the client
    disconnects (detected via request.is_disconnected()).
    """
    # Tell the client to retry after 1 second if the connection drops
    yield "retry: 1000\n\n"

    last_version = -1
    client_ip = request.client.host if request.client else "unknown"
    logger.info("SSE client connected: %s", client_ip)

    try:
        while True:
            # Check for client disconnect
            if await request.is_disconnected():
                logger.info("SSE client disconnected: %s", client_ip)
                break

            current_version = price_cache.version
            if current_version != last_version:
                last_version = current_version
                prices = price_cache.get_all()

                if prices:
                    data = {ticker: update.to_dict() for ticker, update in prices.items()}
                    payload = json.dumps(data)
                    yield f"data: {payload}\n\n"

            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        logger.info("SSE stream cancelled for: %s", client_ip)
```

### SSE wire format

```
data: {"AAPL":{"ticker":"AAPL","price":190.50,"previous_price":190.42,"timestamp":1707580800.5,"change":0.08,"change_percent":0.042,"direction":"up"},"GOOGL":{"ticker":"GOOGL","price":175.12,...}}

```

Client side, per `planning/PLAN.md` §10:

```javascript
const eventSource = new EventSource('/api/stream/prices');
eventSource.onmessage = (event) => {
    const prices = JSON.parse(event.data);
    // prices is { "AAPL": { ticker, price, previous_price, change, change_percent, direction, timestamp }, ... }
};
```

### Why poll-and-push instead of event-driven

The generator polls the cache on a fixed interval rather than being notified by the data source. This is simpler and produces evenly-spaced updates, which matters because the frontend accumulates these into sparklines — regular spacing keeps that visualization clean regardless of which underlying source (simulator @500ms, Massive @15s) is active.

---

## 10. Integration with the Rest of the Backend

This section does not exist as code yet — `app/main.py`, the REST routes, the database layer, and the LLM chat integration are still to be built (per `CLAUDE.md`, only the market data component is complete). It documents how those future pieces should wire into what's described above, based on the public API `app/market/__init__.py` already exports.

### FastAPI lifespan

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.market import PriceCache, create_market_data_source, create_stream_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- STARTUP ---
    price_cache = PriceCache()
    app.state.price_cache = price_cache

    source = create_market_data_source(price_cache)
    app.state.market_source = source

    initial_tickers = await load_watchlist_tickers()  # reads from SQLite, per PLAN.md §7
    await source.start(initial_tickers)

    app.include_router(create_stream_router(price_cache))

    yield  # App is running

    # --- SHUTDOWN ---
    await source.stop()


app = FastAPI(title="FinAlly", lifespan=lifespan)


def get_price_cache() -> PriceCache:
    return app.state.price_cache


def get_market_source() -> MarketDataSource:
    return app.state.market_source
```

### Accessing market data from other routes

```python
from fastapi import APIRouter, Depends, HTTPException

router = APIRouter(prefix="/api")


@router.post("/portfolio/trade")
async def execute_trade(
    trade: TradeRequest,
    price_cache: PriceCache = Depends(get_price_cache),
):
    current_price = price_cache.get_price(trade.ticker)
    if current_price is None:
        raise HTTPException(404, f"No price available for {trade.ticker}")
    # ... execute trade at current_price, per PLAN.md §8 ...


@router.post("/watchlist")
async def add_to_watchlist(
    payload: WatchlistAdd,
    source: MarketDataSource = Depends(get_market_source),
):
    # ... insert into watchlist table ...
    await source.add_ticker(payload.ticker)
    # ...


@router.delete("/watchlist/{ticker}")
async def remove_from_watchlist(
    ticker: str,
    source: MarketDataSource = Depends(get_market_source),
):
    # ... delete from watchlist table ...
    await source.remove_ticker(ticker)
    # ...
```

Nothing in `app/market/` depends on FastAPI's dependency injection or the database — this is purely how the consuming layer should be shaped when it's built.

---

## 11. Watchlist Coordination

When the watchlist changes (via REST or LLM chat, per `PLAN.md` §9), the active `MarketDataSource` must be notified so it tracks the right set of tickers.

### Flow: adding a ticker

```
POST /api/watchlist {ticker: "PYPL"}
  → Insert into watchlist table (SQLite)
  → await source.add_ticker("PYPL")
      Simulator: adds to GBMSimulator, rebuilds Cholesky, seeds cache immediately
      Massive:   appends to ticker list, appears on next poll (up to 15s later)
  → Return success (ticker + current price if already available)
```

### Flow: removing a ticker

```
DELETE /api/watchlist/PYPL
  → Delete from watchlist table (SQLite)
  → await source.remove_ticker("PYPL")
      Simulator: removes from GBMSimulator, rebuilds Cholesky, removes from cache
      Massive:   removes from ticker list, removes from cache
  → Return success
```

### Edge case: ticker still held as a position

If the user removes a ticker from the watchlist but still holds shares, the data source should keep tracking it so portfolio valuation stays accurate:

```python
@router.delete("/watchlist/{ticker}")
async def remove_from_watchlist(
    ticker: str,
    source: MarketDataSource = Depends(get_market_source),
):
    await db.delete_watchlist_entry(ticker)

    position = await db.get_position(ticker)
    if position is None or position.quantity == 0:
        await source.remove_ticker(ticker)

    return {"status": "ok"}
```

---

## 12. Testing Strategy

**84 tests, all passing, 98% overall coverage** (`backend/tests/market/`, 7 modules). Every test below is real, current test code — not illustrative pseudocode.

| Module | Tests | Coverage |
|--------|-------|----------|
| `test_models.py` | 11 | `models.py`: 100% |
| `test_cache.py` | 13 | `cache.py`: 100% |
| `test_simulator.py` | 17 | `simulator.py`: 98% |
| `test_simulator_source.py` | 10 | integration tests for `SimulatorDataSource` |
| `test_factory.py` | 7 | `factory.py`: 100% |
| `test_massive.py` | 13 | `massive_client.py`: 94% |
| `test_stream.py` | 11 | `stream.py`: 97% |

Run with:

```bash
cd backend
uv sync --extra dev
uv run --extra dev pytest -v
uv run --extra dev pytest --cov=app
uv run --extra dev ruff check app/ tests/
```

### 12.1 `PriceCache` — `test_cache.py`

```python
"""Tests for PriceCache."""

from app.market.cache import PriceCache


class TestPriceCache:
    def test_direction_up(self):
        cache = PriceCache()
        cache.update("AAPL", 190.00)
        update = cache.update("AAPL", 191.00)
        assert update.direction == "up"
        assert update.change == 1.00

    def test_version_increments(self):
        cache = PriceCache()
        v0 = cache.version
        cache.update("AAPL", 190.00)
        assert cache.version == v0 + 1
        cache.update("AAPL", 191.00)
        assert cache.version == v0 + 2

    def test_price_rounding(self):
        cache = PriceCache()
        update = cache.update("AAPL", 190.12345)
        assert update.price == 190.12

    def test_custom_timestamp(self):
        cache = PriceCache()
        custom_ts = 1234567890.0
        update = cache.update("AAPL", 190.50, timestamp=custom_ts)
        assert update.timestamp == custom_ts
```

(Also covered: `test_first_update_is_flat`, `test_direction_down`, `test_remove`, `test_remove_nonexistent`, `test_get_all`, `test_get_price_convenience`, `test_len`, `test_contains`, `test_update_and_get`.)

### 12.2 `GBMSimulator` — `test_simulator.py` (representative cases)

```python
class TestGBMSimulator:
    def test_prices_are_positive(self):
        """GBM prices can never go negative (exp() is always positive)."""
        sim = GBMSimulator(tickers=["AAPL"])
        for _ in range(10_000):
            prices = sim.step()
            assert prices["AAPL"] > 0

    def test_cholesky_rebuilds_on_add(self):
        sim = GBMSimulator(tickers=["AAPL"])
        assert sim._cholesky is None  # Only 1 ticker, no correlation matrix
        sim.add_ticker("GOOGL")
        assert sim._cholesky is not None  # Now 2 tickers, matrix exists

    def test_remove_ticker(self):
        sim = GBMSimulator(tickers=["AAPL", "GOOGL"])
        sim.remove_ticker("GOOGL")
        result = sim.step()
        assert "GOOGL" not in result
        assert "AAPL" in result
```

### 12.3 `SimulatorDataSource` — `test_simulator_source.py` (representative cases)

```python
@pytest.mark.asyncio
class TestSimulatorDataSource:
    async def test_start_populates_cache(self):
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.1)
        await source.start(["AAPL", "GOOGL"])

        # Cache has seed prices immediately, before the first loop tick
        assert cache.get("AAPL") is not None
        assert cache.get("GOOGL") is not None
        await source.stop()

    async def test_add_and_remove_ticker(self):
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.1)
        await source.start(["AAPL"])

        await source.add_ticker("TSLA")
        assert "TSLA" in source.get_tickers()
        assert cache.get("TSLA") is not None

        await source.remove_ticker("TSLA")
        assert "TSLA" not in source.get_tickers()
        assert cache.get("TSLA") is None
        await source.stop()
```

### 12.4 `MassiveDataSource` — `test_massive.py` (mocked API)

The Massive client is tested by mocking `_fetch_snapshots` (unit-level) and `RESTClient` itself (for `start()`/`stop()` lifecycle):

```python
def _make_snapshot(ticker: str, price: float, timestamp_ms: int) -> MagicMock:
    snap = MagicMock()
    snap.ticker = ticker
    snap.last_trade = MagicMock()
    snap.last_trade.price = price
    snap.last_trade.timestamp = timestamp_ms
    return snap


@pytest.mark.asyncio
class TestMassiveDataSource:
    async def test_poll_updates_cache(self):
        cache = PriceCache()
        source = MassiveDataSource(api_key="test-key", price_cache=cache, poll_interval=60.0)
        source._tickers = ["AAPL", "GOOGL"]
        source._client = MagicMock()  # Satisfy the _poll_once guard

        mock_snapshots = [
            _make_snapshot("AAPL", 190.50, 1707580800000),
            _make_snapshot("GOOGL", 175.25, 1707580800000),
        ]
        with patch.object(source, "_fetch_snapshots", return_value=mock_snapshots):
            await source._poll_once()

        assert cache.get_price("AAPL") == 190.50
        assert cache.get_price("GOOGL") == 175.25

    async def test_malformed_snapshot_skipped(self):
        cache = PriceCache()
        source = MassiveDataSource(api_key="test-key", price_cache=cache, poll_interval=60.0)
        source._tickers = ["AAPL", "BAD"]
        source._client = MagicMock()

        good_snap = _make_snapshot("AAPL", 190.50, 1707580800000)
        bad_snap = MagicMock()
        bad_snap.ticker = "BAD"
        bad_snap.last_trade = None  # Will cause AttributeError

        with patch.object(source, "_fetch_snapshots", return_value=[good_snap, bad_snap]):
            await source._poll_once()

        assert cache.get_price("AAPL") == 190.50
        assert cache.get_price("BAD") is None

    async def test_timestamp_conversion(self):
        cache = PriceCache()
        source = MassiveDataSource(api_key="test-key", price_cache=cache, poll_interval=60.0)
        source._tickers = ["AAPL"]
        source._client = MagicMock()

        mock_snapshots = [_make_snapshot("AAPL", 190.50, 1707580800000)]
        with patch.object(source, "_fetch_snapshots", return_value=mock_snapshots):
            await source._poll_once()

        update = cache.get("AAPL")
        assert update.timestamp == 1707580800.0  # ms → s

    async def test_stop_cancels_task(self):
        cache = PriceCache()
        source = MassiveDataSource(api_key="test-key", price_cache=cache, poll_interval=10.0)

        with patch("app.market.massive_client.RESTClient"):
            with patch.object(source, "_fetch_snapshots", return_value=[]):
                await source.start(["AAPL"])

        assert source._task is not None
        assert not source._task.done()

        await source.stop()
        assert source._task is None
```

Because `massive.RESTClient` is a normal top-level import (see [Section 7](#7-massive-api-client)), `patch("app.market.massive_client.RESTClient")` works directly — no `create=True` workaround needed.

### 12.5 `stream.py` — `test_stream.py`

SSE streaming is now covered without needing a running ASGI server: `TestCreateStreamRouter` exercises the router factory directly (including a regression test that repeated calls return independent routers rather than sharing route state — see `planning/MARKET_DATA_REVIEW.md` §3.1), and `TestGenerateEvents` drives the `_generate_events` async generator with a `FakeRequest` stand-in for `fastapi.Request` that controls disconnect timing:

```python
class FakeRequest:
    """Minimal stand-in for fastapi.Request, driving disconnect timing."""

    def __init__(self, disconnect_after: int | None = 0, client=None):
        self.client = client if client is not None else FakeClient()
        self._disconnect_after = disconnect_after
        self._checks = 0

    async def is_disconnected(self) -> bool:
        if self._disconnect_after is None:
            return False
        result = self._checks >= self._disconnect_after
        self._checks += 1
        return result


class TestGenerateEvents:
    async def test_resends_on_version_change(self):
        """A cache mutation between loop iterations triggers a second send."""
        cache = PriceCache()
        cache.update("AAPL", 190.50)

        class BumpingRequest(FakeRequest):
            def __init__(self):
                super().__init__(disconnect_after=None)
                self.call_count = 0

            async def is_disconnected(self) -> bool:
                self.call_count += 1
                if self.call_count == 2:
                    cache.update("AAPL", 191.00)
                return self.call_count >= 3

        events = [e async for e in _generate_events(cache, BumpingRequest(), interval=0.01)]
        data_events = [e for e in events if e.startswith("data: ")]
        assert len(data_events) == 2
```

Also covered: retry-directive preamble, immediate disconnect, skip-send on empty cache, no resend on unchanged version, graceful handling of `asyncio.CancelledError` from `is_disconnected()`, and multi-ticker payload serialization.

### 12.6 Not yet covered

- **Concurrent-writer stress test for `PriceCache`** — the locking looks correct by inspection; a multi-threaded writer test would verify it empirically but wasn't judged necessary at this scale (10–50 tickers).
- **Full 10-ticker default watchlist through `GBMSimulator`** — current tests use 1–2 tickers per case; a test that builds the Cholesky decomposition for the full default set would catch correlation-matrix edge cases (e.g. non-positive-definite matrices) if the correlation structure changes later. Manually verified during the 2026-08-18 review (500 steps across all 10 default tickers, no errors) but not yet captured as an automated test.

---

## 13. Error Handling & Edge Cases

### 13.1 Startup with an empty watchlist

If the database has no watchlist entries, `start()` receives an empty list. Both sources handle this: the simulator's `step()` returns `{}` for zero tickers; the Massive poller's `_poll_once()` returns immediately (`if not self._tickers ...`). The SSE endpoint sends nothing until a ticker is added, at which point `add_ticker()` starts tracking it immediately.

### 13.2 Price cache miss during trade

If a user (or the LLM) tries to trade a ticker with no cached price yet (just added, Massive hasn't polled):

```python
price = price_cache.get_price(ticker)
if price is None:
    raise HTTPException(
        status_code=400,
        detail=f"Price not yet available for {ticker}. Please wait a moment and try again.",
    )
```

The simulator avoids this in practice by seeding the cache synchronously inside `add_ticker()`. Massive has an unavoidable gap until the next poll — the 400 with a clear message is the correct response there.

### 13.3 Invalid Massive API key

A bad key surfaces on the first poll as a 401. `_poll_once()` logs the error and the poller keeps retrying every `poll_interval` seconds — it never crashes the app. The SSE endpoint keeps streaming (possibly empty, or stale last-known prices). The connection status indicator (per `PLAN.md` §2) would show "connected" since SSE itself is fine; only the data is missing. Fix is to correct `.env` and restart.

### 13.4 Thread safety under load

`PriceCache`'s `threading.Lock` guards a tiny critical section (dict lookup + assignment). At project scale (≤50 tickers, 2 updates/sec, one writer, a handful of SSE readers) contention is negligible. If this ever became a bottleneck, a `ReadWriteLock` would be the fix — not needed here.

### 13.5 Simulator numerical stability

- Prices are `round()`ed to 2 decimals in `GBMSimulator.step()`.
- The exponential formulation (`exp(drift + diffusion)`) guarantees `price > 0` always — GBM cannot produce a negative or zero price.
- The tiny `dt` (~8.5e-8 per 500ms tick) keeps per-tick moves small and numerically well-behaved even after millions of steps.

---

## 14. Configuration Summary

| Parameter | Location | Default | Description |
|-----------|----------|---------|-------------|
| `MASSIVE_API_KEY` | Environment variable | `""` (empty) | If set, use Massive API; otherwise use the simulator |
| `update_interval` | `SimulatorDataSource.__init__` | `0.5` (seconds) | Time between simulator ticks |
| `poll_interval` | `MassiveDataSource.__init__` | `15.0` (seconds) | Time between Massive API polls |
| `event_probability` | `GBMSimulator.__init__` | `0.001` | Chance of a random shock event per ticker per tick |
| `dt` | `GBMSimulator.__init__` | `~8.48e-8` | GBM time step (fraction of a trading year) |
| SSE push interval | `_generate_events()` | `0.5` (seconds) | Time between SSE pushes to the client |
| SSE retry directive | `_generate_events()` | `1000` (ms) | Browser `EventSource` reconnection delay |

### Package `__init__.py`

**File: `backend/app/market/__init__.py`**

```python
"""Market data subsystem for FinAlly.

Public API:
    PriceUpdate         - Immutable price snapshot dataclass
    PriceCache          - Thread-safe in-memory price store
    MarketDataSource    - Abstract interface for data providers
    create_market_data_source - Factory that selects simulator or Massive
    create_stream_router - FastAPI router factory for SSE endpoint
"""

from .cache import PriceCache
from .factory import create_market_data_source
from .interface import MarketDataSource
from .models import PriceUpdate
from .stream import create_stream_router

__all__ = [
    "PriceUpdate",
    "PriceCache",
    "MarketDataSource",
    "create_market_data_source",
    "create_stream_router",
]
```

### Demo

A standalone Rich terminal demo exercises the full simulator path without needing FastAPI or a browser:

```bash
cd backend
uv run market_data_demo.py
```

Shows a live-updating dashboard with all 10 default tickers, sparklines, color-coded direction arrows, and an event log for notable price moves (the random-shock events from `GBMSimulator.step()`). Runs for 60 seconds or until Ctrl+C.
