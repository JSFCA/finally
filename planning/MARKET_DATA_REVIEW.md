# Market Data Backend — Code Review

**Date:** 2026-08-18
**Scope:** `backend/app/market/` (9 files) and `backend/tests/market/` (7 test files), against `main` at commit `f008fe9`.
**Reviewer note:** An earlier review exists at `planning/archive/MARKET_DATA_REVIEW.md` (2026-02-10). This is a fresh, independent pass against the current code, run end-to-end (`uv sync`, `pytest`, coverage, `ruff`) rather than a re-read of that document.

---

## 1. Test Results

**83 tests collected, 83 passed, 0 failed.**

```
uv sync --extra dev            → resolves and builds cleanly
uv run --extra dev pytest -v   → 83 passed, 83 warnings in 4.30s
uv run --extra dev pytest --cov=app --cov-report=term-missing
uv run --extra dev ruff check app/ tests/  → All checks passed!
```

| Module | Coverage | Missing |
|---|---|---|
| `models.py` | 100% | — |
| `cache.py` | 100% | — |
| `interface.py` | 100% | — |
| `seed_prices.py` | 100% | — |
| `factory.py` | 100% | — |
| `__init__.py` | 100% | — |
| `simulator.py` | 98% | L149 (dup-add guard in `_add_ticker_internal`, only reachable via batch init with a repeated ticker), L268-269 (exception-log branch in `_run_loop`) |
| `massive_client.py` | 94% | L85-87 (`_poll_loop`'s sleep/poll body — never actually looped to completion in tests, by design), L125 (`_fetch_snapshots` body — the real network call, intentionally mocked out) |
| `stream.py` | 97% | L38 (`return StreamingResponse(...)` line itself — no test drives the route through a real ASGI request) |

**Overall: 98%.** All misses are defensive/boundary code that's reasonable to leave untested at this scale (real network calls, infinite-loop bodies, duplicate-add guards).

The one prior blocker — `pyproject.toml` missing `[tool.hatch.build.targets.wheel] packages = ["app"]`, which broke `uv sync` — is fixed. `massive` is a normal top-level dependency now (no lazy import), so the whole suite runs without any environment workarounds. Every issue the 2026-02-10 review flagged as "must fix" or "should fix" (build config, Massive test fragility, `_generate_events` return type, `get_tickers()` private-attribute access, `DEFAULT_CORR` naming, unused test imports, missing SSE tests) has been verified fixed by reading the current source and confirming with grep/tests — I did not just take the summary doc's word for it.

---

## 2. Architecture Assessment

Confirmed against source, not just the design doc: strategy pattern (`MarketDataSource` ABC → `SimulatorDataSource` / `MassiveDataSource`) writing into a shared `PriceCache`, read by the SSE endpoint. Clean separation, single point of truth, source-agnostic downstream. Ran the full 10-ticker default watchlist through `GBMSimulator` directly (500 steps) to confirm the Cholesky decomposition is well-behaved at the real production ticker count, not just the 1-2 ticker cases the unit tests use — no errors, positive-definite matrix throughout.

**Strengths, verified by reading:**
- `PriceUpdate` is `frozen=True, slots=True` — correct for a value object created ~20x/sec.
- GBM math is textbook-correct: `exp((mu - 0.5*sigma^2)*dt + sigma*sqrt(dt)*Z)`, which structurally guarantees `price > 0`.
- Both background loops (`_run_loop`, `_poll_loop`/`_poll_once`) catch and log exceptions per iteration rather than letting one bad tick kill the task — appropriate for a long-running service.
- `threading.Lock` (not `asyncio.Lock`) is the right primitive since `MassiveDataSource` calls into `PriceCache` from `asyncio.to_thread()`, a real OS thread.
- SSE version-counter change detection is minimal and correct; verified the actual behavior in `test_stream.py`'s `test_does_not_resend_unchanged_version` / `test_resends_on_version_change`.

---

## 3. Issues Found

### 3.1 `stream.py` module-level `router` causes duplicate route registration on repeated calls (Severity: Medium) — FIXED

`stream.py:17` declares `router = APIRouter(...)` at module scope, and `create_stream_router()` registers `/prices` on that same shared object via closure every time it's called. I verified this is not just a theoretical footgun — it reproduces immediately:

```python
from app.market.cache import PriceCache
from app.market.stream import create_stream_router

cache = PriceCache()
r1 = create_stream_router(cache)
r2 = create_stream_router(cache)
# r1 is r2  → True
# r1 route paths → ['/api/stream/prices', '/api/stream/prices']
```

`test_stream.py` itself calls `create_stream_router()` twice — once in `test_returns_api_router`, once in `test_registers_prices_route` — so by the time the test module finishes, the shared module-level router already carries two duplicate route registrations. `test_registers_prices_route` doesn't catch this because it collects paths into a `set`, which silently absorbs the duplicate.

In production this is currently harmless because `create_stream_router()` is called exactly once during app startup (per the lifespan example in `planning/MARKET_DATA_DESIGN.md` §10). But it's a real landmine for whoever writes `app/main.py`'s tests: any test fixture that builds a fresh `FastAPI()` app per test and calls `create_stream_router(cache)` each time (a completely natural pattern) will keep appending routes to the same underlying router, and FastAPI resolves routes in registration order — the first (possibly stale-cache) match always wins, silently.

**Fix:** build a fresh `APIRouter()` inside `create_stream_router()` instead of closing over a module-level instance:

```python
def create_stream_router(price_cache: PriceCache) -> APIRouter:
    router = APIRouter(prefix="/api/stream", tags=["streaming"])

    @router.get("/prices")
    async def stream_prices(request: Request) -> StreamingResponse:
        ...

    return router
```

This was flagged in the 2026-02-10 review (§3.6, "nice to have") but was not fixed — the module-level `router` is unchanged since the original implementation commit. I'm raising its severity to Medium because I have concrete reproduction showing it already causes duplicate registrations within the current test suite, not just a hypothetical.

**Fixed:** `create_stream_router()` now builds a fresh `APIRouter()` per call instead of closing over a module-level instance. Verified independently:

```python
r1 = create_stream_router(cache)
r2 = create_stream_router(cache)
# r1 is r2         → False
# len(r1.routes)   → 1
# len(r2.routes)   → 1
```

Added a regression test, `test_repeated_calls_do_not_share_router`, to `test_stream.py` so this can't silently reappear.

### 3.2 `PriceCache.version` read outside the lock (Severity: Low, unchanged from prior review)

`cache.py:64-67` reads `self._version` without acquiring `self._lock`. Correct today under CPython's GIL (a single `int` read/write is atomic), and the class docstring/design doc already call this out as an accepted tradeoff. No action needed unless the project moves to a no-GIL build — noting it here only for continuity with the prior review, not as a new finding.

### 3.3 Minor coverage gaps are all in intentionally-unexercised code (Severity: Trivial)

The three modules below 100% (`simulator.py` 98%, `massive_client.py` 94%, `stream.py` 97%) are missing coverage only on: a duplicate-add guard that's unreachable given how `add_ticker()` already filters duplicates before calling the internal helper, an infinite-loop body that tests intentionally never let run to completion, the literal network call in `_fetch_snapshots` (mocked everywhere, correctly), and the `return StreamingResponse(...)` line of the route handler itself (would need a real ASGI client to hit). None of these represent untested logic — they're boundary/defensive lines. Not worth chasing further at this project's stage.

---

## 4. Design Observations

- **`_pairwise_correlation`'s TSLA special-case** short-circuits before checking the `tech` set even though `TSLA` is itself a member of `CORRELATION_GROUPS["tech"]`. This is intentional and documented (TSLA "does its own thing"), and correctly tested (`test_pairwise_correlation_tsla`). No issue — mentioning only because it's a natural rules-ordering question when reading `seed_prices.py` and `simulator.py` side by side.
- **Massive error handling is appropriately resilient** — 401/429/network errors are all logged and retried on the next poll cycle rather than crashing the background task, and malformed individual snapshots are skipped without discarding the rest of the batch. Verified this directly in `test_malformed_snapshot_skipped` and `test_api_error_does_not_crash`.
- **Nothing in `app/market/` depends on FastAPI DI or the database**, consistent with the design doc's claim — confirmed by inspection of all 9 source files. This should make wiring into `app/main.py` straightforward once that layer exists.

---

## 5. Verdict

The market data backend is in good shape: architecture is sound, math is correct, error handling is resilient, and the test suite is real and thorough (98% coverage, all defensible gaps). The one prior blocking issue (build config) is fixed and verified by an actual clean `uv sync`.

**Fixed in this pass:**
1. §3.1 — `stream.py` no longer closes over a module-level `router`; each `create_stream_router()` call now returns an independent `APIRouter()`. Regression test added.

**No other changes needed.** Items 3.2 and 3.3 are accepted tradeoffs, not defects.

**Status after fix: 84 tests, all passing, 98% coverage, ruff clean.** The market data backend is ready for `app/main.py` to wire in.
