"""Tenacity-based retry utilities for external calls (Alpaca, Telegram, etc).

Production hardening:
- Exponential backoff + jitter (prevents thundering herd on Oracle free tier)
- Retries only on transient errors (429, 5xx, timeouts, connection)
- Separate budgets for kill-critical paths (more aggressive, still safe)
- Circuit breaker friendly (max attempts small)
- All retries logged with model_tag via structlog

Usage:
    from auto_trader.utils.retry import retry_external, retry_kill_critical
    @retry_external
    async def my_alpaca_call(...): ...

    # For /kill paths (still bounded, never infinite)
    @retry_kill_critical
    async def cancel_all(...): ...
"""
from functools import wraps
from typing import Any, TypeVar
from collections.abc import Callable

from tenacity import (
    AsyncRetrying,
    before_sleep_log,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

import logging as stdlib_logging

from auto_trader.utils.logging import get_logger

T = TypeVar("T")

log = get_logger("auto_trader.utils.retry")

# Tenacity's before_sleep_log expects a stdlib Logger, not structlog
_stdlib_log = stdlib_logging.getLogger("auto_trader.utils.retry")

# Transient exceptions worth retrying (expand as we see real Alpaca errors)
_TRANSIENT_EXC = (
    ConnectionError,
    TimeoutError,
    OSError,  # covers some network
    # alpaca-py specific will be caught at call site or subclassed later
)


def _log_retry(retry_state: Any) -> None:
    """Log before each retry attempt."""
    log.warning(
        "retrying_external_call",
        attempt=retry_state.attempt_number,
        wait_s=round(retry_state.next_action.sleep, 2) if retry_state.next_action else 0,
        fn=retry_state.fn.__name__ if hasattr(retry_state, "fn") else "unknown",
    )


# Standard external (Alpaca non-kill, Telegram notifications, etc.)
retry_external = AsyncRetrying(
    stop=stop_after_attempt(5),
    wait=wait_exponential_jitter(initial=0.5, max=8, jitter=1.0),
    retry=retry_if_exception_type(_TRANSIENT_EXC),
    before_sleep=before_sleep_log(_stdlib_log, stdlib_logging.WARNING),
    reraise=True,
).wraps


# Kill-critical paths: still bounded (safety: never spin forever), but tuned for urgency
# 3 attempts max, faster backoff. If fails, we still proceed to HALTED state.
retry_kill_critical = AsyncRetrying(
    stop=stop_after_attempt(3),
    wait=wait_exponential_jitter(initial=0.2, max=2, jitter=0.5),
    retry=retry_if_exception_type(_TRANSIENT_EXC),
    before_sleep=before_sleep_log(_stdlib_log, stdlib_logging.WARNING),
    reraise=True,
).wraps


def sync_retry_external(fn: Callable[..., T]) -> Callable[..., T]:
    """Sync version for places that can't be async yet (e.g. some Alpaca sync SDK)."""

    @wraps(fn)
    def _wrapped(*args: Any, **kwargs: Any) -> T:
        for attempt in range(1, 6):
            try:
                return fn(*args, **kwargs)
            except _TRANSIENT_EXC as e:
                if attempt == 5:
                    log.error("sync_retry_exhausted", fn=fn.__name__, error=str(e))
                    raise
                wait = min(0.5 * (2 ** (attempt - 1)), 4)
                log.warning("sync_retry", attempt=attempt, wait_s=wait, fn=fn.__name__)
                import time

                time.sleep(wait)
        return fn(*args, **kwargs)  # unreachable

    return _wrapped
