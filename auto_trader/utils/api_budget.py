"""Quiet API usage budget guard.

Tracks recent external API call attempts so the bot can protect safety-critical
paths while throttling nonessential work such as candidate discovery.
"""
from __future__ import annotations

from collections import Counter, deque
from dataclasses import dataclass
from threading import Lock
from time import monotonic

from auto_trader.utils.logging import get_logger

log = get_logger("auto_trader.utils.api_budget")


@dataclass(frozen=True)
class ApiBudgetSnapshot:
    total: int
    window_seconds: int
    by_endpoint: dict[str, int]
    warning_threshold: int
    critical_threshold: int


class ApiBudget:
    """Rolling-window call counter with quiet threshold logging."""

    def __init__(
        self,
        *,
        name: str,
        window_seconds: int = 60,
        warning_threshold: int = 50,
        critical_threshold: int = 100,
    ) -> None:
        self.name = name
        self.window_seconds = window_seconds
        self.warning_threshold = warning_threshold
        self.critical_threshold = critical_threshold
        self._events: deque[tuple[float, str]] = deque()
        self._lock = Lock()
        self._last_warning_bucket: int | None = None
        self._last_critical_bucket: int | None = None
        self._last_defer_bucket_by_endpoint: dict[str, int] = {}

    def _trim(self, now: float) -> None:
        cutoff = now - self.window_seconds
        while self._events and self._events[0][0] < cutoff:
            self._events.popleft()

    def snapshot(self) -> ApiBudgetSnapshot:
        now = monotonic()
        with self._lock:
            self._trim(now)
            counts = Counter(endpoint for _, endpoint in self._events)
            return ApiBudgetSnapshot(
                total=len(self._events),
                window_seconds=self.window_seconds,
                by_endpoint=dict(sorted(counts.items())),
                warning_threshold=self.warning_threshold,
                critical_threshold=self.critical_threshold,
            )

    def record(self, endpoint: str, *, essential: bool) -> ApiBudgetSnapshot:
        now = monotonic()
        with self._lock:
            self._events.append((now, endpoint))
            self._trim(now)
            counts = Counter(name for _, name in self._events)
            snapshot = ApiBudgetSnapshot(
                total=len(self._events),
                window_seconds=self.window_seconds,
                by_endpoint=dict(sorted(counts.items())),
                warning_threshold=self.warning_threshold,
                critical_threshold=self.critical_threshold,
            )
            bucket = int(now // self.window_seconds)
            if snapshot.total >= self.critical_threshold and self._last_critical_bucket != bucket:
                self._last_critical_bucket = bucket
                log.error(
                    "api_budget_critical",
                    name=self.name,
                    total=snapshot.total,
                    window_seconds=self.window_seconds,
                    endpoint=endpoint,
                    essential=essential,
                    by_endpoint=snapshot.by_endpoint,
                )
            elif snapshot.total >= self.warning_threshold and self._last_warning_bucket != bucket:
                self._last_warning_bucket = bucket
                log.warning(
                    "api_budget_warning",
                    name=self.name,
                    total=snapshot.total,
                    window_seconds=self.window_seconds,
                    endpoint=endpoint,
                    essential=essential,
                    by_endpoint=snapshot.by_endpoint,
                )
            return snapshot

    def allow_nonessential(self, endpoint: str) -> bool:
        snapshot = self.snapshot()
        if snapshot.total < self.critical_threshold:
            return True
        bucket = int(monotonic() // self.window_seconds)
        with self._lock:
            last_bucket = self._last_defer_bucket_by_endpoint.get(endpoint)
            if last_bucket != bucket:
                self._last_defer_bucket_by_endpoint[endpoint] = bucket
                log.warning(
                    "api_budget_nonessential_deferred",
                    name=self.name,
                    endpoint=endpoint,
                    total=snapshot.total,
                    window_seconds=snapshot.window_seconds,
                    by_endpoint=snapshot.by_endpoint,
                )
        return False
