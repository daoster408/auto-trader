"""System state machine. Enforces the three states with strict transitions.

Non-negotiable: HALTED can only be exited via /resume <token> after manual review.
"""
import asyncio
from datetime import UTC, datetime
from collections.abc import Awaitable, Callable

from auto_trader.core.models import SystemState, SystemSnapshot, KillResult
from auto_trader.utils.logging import get_logger

log = get_logger("auto_trader.core.state_machine")


class StateMachine:
    """In-memory + persistence-backed state machine for v1.

    Persistence hook injected at construction (for later DB wiring).
    """

    def __init__(
        self,
        initial_state: SystemState = SystemState.ACTIVE,
        persist_hook: Callable[[SystemState, str | None], None] | Callable[[SystemState, str | None], Awaitable[None]] | None = None,  # type: ignore[assignment]
    ) -> None:
        self._state = initial_state
        self._persist = persist_hook or (lambda s, r: None)
        self._last_halt_reason: str | None = None
        self._last_halt_at: datetime | None = None
        log.info("state_machine_initialized", state=self._state.value)

    @property
    def state(self) -> SystemState:
        return self._state

    def can_trade(self) -> bool:
        """Only ACTIVE allows new entries."""
        return self._state == SystemState.ACTIVE

    def pause(self, reason: str = "manual") -> None:
        if self._state == SystemState.HALTED:
            raise RuntimeError("Cannot pause from HALTED. Use /resume first.")
        prev = self._state
        self._state = SystemState.PAUSED
        p = self._persist(self._state, reason)
        if asyncio.iscoroutine(p):
            # fire-and-forget safe persist (never block command path)
            asyncio.create_task(p)  # type: ignore[arg-type]
        log.warning("state_transition", prev=prev.value, new="PAUSED", reason=reason)

    def resume(self, token: str, expected_token: str) -> bool:
        """Resume requires exact token match. Only from PAUSED or HALTED."""
        if token != expected_token:
            log.error("resume_token_mismatch")
            return False
        if self._state == SystemState.ACTIVE:
            return True
        prev = self._state
        self._state = SystemState.ACTIVE
        self._last_halt_reason = None
        self._last_halt_at = None
        p = self._persist(self._state, "resume_success")
        if asyncio.iscoroutine(p):
            asyncio.create_task(p)  # type: ignore[arg-type]
        log.info("state_transition", prev=prev.value, new="ACTIVE", reason="authorized_resume")
        return True

    async def halt(self, reason: str, flatten_callback: Callable[[], KillResult] | Callable[[], Awaitable[KillResult]] | None = None) -> KillResult:
        """Move to HALTED (bulletproof). Supports sync or async flatten. Persists immediately.

        /kill and auto-halt paths call this. Never weakens safety.
        """
        prev = self._state
        self._state = SystemState.HALTED
        self._last_halt_reason = reason
        self._last_halt_at = datetime.now(UTC)
        p = self._persist(self._state, reason)
        if asyncio.iscoroutine(p):
            await p  # await persist for HALTED durability

        result = KillResult(
            success=True,
            orders_cancelled=0,
            positions_flattened=0,
            reason=reason,
            incident_report=f"HALTED at {self._last_halt_at.isoformat()}Z: {reason}",
            timestamp=self._last_halt_at,
        )

        if flatten_callback:
            try:
                maybe_result = flatten_callback()
                if asyncio.iscoroutine(maybe_result):
                    result = await maybe_result  # type: ignore[assignment]
                else:
                    result = maybe_result  # type: ignore[assignment]
            except Exception as e:
                log.exception("flatten_during_halt_failed", error=str(e))
                result = KillResult(
                    success=False,
                    orders_cancelled=result.orders_cancelled,
                    positions_flattened=result.positions_flattened,
                    reason=result.reason,
                    incident_report=result.incident_report + f" | Flatten error: {e}",
                    timestamp=result.timestamp,
                )

        log.critical("state_transition", prev=prev.value, new="HALTED", reason=reason)
        return result

    def get_snapshot(self, equity: float = 0.0, **kwargs) -> SystemSnapshot:
        """Build status snapshot for /status and risk engine."""
        return SystemSnapshot(
            state=self._state,
            equity=equity,
            cash=kwargs.get("cash", 0.0),
            open_positions=kwargs.get("open_positions", []),
            daily_pnl=kwargs.get("daily_pnl", 0.0),
            weekly_pnl=kwargs.get("weekly_pnl", 0.0),
            peak_equity=kwargs.get("peak_equity", equity),
            consecutive_losses=kwargs.get("consecutive_losses", 0),
            updated_at=datetime.now(UTC),
        )
