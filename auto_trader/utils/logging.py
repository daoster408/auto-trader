"""Production-grade structured logging for AUTO-TRADER.

Enforces:
- UTC timestamps (ISO8601)
- model_tag injection for all AI/decision paths
- trace_id / correlation
- JSON or console renderer (env driven)
- Compatible with Oracle ARM low-mem (no heavy deps beyond structlog)

Usage:
    from auto_trader.utils.logging import setup_logging, get_logger, bind_context
    setup_logging(level="INFO", model_tag="optimizer/harden-2026-06-01")
    log = get_logger(__name__)
    with bind_context(trace_id="abc123"):
        log.info("event", extra={"symbol": "AAPL"})
"""
import logging
import os
import re
import sys
from contextlib import contextmanager
from datetime import UTC, datetime
from io import StringIO
from typing import Any
from collections.abc import Generator

import structlog

# Force UTC for everything
logging.Formatter.converter = lambda *args: datetime.now(UTC).timetuple()

STRUCTLOG_CONFIGURED = False
_CURRENT_MODEL_TAG: str | None = None

_TELEGRAM_API_TOKEN_RE = re.compile(r"(api\.telegram\.org/bot)[^/\s\"']+")
_TELEGRAM_BOT_TOKEN_RE = re.compile(r"\bbot\d{6,}:[A-Za-z0-9_-]+")
_QUERY_SECRET_RE = re.compile(
    r"([?&](?:token|api_key|apikey|key|secret)=)[^&\s\"']+",
    re.IGNORECASE,
)
_AUTH_HEADER_RE = re.compile(
    r"((?:Authorization|Api-Key|X-API-Key):\s*)(?:Bearer\s+|Basic\s+)?[A-Za-z0-9._~+/\-=:]+",
    re.IGNORECASE,
)
_SENSITIVE_KEY_NAMES = {
    "apikey",
    "authorization",
    "key",
    "password",
    "secret",
    "token",
    "xaccesstoken",
    "xapikey",
    "xauth",
    "xauthorization",
    "apcaapikeyid",
    "apcaapisecretkey",
}


def _is_sensitive_key(key: Any) -> bool:
    normalized = re.sub(r"[^a-z0-9]", "", str(key).lower())
    return normalized in _SENSITIVE_KEY_NAMES or any(
        marker in normalized for marker in ("apikey", "authorization", "password", "secret", "token")
    )


def redact_sensitive(value: Any) -> Any:
    """Redact secrets from log values before they reach stdout/systemd."""
    if isinstance(value, str):
        redacted = _TELEGRAM_API_TOKEN_RE.sub(r"\1<redacted>", value)
        redacted = _TELEGRAM_BOT_TOKEN_RE.sub("bot<redacted>", redacted)
        redacted = _QUERY_SECRET_RE.sub(r"\1<redacted>", redacted)
        return _AUTH_HEADER_RE.sub(r"\1<redacted>", redacted)
    if isinstance(value, tuple):
        return tuple(redact_sensitive(item) for item in value)
    if isinstance(value, list):
        return [redact_sensitive(item) for item in value]
    if isinstance(value, dict):
        return {
            key: "<redacted>" if _is_sensitive_key(key) else redact_sensitive(item)
            for key, item in value.items()
        }
    return value


class RedactingLogFilter(logging.Filter):
    """Stdlib logging filter for third-party libraries such as httpx."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = redact_sensitive(record.msg)
        if record.args:
            record.args = redact_sensitive(record.args)
        return True


class RedactingFormatter(logging.Formatter):
    """Stdlib formatter that also redacts exception/traceback text."""

    def format(self, record: logging.LogRecord) -> str:
        return redact_sensitive(super().format(record))

    def formatException(self, ei: tuple[type[BaseException], BaseException, Any] | tuple[None, None, None]) -> str:
        return redact_sensitive(super().formatException(ei))


def _redact_structlog_event_dict(logger: Any, method_name: str, event_dict: dict[str, Any]) -> dict[str, Any]:
    return redact_sensitive(event_dict)


def _redacting_plain_traceback(sio: Any, exc_info: Any) -> None:
    buffer = StringIO()
    structlog.dev.plain_traceback(buffer, exc_info)
    sio.write(redact_sensitive(buffer.getvalue()))


def setup_logging(
    level: str = "INFO",
    model_tag: str | None = None,
    json_logs: bool = False,
) -> None:
    """Idempotent structured logging setup. Call once at startup.

    - All timestamps UTC.
    - model_tag bound globally for AI decisions.
    - Prepares for cheap hosting: minimal overhead.
    """
    global STRUCTLOG_CONFIGURED, _CURRENT_MODEL_TAG

    if STRUCTLOG_CONFIGURED:
        return

    _CURRENT_MODEL_TAG = model_tag or os.getenv("MODEL_TAG", "auto-trader/v0")

    # Standard logging first (for libs)
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)sZ %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        stream=sys.stdout,
    )
    root_logger = logging.getLogger()
    redacting_filter = RedactingLogFilter()
    root_logger.addFilter(redacting_filter)
    for handler in root_logger.handlers:
        handler.addFilter(redacting_filter)
        handler.setFormatter(
            RedactingFormatter(
                fmt="%(asctime)sZ %(levelname)s %(name)s %(message)s",
                datefmt="%Y-%m-%dT%H:%M:%S",
            )
        )

    # Structlog processors - strict UTC + context
    processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True, key="ts"),
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        _redact_structlog_event_dict,
    ]

    if json_logs or os.getenv("LOG_FORMAT", "").lower() == "json":
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(
            structlog.dev.ConsoleRenderer(
                colors=True,
                exception_formatter=_redacting_plain_traceback,
            )
        )

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, level.upper(), logging.INFO)
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )

    STRUCTLOG_CONFIGURED = True
    get_logger("auto_trader.utils.logging").info(
        "structured_logging_initialized",
        level=level,
        model_tag=_CURRENT_MODEL_TAG,
        json=json_logs,
    )


def get_logger(name: str) -> structlog.BoundLogger:
    """Get structured logger. Auto-injects current model_tag if set."""
    log = structlog.get_logger(name)
    if _CURRENT_MODEL_TAG:
        log = log.bind(model_tag=_CURRENT_MODEL_TAG)
    return log


@contextmanager
def bind_context(**kwargs: Any) -> Generator[None, None, None]:
    """Context manager for per-operation binding (trace_id, symbol, etc)."""
    structlog.contextvars.bind_contextvars(**kwargs)
    try:
        yield
    finally:
        structlog.contextvars.unbind_contextvars(*kwargs.keys())


def get_model_tag() -> str | None:
    return _CURRENT_MODEL_TAG


# Back-compat shim for old logging calls during transition
def get_fallback_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
