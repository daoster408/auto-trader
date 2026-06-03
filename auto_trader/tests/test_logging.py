import logging
import sys
from io import StringIO

from auto_trader.utils.logging import (
    RedactingFormatter,
    RedactingLogFilter,
    _redacting_plain_traceback,
    redact_sensitive,
)


def test_redact_sensitive_removes_telegram_bot_token_from_url():
    raw = (
        "HTTP Request: POST "
        "https://api.telegram.org/bot123456789:ABCdef_GHIjkl-MNO123/getUpdates "
        '"HTTP/1.1 200 OK"'
    )

    redacted = redact_sensitive(raw)

    assert "123456789:ABCdef" not in redacted
    assert "https://api.telegram.org/bot<redacted>/getUpdates" in redacted


def test_redact_sensitive_removes_query_tokens_recursively():
    raw = {
        "quote_url": "https://finnhub.io/api/v1/quote?symbol=AAPL&token=secret-token",
        "nested": ["https://example.test/data?api_key=secret-key&symbol=AAPL"],
    }

    redacted = redact_sensitive(raw)

    assert "secret-token" not in str(redacted)
    assert "secret-key" not in str(redacted)
    assert "token=<redacted>" in redacted["quote_url"]
    assert "api_key=<redacted>" in redacted["nested"][0]


def test_redact_sensitive_removes_structured_secret_fields_by_key():
    raw = {
        "headers": {
            "Authorization": "Bearer auth-secret",
            "X-API-Key": "provider-key",
            "APCA-API-SECRET-KEY": "alpaca-secret",
        },
        "body": {
            "token": "body-token",
            "safe_symbol": "AAPL",
        },
    }

    redacted = redact_sensitive(raw)

    assert "auth-secret" not in str(redacted)
    assert "provider-key" not in str(redacted)
    assert "alpaca-secret" not in str(redacted)
    assert "body-token" not in str(redacted)
    assert redacted["headers"]["Authorization"] == "<redacted>"
    assert redacted["headers"]["X-API-Key"] == "<redacted>"
    assert redacted["headers"]["APCA-API-SECRET-KEY"] == "<redacted>"
    assert redacted["body"]["token"] == "<redacted>"
    assert redacted["body"]["safe_symbol"] == "AAPL"


def test_redacting_log_filter_redacts_stdlib_log_record_args():
    record = logging.LogRecord(
        name="httpx",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="HTTP Request: %s",
        args=("https://api.telegram.org/bot123456789:ABCdef_GHIjkl-MNO123/sendMessage",),
        exc_info=None,
    )

    assert RedactingLogFilter().filter(record) is True

    rendered = record.getMessage()
    assert "123456789:ABCdef" not in rendered
    assert "bot<redacted>/sendMessage" in rendered


def test_redacting_formatter_redacts_stdlib_exception_text():
    try:
        raise RuntimeError("failed https://api.telegram.org/bot123456789:ABCdef_GHIjkl-MNO123/getMe")
    except RuntimeError:
        exc_info = sys.exc_info()

    record = logging.LogRecord(
        name="auto_trader.test",
        level=logging.ERROR,
        pathname=__file__,
        lineno=1,
        msg="request failed",
        args=(),
        exc_info=exc_info,
    )

    rendered = RedactingFormatter("%(message)s").format(record)

    assert "123456789:ABCdef" not in rendered
    assert "bot<redacted>/getMe" in rendered


def test_structlog_plain_traceback_formatter_redacts_exception_text():
    try:
        raise RuntimeError("failed https://finnhub.io/api/v1/quote?symbol=AAPL&token=secret-token")
    except RuntimeError:
        exc_info = sys.exc_info()

    output = StringIO()
    _redacting_plain_traceback(output, exc_info)
    rendered = output.getvalue()

    assert "secret-token" not in rendered
    assert "token=<redacted>" in rendered
