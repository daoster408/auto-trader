import logging

from auto_trader.utils.logging import RedactingLogFilter, redact_sensitive


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
