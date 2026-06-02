"""Configuration and settings (Pydantic v2)."""
from pydantic_settings import BaseSettings
from pydantic import Field
from typing import Literal


class Settings(BaseSettings):
    """Validated environment configuration. Fails fast on missing critical values."""

    # Alpaca
    alpaca_api_key: str = Field(..., alias="ALPACA_API_KEY")
    alpaca_api_secret: str = Field(..., alias="ALPACA_API_SECRET")
    alpaca_paper: bool = Field(True, alias="ALPACA_PAPER")

    # Telegram
    telegram_bot_token: str = Field(..., alias="TELEGRAM_BOT_TOKEN")

    # Safety
    resume_token: str = Field(..., alias="RESUME_TOKEN")

    # Risk (v1 defaults - changes require DECISIONS_LOG entry + restart)
    risk_per_trade_pct: float = Field(0.5, alias="RISK_PER_TRADE_PCT")
    max_new_positions_per_day: int = Field(1, alias="MAX_NEW_POSITIONS_PER_DAY")
    max_gross_exposure_pct: float = Field(25.0, alias="MAX_GROSS_EXPOSURE_PCT")
    daily_loss_halt_pct: float = Field(-1.75, alias="DAILY_LOSS_HALT_PCT")
    weekly_loss_halt_pct: float = Field(-4.0, alias="WEEKLY_LOSS_HALT_PCT")
    peak_drawdown_halt_pct: float = Field(-6.0, alias="PEAK_DRAWDOWN_HALT_PCT")
    consecutive_sl_halt: int = Field(2, alias="CONSECUTIVE_SL_HALT")

    report_timezone: str = Field("America/Los_Angeles", alias="REPORT_TIMEZONE")
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = Field("INFO", alias="LOG_LEVEL")

    # Persistence
    db_path: str = Field("auto_trader.db", alias="DB_PATH")

    # Optional LLM (populated later)
    openai_api_key: str | None = Field(None, alias="OPENAI_API_KEY")
    anthropic_api_key: str | None = Field(None, alias="ANTHROPIC_API_KEY")
    xai_api_key: str | None = Field(None, alias="XAI_API_KEY")

    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "populate_by_name": True,
        "extra": "forbid",
    }


def get_settings() -> Settings:
    """Singleton-style loader (call once at startup)."""
    return Settings()  # type: ignore[call-arg]
