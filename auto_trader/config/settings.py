"""Configuration and settings (Pydantic v2)."""
import os

from pydantic_settings import BaseSettings
from pydantic import Field, model_validator
from typing import Literal


class Settings(BaseSettings):
    """Validated environment configuration. Fails fast on missing critical values."""

    # Alpaca
    alpaca_api_key: str = Field(..., alias="ALPACA_API_KEY")
    alpaca_api_secret: str = Field(..., alias="ALPACA_API_SECRET")
    alpaca_paper: bool = Field(True, alias="ALPACA_PAPER")

    # Telegram
    telegram_bot_token: str = Field(..., alias="TELEGRAM_BOT_TOKEN")
    telegram_allowed_ids: str = Field("", alias="TELEGRAM_ALLOWED_IDS")

    # Safety
    resume_token: str = Field(..., alias="RESUME_TOKEN")
    shutdown_flatten_on_exit: bool = Field(True, alias="SHUTDOWN_FLATTEN_ON_EXIT")

    # Risk (v1 defaults - changes require DECISIONS_LOG entry + restart)
    risk_per_trade_pct: float = Field(0.5, alias="RISK_PER_TRADE_PCT")
    risk_profile: Literal["conservative", "aggressive", "risky"] = Field("conservative", alias="RISK_PROFILE")
    max_new_positions_per_day: int = Field(1, alias="MAX_NEW_POSITIONS_PER_DAY")
    max_gross_exposure_pct: float = Field(25.0, alias="MAX_GROSS_EXPOSURE_PCT")
    daily_loss_halt_pct: float = Field(-1.75, alias="DAILY_LOSS_HALT_PCT")
    weekly_loss_halt_pct: float = Field(-4.0, alias="WEEKLY_LOSS_HALT_PCT")
    peak_drawdown_halt_pct: float = Field(-6.0, alias="PEAK_DRAWDOWN_HALT_PCT")
    consecutive_sl_halt: int = Field(2, alias="CONSECUTIVE_SL_HALT")

    # Supervisor / automation controls (default alert-only)
    reconcile_interval_seconds: int = Field(300, ge=60, alias="RECONCILE_INTERVAL_SECONDS")
    reconcile_lookback_days: int = Field(2, ge=1, alias="RECONCILE_LOOKBACK_DAYS")
    position_monitor_interval_seconds: int = Field(60, ge=15, alias="POSITION_MONITOR_INTERVAL_SECONDS")
    supervisor_tick_timeout_seconds: int = Field(20, ge=1, alias="SUPERVISOR_TICK_TIMEOUT_SECONDS")
    auto_entry_enabled: bool = Field(False, alias="AUTO_ENTRY_ENABLED")
    auto_exit_enabled: bool = Field(False, alias="AUTO_EXIT_ENABLED")
    position_max_loss_pct: float = Field(-5.0, alias="POSITION_MAX_LOSS_PCT")
    position_take_profit_pct: float = Field(8.0, alias="POSITION_TAKE_PROFIT_PCT")
    position_trailing_stop_pct: float = Field(6.0, ge=0.0, alias="POSITION_TRAILING_STOP_PCT")
    position_max_hold_days: int = Field(10, ge=1, alias="POSITION_MAX_HOLD_DAYS")

    report_timezone: str = Field("America/Los_Angeles", alias="REPORT_TIMEZONE")
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = Field("INFO", alias="LOG_LEVEL")

    # Persistence
    db_path: str = Field("auto_trader.db", alias="DB_PATH")
    scoreboard_memory_path: str | None = Field(None, alias="AUTO_TRADER_SCOREBOARD_MEMORY_PATH")

    # Optional LLM (populated later)
    finnhub_api_key: str | None = Field(None, alias="FINNHUB_API_KEY")
    fred_api_key: str | None = Field(None, alias="FRED_API_KEY")
    ai_research_enabled: bool = Field(True, alias="AI_RESEARCH_ENABLED")
    ai_entry_gate_enabled: bool = Field(False, alias="AI_ENTRY_GATE_ENABLED")
    ai_research_provider: Literal["shadow", "openai", "xai", "anthropic", "gemini"] = Field(
        "shadow",
        alias="AI_RESEARCH_PROVIDER",
    )
    ai_research_providers: str = Field("", alias="AI_RESEARCH_PROVIDERS")
    ai_research_model: str = Field("", alias="AI_RESEARCH_MODEL")
    ai_research_openai_model: str = Field("", alias="AI_RESEARCH_OPENAI_MODEL")
    ai_research_xai_model: str = Field("", alias="AI_RESEARCH_XAI_MODEL")
    ai_research_anthropic_model: str = Field("", alias="AI_RESEARCH_ANTHROPIC_MODEL")
    ai_research_gemini_model: str = Field("", alias="AI_RESEARCH_GEMINI_MODEL")
    ai_research_timeout_seconds: float = Field(8.0, ge=1.0, le=15.0, alias="AI_RESEARCH_TIMEOUT_SECONDS")
    ai_research_max_calls_per_day: int = Field(0, ge=0, alias="AI_RESEARCH_MAX_CALLS_PER_DAY")
    ai_high_exposure_unanimous_threshold_pct: float = Field(
        60.0,
        ge=0.0,
        le=100.0,
        alias="AI_HIGH_EXPOSURE_UNANIMOUS_THRESHOLD_PCT",
    )
    ai_paid_prefilter_enabled: bool = Field(True, alias="AI_PAID_PREFILTER_ENABLED")
    ai_paid_prefilter_min_rel_volume: float = Field(1.0, ge=0.0, alias="AI_PAID_PREFILTER_MIN_REL_VOLUME")
    ai_paid_prefilter_strong_rel_volume: float = Field(2.5, ge=0.0, alias="AI_PAID_PREFILTER_STRONG_REL_VOLUME")
    ai_paid_prefilter_high_buffer_pct: float = Field(0.002, ge=0.0, alias="AI_PAID_PREFILTER_HIGH_BUFFER_PCT")
    ai_paid_prefilter_block_inverse_overlap: bool = Field(True, alias="AI_PAID_PREFILTER_BLOCK_INVERSE_OVERLAP")
    ai_research_est_input_tokens: int = Field(15000, ge=0, alias="AI_RESEARCH_EST_INPUT_TOKENS")
    ai_research_est_output_tokens: int = Field(2000, ge=0, alias="AI_RESEARCH_EST_OUTPUT_TOKENS")
    ai_research_input_price_per_mtok: float = Field(5.0, ge=0.0, alias="AI_RESEARCH_INPUT_PRICE_PER_MTOK")
    ai_research_output_price_per_mtok: float = Field(25.0, ge=0.0, alias="AI_RESEARCH_OUTPUT_PRICE_PER_MTOK")
    openai_api_key: str | None = Field(None, alias="OPENAI_API_KEY")
    anthropic_api_key: str | None = Field(None, alias="ANTHROPIC_API_KEY")
    xai_api_key: str | None = Field(None, alias="XAI_API_KEY")
    gemini_api_key: str | None = Field(None, alias="GEMINI_API_KEY")

    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "populate_by_name": True,
        "extra": "forbid",
    }

    @model_validator(mode="after")
    def validate_shutdown_flatten_safety(self) -> "Settings":
        if not self.alpaca_paper and not self.shutdown_flatten_on_exit:
            raise ValueError("SHUTDOWN_FLATTEN_ON_EXIT=false is only allowed in Alpaca paper mode")
        return self


def get_settings() -> Settings:
    """Singleton-style loader (call once at startup)."""
    return Settings(_env_file=os.getenv("AUTO_TRADER_ENV_FILE", ".env"))  # type: ignore[call-arg]
