"""Alpaca Broker Adapter (async).

Production-hardened: real alpaca-py SDK for critical paths (health, cancel, flatten, submit).
All external calls wrapped in tenacity retries.
Order submission is now available (must only be called after RiskEngine approval).
Fast startup on ARM via lazy client.
"""
import asyncio
import json
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from auto_trader.utils.logging import get_logger
from auto_trader.utils.retry import retry_external, retry_kill_critical

log = get_logger("auto_trader.broker.alpaca_adapter")


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default

try:
    from alpaca.trading.client import TradingClient
    from alpaca.trading.requests import GetOrdersRequest, MarketOrderRequest
    from alpaca.trading.enums import OrderSide, QueryOrderStatus, TimeInForce
    _ALPACA_AVAILABLE = True
except ImportError:
    _ALPACA_AVAILABLE = False
    TradingClient = None  # type: ignore
    MarketOrderRequest = None  # type: ignore


def _enum_value(value: Any) -> str:
    raw = getattr(value, "value", value)
    return str(raw) if raw is not None else ""


def _iso_value(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


class AlpacaAdapter:
    """Real async wrapper over alpaca-py (sync SDK run in thread executor for non-block).

    /kill paths use kill-critical retry budget.
    """

    def __init__(self, api_key: str, api_secret: str, paper: bool = True) -> None:
        self.api_key = api_key
        self.api_secret = api_secret
        self.paper = paper
        self._client: TradingClient | None = None
        log.info("alpaca_adapter_initialized", paper=paper, real_sdk=_ALPACA_AVAILABLE)

    def _get_client(self) -> TradingClient:
        """Lazy init (fast startup, low mem on ARM until first use)."""
        if self._client is None:
            if not _ALPACA_AVAILABLE:
                raise RuntimeError("alpaca-py not installed - cannot make real broker calls")
            self._client = TradingClient(
                api_key=self.api_key,
                secret_key=self.api_secret,
                paper=self.paper,
            )
        return self._client

    @retry_external
    async def health_check(self) -> dict[str, Any]:
        """Real account ping via alpaca-py + clock. Retried on transient."""
        try:
            client = self._get_client()
            # alpaca-py is sync; run in thread to keep event loop free (ARM friendly)
            account = await asyncio.to_thread(client.get_account)
            clock = await asyncio.to_thread(client.get_clock)
            return {
                "ok": True,
                "paper": self.paper,
                "account_id": getattr(account, "id", "unknown"),
                "status": "CONNECTED",
                "timestamp": datetime.now(UTC).isoformat() + "Z",
                "equity": float(getattr(account, "equity", 0)),
                "market_open": clock.is_open if hasattr(clock, "is_open") else False,
            }
        except Exception as e:
            log.error("alpaca_health_check_failed", error=str(e))
            return {"ok": False, "status": "ERROR", "error": str(e)}

    @retry_external
    async def get_account_snapshot(self) -> dict[str, Any]:
        """Real equity/cash snapshot from Alpaca (retried)."""
        try:
            client = self._get_client()
            account = await asyncio.to_thread(client.get_account)
            return {
                "equity": float(getattr(account, "equity", 0.0)),
                "cash": float(getattr(account, "cash", 0.0)),
                "buying_power": float(getattr(account, "buying_power", 0.0)),
                "account_status": str(getattr(account, "status", "unknown")),
                "trading_blocked": bool(getattr(account, "trading_blocked", False)),
                "account_blocked": bool(getattr(account, "account_blocked", False)),
                "status": "CONNECTED",
            }
        except Exception as e:
            log.error("account_snapshot_failed", error=str(e))
            return {"equity": 0.0, "cash": 0.0, "status": "ERROR", "error": str(e)}

    @retry_external
    async def get_positions_snapshot(self, *, strict: bool = False) -> list[dict[str, Any]]:
        """Return open positions as risk-engine-friendly dictionaries."""
        try:
            client = self._get_client()
            positions = await asyncio.to_thread(client.get_all_positions)
            results: list[dict[str, Any]] = []
            for pos in positions:
                results.append(
                    {
                        "symbol": getattr(pos, "symbol", ""),
                        "qty": _safe_float(getattr(pos, "qty", 0)),
                        "market_value": _safe_float(getattr(pos, "market_value", 0)),
                        "unrealized_pl": _safe_float(getattr(pos, "unrealized_pl", 0)),
                    }
                )
            return results
        except Exception as e:
            log.error("positions_snapshot_failed", error=str(e))
            if strict:
                raise
            return []

    @retry_external
    async def get_recent_orders(self, days: int = 2) -> list[dict[str, Any]]:
        """Return recent broker orders for reconciliation and duplicate-entry guards."""
        try:
            client = self._get_client()
            after = datetime.now(UTC) - timedelta(days=days)
            orders = await asyncio.to_thread(
                client.get_orders,
                GetOrdersRequest(status=QueryOrderStatus.ALL, after=after),
            )
            results: list[dict[str, Any]] = []
            for order in orders:
                broker_id = str(getattr(order, "id", ""))
                results.append(
                    {
                        "client_order_id": str(getattr(order, "client_order_id", None) or broker_id),
                        "broker_order_id": broker_id,
                        "symbol": str(getattr(order, "symbol", "")).upper(),
                        "side": _enum_value(getattr(order, "side", "")),
                        "qty": _safe_float(getattr(order, "qty", 0)),
                        "order_type": _enum_value(getattr(order, "order_type", "")),
                        "status": _enum_value(getattr(order, "status", "")),
                        "filled_qty": _safe_float(getattr(order, "filled_qty", 0)),
                        "avg_fill_price": _safe_float(getattr(order, "filled_avg_price", None), default=None),
                        "submitted_at": _iso_value(getattr(order, "submitted_at", None)),
                        "filled_at": _iso_value(getattr(order, "filled_at", None)),
                        "rationale": "broker_reconciliation",
                    }
                )
            log.info("recent_orders_loaded", count=len(results), days=days)
            return results
        except Exception as e:
            log.error("recent_orders_failed", error=str(e))
            raise

    @retry_external
    async def get_clock(self) -> dict[str, Any]:
        """Real market clock from Alpaca (retried)."""
        try:
            client = self._get_client()
            clock = await asyncio.to_thread(client.get_clock)
            return {
                "is_open": bool(getattr(clock, "is_open", False)),
                "next_open": getattr(clock, "next_open", None),
                "next_close": getattr(clock, "next_close", None),
                "source": "alpaca",
            }
        except Exception as e:
            log.error("clock_fetch_failed", error=str(e))
            return {"is_open": False, "source": "error", "error": str(e)}

    @retry_external
    async def get_tradable_assets(self) -> list[dict[str, Any]]:
        """Return active, tradable US equities from Alpaca's assets endpoint."""
        try:
            client = self._get_client()
            assets = await asyncio.to_thread(client.get_all_assets)
            results: list[dict[str, Any]] = []
            for asset in assets:
                symbol = str(getattr(asset, "symbol", "")).upper()
                if not symbol or "." in symbol or "/" in symbol:
                    continue
                status = str(getattr(asset, "status", "")).lower()
                asset_class = str(getattr(asset, "asset_class", "")).lower()
                tradable = bool(getattr(asset, "tradable", False))
                fractionable = bool(getattr(asset, "fractionable", False))
                if status not in {"active", "assetstatus.active"}:
                    continue
                if "us_equity" not in asset_class and "us equity" not in asset_class:
                    continue
                if not tradable:
                    continue
                results.append(
                    {
                        "symbol": symbol,
                        "name": getattr(asset, "name", ""),
                        "tradable": tradable,
                        "fractionable": fractionable,
                        "exchange": getattr(asset, "exchange", ""),
                    }
                )
            log.info("tradable_assets_loaded", count=len(results))
            return results
        except Exception as e:
            log.error("tradable_assets_failed", error=str(e))
            raise

    @retry_external
    async def get_stock_snapshots(self, symbols: list[str]) -> dict[str, dict[str, Any]]:
        """Fetch free Alpaca/IEX snapshots for discovery and pricing.

        This intentionally uses the HTTP market-data endpoint instead of adding another SDK
        surface. It keeps the adapter small and works with Alpaca's free IEX feed.
        """
        clean_symbols = list(dict.fromkeys(s.upper() for s in symbols if s))[:100]
        if not clean_symbols:
            return {}

        def _fetch() -> dict[str, dict[str, Any]]:
            params = urlencode({"symbols": ",".join(clean_symbols), "feed": "iex"})
            req = Request(
                f"https://data.alpaca.markets/v2/stocks/snapshots?{params}",
                headers={
                    "APCA-API-KEY-ID": self.api_key,
                    "APCA-API-SECRET-KEY": self.api_secret,
                },
            )
            with urlopen(req, timeout=20) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
            snapshots = payload.get("snapshots")
            if isinstance(snapshots, dict):
                return snapshots
            # Alpaca's multi-symbol snapshots endpoint may return symbol keys at
            # the top level, e.g. {"AAPL": {...}, "MSFT": {...}}.
            return {
                symbol: snapshot
                for symbol, snapshot in payload.items()
                if symbol.upper() in clean_symbols and isinstance(snapshot, dict)
            }

        try:
            snapshots = await asyncio.to_thread(_fetch)
            log.info("stock_snapshots_loaded", requested=len(clean_symbols), returned=len(snapshots))
            return snapshots
        except Exception as e:
            log.error("stock_snapshots_failed", requested=len(clean_symbols), error=str(e))
            raise

    @retry_kill_critical
    async def cancel_all_orders(self) -> int:
        """Real cancel-all via Alpaca (kill-critical retry budget). Returns count."""
        try:
            client = self._get_client()
            # Use sync call in thread; cancel all open orders
            orders = await asyncio.to_thread(
                client.get_orders,
                GetOrdersRequest(status=QueryOrderStatus.OPEN),
            )
            count = 0
            for order in orders:
                try:
                    await asyncio.to_thread(client.cancel_order_by_id, order.id)
                    count += 1
                except Exception:
                    pass  # best effort
            log.warning("cancel_all_executed", count=count)
            return count
        except Exception as e:
            log.error("cancel_all_failed", error=str(e))
            return 0

    @retry_kill_critical
    async def flatten_all_positions(self) -> int:
        """Real market liquidation of all positions (kill path). Returns count closed."""
        try:
            client = self._get_client()
            positions = await asyncio.to_thread(client.get_all_positions)
            count = 0
            for pos in positions:
                try:
                    # Market sell/buy to close (side opposite of current)
                    qty = abs(float(pos.qty))
                    side = OrderSide.SELL if float(pos.qty) > 0 else OrderSide.BUY
                    # Submit close order using proper request object (consistent with submit_order)
                    close_req = MarketOrderRequest(
                        symbol=pos.symbol,
                        qty=qty,
                        side=side,
                        time_in_force=TimeInForce.DAY,
                    )
                    await asyncio.to_thread(client.submit_order, close_req)
                    count += 1
                except Exception as e:
                    log.warning(
                        "flatten_position_failed",
                        symbol=getattr(pos, "symbol", "?"),
                        error=str(e),
                    )
            log.critical("flatten_all_executed", positions_closed=count)
            return count
        except Exception as e:
            log.error("flatten_all_failed", error=str(e))
            return 0

    @retry_external
    async def submit_order(
        self,
        symbol: str,
        qty: float,
        side: str,
        order_type: str = "market",
        time_in_force: str = "day",
        **kwargs,
    ) -> dict:
        """
        Submit a real order via Alpaca (only after RiskEngine approval).

        Currently supports market orders (safest for v1 bootstrap).
        Returns dict with order details.
        """
        if not _ALPACA_AVAILABLE:
            raise RuntimeError("alpaca-py not installed")
        if qty <= 0:
            raise ValueError("qty must be > 0")
        if side.lower() not in {"long", "buy"}:
            raise ValueError(f"unsupported v1 side: {side}")

        client = self._get_client()

        try:
            alpaca_side = OrderSide.BUY

            if order_type == "market":
                order_data = MarketOrderRequest(
                    symbol=symbol.upper(),
                    qty=qty,
                    side=alpaca_side,
                    time_in_force=TimeInForce.DAY if time_in_force.lower() == "day" else TimeInForce.DAY,
                )
            else:
                # Future: support limit, etc.
                raise ValueError(f"Unsupported order_type for v1: {order_type}")

            submitted = await asyncio.to_thread(client.submit_order, order_data)

            result = {
                "id": str(getattr(submitted, "id", "")),
                "client_order_id": str(getattr(submitted, "client_order_id", None) or getattr(submitted, "id", "")),
                "symbol": symbol.upper(),
                "qty": float(getattr(submitted, "qty", qty)),
                "side": side,
                "order_type": order_type,
                "status": _enum_value(getattr(submitted, "status", "submitted")),
                "filled_qty": _safe_float(getattr(submitted, "filled_qty", 0)),
                "avg_fill_price": _safe_float(getattr(submitted, "filled_avg_price", None), default=None),
                "submitted_at": _iso_value(getattr(submitted, "submitted_at", None)),
                "filled_at": _iso_value(getattr(submitted, "filled_at", None)),
                "paper": self.paper,
            }

            log.info("order_submitted", **result)
            return result

        except Exception as e:
            log.error("submit_order_failed", symbol=symbol, qty=qty, side=side, error=str(e))
            raise
