import pytest

from auto_trader.broker.alpaca_adapter import AlpacaAdapter
from auto_trader.utils.api_budget import ApiBudget


def test_api_budget_counts_recent_calls_and_blocks_nonessential_at_critical_threshold():
    budget = ApiBudget(name="test", warning_threshold=2, critical_threshold=3)

    budget.record("account", essential=True)
    budget.record("clock", essential=True)
    snapshot = budget.record("positions", essential=True)

    assert snapshot.total == 3
    assert snapshot.by_endpoint == {"account": 1, "clock": 1, "positions": 1}
    assert budget.allow_nonessential("snapshots") is False


@pytest.mark.asyncio
async def test_alpaca_adapter_defers_snapshot_discovery_when_budget_is_hot(monkeypatch):
    budget = ApiBudget(name="test", warning_threshold=1, critical_threshold=1)
    budget.record("positions", essential=True)
    adapter = AlpacaAdapter("key", "secret", paper=True, api_budget=budget)

    def fake_urlopen(*args, **kwargs):
        raise AssertionError("network should not be called when discovery is deferred")

    monkeypatch.setattr("auto_trader.broker.alpaca_adapter.urlopen", fake_urlopen)

    with pytest.raises(RuntimeError, match="nonessential Alpaca API work deferred"):
        await adapter.get_stock_snapshots(["AAPL"])
