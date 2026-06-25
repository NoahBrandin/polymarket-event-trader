from __future__ import annotations

import pytest

"""
Komponenten- und Integrationstests für polymarket-event-trader.

Ziele:
- keine echten HTTP-/WebSocket-Aufrufe
- keine Live-Credentials
- deterministische Strategieprüfung
- Lifecycle-Prüfung von Producer, Strategy, Execution und Engine
- kompatibel mit Imports über ``src.pm_bot`` und ``pm_bot``

Ablage:
    src/tests/test_all_components.py

Ausführung:
    python -m pytest -q src/tests/test_all_components.py
"""

import asyncio
import importlib
import inspect
import json
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

# ---------------------------------------------------------------------------
# Import-Helfer
# ---------------------------------------------------------------------------

_PACKAGE_ROOTS = ("src.pm_bot", "pm_bot")


def _import_module(relative_name: str):
    errors: list[BaseException] = []
    for root in _PACKAGE_ROOTS:
        module_name = f"{root}.{relative_name}"
        try:
            return importlib.import_module(module_name)
        except (ImportError, ModuleNotFoundError) as error:
            errors.append(error)
    joined = "\n".join(f"- {type(error).__name__}: {error}" for error in errors)
    raise ImportError(
        f"Modul {relative_name!r} konnte unter {_PACKAGE_ROOTS!r} nicht geladen werden:\n{joined}"
    )


def _import_first(*relative_names: str):
    errors: list[BaseException] = []
    for relative_name in relative_names:
        try:
            return _import_module(relative_name)
        except (ImportError, ModuleNotFoundError) as error:
            errors.append(error)
    joined = "\n".join(f"- {type(error).__name__}: {error}" for error in errors)
    raise ImportError(
        f"Keines der Module {relative_names!r} konnte geladen werden:\n{joined}"
    )


def _attribute(module: Any, *names: str) -> Any:
    for name in names:
        if hasattr(module, name):
            return getattr(module, name)
    raise AttributeError(
        f"{module.__name__} exportiert keines der Attribute {names!r}"
    )


engine_module = _import_module("pipeline.engine")
events_module = _import_module("pipeline.events")
queue_module = _import_module("pipeline.queue")
types_module = _import_module("locel_types")
selection_module = _import_module("configuration.selection")
trading_module = _import_module("configuration.trading")
strategy_module = _import_module("consumers.strategy.default_random_strategy")
websocket_module = _import_module("producer.websocket_feed")
factory_module = _import_module("configuration.factory")
config_module = _import_first(
    "configuration.bot_config",
    "configuration.config",
)
execution_package = _import_module("consumers.execution")
execution_models_module = _import_first(
    "consumers.execution.models",
    "consumers.execution",
)
paper_execution_module = _import_first(
    "consumers.execution.paper_execution",
    "consumers.execution",
)


Engine = _attribute(engine_module, "Engine")
EngineConfig = _attribute(engine_module, "EngineConfig")
EventEnvelope = _attribute(events_module, "EventEnvelope")
EventType = _attribute(events_module, "EventType")
AssetUpdatePayload = _attribute(events_module, "AssetUpdatePayload")
MarketUpdatePayload = _attribute(events_module, "MarketUpdatePayload")
EventQueue = _attribute(queue_module, "EventQueue")

ProducerName = _attribute(types_module, "ProducerName")
ProducerDataType = _attribute(types_module, "ProducerDataType")
StrategyName = _attribute(types_module, "StrategyName")
StrategyType = _attribute(types_module, "StrategyType")
SelectionType = _attribute(types_module, "SelectionType")
LogMode = _attribute(types_module, "LogMode")
TradingSide = _attribute(types_module, "TradingSide")

SubscriptionSelection = _attribute(selection_module, "SubscriptionSelection")
StrategyDecision = _attribute(trading_module, "StrategyDecision")
OrderIntent = _attribute(trading_module, "OrderIntent")
DefaultRandomStrategy = _attribute(strategy_module, "DefaultRandomStrategy")
WebsocketFeed = _attribute(websocket_module, "WebsocketFeed")
BotConfig = _attribute(config_module, "BotConfig")

ExecutionMode = _attribute(
    types_module,
    "ExecutionMode",
)
PaperExecution = _attribute(
    paper_execution_module,
    "PaperExecution",
)
PaperExecutionConfig = _attribute(
    paper_execution_module,
    "PaperExecutionConfig",
)

OrderStatus = _attribute(
    types_module,
    "OrderStatus",
)


# ---------------------------------------------------------------------------
# Gemeinsame Fixtures und Konstruktor-Helfer
# ---------------------------------------------------------------------------

NOW = datetime.now(UTC)


def _enum_member(enum_type: Any, *names: str) -> Any:
    for name in names:
        if name in enum_type.__members__:
            return enum_type.__members__[name]
    raise AssertionError(
        f"{enum_type.__name__} besitzt keines der Mitglieder {names!r}; "
        f"vorhanden: {tuple(enum_type.__members__)}"
    )


def _selection(ids: Iterable[str] = ("asset-1",)) -> Any:
    selection_type = _enum_member(
        SelectionType,
        "MARKT_EVENT",
        "MARKET_EVENT",
    )
    return SubscriptionSelection(type=selection_type, ids=list(ids))


def _market_payload() -> Any:
    asset = AssetUpdatePayload(
        asset_id="asset-1",
        best_bid=0.49,
        best_ask=0.51,
    )
    return MarketUpdatePayload(
        event_type=_enum_member(EventType, "BOOK", "DEFAULT"),
        market_id="market-1",
        timestamp=NOW,
        asset_events=[asset],
    )


def _event(payload: Any | None = None) -> Any:
    payload = payload if payload is not None else _market_payload()
    event_type = getattr(payload, "event_type", _enum_member(EventType, "DEFAULT"))
    return EventEnvelope(
        producer_name=str(_enum_member(ProducerName, "WEBSOCKET").value),
        producer_type=_enum_member(ProducerDataType, "WEBSOCKET"),
        timestamp=NOW,
        event_type=event_type,
        payload=payload,
    )


def _engine_config(**overrides: Any) -> Any:
    """
    Baut EngineConfig nur mit Parametern, die die aktuelle Projektversion kennt.
    Dadurch bleibt der Test auch kompatibel, wenn ``testing`` neu hinzugefügt
    oder später wieder entfernt wird.
    """
    parameters = inspect.signature(EngineConfig).parameters
    values = {
        "queue_size": 7,
        "tick_hz": 1_000,
        "print_lifecycle": False,
        "print_events": False,
        "print_execution": False,
        "testing": True,
    }
    values.update(overrides)
    return EngineConfig(
        **{name: value for name, value in values.items() if name in parameters}
    )


def _bot_config(execution_mode: Any, strategy_name: Any | None = None) -> Any:
    """
    Unterstützt sowohl ältere ``run_mode``- als auch neuere
    ``execution_mode``-BotConfig-Versionen.
    """
    strategy_name = strategy_name or _enum_member(
        StrategyName,
        "DEFAULT_RANDOM_STRATEGY",
    )
    parameters = inspect.signature(BotConfig).parameters
    kwargs: dict[str, Any] = {
        "name": "pytest-components",
        "log_mode": _enum_member(LogMode, "DEBUG", "INFO"),
        "producer_name": _enum_member(ProducerName, "WEBSOCKET"),
        "strategy_name": strategy_name,
    }

    if "execution_mode" in parameters:
        kwargs["execution_mode"] = execution_mode
    elif "run_mode" in parameters:
        kwargs["run_mode"] = execution_mode
    else:
        raise AssertionError(
            "BotConfig hat weder execution_mode noch run_mode"
        )

    if "source_mode" in parameters:
        SourceMode = _attribute(types_module, "SourceMode")
        kwargs["source_mode"] = _enum_member(SourceMode, "LIVE")

    return BotConfig(**kwargs)


# ---------------------------------------------------------------------------
# Queue und Datenmodelle
# ---------------------------------------------------------------------------

def test_event_queue_put_get_join_and_stats() -> None:
    async def scenario() -> None:
        queue = EventQueue(maxsize=2)
        envelope = _event()

        await queue.put(envelope)

        before = queue.stats
        assert before.current_size == 1
        assert before.maximum_size == 2
        assert before.published_messages == 1
        assert before.consumed_messages == 0

        received = await queue.get()
        assert received is envelope

        queue.task_done()
        await queue.join()

        after = queue.stats
        assert after.current_size == 0
        assert after.published_messages == 1
        assert after.consumed_messages == 1

    asyncio.run(scenario())


def test_subscription_selection_default_does_not_share_a_mutable_list() -> None:
    ids_parameter = inspect.signature(SubscriptionSelection).parameters.get("ids")
    if ids_parameter is None or ids_parameter.default is inspect.Signature.empty:
        pytest.skip("SubscriptionSelection verlangt ids explizit")

    first = SubscriptionSelection(
        type=_enum_member(SelectionType, "MARKT_EVENT", "MARKET_EVENT")
    )
    second = SubscriptionSelection(
        type=_enum_member(SelectionType, "MARKT_EVENT", "MARKET_EVENT")
    )

    assert first.ids == []
    assert second.ids == []
    assert first.ids is not second.ids


def test_order_intent_rejects_invalid_values() -> None:
    with pytest.raises(ValueError):
        OrderIntent(
            strategy_name="test",
            asset_id="asset-1",
            market_id="market-1",
            side=_enum_member(TradingSide, "BUY"),
            size=Decimal("0"),
            limit_price=Decimal("0.50"),
        )

    with pytest.raises(ValueError):
        OrderIntent(
            strategy_name="test",
            asset_id="asset-1",
            market_id="market-1",
            side=_enum_member(TradingSide, "BUY"),
            size=Decimal("1"),
            limit_price=Decimal("1"),
        )


# ---------------------------------------------------------------------------
# Strategy
# ---------------------------------------------------------------------------

def test_default_random_strategy_ignores_unknown_payload() -> None:
    strategy = DefaultRandomStrategy()
    envelope = _event(payload=SimpleNamespace(event_type=_enum_member(EventType, "DEFAULT")))

    decision = asyncio.run(strategy.on_event(envelope))

    assert decision is None


# ---------------------------------------------------------------------------
# WebSocket-Producer — ohne Netzwerk
# ---------------------------------------------------------------------------

def test_websocket_feed_builds_default_selection_from_mocked_gamma(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeGammaAPI:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

        async def aclose(self) -> None:
            return None

        async def get_trending_markets(self):
            return [
                SimpleNamespace(clob_token_ids=["asset-1", "asset-2"]),
                SimpleNamespace(clob_token_ids=["asset-3"]),
            ]

    monkeypatch.setattr(websocket_module, "GammaAPI", FakeGammaAPI)

    producer = WebsocketFeed()
    selection = asyncio.run(producer.get_default_subscription_selection())

    assert selection.type is _enum_member(
        SelectionType,
        "MARKT_EVENT",
        "MARKET_EVENT",
    )
    assert selection.ids == ["asset-1", "asset-2", "asset-3"]


def test_websocket_feed_normalizes_and_publishes_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = _market_payload()
    monkeypatch.setattr(
        websocket_module,
        "parse_market_ws_message",
        lambda _raw: payload,
    )

    async def scenario() -> None:
        producer = WebsocketFeed()
        queue = EventQueue(maxsize=2)

        await producer._on_message(
            json.dumps({"event_type": "book", "asset_id": "asset-1"}),
            queue,
        )

        envelope = await queue.get()
        queue.task_done()

        assert envelope.event_type == payload.event_type
        assert envelope.payload is payload
        assert envelope.producer_type is _enum_member(
            ProducerDataType,
            "WEBSOCKET",
        )

    asyncio.run(scenario())


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def test_factory_builds_websocket_strategy_without_execution() -> None:
    none_mode = _enum_member(ExecutionMode, "NONE")
    components = factory_module.get_components(_bot_config(none_mode))

    assert isinstance(components["producer"], WebsocketFeed)
    assert isinstance(components["strategy"], DefaultRandomStrategy)
    assert components["execution"] is None


def test_factory_builds_paper_execution(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Prüft die reale Factory, verwendet aber ausschließlich Paper-Konfiguration.
    Ein Live-Provider wird hier bewusst nicht konstruiert.
    """
    paper_mode = _enum_member(ExecutionMode, "PAPER")
    monkeypatch.setenv("EXECUTION_MODE", "paper")
    monkeypatch.setenv("PAPER_INITIAL_CASH", "100")
    monkeypatch.setenv("PAPER_TICK_SECONDS", "0.01")
    monkeypatch.delenv("CLOB_ALLOW_LIVE_TRADING", raising=False)

    components = factory_module.get_components(_bot_config(paper_mode))

    assert isinstance(components["producer"], WebsocketFeed)
    assert isinstance(components["strategy"], DefaultRandomStrategy)
    assert isinstance(components["execution"], PaperExecution)


# ---------------------------------------------------------------------------
# Paper-Execution
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class _Level:
    price: Decimal
    size: Decimal


@dataclass(frozen=True)
class _Book:
    bids: tuple[_Level, ...]
    asks: tuple[_Level, ...]
    min_order_size: Decimal = Decimal("1")
    tick_size: Decimal = Decimal("0.01")
    neg_risk: bool = False


class _FakeMarketAPI:
    def __init__(self, book: _Book) -> None:
        self.book = book
        self.calls: list[str] = []

    async def get_order_book(self, target: str) -> _Book:
        self.calls.append(target)
        return self.book


# ---------------------------------------------------------------------------
# Vollständige Engine-Integration mit Test-Doubles
# ---------------------------------------------------------------------------

class _FakeProducer:
    def __init__(self) -> None:
        self.config = SimpleNamespace(
            name=_enum_member(ProducerName, "WEBSOCKET"),
            type=_enum_member(ProducerDataType, "WEBSOCKET"),
            selection_type=_enum_member(
                SelectionType,
                "MARKT_EVENT",
                "MARKET_EVENT",
            ),
        )
        self.selection = _selection()
        self.started = False
        self.stopped = False

    async def get_default_subscription_selection(self):
        return self.selection

    async def set_subscription_selection(self, selection):
        self.selection = selection

    async def run(self, event_queue):
        self.started = True
        await event_queue.put(_event())

    async def stop(self):
        self.stopped = True


class _FakeStrategy:
    def __init__(self) -> None:
        self.config = SimpleNamespace(
            strategy_name=_enum_member(
                StrategyName,
                "DEFAULT_RANDOM_STRATEGY",
            ),
            strategy_type=_enum_member(
                StrategyType,
                "UPDATE_DRIVEN",
                "TICK_DRIVEN",
            ),
            producer_type=_enum_member(
                ProducerDataType,
                "WEBSOCKET",
            ),
        )
        self.started = False
        self.stopped = False
        self.events: list[Any] = []
        self.execution_reports: list[Any] = []

    async def get_subscription_selection(self):
        return None

    async def on_start(self):
        self.started = True
        return None

    async def on_event(self, envelope):
        self.events.append(envelope)
        order = OrderIntent(
            strategy_name=str(self.config.strategy_name.value),
            asset_id="asset-1",
            market_id="market-1",
            side=_enum_member(TradingSide, "BUY"),
            size=Decimal("1"),
            limit_price=Decimal("0.50"),
        )
        return StrategyDecision(orders=[order])

    async def on_execution(self, report):
        self.execution_reports.append(report)
        return None

    async def on_stop(self):
        self.stopped = True


class _FakeExecution:
    def __init__(self) -> None:
        paper_mode = ExecutionMode.PAPER

        self.config = SimpleNamespace(
            execution_name="FakeExecution",
            mode=paper_mode,
        )

        self.run_mode = paper_mode
        self.mode = paper_mode
        self.started = False
        self.stopped = False
        self.orders = []

