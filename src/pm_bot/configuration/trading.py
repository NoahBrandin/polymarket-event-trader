from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any

from pm_bot.configuration.selection import SubscriptionSelection
from pm_bot.locel_types import OrderStatus, TimeInForce, TradingSide


@dataclass(slots=True, frozen=True)
class StrategyDecision:
    """Entscheidung einer Strategie, die Orders und/oder neue Subscriptions

    enthält.
    """

    # FIX: default_factory sorgt für eine frische, saubere Liste pro Instanz
    orders: list[OrderIntent] = field(default_factory=list)
    subscription_selection: SubscriptionSelection | None = None

    @classmethod
    def empty(cls) -> "StrategyDecision":
        return cls()

    def __str__(self) -> str:
        # Dynamischen Titel generieren (z.B. [OrdersDecision] oder [OrdersAndSubscriptionSelectionDecision])
        components = []
        if self.orders:
            components.append("Orders")
        if self.subscription_selection:
            if components:
                components.append("And")
            components.append("SubscriptionSelection")

        decision_title = (
            "".join(components) if components else "Empty"
        ) + "Decision"

        return (f"[{decision_title}] "
                f"orders={f"[{', '.join(str(order) for order in self.orders)}]" if self.orders else "[]"}, "
                f"subscription={self.subscription_selection}")


@dataclass(slots=True, frozen=True, kw_only=True)
class OrderIntent:
    """Broker-unabhängiger Auftrag, den eine Strategie erzeugt."""

    strategy_name: str
    asset_id: str
    market_id: str | None
    side: TradingSide
    size: Decimal
    limit_price: Decimal
    time_in_force: TimeInForce = TimeInForce.GTC

    def __post_init__(self) -> None:
        if not self.strategy_name.strip():
            raise ValueError("strategy_name darf nicht leer sein")
        if not self.asset_id.strip():
            raise ValueError("asset_id darf nicht leer sein")
        if self.size <= 0:
            raise ValueError("size muss positiv sein")
        if not Decimal("0") < self.limit_price < Decimal("1"):
            raise ValueError("limit_price muss zwischen 0 und 1 liegen")

    def __str__(self) -> str:
        # Die absoluten Kern-Daten der Order, die IMMER da sind
        return (f"OrderIntent {{"
            f"asset_id={self.asset_id[:8]}... "  # Gekürzte UUID für bessere Lesbarkeit im Log
            f"strategy={self.strategy_name} "
            f"side={self.side.name} "
            f"size={self.size} "
            f"limit={self.limit_price} "
            f"tif={self.time_in_force.name} }}")


@dataclass(slots=True, frozen=True, kw_only=True)
class ExecutionReport:
    execution_name: str
    order: OrderIntent
    status: OrderStatus
    timestamp: datetime
    filled_size: Decimal = Decimal("0")
    message: str | None = None
    raw: Mapping[str, Any] = field(default_factory=dict)

    def __str__(self) -> str:
        return (f"execution_name={self.execution_name} "
                f"order={self.order} "
                f"status={self.status} "
                f"timestamp={self.timestamp} "
                f"filled_size={self.filled_size} "
                f"message={self.message}")


