from dataclasses import dataclass, field
from datetime import timezone, datetime
from decimal import Decimal
from typing import Mapping, Any
from uuid import uuid4


from src.pm_bot.configuration.selection import SubscriptionSelection
from src.pm_bot.locel_types import TradingSide, TimeInForce, ExecutionStatus


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
    tick_size: Decimal | None = None
    neg_risk: bool | None = None
    expiration: datetime | None = None
    post_only: bool = False
    client_order_id: str = field(default_factory=lambda: uuid4().hex)
    metadata: Mapping[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def __post_init__(self) -> None:
        if not self.strategy_name.strip():
            raise ValueError("strategy_name darf nicht leer sein")
        if not self.asset_id.strip():
            raise ValueError("asset_id darf nicht leer sein")
        if self.size <= 0:
            raise ValueError("size muss positiv sein")
        if not Decimal("0") < self.limit_price < Decimal("1"):
            raise ValueError("limit_price muss zwischen 0 und 1 liegen")
        if self.tick_size is not None and self.tick_size <= 0:
            raise ValueError("tick_size muss positiv sein")
        if self.post_only and self.time_in_force in {TimeInForce.FOK, TimeInForce.FAK}:
            raise ValueError("post_only ist nur mit GTC oder GTD zulässig")
        if self.time_in_force is TimeInForce.GTD:
            if self.expiration is None:
                raise ValueError("GTD benötigt expiration")
            if self.expiration.tzinfo is None or self.expiration.utcoffset() is None:
                raise ValueError("GTD expiration muss timezone-aware sein")

    def __str__(self) -> str:
        # Die absoluten Kern-Daten der Order, die IMMER da sind
        core_info = (
            f"id={self.client_order_id[:8]}... "  # Gekürzte UUID für bessere Lesbarkeit im Log
            f"strategy={self.strategy_name} "
            f"asset={self.asset_id} "
            f"side={self.side.name} "  # .name falls TradingSide ein Enum ist
            f"size={self.size} "
            f"limit={self.limit_price} "
            f"tif={self.time_in_force.name}"
        )

        # Optionale Parameter dynamisch sammeln, um das Log sauber zu halten
        optional_flags = []
        if self.market_id:
            optional_flags.append(f"market={self.market_id}")
        if self.post_only:
            optional_flags.append("POST_ONLY")
        if self.neg_risk:
            optional_flags.append("neg_risk=True")
        if self.time_in_force is TimeInForce.GTD and self.expiration:
            optional_flags.append(f"exp={self.expiration.isoformat()}")

        # Zusammenbauen mit geschweiften Klammern
        flags_str = f" [{', '.join(optional_flags)}]" if optional_flags else ""
        return f"OrderIntent {{{core_info}}}{flags_str}"


@dataclass(slots=True, frozen=True, kw_only=True)
class ExecutionReport:
    execution_name: str
    order: OrderIntent
    status: ExecutionStatus
    timestamp: datetime
    filled_size: Decimal = Decimal("0")
    average_price: Decimal | None = None
    remaining_size: Decimal = Decimal("0")
    exchange_order_id: str | None = None
    message: str | None = None
    raw: Mapping[str, Any] = field(default_factory=dict)

