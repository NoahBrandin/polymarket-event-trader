from dataclasses import dataclass, field
from datetime import timezone, datetime
from decimal import Decimal
from typing import Mapping, Any
from uuid import uuid4


from src.pm_bot.configuration.selection import SubscriptionSelection
from src.pm_bot.locel_types import TradingSide, TimeInForce, ExecutionStatus


@dataclass(slots=True, frozen=True, kw_only=True)
class StrategyDecision:
    orders: tuple[OrderIntent, ...] = ()
    subscription_selection: SubscriptionSelection | None = None

    @classmethod
    def empty(cls) -> "StrategyDecision":
        return cls()


@dataclass(slots=True, frozen=True, kw_only=True)
class OrderIntent:
    """Broker-unabhängiger Auftrag, den eine Strategie erzeugt."""

    strategy_name: str
    asset_id: str
    market: str | None
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

