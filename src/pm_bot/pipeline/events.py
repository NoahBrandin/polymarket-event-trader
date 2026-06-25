from dataclasses import dataclass, field, fields
from datetime import UTC, datetime
from enum import Enum
from typing import Any

from pm_bot.locel_types import ProducerDataType


class EventType(Enum):
    DEFAULT = "default"

    ERROR = "error"

    # --- Websocket ---
    BOOK = "book"
    PRICE_CHANGE = "price_change"
    TICK_SIZE_CHANGE = "tick_size_change"
    LAST_TRADED_PRICE = "last_price_change"
    BEST_BID_ASK = "best_bid_ask"
    NEW_MARKET = "new_market"
    MARKET_RESOLVED = "market_resolved"

    HEARTBEAT = "heartbeat"


@dataclass(slots=True, frozen=True, kw_only=True)
class EventEnvelope:
    """
    Einheitliche Hülle für alle Marktdaten.

    Die Engine verarbeitet nur dieses Format. Ob das enthaltene Event live über
    einen WebSocket oder historisch aus einer Datei kam, ist lediglich Metadatum.
    """

    producer_name: str
    producer_type: ProducerDataType
    sequence: int = -1

    received_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    timestamp: datetime

    event_type: EventType

    payload: Any

    def __str__(self) -> str:
        def _format_time(value: datetime | None) -> str:
            try:
                return "-" if value is None else value.isoformat()
            except Exception:
                raise

        return (
            f"[{self.event_type.name}] producer={self.producer_name} "
            f"producer_type={self.producer_type} "
            f"seq={self.sequence} "
            f"received={_format_time(self.received_at)} "
            f"event_time={_format_time(self.timestamp)} "
            f"payload={{{str(self.payload)}}}"
        )

class BasePayload:
    """Basisklasse für alle Payloads, die eine einheitliche String-Repräsentation

    garantiert.
    """

    def __str__(self) -> str:
        kv_pairs = []
        for f in fields(self):
            value = getattr(self, f.name)
            if value is None:
                continue
            # Formatierung für Listen (z.B. bids/asks), um das Log sauber zu halten
            if isinstance(value, list):
                kv_pairs.append(f"{f.name}={value}]")
            else:
                kv_pairs.append(f"{f.name}={str(value)}")

        # Gibt z.B. zurück: [AssetUpdatePayload] asset_id=123 side=buy best_bid=1.0
        return f"[{self.__class__.__name__}] {' '.join(kv_pairs)}"

@dataclass(slots=True, frozen=True, kw_only=True)
class HeartbeatPayload(BasePayload):
    event_type = EventType.HEARTBEAT
    timestamp = datetime.now(UTC)


@dataclass(slots=True, frozen=True, kw_only=True)
class ErrorPayload(BasePayload):
    event_type = EventType.ERROR
    message: str
    timestamp: datetime = datetime.now(UTC)
    details: Any | None


@dataclass(slots=True, frozen=True, kw_only=True)
class AssetUpdatePayload(BasePayload):
    asset_id: str | None = None
    side: str | None = None
    best_bid: float | None = None
    best_ask: float | None = None
    old_tick_size: float | None = None
    new_tick_size: float | None = None
    fee_rate_bps: float | None = None

    bids: list[dict[str, Any]] | None = None
    asks: list[dict[str, Any]] | None = None


@dataclass(slots=True, frozen=True, kw_only=True)
class MarketUpdatePayload(BasePayload):
    event_type: EventType
    market_id: str
    timestamp: datetime

    asset_events: list[AssetUpdatePayload]