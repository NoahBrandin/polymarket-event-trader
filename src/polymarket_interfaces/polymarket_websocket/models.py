"""Typisierte Datenmodelle für den öffentlichen Polymarket-Market-Channel."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any


class ConnectionState(str, Enum):
    DISCONNECTED = "DISCONNECTED"
    CONNECTING = "CONNECTING"
    SUBSCRIBED = "SUBSCRIBED"
    RECONNECTING = "RECONNECTING"
    CLOSING = "CLOSING"
    CLOSED = "CLOSED"
    FAILED = "FAILED"


class MarketEventType(str, Enum):
    """
    Typisierte Market-Channel-Modelle.

    Enums und unveränderliche Dataclasses repräsentieren alle unterstützten
    Polymarket-Events ohne rohe JSON-Dictionaries weiterzureichen.
    """
    BOOK = "book"
    PRICE_CHANGE = "price_change"
    TICK_SIZE_CHANGE = "tick_size_change"
    LAST_TRADE_PRICE = "last_trade_price"
    BEST_BID_ASK = "best_bid_ask"
    NEW_MARKET = "new_market"
    MARKET_RESOLVED = "market_resolved"
    UNKNOWN = "unknown"


class Side(str, Enum):
    BUY = "BUY"
    SELL = "SELL"
    UNKNOWN = "UNKNOWN"


@dataclass(slots=True, frozen=True, kw_only=True)
class OrderBookLevel:
    price: Decimal
    size: Decimal


@dataclass(slots=True, frozen=True, kw_only=True)
class PriceLevelChange:
    asset_id: str
    price: Decimal
    size: Decimal
    side: Side
    order_hash: str | None
    best_bid: Decimal | None
    best_ask: Decimal | None


@dataclass(slots=True, frozen=True, kw_only=True)
class MarketEvent:
    event_type: MarketEventType
    timestamp: datetime | None
    raw: Mapping[str, Any]


@dataclass(slots=True, frozen=True, kw_only=True)
class BookEvent(MarketEvent):
    asset_id: str
    market: str
    bids: tuple[OrderBookLevel, ...]
    asks: tuple[OrderBookLevel, ...]
    book_hash: str | None


@dataclass(slots=True, frozen=True, kw_only=True)
class PriceChangeEvent(MarketEvent):
    market: str
    changes: tuple[PriceLevelChange, ...]


@dataclass(slots=True, frozen=True, kw_only=True)
class TickSizeChangeEvent(MarketEvent):
    asset_id: str
    market: str
    old_tick_size: Decimal
    new_tick_size: Decimal


@dataclass(slots=True, frozen=True, kw_only=True)
class LastTradePriceEvent(MarketEvent):
    asset_id: str
    market: str
    price: Decimal
    size: Decimal
    side: Side
    fee_rate_bps: Decimal | None
    transaction_hash: str | None


@dataclass(slots=True, frozen=True, kw_only=True)
class BestBidAskEvent(MarketEvent):
    asset_id: str
    market: str
    best_bid: Decimal | None
    best_ask: Decimal | None
    spread: Decimal | None


@dataclass(slots=True, frozen=True, kw_only=True)
class NewMarketEvent(MarketEvent):
    market_id: str
    market: str
    condition_id: str
    slug: str
    question: str
    description: str | None
    asset_ids: tuple[str, ...]
    outcomes: tuple[str, ...]
    tags: tuple[str, ...]
    active: bool
    min_tick_size: Decimal | None


@dataclass(slots=True, frozen=True, kw_only=True)
class MarketResolvedEvent(MarketEvent):
    market_id: str
    market: str
    slug: str
    question: str
    asset_ids: tuple[str, ...]
    outcomes: tuple[str, ...]
    winning_asset_id: str | None
    winning_outcome: str | None


@dataclass(slots=True, frozen=True, kw_only=True)
class UnknownMarketEvent(MarketEvent):
    original_event_type: str | None


MarketEventMessage = (
    BookEvent
    | PriceChangeEvent
    | TickSizeChangeEvent
    | LastTradePriceEvent
    | BestBidAskEvent
    | NewMarketEvent
    | MarketResolvedEvent
    | UnknownMarketEvent
)
