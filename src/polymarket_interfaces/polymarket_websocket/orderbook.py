"""Lokaler Level-2-Orderbook-Speicher auf Basis der WebSocket-Ereignisse."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal

from .models import (
    BestBidAskEvent,
    BookEvent,
    MarketEventMessage,
    PriceChangeEvent,
    Side,
)


@dataclass(slots=True, frozen=True)
class OrderBookView:
    """
    Lokales Level-2-Orderbook.

    Snapshots ersetzen den Zustand; Price-Change-Deltas aktualisieren oder löschen
    einzelne Preisstufen.
    """
    asset_id: str
    market: str
    bids: tuple[tuple[Decimal, Decimal], ...]
    asks: tuple[tuple[Decimal, Decimal], ...]
    best_bid: Decimal | None
    best_ask: Decimal | None
    spread: Decimal | None
    midpoint: Decimal | None
    timestamp: datetime | None
    book_hash: str | None


@dataclass(slots=True)
class _MutableBook:
    market: str = ""
    bids: dict[Decimal, Decimal] = field(default_factory=dict)
    asks: dict[Decimal, Decimal] = field(default_factory=dict)
    timestamp: datetime | None = None
    book_hash: str | None = None


@dataclass(slots=True, frozen=True)
class _TopQuote:
    best_bid: Decimal | None
    best_ask: Decimal | None
    spread: Decimal | None
    timestamp: datetime | None


class OrderBookStore:
    """
    Hält für jedes Asset einen lokalen Orderbook-Zustand.

    `book` ersetzt den vollständigen Zustand. `price_change` aktualisiert nur
    einzelne Preisstufen; eine Größe von 0 entfernt die Preisstufe.
    """

    def __init__(self) -> None:
        self._books: dict[str, _MutableBook] = {}
        self._top_quotes: dict[str, _TopQuote] = {}

    def apply(self, event: MarketEventMessage) -> None:
        if isinstance(event, BookEvent):
            self._books[event.asset_id] = _MutableBook(
                market=event.market,
                bids={level.price: level.size for level in event.bids if level.size > 0},
                asks={level.price: level.size for level in event.asks if level.size > 0},
                timestamp=event.timestamp,
                book_hash=event.book_hash,
            )
            return

        if isinstance(event, PriceChangeEvent):
            for change in event.changes:
                book = self._books.setdefault(change.asset_id, _MutableBook(market=event.market))
                book.market = event.market
                book.timestamp = event.timestamp

                side_levels = book.bids if change.side is Side.BUY else book.asks
                if change.side is Side.UNKNOWN:
                    continue
                if change.size == 0:
                    side_levels.pop(change.price, None)
                else:
                    side_levels[change.price] = change.size

                if change.best_bid is not None or change.best_ask is not None:
                    spread = None
                    if change.best_bid is not None and change.best_ask is not None:
                        spread = change.best_ask - change.best_bid
                    self._top_quotes[change.asset_id] = _TopQuote(
                        best_bid=change.best_bid,
                        best_ask=change.best_ask,
                        spread=spread,
                        timestamp=event.timestamp,
                    )
            return

        if isinstance(event, BestBidAskEvent):
            self._top_quotes[event.asset_id] = _TopQuote(
                best_bid=event.best_bid,
                best_ask=event.best_ask,
                spread=event.spread,
                timestamp=event.timestamp,
            )

    def get(self, asset_id: str, *, depth: int | None = None) -> OrderBookView | None:
        book = self._books.get(asset_id)
        quote = self._top_quotes.get(asset_id)
        if book is None and quote is None:
            return None

        bids = sorted((book.bids.items() if book else ()), key=lambda item: item[0], reverse=True)
        asks = sorted((book.asks.items() if book else ()), key=lambda item: item[0])
        if depth is not None:
            bids = bids[:depth]
            asks = asks[:depth]

        computed_bid = bids[0][0] if bids else None
        computed_ask = asks[0][0] if asks else None
        best_bid = quote.best_bid if quote and quote.best_bid is not None else computed_bid
        best_ask = quote.best_ask if quote and quote.best_ask is not None else computed_ask

        spread = quote.spread if quote and quote.spread is not None else None
        if spread is None and best_bid is not None and best_ask is not None:
            spread = best_ask - best_bid

        midpoint = None
        if best_bid is not None and best_ask is not None:
            midpoint = (best_bid + best_ask) / Decimal(2)

        timestamp = quote.timestamp if quote and quote.timestamp is not None else (book.timestamp if book else None)
        return OrderBookView(
            asset_id=asset_id,
            market=book.market if book else "",
            bids=tuple(bids),
            asks=tuple(asks),
            best_bid=best_bid,
            best_ask=best_ask,
            spread=spread,
            midpoint=midpoint,
            timestamp=timestamp,
            book_hash=book.book_hash if book else None,
        )

    def remove(self, asset_id: str) -> None:
        self._books.pop(asset_id, None)
        self._top_quotes.pop(asset_id, None)

    def clear(self) -> None:
        self._books.clear()
        self._top_quotes.clear()

    @property
    def asset_ids(self) -> frozenset[str]:
        return frozenset(self._books.keys() | self._top_quotes.keys())
