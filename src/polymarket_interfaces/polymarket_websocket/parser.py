"""Parsing und Normalisierung der rohen WebSocket-Nachrichten."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from .errors import PolymarketWebSocketError, WebSocketErrorCode
from .models import (
    BestBidAskEvent,
    BookEvent,
    LastTradePriceEvent,
    MarketEventMessage,
    MarketEventType,
    MarketResolvedEvent,
    NewMarketEvent,
    OrderBookLevel,
    PriceChangeEvent,
    PriceLevelChange,
    Side,
    TickSizeChangeEvent,
    UnknownMarketEvent,
)


class MarketEventParser:
    """
    Nachrichtenparser.

    Der Parser akzeptiert einzelne Objekte oder Batches und delegiert jeden
    event_type an eine spezialisierte Konvertierungsfunktion.

    Wandelt JSON-Nachrichten in typisierte Python-Datenklassen um.
    """

    def __init__(self, *, strict: bool = False) -> None:
        self.strict = strict

    def parse_message(self, message: str | bytes) -> list[MarketEventMessage]:
        """
        Öffentlicher Einstieg: Transportframes werden dekodiert, PING/PONG ignoriert
        und unbekannte Eventtypen kontrolliert als UnknownMarketEvent erhalten.
        """
        if isinstance(message, bytes):
            try:
                message = message.decode("utf-8")
            except UnicodeDecodeError as error:
                raise self._invalid("Binärnachricht ist kein UTF-8", error) from error

        text = message.strip()
        if not text or text.upper() in {"PING", "PONG"}:
            return []

        try:
            payload = json.loads(text)
        except json.JSONDecodeError as error:
            raise self._invalid("WebSocket-Nachricht enthält kein gültiges JSON", error) from error

        items: Sequence[Any]
        if isinstance(payload, list):
            items = payload
        else:
            items = [payload]

        parsed: list[MarketEventMessage] = []
        for item in items:
            if not isinstance(item, Mapping):
                if self.strict:
                    raise self._invalid("WebSocket-Nachricht ist kein JSON-Objekt")
                continue
            parsed.append(self.parse_event(item))
        return parsed

    def parse_event(self, payload: Mapping[str, Any]) -> MarketEventMessage:
        raw_type = _optional_str(payload.get("event_type"))
        timestamp = _parse_timestamp(payload.get("timestamp"))

        try:
            event_type = MarketEventType(raw_type) if raw_type else MarketEventType.UNKNOWN
        except ValueError:
            event_type = MarketEventType.UNKNOWN

        common = {
            "event_type": event_type,
            "timestamp": timestamp,
            "raw": dict(payload),
        }

        if event_type is MarketEventType.BOOK:
            return BookEvent(
                **common,
                asset_id=_required_str(payload, "asset_id"),
                market=_required_str(payload, "market"),
                bids=_parse_levels(payload.get("bids")),
                asks=_parse_levels(payload.get("asks")),
                book_hash=_optional_str(payload.get("hash")),
            )

        if event_type is MarketEventType.PRICE_CHANGE:
            raw_changes = payload.get("price_changes")
            changes: list[PriceLevelChange] = []
            if isinstance(raw_changes, Sequence) and not isinstance(raw_changes, (str, bytes)):
                for item in raw_changes:
                    if not isinstance(item, Mapping):
                        continue
                    changes.append(
                        PriceLevelChange(
                            asset_id=_required_str(item, "asset_id"),
                            price=_required_decimal(item, "price"),
                            size=_required_decimal(item, "size"),
                            side=_parse_side(item.get("side")),
                            order_hash=_optional_str(item.get("hash")),
                            best_bid=_optional_decimal(item.get("best_bid")),
                            best_ask=_optional_decimal(item.get("best_ask")),
                        )
                    )
            return PriceChangeEvent(
                **common,
                market=_required_str(payload, "market"),
                changes=tuple(changes),
            )

        if event_type is MarketEventType.TICK_SIZE_CHANGE:
            return TickSizeChangeEvent(
                **common,
                asset_id=_required_str(payload, "asset_id"),
                market=_required_str(payload, "market"),
                old_tick_size=_required_decimal(payload, "old_tick_size"),
                new_tick_size=_required_decimal(payload, "new_tick_size"),
            )

        if event_type is MarketEventType.LAST_TRADE_PRICE:
            return LastTradePriceEvent(
                **common,
                asset_id=_required_str(payload, "asset_id"),
                market=_required_str(payload, "market"),
                price=_required_decimal(payload, "price"),
                size=_required_decimal(payload, "size"),
                side=_parse_side(payload.get("side")),
                fee_rate_bps=_optional_decimal(payload.get("fee_rate_bps")),
                transaction_hash=_optional_str(payload.get("transaction_hash")),
            )

        if event_type is MarketEventType.BEST_BID_ASK:
            return BestBidAskEvent(
                **common,
                asset_id=_required_str(payload, "asset_id"),
                market=_required_str(payload, "market"),
                best_bid=_optional_decimal(payload.get("best_bid")),
                best_ask=_optional_decimal(payload.get("best_ask")),
                spread=_optional_decimal(payload.get("spread")),
            )

        if event_type is MarketEventType.NEW_MARKET:
            asset_ids = _string_tuple(
                payload.get("assets_ids") or payload.get("clob_token_ids")
            )
            return NewMarketEvent(
                **common,
                market_id=_to_str(payload.get("id")),
                market=_required_str(payload, "market"),
                condition_id=_to_str(payload.get("condition_id") or payload.get("market")),
                slug=_to_str(payload.get("slug")),
                question=_to_str(payload.get("question")),
                description=_optional_str(payload.get("description")),
                asset_ids=asset_ids,
                outcomes=_string_tuple(payload.get("outcomes")),
                tags=_string_tuple(payload.get("tags")),
                active=bool(payload.get("active", True)),
                min_tick_size=_optional_decimal(payload.get("order_price_min_tick_size")),
            )

        if event_type is MarketEventType.MARKET_RESOLVED:
            return MarketResolvedEvent(
                **common,
                market_id=_to_str(payload.get("id")),
                market=_required_str(payload, "market"),
                slug=_to_str(payload.get("slug")),
                question=_to_str(payload.get("question")),
                asset_ids=_string_tuple(payload.get("assets_ids")),
                outcomes=_string_tuple(payload.get("outcomes")),
                winning_asset_id=_optional_str(payload.get("winning_asset_id")),
                winning_outcome=_optional_str(payload.get("winning_outcome")),
            )

        if self.strict:
            raise self._invalid(f"Unbekannter event_type: {raw_type!r}")

        return UnknownMarketEvent(
            **common,
            original_event_type=raw_type,
        )

    @staticmethod
    def _invalid(
        message: str,
        original_error: BaseException | None = None,
    ) -> PolymarketWebSocketError:
        return PolymarketWebSocketError(
            WebSocketErrorCode.INVALID_MESSAGE,
            message,
            retryable=False,
            original_error=original_error,
        )


def _parse_levels(value: Any) -> tuple[OrderBookLevel, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return ()

    levels: list[OrderBookLevel] = []
    for item in value:
        if not isinstance(item, Mapping):
            continue
        try:
            levels.append(
                OrderBookLevel(
                    price=_required_decimal(item, "price"),
                    size=_required_decimal(item, "size"),
                )
            )
        except PolymarketWebSocketError:
            continue
    return tuple(levels)


def _parse_side(value: Any) -> Side:
    text = _to_str(value).upper()
    try:
        return Side(text)
    except ValueError:
        return Side.UNKNOWN


def _parse_timestamp(value: Any) -> datetime | None:
    if value is None or value == "":
        return None

    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if any(character in text for character in "-T:Z+"):
            iso = text[:-1] + "+00:00" if text.endswith("Z") else text
            try:
                parsed = datetime.fromisoformat(iso)
            except ValueError:
                return None
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    else:
        text = str(value)

    try:
        numeric = Decimal(text)
    except InvalidOperation:
        return None

    # WebSocket-Beispiele enthalten sowohl Unix-Sekunden als auch Millisekunden.
    seconds = numeric / Decimal(1000) if abs(numeric) >= Decimal("100000000000") else numeric
    try:
        return datetime.fromtimestamp(float(seconds), tz=UTC)
    except (OverflowError, OSError, ValueError):
        return None


def _required_str(mapping: Mapping[str, Any], key: str) -> str:
    value = _optional_str(mapping.get(key))
    if value is None:
        raise PolymarketWebSocketError(
            WebSocketErrorCode.INVALID_MESSAGE,
            f"Pflichtfeld {key!r} fehlt",
        )
    return value


def _required_decimal(mapping: Mapping[str, Any], key: str) -> Decimal:
    value = _optional_decimal(mapping.get(key))
    if value is None:
        raise PolymarketWebSocketError(
            WebSocketErrorCode.INVALID_MESSAGE,
            f"Pflichtfeld {key!r} ist keine Zahl",
        )
    return value


def _optional_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except InvalidOperation:
        return None


def _optional_str(value: Any) -> str | None:
    if value is None or value == "":
        return None
    return str(value)


def _to_str(value: Any) -> str:
    return "" if value is None else str(value)


def _string_tuple(value: Any) -> tuple[str, ...]:
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            return (value,) if value else ()
        value = decoded

    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return ()
    return tuple(str(item) for item in value if item is not None)
