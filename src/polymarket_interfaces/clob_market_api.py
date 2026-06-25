"""
Asynchroner Client für die öffentlichen Polymarket-CLOB-Leseendpunkte.

Die Klasse folgt dem Aufbau von ``GammaAPI``: ein injizierbarer ``httpx``-
Client, gemeinsames Rate-Limiting, stabile ``PolymarketError``-Fehlercodes und
typisierte, normalisierte Rückgabewerte.

Sie signiert keine Orders und versendet keine Handelsaufträge. Authentifizierte
L1-/L2-Operationen sollten über das offizielle Polymarket-SDK erfolgen.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Any

import httpx

from .utils.utils import (
    ApiType,
    ErrorCode,
    PolymarketError,
    RateLimiter,
    UnifiedCache,
)

CLOB_API_BASE = "https://clob.polymarket.com"
INITIAL_CURSOR = "MA=="


class ClobSide(StrEnum):
    """Handelsseite im Query- und Payload-Format der CLOB API."""
    BUY = "BUY"
    SELL = "SELL"


class PriceHistoryInterval(StrEnum):
    """Von den REST-History-Endpunkten unterstützte Zeiträume."""
    MAX = "max"
    ALL = "all"
    ONE_MONTH = "1m"
    ONE_WEEK = "1w"
    ONE_DAY = "1d"
    SIX_HOURS = "6h"
    ONE_HOUR = "1h"


@dataclass(frozen=True, slots=True)
class BookRequest:
    """Ein Token-Request für die Batch-Endpunkte der CLOB API."""

    token_id: str
    side: ClobSide | None = None

    def __post_init__(self) -> None:
        _require_identifier(self.token_id, "token_id")

    def to_payload(self) -> dict[str, str]:
        payload = {"token_id": self.token_id}
        if self.side is not None:
            payload["side"] = self.side.value
        return payload


@dataclass(frozen=True, slots=True)
class ClobOrderBookLevel:
    """Ein Preisniveau mit Preis und verfügbarer Größe."""
    price: Decimal
    size: Decimal


@dataclass(frozen=True, slots=True)
class OrderBook:
    """Normalisierter Snapshot eines einzelnen CLOB-Orderbuchs."""

    market: str
    asset_id: str
    timestamp: str | None
    bids: tuple[ClobOrderBookLevel, ...]
    asks: tuple[ClobOrderBookLevel, ...]
    min_order_size: Decimal | None
    tick_size: Decimal | None
    neg_risk: bool
    last_trade_price: Decimal | None
    hash: str | None

    @property
    def best_bid(self) -> Decimal | None:
        return max((level.price for level in self.bids), default=None)

    @property
    def best_ask(self) -> Decimal | None:
        return min((level.price for level in self.asks), default=None)

    @property
    def midpoint(self) -> Decimal | None:
        best_bid = self.best_bid
        best_ask = self.best_ask
        if best_bid is None or best_ask is None:
            return None
        return (best_bid + best_ask) / Decimal("2")

    @property
    def spread(self) -> Decimal | None:
        best_bid = self.best_bid
        best_ask = self.best_ask
        if best_bid is None or best_ask is None:
            return None
        return best_ask - best_bid


@dataclass(frozen=True, slots=True)
class PriceQuote:
    """Beste Kauf- und Verkaufspreise eines Tokens."""

    token_id: str
    buy: Decimal | None = None
    sell: Decimal | None = None


@dataclass(frozen=True, slots=True)
class LastTradePrice:
    """Letzter ausgeführter Preis eines Tokens."""

    token_id: str | None
    price: Decimal
    side: ClobSide | None


@dataclass(frozen=True, slots=True)
class PricePoint:
    """Ein historischer Preiswert mit Unix-Zeitstempel."""

    timestamp: int
    price: Decimal


@dataclass(frozen=True, slots=True)
class PriceHistoryParams:
    """Filter für ``GET /prices-history``."""

    market: str
    start_ts: int | None = None
    end_ts: int | None = None
    fidelity: int | None = None
    interval: PriceHistoryInterval | None = None

    def __post_init__(self) -> None:
        _require_identifier(self.market, "market")
        if self.interval is None and (self.start_ts is None or self.end_ts is None):
            raise ValueError(
                "PriceHistoryParams benötigt interval oder start_ts und end_ts"
            )
        if self.start_ts is not None and self.end_ts is not None:
            if self.start_ts > self.end_ts:
                raise ValueError("start_ts darf nicht nach end_ts liegen")
        if self.fidelity is not None and self.fidelity <= 0:
            raise ValueError("fidelity muss positiv sein")

    def to_query(self) -> dict[str, str | int]:
        query: dict[str, str | int] = {"market": self.market}
        if self.start_ts is not None:
            query["startTs"] = self.start_ts
        if self.end_ts is not None:
            query["endTs"] = self.end_ts
        if self.fidelity is not None:
            query["fidelity"] = self.fidelity
        if self.interval is not None:
            query["interval"] = self.interval.value
        return query


@dataclass(frozen=True, slots=True)
class BatchPriceHistoryParams:
    """Filter für ``POST /batch-prices-history`` (maximal 20 Tokens)."""

    markets: tuple[str, ...]
    start_ts: int | None = None
    end_ts: int | None = None
    fidelity: int | None = None
    interval: PriceHistoryInterval | None = None

    def __post_init__(self) -> None:
        normalized_markets = tuple(self.markets)
        if not 1 <= len(normalized_markets) <= 20:
            raise ValueError("markets muss zwischen 1 und 20 Token-IDs enthalten")
        for market in normalized_markets:
            _require_identifier(market, "market")
        object.__setattr__(self, "markets", normalized_markets)

        if self.interval is None and (self.start_ts is None or self.end_ts is None):
            raise ValueError(
                "BatchPriceHistoryParams benötigt interval oder start_ts und end_ts"
            )
        if self.start_ts is not None and self.end_ts is not None:
            if self.start_ts > self.end_ts:
                raise ValueError("start_ts darf nicht nach end_ts liegen")
        if self.fidelity is not None and self.fidelity <= 0:
            raise ValueError("fidelity muss positiv sein")

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"markets": list(self.markets)}
        if self.start_ts is not None:
            payload["start_ts"] = self.start_ts
        if self.end_ts is not None:
            payload["end_ts"] = self.end_ts
        if self.fidelity is not None:
            payload["fidelity"] = self.fidelity
        if self.interval is not None:
            payload["interval"] = self.interval.value
        return payload


@dataclass(frozen=True, slots=True)
class ClobMarketToken:
    token_id: str
    outcome: str
    price: Decimal | None
    winner: bool


@dataclass(frozen=True, slots=True)
class ClobMarket:
    """Für den Bot relevante Felder eines CLOB-Markts."""

    condition_id: str
    question_id: str | None
    question: str
    description: str | None
    market_slug: str | None
    active: bool
    closed: bool
    archived: bool
    accepting_orders: bool
    enable_order_book: bool
    minimum_order_size: Decimal | None
    minimum_tick_size: Decimal | None
    maker_base_fee: int | None
    taker_base_fee: int | None
    neg_risk: bool
    seconds_delay: int | None
    tokens: tuple[ClobMarketToken, ...]
    tags: tuple[str, ...]
    raw: Mapping[str, Any] = field(repr=False, compare=False)


@dataclass(frozen=True, slots=True)
class ClobMarketPage:
    """Cursor-basierte Antwort von ``GET /markets``."""

    data: tuple[ClobMarket, ...]
    next_cursor: str | None
    limit: int | None
    count: int | None

class ClobMarketAPI:
    """
    Client für öffentliche CLOB-Markt-, Preis- und Orderbuchdaten.

    Beispiel::

        async with CLOBAPI() as client:
            book = await client.get_order_book(token_id)
            price = await client.get_price(token_id, ClobSide.BUY)
    """

    def __init__(
        self,
        rate_limiter: RateLimiter | None = None,
        cache: UnifiedCache | None = None,
        *,
        http_client: httpx.AsyncClient | None = None,
        timeout: float = 20.0,
    ) -> None:
        self.rate_limiter = rate_limiter or RateLimiter()
        self.cache = cache  # Analog zu GammaAPI; derzeit nicht verwendet.

        self._owns_http_client = http_client is None
        self._http = http_client or httpx.AsyncClient(
            base_url=CLOB_API_BASE,
            timeout=timeout,
            headers={"Accept": "application/json"},
        )

    async def __aenter__(self) -> ClobMarketAPI:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self._owns_http_client:
            await self._http.aclose()

    async def is_healthy(self) -> bool:
        """Prüft ``GET /ok``; jede erfolgreiche HTTP-Antwort gilt als gesund."""

        await self._request("GET", "/ok", require_json=False)
        return True

    async def get_server_time(self) -> int:
        """Lädt die aktuelle Unix-Zeit des CLOB-Servers."""

        data = await self._request("GET", "/time")
        if isinstance(data, Mapping):
            data = data.get("timestamp", data.get("time"))
        return _required_int(data, context="GET /time")

    async def get_market(self, condition_id: str) -> ClobMarket | None:
        """Lädt einen CLOB-Markt anhand seiner Condition-ID."""

        _require_identifier(condition_id, "condition_id")
        try:
            data = await self._request("GET", f"/markets/{condition_id}")
        except PolymarketError as error:
            if error.code is ErrorCode.MARKET_NOT_FOUND:
                return None
            raise

        return self._normalize_market(_required_mapping(data, "GET /markets/{id}"))

    async def get_markets(self, next_cursor: str = INITIAL_CURSOR) -> ClobMarketPage:
        """Lädt eine cursor-basierte Seite vollständiger CLOB-Märkte."""

        data = await self._request(
            "GET",
            "/markets",
            params={"next_cursor": next_cursor},
        )
        payload = _required_mapping(data, "GET /markets")
        raw_markets = payload.get("data", [])
        if not _is_sequence(raw_markets):
            raise _invalid_response("GET /markets: data ist keine Liste")

        markets = tuple(
            self._normalize_market(
                _required_mapping(item, f"GET /markets data[{index}]")
            )
            for index, item in enumerate(raw_markets)
        )
        return ClobMarketPage(
            data=markets,
            next_cursor=_optional_str(payload.get("next_cursor")),
            limit=_optional_int(payload.get("limit")),
            count=_optional_int(payload.get("count")),
        )

    async def get_order_book(self, token_id: str) -> OrderBook:
        """Lädt und normalisiert das Orderbuch eines Tokens."""

        _require_identifier(token_id, "token_id")
        data = await self._request(
            "GET",
            "/book",
            params={"token_id": token_id},
        )
        return self._normalize_order_book(
            _required_mapping(data, "GET /book")
        )

    async def get_order_books(
        self,
        requests: Sequence[BookRequest | str],
    ) -> list[OrderBook]:
        """Lädt mehrere Orderbücher über ``POST /books``."""

        payload = _book_payload(requests)
        data = await self._request("POST", "/books", json_body=payload)
        if not _is_sequence(data):
            raise _invalid_response("POST /books lieferte keine Liste")
        return [
            self._normalize_order_book(
                _required_mapping(item, f"POST /books response[{index}]")
            )
            for index, item in enumerate(data)
        ]

    async def get_price(self, token_id: str, side: ClobSide) -> Decimal:
        """Lädt den besten ausführbaren Preis für Token und Handelsseite."""

        _require_identifier(token_id, "token_id")
        data = await self._request(
            "GET",
            "/price",
            params={"token_id": token_id, "side": side.value},
        )
        payload = _required_mapping(data, "GET /price")
        return _required_decimal(payload.get("price"), context="GET /price")

    async def get_prices(
        self,
        requests: Sequence[BookRequest],
    ) -> dict[str, PriceQuote]:
        """Lädt Kauf-/Verkaufspreise für mehrere Token."""

        data = await self._request(
            "POST",
            "/prices",
            json_body=_price_payload(requests),
        )
        payload = _required_mapping(data, "POST /prices")
        result: dict[str, PriceQuote] = {}

        for token_id, raw_quote in payload.items():
            quote = _required_mapping(
                raw_quote, f"POST /prices quote for {token_id}"
            )
            result[str(token_id)] = PriceQuote(
                token_id=str(token_id),
                buy=_decimal_field_if_present(
                    quote, "BUY", context=f"POST /prices {token_id}.BUY"
                ),
                sell=_decimal_field_if_present(
                    quote, "SELL", context=f"POST /prices {token_id}.SELL"
                ),
            )
        return result

    async def get_midpoint(self, token_id: str) -> Decimal:
        """Lädt den Mittelpunkt aus bestem Bid und bestem Ask."""

        _require_identifier(token_id, "token_id")
        data = await self._request(
            "GET",
            "/midpoint",
            params={"token_id": token_id},
        )
        payload = _required_mapping(data, "GET /midpoint")
        midpoint = payload.get("mid")
        if midpoint in (None, ""):
            midpoint = payload.get("mid_price")
        return _required_decimal(midpoint, context="GET /midpoint")

    async def get_midpoints(
        self,
        requests: Sequence[BookRequest | str],
    ) -> dict[str, Decimal]:
        """Lädt Mittelpunkte für mehrere Token."""

        data = await self._request(
            "POST",
            "/midpoints",
            json_body=_book_payload(requests),
        )
        return _normalize_decimal_map(data, "POST /midpoints")

    async def get_spread(self, token_id: str) -> Decimal:
        """Lädt die Differenz zwischen bestem Ask und bestem Bid."""

        _require_identifier(token_id, "token_id")
        data = await self._request(
            "GET",
            "/spread",
            params={"token_id": token_id},
        )
        payload = _required_mapping(data, "GET /spread")
        return _required_decimal(payload.get("spread"), context="GET /spread")

    async def get_spreads(
        self,
        requests: Sequence[BookRequest | str],
    ) -> dict[str, Decimal]:
        """Lädt Spreads für mehrere Token."""

        data = await self._request(
            "POST",
            "/spreads",
            json_body=_book_payload(requests),
        )
        return _normalize_decimal_map(data, "POST /spreads")

    async def get_last_trade_price(self, token_id: str) -> LastTradePrice:
        """Lädt den letzten ausgeführten Trade eines Tokens."""

        _require_identifier(token_id, "token_id")
        data = await self._request(
            "GET",
            "/last-trade-price",
            params={"token_id": token_id},
        )
        payload = _required_mapping(data, "GET /last-trade-price")
        return self._normalize_last_trade(payload, fallback_token_id=token_id)

    async def get_last_trade_prices(
        self,
        requests: Sequence[BookRequest | str],
    ) -> list[LastTradePrice]:
        """Lädt die letzten Trades mehrerer Token."""

        data = await self._request(
            "POST",
            "/last-trades-prices",
            json_body=_book_payload(requests),
        )
        if not _is_sequence(data):
            raise _invalid_response(
                "POST /last-trades-prices lieferte keine Liste"
            )
        return [
            self._normalize_last_trade(
                _required_mapping(
                    item, f"POST /last-trades-prices response[{index}]"
                )
            )
            for index, item in enumerate(data)
        ]

    async def get_price_history(
        self,
        params: PriceHistoryParams,
    ) -> list[PricePoint]:
        """Lädt historische Preiswerte eines Outcome-Tokens."""

        data = await self._request(
            "GET",
            "/prices-history",
            params=params.to_query(),
        )
        payload = _required_mapping(data, "GET /prices-history")
        raw_history = payload.get("history", [])
        if not _is_sequence(raw_history):
            raise _invalid_response(
                "GET /prices-history: history ist keine Liste"
            )

        return _normalize_price_points(raw_history, context="GET /prices-history")

    async def get_batch_price_history(
        self,
        params: BatchPriceHistoryParams,
    ) -> dict[str, list[PricePoint]]:
        """Lädt historische Preiswerte für bis zu 20 Outcome-Tokens."""

        if not isinstance(params, BatchPriceHistoryParams):
            raise TypeError("params muss BatchPriceHistoryParams sein")

        data = await self._request(
            "POST",
            "/batch-prices-history",
            json_body=params.to_payload(),
        )
        payload = _required_mapping(data, "POST /batch-prices-history")
        raw_history = _required_mapping(
            payload.get("history"), "POST /batch-prices-history history"
        )

        result: dict[str, list[PricePoint]] = {}
        for token_id, points in raw_history.items():
            if not _is_sequence(points):
                raise _invalid_response(
                    f"POST /batch-prices-history {token_id}: keine Liste"
                )
            result[str(token_id)] = _normalize_price_points(
                points,
                context=f"POST /batch-prices-history {token_id}",
            )
        return result

    async def get_tick_size(self, token_id: str) -> Decimal:
        """Lädt die minimale Preisänderung eines Tokens."""

        _require_identifier(token_id, "token_id")
        data = await self._request(
            "GET",
            "/tick-size",
            params={"token_id": token_id},
        )
        payload = _required_mapping(data, "GET /tick-size")
        return _required_decimal(
            payload.get("minimum_tick_size"),
            context="GET /tick-size",
        )

    async def get_neg_risk(self, token_id: str) -> bool:
        """Prüft, ob der Markt das Neg-Risk-Modell verwendet."""

        _require_identifier(token_id, "token_id")
        data = await self._request(
            "GET",
            "/neg-risk",
            params={"token_id": token_id},
        )
        payload = _required_mapping(data, "GET /neg-risk")
        return _required_bool(payload.get("neg_risk"), context="GET /neg-risk")

    async def get_fee_rate_bps(self, token_id: str) -> int:
        """Lädt die Basisgebühr des Tokens in Basispunkten."""

        _require_identifier(token_id, "token_id")
        data = await self._request(
            "GET",
            "/fee-rate",
            params={"token_id": token_id},
        )
        payload = _required_mapping(data, "GET /fee-rate")
        value = payload.get("base_fee", 0)
        return _required_int(value, context="GET /fee-rate")

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, str | int] | None = None,
        json_body: Any = None,
        require_json: bool = True,
    ) -> Any:
        """Zentralisiert HTTP, Rate-Limiting und Fehlerabbildung."""

        async def operation() -> Any:
            try:
                response = await self._http.request(
                    method,
                    path,
                    params=params,
                    json=json_body,
                )
            except httpx.TimeoutException as error:
                raise PolymarketError(
                    ErrorCode.TIMEOUT,
                    f"Zeitüberschreitung bei {method} {path}",
                    retryable=True,
                    original_error=error,
                ) from error
            except httpx.RequestError as error:
                raise PolymarketError(
                    ErrorCode.NETWORK_ERROR,
                    f"Netzwerkfehler bei {method} {path}: {error}",
                    retryable=True,
                    original_error=error,
                ) from error

            if not response.is_success:
                body = _response_error_body(response)
                raise PolymarketError.from_http_error(
                    response.status_code,
                    body,
                )

            if not require_json:
                return response.text
            if response.status_code == 204 or not response.content:
                return None

            try:
                return response.json()
            except ValueError as error:
                raise PolymarketError(
                    ErrorCode.INVALID_RESPONSE,
                    f"{method} {path} lieferte kein gültiges JSON",
                    original_error=error,
                ) from error

        return await self.rate_limiter.execute(ApiType.CLOB_API, operation)

    def _normalize_order_book(self, book: Mapping[str, Any]) -> OrderBook:
        bids = _normalize_levels(book.get("bids"), context="bids")
        asks = _normalize_levels(book.get("asks"), context="asks")

        return OrderBook(
            market=_required_str(book.get("market"), context="order book market"),
            asset_id=_required_str(
                book.get("asset_id"), context="order book asset_id"
            ),
            timestamp=_optional_str(book.get("timestamp")),
            bids=bids,
            asks=asks,
            min_order_size=_optional_decimal(book.get("min_order_size")),
            tick_size=_optional_decimal(book.get("tick_size")),
            neg_risk=_optional_bool(book.get("neg_risk"), default=False),
            last_trade_price=_decimal_field_if_present(
                book, "last_trade_price", context="order book last_trade_price"
            ),
            hash=_optional_str(book.get("hash")),
        )

    def _normalize_last_trade(
        self,
        trade: Mapping[str, Any],
        *,
        fallback_token_id: str | None = None,
    ) -> LastTradePrice:
        return LastTradePrice(
            token_id=_optional_str(trade.get("token_id")) or fallback_token_id,
            price=_required_decimal(
                trade.get("price"), context="last trade price"
            ),
            side=_optional_side(trade.get("side")),
        )

    def _normalize_market(self, market: Mapping[str, Any]) -> ClobMarket:
        raw_tokens = market.get("tokens", [])
        if not _is_sequence(raw_tokens):
            raise _invalid_response("CLOB-Markt: tokens ist keine Liste")

        tokens = tuple(
            self._normalize_market_token(
                _required_mapping(token, f"CLOB-Markt tokens[{index}]")
            )
            for index, token in enumerate(raw_tokens)
        )

        raw_tags = market.get("tags", [])
        if not _is_sequence(raw_tags):
            raise _invalid_response("CLOB-Markt: tags ist keine Liste")
        tags = tuple(
            _required_str(tag, context=f"market tags[{index}]")
            for index, tag in enumerate(raw_tags)
        )

        return ClobMarket(
            condition_id=_required_str(
                market.get("condition_id"), context="market condition_id"
            ),
            question_id=_optional_str(market.get("question_id")),
            question=_required_str(
                market.get("question"), context="market question"
            ),
            description=_optional_str(market.get("description")),
            market_slug=_optional_str(market.get("market_slug")),
            active=_optional_bool(market.get("active"), default=False),
            closed=_optional_bool(market.get("closed"), default=False),
            archived=_optional_bool(market.get("archived"), default=False),
            accepting_orders=_optional_bool(
                market.get("accepting_orders"), default=False
            ),
            enable_order_book=_optional_bool(
                market.get("enable_order_book"), default=False
            ),
            minimum_order_size=_optional_decimal(
                market.get("minimum_order_size")
            ),
            minimum_tick_size=_optional_decimal(
                market.get("minimum_tick_size")
            ),
            maker_base_fee=_optional_int(market.get("maker_base_fee")),
            taker_base_fee=_optional_int(market.get("taker_base_fee")),
            neg_risk=_optional_bool(market.get("neg_risk"), default=False),
            seconds_delay=_optional_int(market.get("seconds_delay")),
            tokens=tokens,
            tags=tags,
            raw=dict(market),
        )

    @staticmethod
    def _normalize_market_token(token: Mapping[str, Any]) -> ClobMarketToken:
        return ClobMarketToken(
            token_id=_required_str(
                token.get("token_id"), context="market token_id"
            ),
            outcome=_required_str(
                token.get("outcome"), context="market outcome"
            ),
            price=_decimal_field_if_present(
                token, "price", context="market token price"
            ),
            winner=_optional_bool(token.get("winner"), default=False),
        )


def _book_payload(
    requests: Sequence[BookRequest | str],
) -> list[dict[str, str]]:
    if not requests:
        raise ValueError("Mindestens ein Token-Request ist erforderlich")

    payload: list[dict[str, str]] = []
    for request in requests:
        if isinstance(request, str):
            request = BookRequest(token_id=request)
        if not isinstance(request, BookRequest):
            raise TypeError("Batch-Requests müssen BookRequest oder str sein")
        payload.append(request.to_payload())
    return payload


def _price_payload(requests: Sequence[BookRequest]) -> list[dict[str, str]]:
    payload = _book_payload(requests)
    for index, request in enumerate(payload):
        if "side" not in request:
            raise ValueError(
                f"Price-Request an Position {index} benötigt BUY oder SELL"
            )
    return payload


def _normalize_levels(value: Any, *, context: str) -> tuple[ClobOrderBookLevel, ...]:
    if value is None:
        return ()
    if not _is_sequence(value):
        raise _invalid_response(f"Orderbuch {context} ist keine Liste")

    levels: list[ClobOrderBookLevel] = []
    for item in value:
        if not isinstance(item, Mapping):
            raise _invalid_response(f"Orderbuch {context} enthält kein Objekt")
        levels.append(
            ClobOrderBookLevel(
                price=_required_decimal(
                    item.get("price"), context=f"{context}.price"
                ),
                size=_required_decimal(
                    item.get("size"), context=f"{context}.size"
                ),
            )
        )
    return tuple(levels)


def _normalize_price_points(value: Sequence[Any], *, context: str) -> list[PricePoint]:
    points: list[PricePoint] = []
    for index, item in enumerate(value):
        point = _required_mapping(item, f"{context}[{index}]")
        points.append(
            PricePoint(
                timestamp=_required_int(
                    point.get("t"), context=f"{context}[{index}].t"
                ),
                price=_required_decimal(
                    point.get("p"), context=f"{context}[{index}].p"
                ),
            )
        )
    return points


def _decimal_field_if_present(
    data: Mapping[str, Any],
    key: str,
    *,
    context: str,
) -> Decimal | None:
    if key not in data or data[key] in (None, ""):
        return None
    return _required_decimal(data[key], context=context)


def _normalize_decimal_map(data: Any, context: str) -> dict[str, Decimal]:
    payload = _required_mapping(data, context)
    return {
        str(token_id): _required_decimal(value, context=f"{context} {token_id}")
        for token_id, value in payload.items()
    }


def _response_error_body(response: httpx.Response) -> Any:
    try:
        body = response.json()
    except ValueError:
        text = response.text.strip()
        return {"message": text} if text else None
    return body


def _required_mapping(value: Any, context: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise _invalid_response(f"{context} lieferte kein JSON-Objekt")
    return value


def _required_decimal(value: Any, *, context: str) -> Decimal:
    parsed = _optional_decimal(value)
    if parsed is None:
        raise _invalid_response(f"{context}: ungültiger oder fehlender Zahlenwert")
    return parsed


def _optional_decimal(value: Any) -> Decimal | None:
    if value in (None, "") or isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    if not parsed.is_finite():
        return None
    return parsed


def _required_int(value: Any, *, context: str) -> int:
    parsed = _optional_int(value)
    if parsed is None:
        raise _invalid_response(f"{context}: ungültiger oder fehlender Integer")
    return parsed


def _optional_int(value: Any) -> int | None:
    if value in (None, "") or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _required_bool(value: Any, *, context: str) -> bool:
    parsed = _parse_bool(value)
    if parsed is None:
        raise _invalid_response(f"{context}: ungültiger boolescher Wert")
    return parsed


def _optional_bool(value: Any, *, default: bool) -> bool:
    parsed = _parse_bool(value)
    return default if parsed is None else parsed


def _parse_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in (0, 1):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes"}:
            return True
        if normalized in {"false", "0", "no"}:
            return False
    return None


def _required_str(value: Any, *, context: str) -> str:
    parsed = _optional_str(value)
    if parsed is None:
        raise _invalid_response(f"{context}: fehlender String")
    return parsed


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None


def _optional_side(value: Any) -> ClobSide | None:
    if value in (None, ""):
        return None
    try:
        return ClobSide(str(value).upper())
    except ValueError:
        return None


def _require_identifier(value: str, name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} darf nicht leer sein")


def _is_sequence(value: Any) -> bool:
    return isinstance(value, Sequence) and not isinstance(value, (str, bytes))


def _invalid_response(message: str) -> PolymarketError:
    return PolymarketError(ErrorCode.INVALID_RESPONSE, message)
