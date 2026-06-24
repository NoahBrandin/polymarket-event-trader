"""
Asynchroner Python-Client für die öffentliche Polymarket Gamma API.

Übertragung von:
https://github.com/MrFadiAi/Polymarket-bot/blob/main/src/clients/gamma-api.ts

Abhängigkeit:
    pip install httpx

Die Gamma API dient der Markt- und Event-Suche. Sie ist kein Trading-Client.
"""

from __future__ import annotations

import json
from collections.abc import  Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import httpx

from .utils.utils import (
    ApiType,
    ErrorCode,
    PolymarketError,
    RateLimiter,
    UnifiedCache,
)

GAMMA_API_BASE = "https://gamma-api.polymarket.com"

@dataclass(slots=True)
class GammaMarket:
    """Repräsentiert einen normalisierten Polymarket-Markt."""
    id: str
    condition_id: str
    slug: str
    question: str
    description: str | None
    outcomes: list[str]
    outcome_prices: list[float]
    volume: float
    volume_24hr: float | None
    volume_1wk: float | None
    liquidity: float
    spread: float | None
    one_day_price_change: float | None
    one_week_price_change: float | None
    last_trade_price: float | None
    best_bid: float | None
    best_ask: float | None
    end_date: datetime
    created_at: datetime | None
    start_date: datetime | None
    accepting_orders_timestamp: datetime | None
    active: bool
    closed: bool
    image: str | None
    icon: str | None
    tags: list[str] | None
    clob_token_ids: list[str]


@dataclass(slots=True)
class GammaEvent:
    id: str
    slug: str
    title: str
    description: str | None
    markets: list[GammaMarket]
    start_date: datetime | None
    end_date: datetime | None
    image: str | None


@dataclass(slots=True)
class MarketSearchParams:
    slug: str | None = None
    condition_id: str | None = None
    active: bool | None = None
    closed: bool | None = None
    limit: int | None = None
    offset: int | None = None
    order: str | None = None
    ascending: bool | None = None

    # Aktuelle API-Referenz: numerische Tag-ID.
    tag_id: int | None = None

    # Kompatibilität mit dem Ausgangsprojekt, das einen String unter "tag"
    # sendet. Für neuen Code ist tag_id vorzuziehen.
    tag: str | None = None


@dataclass(slots=True)
class EventSearchParams:
    slug: str | None = None
    active: bool | None = None
    closed: bool | None = None
    limit: int | None = None
    offset: int | None = None
    order: str | None = None
    ascending: bool | None = None


class GammaAPI:
    """
    Client für Marktsuche, Events und Markt-Metadaten.

    Verwendung:
        async with GammaApiClient() as client:
            markets = await client.get_trending_markets(10)
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
        self.cache = cache  # Wie im TS-Original vorhanden, aktuell nicht benutzt.

        self._owns_http_client = http_client is None
        self._http = http_client or httpx.AsyncClient(
            base_url=GAMMA_API_BASE,
            timeout=timeout,
            headers={"Accept": "application/json"},
        )

    async def __aenter__(self) -> "GammaAPI":
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self._owns_http_client:
            await self._http.aclose()

    async def get_markets(
        self,
        params: MarketSearchParams | None = None,
    ) -> list[GammaMarket]:
        """Lädt Märkte mit optionalen Filtern, Sortierung und Pagination."""

        query: dict[str, str | int] = {}

        if params is not None:
            if params.slug is not None:
                query["slug"] = params.slug

            if params.condition_id is not None:
                # Aktuelle Referenz verwendet den pluralisierten Parameter.
                query["condition_ids"] = params.condition_id

            if params.active is not None:
                query["active"] = _bool_query(params.active)

            if params.closed is not None:
                query["closed"] = _bool_query(params.closed)

            if params.limit is not None:
                query["limit"] = params.limit

            if params.offset is not None:
                query["offset"] = params.offset

            if params.order is not None:
                query["order"] = params.order

            if params.ascending is not None:
                query["ascending"] = _bool_query(params.ascending)

            if params.tag_id is not None:
                query["tag_id"] = params.tag_id
            elif params.tag is not None:
                # Legacy-Kompatibilität zum TypeScript-Ausgangscode.
                query["tag"] = params.tag

        data = await self._get_json("/markets", params=query)
        if not isinstance(data, list):
            return []

        return [
            self._normalize_market(item)
            for item in data
            if isinstance(item, Mapping)
        ]

    async def get_market_by_slug(self, slug: str) -> GammaMarket | None:
        """Lädt den ersten Markt mit dem angegebenen Slug."""

        markets = await self.get_markets(
            MarketSearchParams(slug=slug, limit=1)
        )
        return markets[0] if markets else None

    async def get_market_by_condition_id(
        self,
        condition_id: str,
    ) -> GammaMarket | None:
        """Lädt den ersten Markt mit der angegebenen Condition-ID."""

        markets = await self.get_markets(
            MarketSearchParams(condition_id=condition_id, limit=1)
        )
        return markets[0] if markets else None

    async def get_events(
        self,
        params: EventSearchParams | None = None,
    ) -> list[GammaEvent]:
        """Lädt Events mit optionalen Filtern."""

        query: dict[str, str | int] = {}

        if params is not None:
            if params.slug is not None:
                query["slug"] = params.slug

            if params.active is not None:
                query["active"] = _bool_query(params.active)

            if params.closed is not None:
                query["closed"] = _bool_query(params.closed)

            if params.limit is not None:
                query["limit"] = params.limit

            if params.offset is not None:
                query["offset"] = params.offset

            if params.order is not None:
                query["order"] = params.order

            if params.ascending is not None:
                query["ascending"] = _bool_query(params.ascending)

        data = await self._get_json("/events", params=query)
        if not isinstance(data, list):
            return []

        return [
            self._normalize_event(item)
            for item in data
            if isinstance(item, Mapping)
        ]

    async def get_event_by_slug(self, slug: str) -> GammaEvent | None:
        """Lädt das erste Event mit dem angegebenen Slug."""

        events = await self.get_events(
            EventSearchParams(slug=slug, limit=1)
        )
        return events[0] if events else None

    async def get_event_by_id(self, event_id: str) -> GammaEvent | None:
        """Lädt ein Event über dessen interne ID; 404 wird zu None."""

        try:
            data = await self._get_json(f"/events/{event_id}")
        except PolymarketError as error:
            if error.code is ErrorCode.MARKET_NOT_FOUND:
                return None
            raise

        if not isinstance(data, Mapping):
            raise PolymarketError(
                ErrorCode.INVALID_RESPONSE,
                "Die Event-Antwort ist kein JSON-Objekt",
            )

        return self._normalize_event(data)

    async def get_trending_markets(
        self,
        limit: int = 20,
    ) -> list[GammaMarket]:
        """
        Aktive, nicht geschlossene Märkte nach 24h-Volumen absteigend.

        Die aktuelle Polymarket-Dokumentation verwendet als Sortierfeld
        `volume_24hr`. Das TypeScript-Ausgangsprojekt verwendet `volume24hr`.
        """

        return await self.get_markets(
            MarketSearchParams(
                active=True,
                closed=False,
                order="volume_24hr",
                ascending=False,
                limit=limit,
            )
        )

    async def _get_json(
        self,
        path: str,
        *,
        params: Mapping[str, str | int] | None = None,
    ) -> Any:
        """
        Zentraler HTTP-Pfad: Jeder Request läuft durch denselben Rate-Limiter und
        wird in stabile PolymarketError-Fehlercodes übersetzt.
        """
        async def operation() -> Any:
            try:
                response = await self._http.get(path, params=params)
            except httpx.TimeoutException as error:
                raise PolymarketError(
                    ErrorCode.TIMEOUT,
                    f"Zeitüberschreitung bei GET {path}",
                    retryable=True,
                    original_error=error,
                ) from error
            except httpx.RequestError as error:
                raise PolymarketError(
                    ErrorCode.NETWORK_ERROR,
                    f"Netzwerkfehler bei GET {path}: {error}",
                    retryable=True,
                    original_error=error,
                ) from error

            if not response.is_success:
                try:
                    body: Any = response.json()
                except ValueError:
                    body = None
                raise PolymarketError.from_http_error(
                    response.status_code,
                    body,
                )

            try:
                return response.json()
            except ValueError as error:
                raise PolymarketError(
                    ErrorCode.INVALID_RESPONSE,
                    f"GET {path} lieferte kein gültiges JSON",
                    original_error=error,
                ) from error

        return await self.rate_limiter.execute(
            ApiType.GAMMA_API,
            operation,
        )

    def _normalize_market(self, market: Mapping[str, Any]) -> GammaMarket:
        """
        API-Normalisierung: Gamma liefert Zahlen und Listen teilweise als Strings.
        Hier werden sie defensiv in die typisierten Python-Modelle überführt.
        """
        outcomes_raw = _parse_json_array(
            market.get("outcomes"),
            fallback=["Yes", "No"],
        )
        prices_raw = _parse_json_array(
            market.get("outcomePrices"),
            fallback=[0.5, 0.5],
        )

        return GammaMarket(
            id=_to_str(market.get("id")),
            condition_id=_to_str(market.get("conditionId")),
            slug=_to_str(market.get("slug")),
            question=_to_str(market.get("question")),
            description=_optional_str(market.get("description")),
            outcomes=[str(value) for value in outcomes_raw],
            outcome_prices=[
                _to_float(value, default=0.0)
                for value in prices_raw
            ],
            volume=_to_float(market.get("volume"), default=0.0),
            volume_24hr=_optional_float(market.get("volume24hr")),
            volume_1wk=_optional_float(market.get("volume1wk")),
            liquidity=_to_float(market.get("liquidity"), default=0.0),
            spread=_optional_float(market.get("spread")),
            one_day_price_change=_optional_float(
                market.get("oneDayPriceChange")
            ),
            one_week_price_change=_optional_float(
                market.get("oneWeekPriceChange")
            ),
            last_trade_price=_optional_float(
                market.get("lastTradePrice")
            ),
            best_bid=_optional_float(market.get("bestBid")),
            best_ask=_optional_float(market.get("bestAsk")),
            end_date=_parse_datetime(
                market.get("endDate"),
                fallback=datetime.now(timezone.utc),
            ),
            created_at=_parse_optional_datetime(
                market.get("createdAt")
            ),
            start_date=_parse_optional_datetime(
                market.get("startDate")
            ),
            accepting_orders_timestamp=_parse_optional_datetime(
                market.get("acceptingOrdersTimestamp")
            ),
            active=_to_bool(market.get("active")),
            closed=_to_bool(market.get("closed")),
            image=_optional_str(market.get("image")),
            icon=_optional_str(market.get("icon")),
            tags=_normalize_tags(market.get("tags")),
            clob_token_ids=[
                str(value)
                for value in _parse_json_array(
                    market.get("clobTokenIds"),
                    fallback=[],
                )
            ],
        )

    def _normalize_event(self, event: Mapping[str, Any]) -> GammaEvent:
        raw_markets = event.get("markets")

        markets = (
            [
                self._normalize_market(item)
                for item in raw_markets
                if isinstance(item, Mapping)
            ]
            if isinstance(raw_markets, Sequence)
            and not isinstance(raw_markets, (str, bytes))
            else []
        )

        return GammaEvent(
            id=_to_str(event.get("id")),
            slug=_to_str(event.get("slug")),
            title=_to_str(event.get("title")),
            description=_optional_str(event.get("description")),
            markets=markets,
            start_date=_parse_optional_datetime(
                event.get("startDate")
            ),
            end_date=_parse_optional_datetime(
                event.get("endDate")
            ),
            image=_optional_str(event.get("image")),
        )


def _bool_query(value: bool) -> str:
    """Konvertiert einen booleschen Wert in das Query-Format der Gamma API."""
    return "true" if value else "false"


def _to_str(value: Any) -> str:
    return "" if value is None else str(value)


def _optional_str(value: Any) -> str | None:
    if value is None or value == "":
        return None
    return str(value)


def _to_float(value: Any, *, default: float) -> float:
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _optional_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value

    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes"}:
            return True
        if normalized in {"false", "0", "no", ""}:
            return False

    return bool(value)


def _parse_json_array(value: Any, *, fallback: list[T]) -> list[Any]:
    if isinstance(value, list):
        return value

    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return list(fallback)

        return parsed if isinstance(parsed, list) else list(fallback)

    return list(fallback)


def _parse_datetime(
    value: Any,
    *,
    fallback: datetime,
) -> datetime:
    parsed = _parse_optional_datetime(value)
    return parsed if parsed is not None else fallback


def _parse_optional_datetime(value: Any) -> datetime | None:
    if value is None or value == "":
        return None

    text = str(value).strip()

    # datetime.fromisoformat akzeptiert "+00:00", aber nicht in allen
    # Python-Versionen ein abschließendes "Z".
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"

    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)

    return parsed


def _normalize_tags(value: Any) -> list[str] | None:
    """
    Das Ausgangsprojekt typisiert Tags als String-Liste. Die aktuelle API kann
    jedoch auch Tag-Objekte mit id/label/slug liefern. Diese Funktion bildet
    beides auf eine einfache String-Liste ab.
    """

    raw_tags = _parse_json_array(value, fallback=[])
    if not raw_tags:
        return None

    result: list[str] = []

    for tag in raw_tags:
        if isinstance(tag, str):
            result.append(tag)
        elif isinstance(tag, Mapping):
            candidate = (
                tag.get("slug")
                or tag.get("label")
                or tag.get("id")
            )
            if candidate is not None:
                result.append(str(candidate))

    return result or None
