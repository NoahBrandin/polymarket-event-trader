"""Asynchroner Client für die öffentliche Polymarket Data API.

Die Data API liefert nutzer- und marktbezogene Analysedaten wie Positionen,
Trades, Aktivität, Open Interest, Holder und Leaderboards. Alle hier
verwendeten Endpunkte sind öffentlich und benötigen keine Authentifizierung.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any

import httpx

from .gamma_api import ApiType, ErrorCode, PolymarketError, RateLimiter

DATA_API_BASE = "https://data-api.polymarket.com"


class SortDirection(str, Enum):
    """Sortierrichtung der Data-API-Endpunkte."""

    ASC = "ASC"
    DESC = "DESC"


class PositionSortBy(str, Enum):
    """Zulässige Sortierfelder für offene Positionen."""

    CURRENT = "CURRENT"
    INITIAL = "INITIAL"
    CASHPNL = "CASHPNL"
    PERCENTPNL = "PERCENTPNL"
    TITLE = "TITLE"
    RESOLVING = "RESOLVING"
    PRICE = "PRICE"
    AVGPRICE = "AVGPRICE"


class ClosedPositionSortBy(str, Enum):
    """Zulässige Sortierfelder für geschlossene Positionen."""

    REALIZEDPNL = "REALIZEDPNL"
    TITLE = "TITLE"
    PRICE = "PRICE"
    AVGPRICE = "AVGPRICE"
    TIMESTAMP = "TIMESTAMP"


@dataclass(slots=True)
class PositionQuery:
    """Filter und Pagination für ``GET /positions``."""

    user: str
    markets: Sequence[str] | None = None
    event_ids: Sequence[int] | None = None
    size_threshold: float | None = None
    redeemable: bool | None = None
    mergeable: bool | None = None
    limit: int | None = None
    offset: int | None = None
    sort_by: PositionSortBy | str | None = None
    sort_direction: SortDirection | str | None = None
    title: str | None = None


@dataclass(slots=True)
class ClosedPositionQuery:
    """Filter und Pagination für ``GET /closed-positions``."""

    user: str
    markets: Sequence[str] | None = None
    event_ids: Sequence[int] | None = None
    title: str | None = None
    limit: int | None = None
    offset: int | None = None
    sort_by: ClosedPositionSortBy | str | None = None
    sort_direction: SortDirection | str | None = None


@dataclass(slots=True)
class TradeQuery:
    """Filter und Pagination für ``GET /trades``."""

    user: str | None = None
    markets: Sequence[str] | None = None
    event_ids: Sequence[int] | None = None
    taker_only: bool | None = None
    filter_type: str | None = None
    filter_amount: float | None = None
    side: str | None = None
    limit: int | None = None
    offset: int | None = None


@dataclass(slots=True)
class ActivityQuery:
    """Filter und Pagination für ``GET /activity``."""

    user: str
    markets: Sequence[str] | None = None
    event_ids: Sequence[int] | None = None
    activity_types: Sequence[str] | None = None
    start: int | None = None
    end: int | None = None
    sort_by: str | None = None
    sort_direction: SortDirection | str | None = None
    limit: int | None = None
    offset: int | None = None


@dataclass(slots=True)
class DataPosition:
    proxy_wallet: str
    asset: str
    condition_id: str
    size: float
    avg_price: float
    initial_value: float
    current_value: float
    cash_pnl: float
    percent_pnl: float
    total_bought: float
    realized_pnl: float
    percent_realized_pnl: float
    current_price: float
    redeemable: bool
    mergeable: bool
    title: str
    slug: str
    icon: str | None
    event_slug: str
    outcome: str
    outcome_index: int
    opposite_outcome: str | None
    opposite_asset: str | None
    end_date: datetime | None
    negative_risk: bool


@dataclass(slots=True)
class ClosedPosition:
    proxy_wallet: str
    asset: str
    condition_id: str
    avg_price: float
    total_bought: float
    realized_pnl: float
    current_price: float
    timestamp: int
    title: str
    slug: str
    icon: str | None
    event_slug: str
    outcome: str
    outcome_index: int
    opposite_outcome: str | None
    opposite_asset: str | None
    end_date: datetime | None


@dataclass(slots=True)
class DataTrade:
    proxy_wallet: str
    side: str
    asset: str
    condition_id: str
    size: float
    price: float
    timestamp: int
    title: str
    slug: str
    icon: str | None
    event_slug: str
    outcome: str
    outcome_index: int
    name: str | None
    pseudonym: str | None
    bio: str | None
    profile_image: str | None
    profile_image_optimized: str | None
    transaction_hash: str | None


@dataclass(slots=True)
class UserActivity:
    proxy_wallet: str
    timestamp: int
    condition_id: str
    activity_type: str
    size: float
    usdc_size: float
    transaction_hash: str | None
    price: float | None
    asset: str | None
    side: str | None
    outcome_index: int | None
    title: str | None
    slug: str | None
    icon: str | None
    event_slug: str | None
    outcome: str | None
    name: str | None
    pseudonym: str | None
    bio: str | None
    profile_image: str | None
    profile_image_optimized: str | None


@dataclass(slots=True)
class MarketHolder:
    proxy_wallet: str
    amount: float
    outcome_index: int | None
    name: str | None
    pseudonym: str | None
    bio: str | None
    profile_image: str | None
    profile_image_optimized: str | None


@dataclass(slots=True)
class LeaderboardEntry:
    rank: int
    proxy_wallet: str
    user_name: str | None
    volume: float
    pnl: float
    profile_image: str | None
    x_username: str | None
    verified_badge: bool


class DataApiClient:
    """Client für Positionen, Trades, Aktivität und Data-API-Analysen."""

    def __init__(
        self,
        rate_limiter: RateLimiter | None = None,
        *,
        http_client: httpx.AsyncClient | None = None,
        timeout: float = 20.0,
    ) -> None:
        self.rate_limiter = rate_limiter or RateLimiter()
        self._owns_http_client = http_client is None
        self._http = http_client or httpx.AsyncClient(
            base_url=DATA_API_BASE,
            timeout=timeout,
            headers={"Accept": "application/json"},
        )

    async def __aenter__(self) -> "DataApiClient":
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self._owns_http_client:
            await self._http.aclose()

    async def get_positions(self, query: PositionQuery) -> list[DataPosition]:
        """Lädt aktuelle Positionen eines Nutzers."""

        data = await self._get_json("/positions", params=_position_params(query))
        return [self._normalize_position(item) for item in _mapping_list(data)]

    async def get_closed_positions(
        self, query: ClosedPositionQuery
    ) -> list[ClosedPosition]:
        """Lädt geschlossene Positionen eines Nutzers."""

        data = await self._get_json(
            "/closed-positions", params=_closed_position_params(query)
        )
        return [self._normalize_closed_position(item) for item in _mapping_list(data)]

    async def get_trades(self, query: TradeQuery | None = None) -> list[DataTrade]:
        """Lädt öffentliche Trades für Nutzer, Märkte oder Events."""

        data = await self._get_json(
            "/trades", params=_trade_params(query) if query else None
        )
        return [self._normalize_trade(item) for item in _mapping_list(data)]

    async def get_activity(self, query: ActivityQuery) -> list[UserActivity]:
        """Lädt die Onchain-Aktivität eines Nutzers."""

        data = await self._get_json("/activity", params=_activity_params(query))
        return [self._normalize_activity(item) for item in _mapping_list(data)]

    async def get_position_value(self, user: str) -> float:
        """Lädt den Gesamtwert der offenen Positionen eines Nutzers."""

        data = await self._get_json("/value", params={"user": user})
        if isinstance(data, Sequence) and not isinstance(data, (str, bytes)):
            if not data:
                return 0.0
            first = data[0]
            if isinstance(first, Mapping):
                return _to_float(first.get("value"), default=0.0)
        if isinstance(data, Mapping):
            return _to_float(data.get("value"), default=0.0)
        raise PolymarketError(
            ErrorCode.INVALID_RESPONSE,
            "Die Value-Antwort besitzt kein unterstütztes Format",
        )

    async def get_open_interest(self, condition_ids: Sequence[str]) -> dict[str, float]:
        """Lädt Open Interest für eine oder mehrere Condition-IDs."""

        data = await self._get_json(
            "/oi", params={"market": _csv(condition_ids)}
        )
        result: dict[str, float] = {}
        for item in _mapping_list(data):
            condition_id = _to_str(item.get("market") or item.get("conditionId"))
            result[condition_id] = _to_float(
                item.get("value") or item.get("openInterest"), default=0.0
            )
        return result

    async def get_holders(
        self, condition_ids: Sequence[str], *, limit: int | None = None
    ) -> dict[str, list[MarketHolder]]:
        """Lädt die größten Holder je Markt."""

        params: dict[str, str | int] = {"market": _csv(condition_ids)}
        if limit is not None:
            params["limit"] = limit
        data = await self._get_json("/holders", params=params)
        result: dict[str, list[MarketHolder]] = {}
        for market in _mapping_list(data):
            condition_id = _to_str(
                market.get("market") or market.get("conditionId")
            )
            raw_holders = market.get("holders")
            result[condition_id] = [
                self._normalize_holder(holder)
                for holder in _mapping_list(raw_holders)
            ]
        return result

    async def get_leaderboard(
        self,
        *,
        category: str = "OVERALL",
        time_period: str = "DAY",
        order_by: str = "PNL",
        limit: int = 25,
        offset: int = 0,
    ) -> list[LeaderboardEntry]:
        """Lädt das öffentliche Trader-Leaderboard."""

        data = await self._get_json(
            "/v1/leaderboard",
            params={
                "category": category,
                "timePeriod": time_period,
                "orderBy": order_by,
                "limit": limit,
                "offset": offset,
            },
        )
        return [self._normalize_leaderboard(item) for item in _mapping_list(data)]

    async def get_traded_market_count(self, user: str) -> int:
        """Lädt die Zahl der Märkte, in denen ein Nutzer gehandelt hat."""

        data = await self._get_json("/traded", params={"user": user})
        if isinstance(data, Mapping):
            return _to_int(
                data.get("traded")
                or data.get("count")
                or data.get("total"),
                default=0,
            )
        return _to_int(data, default=0)

    async def _get_json(
        self,
        path: str,
        *,
        params: Mapping[str, str | int | float] | None = None,
    ) -> Any:
        """Führt einen GET-Request mit Rate-Limit und stabiler Fehlerabbildung aus."""

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
                raise PolymarketError.from_http_error(response.status_code, body)

            try:
                return response.json()
            except ValueError as error:
                raise PolymarketError(
                    ErrorCode.INVALID_RESPONSE,
                    f"GET {path} lieferte kein gültiges JSON",
                    original_error=error,
                ) from error

        return await self.rate_limiter.execute(ApiType.DATA_API, operation)

    def _normalize_position(self, item: Mapping[str, Any]) -> DataPosition:
        return DataPosition(
            proxy_wallet=_to_str(item.get("proxyWallet")),
            asset=_to_str(item.get("asset")),
            condition_id=_to_str(item.get("conditionId")),
            size=_to_float(item.get("size"), default=0.0),
            avg_price=_to_float(item.get("avgPrice"), default=0.0),
            initial_value=_to_float(item.get("initialValue"), default=0.0),
            current_value=_to_float(item.get("currentValue"), default=0.0),
            cash_pnl=_to_float(item.get("cashPnl"), default=0.0),
            percent_pnl=_to_float(item.get("percentPnl"), default=0.0),
            total_bought=_to_float(item.get("totalBought"), default=0.0),
            realized_pnl=_to_float(item.get("realizedPnl"), default=0.0),
            percent_realized_pnl=_to_float(
                item.get("percentRealizedPnl"), default=0.0
            ),
            current_price=_to_float(item.get("curPrice"), default=0.0),
            redeemable=_to_bool(item.get("redeemable")),
            mergeable=_to_bool(item.get("mergeable")),
            title=_to_str(item.get("title")),
            slug=_to_str(item.get("slug")),
            icon=_optional_str(item.get("icon")),
            event_slug=_to_str(item.get("eventSlug")),
            outcome=_to_str(item.get("outcome")),
            outcome_index=_to_int(item.get("outcomeIndex"), default=0),
            opposite_outcome=_optional_str(item.get("oppositeOutcome")),
            opposite_asset=_optional_str(item.get("oppositeAsset")),
            end_date=_parse_optional_datetime(item.get("endDate")),
            negative_risk=_to_bool(item.get("negativeRisk")),
        )

    def _normalize_closed_position(self, item: Mapping[str, Any]) -> ClosedPosition:
        return ClosedPosition(
            proxy_wallet=_to_str(item.get("proxyWallet")),
            asset=_to_str(item.get("asset")),
            condition_id=_to_str(item.get("conditionId")),
            avg_price=_to_float(item.get("avgPrice"), default=0.0),
            total_bought=_to_float(item.get("totalBought"), default=0.0),
            realized_pnl=_to_float(item.get("realizedPnl"), default=0.0),
            current_price=_to_float(item.get("curPrice"), default=0.0),
            timestamp=_to_int(item.get("timestamp"), default=0),
            title=_to_str(item.get("title")),
            slug=_to_str(item.get("slug")),
            icon=_optional_str(item.get("icon")),
            event_slug=_to_str(item.get("eventSlug")),
            outcome=_to_str(item.get("outcome")),
            outcome_index=_to_int(item.get("outcomeIndex"), default=0),
            opposite_outcome=_optional_str(item.get("oppositeOutcome")),
            opposite_asset=_optional_str(item.get("oppositeAsset")),
            end_date=_parse_optional_datetime(item.get("endDate")),
        )

    def _normalize_trade(self, item: Mapping[str, Any]) -> DataTrade:
        return DataTrade(
            proxy_wallet=_to_str(item.get("proxyWallet")),
            side=_to_str(item.get("side")),
            asset=_to_str(item.get("asset")),
            condition_id=_to_str(item.get("conditionId")),
            size=_to_float(item.get("size"), default=0.0),
            price=_to_float(item.get("price"), default=0.0),
            timestamp=_to_int(item.get("timestamp"), default=0),
            title=_to_str(item.get("title")),
            slug=_to_str(item.get("slug")),
            icon=_optional_str(item.get("icon")),
            event_slug=_to_str(item.get("eventSlug")),
            outcome=_to_str(item.get("outcome")),
            outcome_index=_to_int(item.get("outcomeIndex"), default=0),
            name=_optional_str(item.get("name")),
            pseudonym=_optional_str(item.get("pseudonym")),
            bio=_optional_str(item.get("bio")),
            profile_image=_optional_str(item.get("profileImage")),
            profile_image_optimized=_optional_str(
                item.get("profileImageOptimized")
            ),
            transaction_hash=_optional_str(item.get("transactionHash")),
        )

    def _normalize_activity(self, item: Mapping[str, Any]) -> UserActivity:
        return UserActivity(
            proxy_wallet=_to_str(item.get("proxyWallet")),
            timestamp=_to_int(item.get("timestamp"), default=0),
            condition_id=_to_str(item.get("conditionId")),
            activity_type=_to_str(item.get("type")),
            size=_to_float(item.get("size"), default=0.0),
            usdc_size=_to_float(item.get("usdcSize"), default=0.0),
            transaction_hash=_optional_str(item.get("transactionHash")),
            price=_optional_float(item.get("price")),
            asset=_optional_str(item.get("asset")),
            side=_optional_str(item.get("side")),
            outcome_index=_optional_int(item.get("outcomeIndex")),
            title=_optional_str(item.get("title")),
            slug=_optional_str(item.get("slug")),
            icon=_optional_str(item.get("icon")),
            event_slug=_optional_str(item.get("eventSlug")),
            outcome=_optional_str(item.get("outcome")),
            name=_optional_str(item.get("name")),
            pseudonym=_optional_str(item.get("pseudonym")),
            bio=_optional_str(item.get("bio")),
            profile_image=_optional_str(item.get("profileImage")),
            profile_image_optimized=_optional_str(
                item.get("profileImageOptimized")
            ),
        )

    def _normalize_holder(self, item: Mapping[str, Any]) -> MarketHolder:
        return MarketHolder(
            proxy_wallet=_to_str(item.get("proxyWallet")),
            amount=_to_float(item.get("amount"), default=0.0),
            outcome_index=_optional_int(item.get("outcomeIndex")),
            name=_optional_str(item.get("name")),
            pseudonym=_optional_str(item.get("pseudonym")),
            bio=_optional_str(item.get("bio")),
            profile_image=_optional_str(item.get("profileImage")),
            profile_image_optimized=_optional_str(
                item.get("profileImageOptimized")
            ),
        )

    def _normalize_leaderboard(self, item: Mapping[str, Any]) -> LeaderboardEntry:
        return LeaderboardEntry(
            rank=_to_int(item.get("rank"), default=0),
            proxy_wallet=_to_str(item.get("proxyWallet")),
            user_name=_optional_str(item.get("userName")),
            volume=_to_float(item.get("vol"), default=0.0),
            pnl=_to_float(item.get("pnl"), default=0.0),
            profile_image=_optional_str(item.get("profileImage")),
            x_username=_optional_str(item.get("xUsername")),
            verified_badge=_to_bool(item.get("verifiedBadge")),
        )


def _position_params(query: PositionQuery) -> dict[str, str | int | float]:
    params: dict[str, str | int | float] = {"user": query.user}
    _add_common_market_filters(params, query.markets, query.event_ids)
    _add(params, "sizeThreshold", query.size_threshold)
    _add_bool(params, "redeemable", query.redeemable)
    _add_bool(params, "mergeable", query.mergeable)
    _add(params, "limit", query.limit)
    _add(params, "offset", query.offset)
    _add(params, "sortBy", _enum_value(query.sort_by))
    _add(params, "sortDirection", _enum_value(query.sort_direction))
    _add(params, "title", query.title)
    return params


def _closed_position_params(
    query: ClosedPositionQuery,
) -> dict[str, str | int | float]:
    params: dict[str, str | int | float] = {"user": query.user}
    _add_common_market_filters(params, query.markets, query.event_ids)
    _add(params, "title", query.title)
    _add(params, "limit", query.limit)
    _add(params, "offset", query.offset)
    _add(params, "sortBy", _enum_value(query.sort_by))
    _add(params, "sortDirection", _enum_value(query.sort_direction))
    return params


def _trade_params(query: TradeQuery) -> dict[str, str | int | float]:
    params: dict[str, str | int | float] = {}
    _add(params, "user", query.user)
    _add_common_market_filters(params, query.markets, query.event_ids)
    _add_bool(params, "takerOnly", query.taker_only)
    _add(params, "filterType", query.filter_type)
    _add(params, "filterAmount", query.filter_amount)
    _add(params, "side", query.side)
    _add(params, "limit", query.limit)
    _add(params, "offset", query.offset)
    return params


def _activity_params(query: ActivityQuery) -> dict[str, str | int | float]:
    params: dict[str, str | int | float] = {"user": query.user}
    _add_common_market_filters(params, query.markets, query.event_ids)
    if query.activity_types:
        params["type"] = _csv(query.activity_types)
    _add(params, "start", query.start)
    _add(params, "end", query.end)
    _add(params, "sortBy", query.sort_by)
    _add(params, "sortDirection", _enum_value(query.sort_direction))
    _add(params, "limit", query.limit)
    _add(params, "offset", query.offset)
    return params


def _add_common_market_filters(
    params: dict[str, str | int | float],
    markets: Sequence[str] | None,
    event_ids: Sequence[int] | None,
) -> None:
    if markets and event_ids:
        raise ValueError("markets und event_ids sind gegenseitig ausgeschlossen")
    if markets:
        params["market"] = _csv(markets)
    if event_ids:
        params["eventId"] = _csv(event_ids)


def _add(
    params: dict[str, str | int | float],
    name: str,
    value: str | int | float | None,
) -> None:
    if value is not None:
        params[name] = value


def _add_bool(
    params: dict[str, str | int | float], name: str, value: bool | None
) -> None:
    if value is not None:
        params[name] = "true" if value else "false"


def _enum_value(value: Enum | str | None) -> str | None:
    if value is None:
        return None
    return str(value.value) if isinstance(value, Enum) else str(value)


def _csv(values: Sequence[Any]) -> str:
    return ",".join(str(value) for value in values)


def _mapping_list(value: Any) -> list[Mapping[str, Any]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return []
    return [item for item in value if isinstance(item, Mapping)]


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


def _to_int(value: Any, *, default: int) -> int:
    if value is None or value == "":
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
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


def _parse_optional_datetime(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    text = str(value).strip()
    if text.isdigit():
        return datetime.fromtimestamp(int(text), tz=timezone.utc)
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed
