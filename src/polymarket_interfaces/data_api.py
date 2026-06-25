"""Asynchroner, typisierter Client für die öffentliche Polymarket Data API.

Die Data API liefert nutzer- und positionsbezogene Daten. Sie ist read-only und
benötigt für die hier implementierten Endpunkte keine CLOB-Authentifizierung.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any

import httpx

from .utils.utils import ApiType, ErrorCode, PolymarketError, RateLimiter

DATA_API_BASE = "https://data-api.polymarket.com"

_WALLET_PATTERN = re.compile(r"^0x[a-fA-F0-9]{40}$")
_CONDITION_PATTERN = re.compile(r"^0x[a-fA-F0-9]{64}$")
_ZERO = Decimal("0")


class SortDirection(str, Enum):
    ASC = "ASC"
    DESC = "DESC"


class PositionSortBy(str, Enum):
    CURRENT = "CURRENT"
    INITIAL = "INITIAL"
    TOKENS = "TOKENS"
    CASH_PNL = "CASHPNL"
    PERCENT_PNL = "PERCENTPNL"
    TITLE = "TITLE"
    RESOLVING = "RESOLVING"
    PRICE = "PRICE"
    AVG_PRICE = "AVGPRICE"


class ClosedPositionSortBy(str, Enum):
    REALIZED_PNL = "REALIZEDPNL"
    TITLE = "TITLE"
    PRICE = "PRICE"
    AVG_PRICE = "AVGPRICE"
    TIMESTAMP = "TIMESTAMP"


class TradeSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class TradeFilterType(str, Enum):
    CASH = "CASH"
    TOKENS = "TOKENS"


class ActivityType(str, Enum):
    TRADE = "TRADE"
    SPLIT = "SPLIT"
    MERGE = "MERGE"
    REDEEM = "REDEEM"
    REWARD = "REWARD"
    CONVERSION = "CONVERSION"
    MAKER_REBATE = "MAKER_REBATE"
    TAKER_REBATE = "TAKER_REBATE"
    REFERRAL_REWARD = "REFERRAL_REWARD"


class ActivitySortBy(str, Enum):
    TIMESTAMP = "TIMESTAMP"
    TOKENS = "TOKENS"
    CASH = "CASH"


@dataclass(frozen=True, slots=True)
class PositionQuery:
    markets: tuple[str, ...] = ()
    event_ids: tuple[int, ...] = ()
    size_threshold: Decimal = Decimal("1")
    redeemable: bool = False
    mergeable: bool = False
    limit: int = 100
    offset: int = 0
    sort_by: PositionSortBy = PositionSortBy.TOKENS
    sort_direction: SortDirection = SortDirection.DESC
    title: str | None = None


@dataclass(frozen=True, slots=True)
class ClosedPositionQuery:
    markets: tuple[str, ...] = ()
    event_ids: tuple[int, ...] = ()
    title: str | None = None
    limit: int = 10
    offset: int = 0
    sort_by: ClosedPositionSortBy = ClosedPositionSortBy.REALIZED_PNL
    sort_direction: SortDirection = SortDirection.DESC


@dataclass(frozen=True, slots=True)
class TradeQuery:
    user: str | None = None
    markets: tuple[str, ...] = ()
    event_ids: tuple[int, ...] = ()
    side: TradeSide | None = None
    taker_only: bool = True
    filter_type: TradeFilterType | None = None
    filter_amount: Decimal | None = None
    limit: int = 100
    offset: int = 0


@dataclass(frozen=True, slots=True)
class ActivityQuery:
    markets: tuple[str, ...] = ()
    event_ids: tuple[int, ...] = ()
    types: tuple[ActivityType, ...] = ()
    start: int | None = None
    end: int | None = None
    side: TradeSide | None = None
    limit: int = 100
    offset: int = 0
    sort_by: ActivitySortBy = ActivitySortBy.TIMESTAMP
    sort_direction: SortDirection = SortDirection.DESC


@dataclass(frozen=True, slots=True)
class Position:
    proxy_wallet: str
    asset: str
    condition_id: str
    size: Decimal
    average_price: Decimal
    initial_value: Decimal
    current_value: Decimal
    cash_pnl: Decimal
    percent_pnl: Decimal
    total_bought: Decimal
    realized_pnl: Decimal
    percent_realized_pnl: Decimal
    current_price: Decimal
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

    @property
    def avg_price(self) -> Decimal:
        """Alias entsprechend dem Data-API-Feld ``avgPrice``."""
        return self.average_price


@dataclass(frozen=True, slots=True)
class ClosedPosition:
    proxy_wallet: str
    asset: str
    condition_id: str
    average_price: Decimal
    total_bought: Decimal
    realized_pnl: Decimal
    current_price: Decimal
    timestamp: datetime | None
    title: str
    slug: str
    icon: str | None
    event_slug: str
    outcome: str
    outcome_index: int
    opposite_outcome: str | None
    opposite_asset: str | None
    end_date: datetime | None


@dataclass(frozen=True, slots=True)
class Trade:
    proxy_wallet: str
    side: TradeSide | None
    asset: str
    condition_id: str
    size: Decimal
    price: Decimal
    timestamp: datetime | None
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


@dataclass(frozen=True, slots=True)
class Activity:
    proxy_wallet: str
    timestamp: datetime | None
    condition_id: str
    activity_type: ActivityType | None
    size: Decimal
    usdc_size: Decimal
    transaction_hash: str | None
    price: Decimal
    asset: str
    side: TradeSide | None
    outcome_index: int
    title: str
    slug: str
    icon: str | None
    event_slug: str
    outcome: str
    name: str | None
    pseudonym: str | None
    bio: str | None
    profile_image: str | None
    profile_image_optimized: str | None
    is_combo: bool


@dataclass(frozen=True, slots=True)
class PositionValue:
    user: str
    value: Decimal


class DataAPI:
    """Read-only Client für Positionen, Trades, Aktivität und Portfoliowert."""

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

    async def __aenter__(self) -> "DataAPI":
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self._owns_http_client:
            await self._http.aclose()

    async def get_positions(
        self,
        user: str,
        query: PositionQuery | None = None,
    ) -> list[Position]:
        """Lädt eine Seite offener Positionen eines Proxy-Wallets."""
        _validate_wallet(user)
        query = query or PositionQuery()
        params = _position_query_params(user, query)
        data = await self._get_json("/positions", params=params)
        return _normalize_list(data, self._normalize_position, "Positionen")

    async def get_all_positions(
        self,
        user: str,
        query: PositionQuery | None = None,
    ) -> list[Position]:
        """Lädt alle offenen Positionen innerhalb des API-Offset-Limits."""
        base = query or PositionQuery()
        page_size = min(max(base.limit, 1), 500)
        offset = base.offset
        result: list[Position] = []

        while offset <= 10_000:
            page_query = replace(base, limit=page_size, offset=offset)
            page = await self.get_positions(user, page_query)
            result.extend(page)
            if len(page) < page_size:
                break
            offset += page_size

        return result

    async def get_position(
        self,
        user: str,
        asset_id: str,
    ) -> Position | None:
        """Sucht eine offene Position anhand der CLOB-Asset-/Token-ID."""
        _validate_asset(asset_id)
        positions = await self.get_all_positions(
            user,
            PositionQuery(size_threshold=_ZERO, limit=500),
        )
        return next(
            (position for position in positions if position.asset == asset_id),
            None,
        )

    async def get_asset_average_price(
        self,
        user: str,
        asset_id: str,
    ) -> Decimal | None:
        """Gibt den durchschnittlichen Einstiegspreis einer offenen Position zurück."""
        position = await self.get_position(user, asset_id)
        return None if position is None else position.average_price


    async def get_closed_positions(
        self,
        user: str,
        query: ClosedPositionQuery | None = None,
    ) -> list[ClosedPosition]:
        """Lädt eine Seite geschlossener Positionen eines Proxy-Wallets."""
        _validate_wallet(user)
        query = query or ClosedPositionQuery()
        params = _closed_position_query_params(user, query)
        data = await self._get_json("/closed-positions", params=params)
        return _normalize_list(
            data,
            self._normalize_closed_position,
            "Geschlossene Positionen",
        )

    async def get_trades(
        self,
        query: TradeQuery | None = None,
    ) -> list[Trade]:
        """Lädt Trades eines Nutzers und/oder ausgewählter Märkte."""
        query = query or TradeQuery()
        params = _trade_query_params(query)
        data = await self._get_json("/trades", params=params)
        return _normalize_list(data, self._normalize_trade, "Trades")

    async def get_activity(
        self,
        user: str,
        query: ActivityQuery | None = None,
    ) -> list[Activity]:
        """Lädt die Aktivität eines Proxy-Wallets."""
        _validate_wallet(user)
        query = query or ActivityQuery()
        params = _activity_query_params(user, query)
        data = await self._get_json("/activity", params=params)
        return _normalize_list(data, self._normalize_activity, "Aktivität")

    async def get_position_values(
        self,
        user: str,
        markets: Sequence[str] = (),
    ) -> list[PositionValue]:
        """Lädt den aggregierten aktuellen Positionswert."""
        _validate_wallet(user)
        _validate_markets(markets)
        params: dict[str, str] = {"user": user}
        if markets:
            params["market"] = _csv(markets)

        data = await self._get_json("/value", params=params)
        return _normalize_list(data, self._normalize_position_value, "Positionswerte")

    async def get_total_position_value(
        self,
        user: str,
        markets: Sequence[str] = (),
    ) -> Decimal:
        """Summiert die von ``/value`` gelieferten Werte für das Wallet."""
        values = await self.get_position_values(user, markets)
        normalized_user = user.lower()
        return sum(
            (
                item.value
                for item in values
                if not item.user or item.user.lower() == normalized_user
            ),
            start=_ZERO,
        )

    async def _get_json(
        self,
        path: str,
        *,
        params: Mapping[str, str | int] | None = None,
    ) -> Any:
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

    @staticmethod
    def _normalize_position(item: Mapping[str, Any]) -> Position:
        return Position(
            proxy_wallet=_to_str(item.get("proxyWallet")),
            asset=_to_str(item.get("asset")),
            condition_id=_to_str(item.get("conditionId")),
            size=_to_decimal(item.get("size")),
            average_price=_to_decimal(item.get("avgPrice")),
            initial_value=_to_decimal(item.get("initialValue")),
            current_value=_to_decimal(item.get("currentValue")),
            cash_pnl=_to_decimal(item.get("cashPnl")),
            percent_pnl=_to_decimal(item.get("percentPnl")),
            total_bought=_to_decimal(item.get("totalBought")),
            realized_pnl=_to_decimal(item.get("realizedPnl")),
            percent_realized_pnl=_to_decimal(item.get("percentRealizedPnl")),
            current_price=_to_decimal(item.get("curPrice")),
            redeemable=_to_bool(item.get("redeemable")),
            mergeable=_to_bool(item.get("mergeable")),
            title=_to_str(item.get("title")),
            slug=_to_str(item.get("slug")),
            icon=_optional_str(item.get("icon")),
            event_slug=_to_str(item.get("eventSlug")),
            outcome=_to_str(item.get("outcome")),
            outcome_index=_to_int(item.get("outcomeIndex")),
            opposite_outcome=_optional_str(item.get("oppositeOutcome")),
            opposite_asset=_optional_str(item.get("oppositeAsset")),
            end_date=_parse_iso_datetime(item.get("endDate")),
            negative_risk=_to_bool(item.get("negativeRisk")),
        )

    @staticmethod
    def _normalize_closed_position(item: Mapping[str, Any]) -> ClosedPosition:
        return ClosedPosition(
            proxy_wallet=_to_str(item.get("proxyWallet")),
            asset=_to_str(item.get("asset")),
            condition_id=_to_str(item.get("conditionId")),
            average_price=_to_decimal(item.get("avgPrice")),
            total_bought=_to_decimal(item.get("totalBought")),
            realized_pnl=_to_decimal(item.get("realizedPnl")),
            current_price=_to_decimal(item.get("curPrice")),
            timestamp=_parse_epoch(item.get("timestamp")),
            title=_to_str(item.get("title")),
            slug=_to_str(item.get("slug")),
            icon=_optional_str(item.get("icon")),
            event_slug=_to_str(item.get("eventSlug")),
            outcome=_to_str(item.get("outcome")),
            outcome_index=_to_int(item.get("outcomeIndex")),
            opposite_outcome=_optional_str(item.get("oppositeOutcome")),
            opposite_asset=_optional_str(item.get("oppositeAsset")),
            end_date=_parse_iso_datetime(item.get("endDate")),
        )

    @staticmethod
    def _normalize_trade(item: Mapping[str, Any]) -> Trade:
        return Trade(
            proxy_wallet=_to_str(item.get("proxyWallet")),
            side=_optional_enum(TradeSide, item.get("side")),
            asset=_to_str(item.get("asset")),
            condition_id=_to_str(item.get("conditionId")),
            size=_to_decimal(item.get("size")),
            price=_to_decimal(item.get("price")),
            timestamp=_parse_epoch(item.get("timestamp")),
            title=_to_str(item.get("title")),
            slug=_to_str(item.get("slug")),
            icon=_optional_str(item.get("icon")),
            event_slug=_to_str(item.get("eventSlug")),
            outcome=_to_str(item.get("outcome")),
            outcome_index=_to_int(item.get("outcomeIndex")),
            name=_optional_str(item.get("name")),
            pseudonym=_optional_str(item.get("pseudonym")),
            bio=_optional_str(item.get("bio")),
            profile_image=_optional_str(item.get("profileImage")),
            profile_image_optimized=_optional_str(item.get("profileImageOptimized")),
            transaction_hash=_optional_str(item.get("transactionHash")),
        )

    @staticmethod
    def _normalize_activity(item: Mapping[str, Any]) -> Activity:
        return Activity(
            proxy_wallet=_to_str(item.get("proxyWallet")),
            timestamp=_parse_epoch(item.get("timestamp")),
            condition_id=_to_str(item.get("conditionId")),
            activity_type=_optional_enum(ActivityType, item.get("type")),
            size=_to_decimal(item.get("size")),
            usdc_size=_to_decimal(item.get("usdcSize")),
            transaction_hash=_optional_str(item.get("transactionHash")),
            price=_to_decimal(item.get("price")),
            asset=_to_str(item.get("asset")),
            side=_optional_enum(TradeSide, item.get("side")),
            outcome_index=_to_int(item.get("outcomeIndex")),
            title=_to_str(item.get("title")),
            slug=_to_str(item.get("slug")),
            icon=_optional_str(item.get("icon")),
            event_slug=_to_str(item.get("eventSlug")),
            outcome=_to_str(item.get("outcome")),
            name=_optional_str(item.get("name")),
            pseudonym=_optional_str(item.get("pseudonym")),
            bio=_optional_str(item.get("bio")),
            profile_image=_optional_str(item.get("profileImage")),
            profile_image_optimized=_optional_str(item.get("profileImageOptimized")),
            is_combo=_to_bool(item.get("isCombo")),
        )

    @staticmethod
    def _normalize_position_value(item: Mapping[str, Any]) -> PositionValue:
        return PositionValue(
            user=_to_str(item.get("user")),
            value=_to_decimal(item.get("value")),
        )


def _position_query_params(user: str, query: PositionQuery) -> dict[str, str | int]:
    _validate_market_event_filters(query.markets, query.event_ids)
    _validate_range("size_threshold", query.size_threshold, minimum=_ZERO)
    _validate_int_range("limit", query.limit, 0, 500)
    _validate_int_range("offset", query.offset, 0, 10_000)
    _validate_title(query.title)

    params: dict[str, str | int] = {
        "user": user,
        "sizeThreshold": _decimal_query(query.size_threshold),
        "redeemable": _bool_query(query.redeemable),
        "mergeable": _bool_query(query.mergeable),
        "limit": query.limit,
        "offset": query.offset,
        "sortBy": query.sort_by.value,
        "sortDirection": query.sort_direction.value,
    }
    _add_market_event_params(params, query.markets, query.event_ids)
    if query.title is not None:
        params["title"] = query.title
    return params


def _closed_position_query_params(
    user: str,
    query: ClosedPositionQuery,
) -> dict[str, str | int]:
    _validate_market_event_filters(query.markets, query.event_ids)
    _validate_int_range("limit", query.limit, 0, 50)
    _validate_int_range("offset", query.offset, 0, 100_000)
    _validate_title(query.title)

    params: dict[str, str | int] = {
        "user": user,
        "limit": query.limit,
        "offset": query.offset,
        "sortBy": query.sort_by.value,
        "sortDirection": query.sort_direction.value,
    }
    _add_market_event_params(params, query.markets, query.event_ids)
    if query.title is not None:
        params["title"] = query.title
    return params


def _trade_query_params(query: TradeQuery) -> dict[str, str | int]:
    _validate_market_event_filters(query.markets, query.event_ids)
    _validate_int_range("limit", query.limit, 0, 10_000)
    _validate_int_range("offset", query.offset, 0, 10_000)
    if query.user is not None:
        _validate_wallet(query.user)
    if (query.filter_type is None) != (query.filter_amount is None):
        raise ValueError("filter_type und filter_amount müssen gemeinsam gesetzt werden")
    if query.filter_amount is not None:
        _validate_range("filter_amount", query.filter_amount, minimum=_ZERO)

    params: dict[str, str | int] = {
        "limit": query.limit,
        "offset": query.offset,
        "takerOnly": _bool_query(query.taker_only),
    }
    _add_market_event_params(params, query.markets, query.event_ids)
    if query.user is not None:
        params["user"] = query.user
    if query.side is not None:
        params["side"] = query.side.value
    if query.filter_type is not None and query.filter_amount is not None:
        params["filterType"] = query.filter_type.value
        params["filterAmount"] = _decimal_query(query.filter_amount)
    return params


def _activity_query_params(
    user: str,
    query: ActivityQuery,
) -> dict[str, str | int]:
    _validate_market_event_filters(query.markets, query.event_ids)
    _validate_int_range("limit", query.limit, 0, 500)
    _validate_int_range("offset", query.offset, 0, 10_000)
    if query.start is not None and query.start < 0:
        raise ValueError("start muss >= 0 sein")
    if query.end is not None and query.end < 0:
        raise ValueError("end muss >= 0 sein")
    if query.start is not None and query.end is not None and query.start > query.end:
        raise ValueError("start darf nicht nach end liegen")

    params: dict[str, str | int] = {
        "user": user,
        "limit": query.limit,
        "offset": query.offset,
        "sortBy": query.sort_by.value,
        "sortDirection": query.sort_direction.value,
    }
    _add_market_event_params(params, query.markets, query.event_ids)
    if query.types:
        params["type"] = _csv(item.value for item in query.types)
    if query.start is not None:
        params["start"] = query.start
    if query.end is not None:
        params["end"] = query.end
    if query.side is not None:
        params["side"] = query.side.value
    return params


def _add_market_event_params(
    params: dict[str, str | int],
    markets: Sequence[str],
    event_ids: Sequence[int],
) -> None:
    if markets:
        params["market"] = _csv(markets)
    if event_ids:
        params["eventId"] = _csv(str(item) for item in event_ids)


def _validate_market_event_filters(
    markets: Sequence[str],
    event_ids: Sequence[int],
) -> None:
    if markets and event_ids:
        raise ValueError("markets und event_ids sind gegenseitig ausschließend")
    _validate_markets(markets)
    for event_id in event_ids:
        if event_id < 1:
            raise ValueError("event_ids müssen >= 1 sein")


def _validate_markets(markets: Sequence[str]) -> None:
    for condition_id in markets:
        if not _CONDITION_PATTERN.fullmatch(condition_id):
            raise ValueError(f"Ungültige Condition-ID: {condition_id}")


def _validate_wallet(user: str) -> None:
    if not _WALLET_PATTERN.fullmatch(user):
        raise ValueError("user muss eine 0x-prefixed Proxy-Wallet-Adresse sein")


def _validate_asset(asset_id: str) -> None:
    if not asset_id or not asset_id.strip():
        raise ValueError("asset_id darf nicht leer sein")


def _validate_title(title: str | None) -> None:
    if title is not None and len(title) > 100:
        raise ValueError("title darf höchstens 100 Zeichen lang sein")


def _validate_int_range(name: str, value: int, minimum: int, maximum: int) -> None:
    if not minimum <= value <= maximum:
        raise ValueError(f"{name} muss zwischen {minimum} und {maximum} liegen")


def _validate_range(name: str, value: Decimal, *, minimum: Decimal) -> None:
    if value < minimum:
        raise ValueError(f"{name} muss >= {minimum} sein")


def _normalize_list(
    data: Any,
    normalizer: Any,
    resource_name: str,
) -> list[Any]:
    if not isinstance(data, list):
        raise PolymarketError(
            ErrorCode.INVALID_RESPONSE,
            f"{resource_name}: API-Antwort ist keine Liste",
        )
    return [normalizer(item) for item in data if isinstance(item, Mapping)]


def _csv(values: Sequence[Any] | Any) -> str:
    return ",".join(str(value) for value in values)


def _bool_query(value: bool) -> str:
    return "true" if value else "false"


def _decimal_query(value: Decimal) -> str:
    return format(value, "f")


def _to_decimal(value: Any) -> Decimal:
    if value is None or value == "":
        return _ZERO
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return _ZERO


def _to_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _to_str(value: Any) -> str:
    return "" if value is None else str(value)


def _optional_str(value: Any) -> str | None:
    if value is None or value == "":
        return None
    return str(value)


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


def _parse_epoch(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    try:
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    except (TypeError, ValueError, OSError, OverflowError):
        return None


def _parse_iso_datetime(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    text = str(value).strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _optional_enum(enum_type: type[Enum], value: Any) -> Any:
    if value is None or value == "":
        return None
    try:
        return enum_type(str(value).upper())
    except ValueError:
        return None
