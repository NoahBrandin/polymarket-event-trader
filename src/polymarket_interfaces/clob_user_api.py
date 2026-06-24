"""Authentifizierter, asynchroner Zugriff auf Polymarket-CLOB-Userdaten.

Die öffentliche :class:`~polymarket_interfaces.clob_api.CLOBAPI` bleibt bewusst
unauthentifiziert. ``CLOBUserAPI`` ergänzt sie um L1-/L2-Authentifizierung,
Account-Daten sowie Order-Erstellung und -Stornierung über den offiziellen
``py-clob-client-v2``.

Das offizielle SDK arbeitet synchron. Alle SDK-Aufrufe werden deshalb mit
``asyncio.to_thread`` ausgeführt und durch denselben ``RateLimiter`` geleitet,
den auch ``GammaAPI`` und ``CLOBAPI`` verwenden.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from enum import Enum
from types import SimpleNamespace
from typing import Any

from .clob_market_api import CLOB_API_BASE, ClobSide
from .utils.utils import ApiType, ErrorCode, PolymarketError, RateLimiter, UnifiedCache


POLYGON_CHAIN_ID = 137
_ALLOWED_TICK_SIZES = {"0.1", "0.01", "0.001", "0.0001"}


class ClobOrderType(str, Enum):
    """Unterstützte Time-in-Force-Varianten."""

    GTC = "GTC"
    GTD = "GTD"
    FOK = "FOK"
    FAK = "FAK"


class ClobAssetType(str, Enum):
    """Asset-Klassen für Balance- und Allowance-Abfragen."""

    COLLATERAL = "COLLATERAL"
    CONDITIONAL = "CONDITIONAL"


@dataclass(frozen=True, slots=True)
class CLOBApiCredentials:
    """L2-HMAC-Zugangsdaten; Secrets werden nicht in ``repr`` ausgegeben."""

    api_key: str
    api_secret: str = field(repr=False)
    api_passphrase: str = field(repr=False)

    def __post_init__(self) -> None:
        _require_text(self.api_key, "api_key")
        _require_text(self.api_secret, "api_secret")
        _require_text(self.api_passphrase, "api_passphrase")


@dataclass(frozen=True, slots=True)
class LimitOrderRequest:
    """Parameter einer signierten Limit-Order.

    ``size`` bezeichnet die Anzahl Outcome-Token. ``price`` muss zwischen
    0 und 1 liegen. GTD-Orders benötigen einen Unix-Zeitstempel in
    ``expiration``.
    """

    token_id: str
    side: ClobSide
    price: Decimal
    size: Decimal
    order_type: ClobOrderType = ClobOrderType.GTC
    expiration: int = 0
    tick_size: Decimal | None = None
    neg_risk: bool | None = None
    post_only: bool = False
    defer_execution: bool = False
    user_usdc_balance: Decimal | None = None
    builder_code: str | None = None
    metadata: str | None = None

    def __post_init__(self) -> None:
        _require_text(self.token_id, "token_id")
        _require_side(self.side)
        price = _positive_decimal(self.price, "price")
        size = _positive_decimal(self.size, "size")
        if price >= Decimal("1"):
            raise ValueError("price muss kleiner als 1 sein")
        object.__setattr__(self, "price", price)
        object.__setattr__(self, "size", size)

        if self.order_type not in (ClobOrderType.GTC, ClobOrderType.GTD):
            raise ValueError("Limit-Orders unterstützen nur GTC oder GTD")
        if self.order_type is ClobOrderType.GTD and self.expiration <= 0:
            raise ValueError("GTD benötigt expiration als positiven Unix-Zeitstempel")
        if self.order_type is ClobOrderType.GTC and self.expiration < 0:
            raise ValueError("expiration darf nicht negativ sein")

        object.__setattr__(self, "tick_size", _normalize_tick_size(self.tick_size))
        if self.user_usdc_balance is not None:
            object.__setattr__(
                self,
                "user_usdc_balance",
                _non_negative_decimal(self.user_usdc_balance, "user_usdc_balance"),
            )


@dataclass(frozen=True, slots=True)
class MarketOrderRequest:
    """Parameter einer sofort ausführbaren Market-Order.

    Bei BUY ist ``amount`` ein USDC-Betrag, bei SELL eine Anzahl Outcome-Token.
    ``price`` ist optional; 0 überlässt die Preisermittlung dem SDK.
    """

    token_id: str
    side: ClobSide
    amount: Decimal
    order_type: ClobOrderType = ClobOrderType.FOK
    price: Decimal | None = None
    tick_size: Decimal | None = None
    neg_risk: bool | None = None
    defer_execution: bool = False
    user_usdc_balance: Decimal | None = None
    builder_code: str | None = None
    metadata: str | None = None

    def __post_init__(self) -> None:
        _require_text(self.token_id, "token_id")
        _require_side(self.side)
        object.__setattr__(self, "amount", _positive_decimal(self.amount, "amount"))
        if self.order_type not in (ClobOrderType.FOK, ClobOrderType.FAK):
            raise ValueError("Market-Orders unterstützen nur FOK oder FAK")
        if self.price is not None:
            price = _positive_decimal(self.price, "price")
            if price >= Decimal("1"):
                raise ValueError("price muss kleiner als 1 sein")
            object.__setattr__(self, "price", price)
        object.__setattr__(self, "tick_size", _normalize_tick_size(self.tick_size))
        if self.user_usdc_balance is not None:
            object.__setattr__(
                self,
                "user_usdc_balance",
                _non_negative_decimal(self.user_usdc_balance, "user_usdc_balance"),
            )


@dataclass(frozen=True, slots=True)
class OrderExecutionResult:
    """Normalisierte Antwort auf eine Order-Ausführung."""

    success: bool
    order_id: str | None
    status: str | None
    error_message: str | None
    making_amount: Decimal | None
    taking_amount: Decimal | None
    transaction_hashes: tuple[str, ...]
    raw: Mapping[str, Any] = field(repr=False, compare=False)


class ClobUserAPI:
    """Authentifizierter asynchroner CLOB-Client für Userdaten und Orders.

    Args:
        private_key: Private Wallet-Key für EIP-712-Signaturen. Niemals loggen
            oder committen.
        credentials: Bereits vorhandene L2-Credentials. Fehlen sie, werden sie
            beim ersten authentifizierten Aufruf über L1 erzeugt/abgeleitet.
        allow_live_trading: Muss für Platzierung und Stornierung von Orders
            explizit ``True`` sein.
        sdk_loader: Dependency-Injection-Hook für Tests.
    """

    def __init__(
        self,
        private_key: str,
        credentials: CLOBApiCredentials | None = None,
        rate_limiter: RateLimiter | None = None,
        cache: UnifiedCache | None = None,
        *,
        host: str = CLOB_API_BASE,
        chain_id: int = POLYGON_CHAIN_ID,
        signature_type: int = 0,
        funder: str | None = None,
        auto_derive_credentials: bool = True,
        allow_live_trading: bool = False,
        use_server_time: bool = False,
        retry_on_error: bool = False,
        sdk_loader: Callable[[], Any] | None = None,
    ) -> None:
        _require_text(private_key, "private_key")
        _require_text(host, "host")
        if chain_id <= 0:
            raise ValueError("chain_id muss positiv sein")
        if signature_type < 0:
            raise ValueError("signature_type darf nicht negativ sein")

        self.rate_limiter = rate_limiter or RateLimiter()
        self.cache = cache  # Analog zu GammaAPI/CLOBAPI; derzeit nicht benutzt.
        self.host = host.rstrip("/")
        self.chain_id = chain_id
        self.signature_type = signature_type
        self.funder = funder
        self.auto_derive_credentials = auto_derive_credentials
        self.allow_live_trading = allow_live_trading
        self.use_server_time = use_server_time
        self.retry_on_error = retry_on_error

        self._private_key = private_key
        self._credentials = credentials
        self._sdk_loader = sdk_loader or _load_sdk
        self._sdk: Any | None = None
        self._client: Any | None = None
        self._auth_lock = asyncio.Lock()

    @classmethod
    def from_env(
        cls,
        *,
        private_key_env: str = "POLYMARKET_PRIVATE_KEY",
        allow_live_trading: bool = False,
        **kwargs: Any,
    ) -> "ClobUserAPI":
        """Erzeugt den Client aus Umgebungsvariablen.

        Für den Private Key wird zusätzlich ``PK`` als offizieller SDK-Fallback
        akzeptiert. L2-Credentials sind optional und werden nur übernommen,
        wenn alle drei Werte vorhanden sind.
        """

        private_key = os.getenv(private_key_env) or os.getenv("PK")
        if not private_key:
            raise ValueError(
                f"Private Key fehlt: {private_key_env} oder PK setzen"
            )

        api_key = os.getenv("CLOB_API_KEY")
        api_secret = os.getenv("CLOB_API_SECRET") or os.getenv("CLOB_SECRET")
        passphrase = (
            os.getenv("CLOB_API_PASSPHRASE")
            or os.getenv("CLOB_PASS_PHRASE")
        )
        values = (api_key, api_secret, passphrase)
        if any(values) and not all(values):
            raise ValueError("L2-Credentials müssen vollständig gesetzt sein")

        credentials = (
            CLOBApiCredentials(api_key, api_secret, passphrase)
            if all(values)
            else None
        )
        return cls(
            private_key=private_key,
            credentials=credentials,
            allow_live_trading=allow_live_trading,
            **kwargs,
        )

    async def __aenter__(self) -> "ClobUserAPI":
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        """Kompatibilitätsmethode; das offizielle SDK hält keinen AsyncClient."""

    @property
    def is_authenticated(self) -> bool:
        return self._credentials is not None

    @property
    def credentials(self) -> CLOBApiCredentials | None:
        """Aktuelle L2-Credentials; Secrets bleiben in ``repr`` verborgen."""

        return self._credentials

    async def authenticate(self, nonce: int | None = None) -> CLOBApiCredentials:
        """Erzeugt oder leitet API-Credentials per L1 ab und aktiviert L2."""

        async with self._auth_lock:
            if self._credentials is not None:
                await self._ensure_client()
                return self._credentials

            client = await self._ensure_client()
            sdk_creds = await self._execute_sdk(
                "create_or_derive_api_key",
                lambda: client.create_or_derive_api_key(nonce=nonce),
            )
            credentials = CLOBApiCredentials(
                api_key=str(sdk_creds.api_key),
                api_secret=str(sdk_creds.api_secret),
                api_passphrase=str(sdk_creds.api_passphrase),
            )
            client.set_api_creds(sdk_creds)
            self._credentials = credentials
            return credentials

    async def get_wallet_address(self) -> str:
        client = await self._ensure_client()
        return str(await self._execute_sdk("get_address", client.get_address))

    async def get_order(self, order_id: str) -> Mapping[str, Any]:
        _require_text(order_id, "order_id")
        client = await self._authenticated_client()
        return _mapping_result(
            await self._execute_sdk("get_order", lambda: client.get_order(order_id)),
            "get_order",
        )

    async def get_open_orders(
        self,
        *,
        order_id: str | None = None,
        market: str | None = None,
        asset_id: str | None = None,
        only_first_page: bool = False,
        next_cursor: str | None = None,
    ) -> list[Mapping[str, Any]]:
        client = await self._authenticated_client()
        sdk = self._get_sdk()
        params = sdk.OpenOrderParams(id=order_id, market=market, asset_id=asset_id)
        result = await self._execute_sdk(
            "get_open_orders",
            lambda: client.get_open_orders(
                params=params,
                only_first_page=only_first_page,
                next_cursor=next_cursor,
            ),
        )
        return _mapping_list(result, "get_open_orders")

    async def get_trades(
        self,
        *,
        trade_id: str | None = None,
        maker_address: str | None = None,
        market: str | None = None,
        asset_id: str | None = None,
        before: int | None = None,
        after: int | None = None,
        only_first_page: bool = False,
        next_cursor: str | None = None,
    ) -> list[Mapping[str, Any]]:
        if before is not None and after is not None and after > before:
            raise ValueError("after darf nicht nach before liegen")
        client = await self._authenticated_client()
        sdk = self._get_sdk()
        params = sdk.TradeParams(
            id=trade_id,
            maker_address=maker_address,
            market=market,
            asset_id=asset_id,
            before=before,
            after=after,
        )
        result = await self._execute_sdk(
            "get_trades",
            lambda: client.get_trades(
                params=params,
                only_first_page=only_first_page,
                next_cursor=next_cursor,
            ),
        )
        return _mapping_list(result, "get_trades")

    async def get_balance_allowance(
        self,
        asset_type: ClobAssetType,
        token_id: str | None = None,
    ) -> Mapping[str, Any]:
        if not isinstance(asset_type, ClobAssetType):
            raise TypeError("asset_type muss ClobAssetType sein")
        if asset_type is ClobAssetType.CONDITIONAL:
            _require_text(token_id, "token_id")
        client = await self._authenticated_client()
        sdk = self._get_sdk()
        params = sdk.BalanceAllowanceParams(
            asset_type=asset_type.value,
            token_id=token_id,
            signature_type=self.signature_type,
        )
        result = await self._execute_sdk(
            "get_balance_allowance",
            lambda: client.get_balance_allowance(params),
        )
        return _mapping_result(result, "get_balance_allowance")

    async def update_balance_allowance(
        self,
        asset_type: ClobAssetType,
        token_id: str | None = None,
    ) -> Mapping[str, Any]:
        if not isinstance(asset_type, ClobAssetType):
            raise TypeError("asset_type muss ClobAssetType sein")
        if asset_type is ClobAssetType.CONDITIONAL:
            _require_text(token_id, "token_id")
        client = await self._authenticated_client()
        sdk = self._get_sdk()
        params = sdk.BalanceAllowanceParams(
            asset_type=asset_type.value,
            token_id=token_id,
            signature_type=self.signature_type,
        )
        result = await self._execute_sdk(
            "update_balance_allowance",
            lambda: client.update_balance_allowance(params),
        )
        return _mapping_result(result, "update_balance_allowance")

    async def get_notifications(self) -> list[Mapping[str, Any]]:
        client = await self._authenticated_client()
        result = await self._execute_sdk("get_notifications", client.get_notifications)
        return _mapping_list(result, "get_notifications")

    async def place_limit_order(
        self,
        request: LimitOrderRequest,
    ) -> OrderExecutionResult:
        if not isinstance(request, LimitOrderRequest):
            raise TypeError("request muss LimitOrderRequest sein")
        self._assert_live_trading_enabled()
        client = await self._authenticated_client()
        sdk = self._get_sdk()

        kwargs: dict[str, Any] = {
            "token_id": request.token_id,
            "price": float(request.price),
            "size": float(request.size),
            "side": _sdk_side(sdk, request.side),
            "expiration": request.expiration,
        }
        _put_optional(kwargs, "user_usdc_balance", request.user_usdc_balance, float)
        _put_optional(kwargs, "builder_code", request.builder_code)
        _put_optional(kwargs, "metadata", request.metadata)

        order_args = sdk.OrderArgs(**kwargs)
        options = sdk.PartialCreateOrderOptions(
            tick_size=_tick_size_string(request.tick_size),
            neg_risk=request.neg_risk,
        )
        result = await self._execute_sdk(
            "create_and_post_order",
            lambda: client.create_and_post_order(
                order_args=order_args,
                options=options,
                order_type=getattr(sdk.OrderType, request.order_type.value),
                post_only=request.post_only,
                defer_exec=request.defer_execution,
            ),
        )
        return _normalize_execution(result)

    async def place_market_order(
        self,
        request: MarketOrderRequest,
    ) -> OrderExecutionResult:
        if not isinstance(request, MarketOrderRequest):
            raise TypeError("request muss MarketOrderRequest sein")
        self._assert_live_trading_enabled()
        client = await self._authenticated_client()
        sdk = self._get_sdk()
        sdk_order_type = getattr(sdk.OrderType, request.order_type.value)

        kwargs: dict[str, Any] = {
            "token_id": request.token_id,
            "amount": float(request.amount),
            "side": _sdk_side(sdk, request.side),
            "price": float(request.price) if request.price is not None else 0,
            "order_type": sdk_order_type,
        }
        _put_optional(kwargs, "user_usdc_balance", request.user_usdc_balance, float)
        _put_optional(kwargs, "builder_code", request.builder_code)
        _put_optional(kwargs, "metadata", request.metadata)

        order_args = sdk.MarketOrderArgs(**kwargs)
        options = sdk.PartialCreateOrderOptions(
            tick_size=_tick_size_string(request.tick_size),
            neg_risk=request.neg_risk,
        )
        result = await self._execute_sdk(
            "create_and_post_market_order",
            lambda: client.create_and_post_market_order(
                order_args=order_args,
                options=options,
                order_type=sdk_order_type,
                defer_exec=request.defer_execution,
            ),
        )
        return _normalize_execution(result)

    async def cancel_order(self, order_id: str) -> Mapping[str, Any]:
        _require_text(order_id, "order_id")
        self._assert_live_trading_enabled()
        client = await self._authenticated_client()
        sdk = self._get_sdk()
        result = await self._execute_sdk(
            "cancel_order",
            lambda: client.cancel_order(sdk.OrderPayload(orderID=order_id)),
        )
        return _mapping_result(result, "cancel_order")

    async def cancel_orders(self, order_ids: Sequence[str]) -> Mapping[str, Any]:
        normalized = [_required_string(item, "order_id") for item in order_ids]
        if not normalized:
            raise ValueError("order_ids darf nicht leer sein")
        self._assert_live_trading_enabled()
        client = await self._authenticated_client()
        result = await self._execute_sdk(
            "cancel_orders", lambda: client.cancel_orders(normalized)
        )
        return _mapping_result(result, "cancel_orders")

    async def cancel_all_orders(self) -> Mapping[str, Any]:
        self._assert_live_trading_enabled()
        client = await self._authenticated_client()
        result = await self._execute_sdk("cancel_all", client.cancel_all)
        return _mapping_result(result, "cancel_all")

    async def cancel_market_orders(
        self,
        *,
        market: str | None = None,
        asset_id: str | None = None,
    ) -> Mapping[str, Any]:
        if not market and not asset_id:
            raise ValueError("market oder asset_id muss gesetzt sein")
        self._assert_live_trading_enabled()
        client = await self._authenticated_client()
        sdk = self._get_sdk()
        payload = sdk.OrderMarketCancelParams(market=market, asset_id=asset_id)
        result = await self._execute_sdk(
            "cancel_market_orders", lambda: client.cancel_market_orders(payload)
        )
        return _mapping_result(result, "cancel_market_orders")

    async def _authenticated_client(self) -> Any:
        if self._credentials is None:
            if not self.auto_derive_credentials:
                raise PolymarketError(
                    ErrorCode.AUTH_FAILED,
                    "Keine L2-Credentials vorhanden; authenticate() aufrufen oder "
                    "auto_derive_credentials aktivieren",
                )
            await self.authenticate()
        return await self._ensure_client()

    async def _ensure_client(self) -> Any:
        if self._client is not None:
            return self._client
        sdk = self._get_sdk()
        sdk_credentials = None
        if self._credentials is not None:
            sdk_credentials = sdk.ApiCreds(
                api_key=self._credentials.api_key,
                api_secret=self._credentials.api_secret,
                api_passphrase=self._credentials.api_passphrase,
            )
        self._client = sdk.ClobClient(
            host=self.host,
            chain_id=self.chain_id,
            key=self._private_key,
            creds=sdk_credentials,
            signature_type=self.signature_type,
            funder=self.funder,
            use_server_time=self.use_server_time,
            retry_on_error=self.retry_on_error,
        )
        return self._client

    def _get_sdk(self) -> Any:
        if self._sdk is None:
            self._sdk = self._sdk_loader()
        return self._sdk

    def _assert_live_trading_enabled(self) -> None:
        if not self.allow_live_trading:
            raise PolymarketError(
                ErrorCode.AUTH_FAILED,
                "Order-Ausführung ist deaktiviert. allow_live_trading=True explizit setzen.",
            )

    async def _execute_sdk(self, operation_name: str, operation: Callable[[], Any]) -> Any:
        async def guarded_operation() -> Any:
            try:
                return await asyncio.to_thread(operation)
            except PolymarketError:
                raise
            except TimeoutError as error:
                raise PolymarketError(
                    ErrorCode.TIMEOUT,
                    f"Zeitüberschreitung bei CLOB {operation_name}",
                    retryable=True,
                    original_error=error,
                ) from error
            except Exception as error:
                raise _map_sdk_error(operation_name, error) from error

        return await self.rate_limiter.execute(ApiType.CLOB_API, guarded_operation)


def _load_sdk() -> Any:
    try:
        from py_clob_client_v2 import (
            ApiCreds,
            ClobClient,
            MarketOrderArgs,
            OrderArgs,
            OrderType,
            PartialCreateOrderOptions,
            Side,
        )
        from py_clob_client_v2.clob_types import (
            BalanceAllowanceParams,
            OpenOrderParams,
            OrderMarketCancelParams,
            OrderPayload,
            TradeParams,
        )
    except ImportError as error:
        raise RuntimeError(
            "CLOBUserAPI benötigt 'py_clob_client_v2>=1.0.1'. "
            "Installiere die Projektabhängigkeiten erneut."
        ) from error

    return SimpleNamespace(
        ApiCreds=ApiCreds,
        BalanceAllowanceParams=BalanceAllowanceParams,
        ClobClient=ClobClient,
        MarketOrderArgs=MarketOrderArgs,
        OpenOrderParams=OpenOrderParams,
        OrderArgs=OrderArgs,
        OrderMarketCancelParams=OrderMarketCancelParams,
        OrderPayload=OrderPayload,
        OrderType=OrderType,
        PartialCreateOrderOptions=PartialCreateOrderOptions,
        Side=Side,
        TradeParams=TradeParams,
    )


def _sdk_side(sdk: Any, side: ClobSide) -> Any:
    return sdk.Side.BUY if side is ClobSide.BUY else sdk.Side.SELL


def _normalize_execution(value: Any) -> OrderExecutionResult:
    payload = _mapping_result(value, "order execution")
    success_value = payload.get("success")
    success = bool(success_value) if success_value is not None else not bool(
        payload.get("errorMsg") or payload.get("error") or payload.get("error_message")
    )
    raw_hashes = (
        payload.get("transactionsHashes")
        or payload.get("transactionHashes")
        or payload.get("transaction_hashes")
        or []
    )
    hashes = (
        tuple(str(item) for item in raw_hashes)
        if isinstance(raw_hashes, Sequence) and not isinstance(raw_hashes, (str, bytes))
        else ()
    )
    return OrderExecutionResult(
        success=success,
        order_id=_optional_string(
            payload.get("orderID") or payload.get("orderId") or payload.get("order_id")
        ),
        status=_optional_string(payload.get("status")),
        error_message=_optional_string(
            payload.get("errorMsg")
            or payload.get("error_message")
            or payload.get("error")
        ),
        making_amount=_optional_decimal(
            payload.get("makingAmount") or payload.get("making_amount")
        ),
        taking_amount=_optional_decimal(
            payload.get("takingAmount") or payload.get("taking_amount")
        ),
        transaction_hashes=hashes,
        raw=dict(payload),
    )


def _map_sdk_error(operation_name: str, error: Exception) -> PolymarketError:
    status = getattr(error, "status_code", None)
    response = getattr(error, "response", None)
    if status is None and response is not None:
        status = getattr(response, "status_code", None)

    body: Any = (
        getattr(error, "error_msg", None)
        or getattr(error, "message", None)
        or str(error)
    )
    if isinstance(status, int):
        mapped = PolymarketError.from_http_error(status, {"message": body})
        mapped.original_error = error
        return mapped

    class_name = error.__class__.__name__.lower()
    message = str(error).lower()
    if isinstance(error, (ConnectionError, OSError)) or any(
        token in class_name or token in message
        for token in ("connection", "network", "dns")
    ):
        return PolymarketError(
            ErrorCode.NETWORK_ERROR,
            f"Netzwerkfehler bei CLOB {operation_name}: {error}",
            retryable=True,
            original_error=error,
        )
    if "auth" in message or "credential" in message or "signature" in message:
        return PolymarketError(
            ErrorCode.AUTH_FAILED,
            f"Authentifizierung bei CLOB {operation_name} fehlgeschlagen: {error}",
            original_error=error,
        )
    return PolymarketError(
        ErrorCode.API_ERROR,
        f"CLOB {operation_name} fehlgeschlagen: {error}",
        original_error=error,
    )


def _mapping_result(value: Any, context: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise PolymarketError(
            ErrorCode.INVALID_RESPONSE,
            f"{context} lieferte kein JSON-Objekt",
        )
    return value


def _mapping_list(value: Any, context: str) -> list[Mapping[str, Any]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise PolymarketError(
            ErrorCode.INVALID_RESPONSE,
            f"{context} lieferte keine Liste",
        )
    result: list[Mapping[str, Any]] = []
    for index, item in enumerate(value):
        result.append(_mapping_result(item, f"{context}[{index}]"))
    return result


def _normalize_tick_size(value: Decimal | None) -> Decimal | None:
    if value is None:
        return None
    parsed = _positive_decimal(value, "tick_size")
    if format(parsed, "f") not in _ALLOWED_TICK_SIZES:
        raise ValueError(
            "tick_size muss 0.1, 0.01, 0.001 oder 0.0001 sein"
        )
    return parsed


def _tick_size_string(value: Decimal | None) -> str | None:
    return None if value is None else format(value, "f")


def _positive_decimal(value: Any, name: str) -> Decimal:
    parsed = _to_decimal(value, name)
    if parsed <= 0:
        raise ValueError(f"{name} muss positiv sein")
    return parsed


def _non_negative_decimal(value: Any, name: str) -> Decimal:
    parsed = _to_decimal(value, name)
    if parsed < 0:
        raise ValueError(f"{name} darf nicht negativ sein")
    return parsed


def _to_decimal(value: Any, name: str) -> Decimal:
    if isinstance(value, bool):
        raise ValueError(f"{name} muss eine Zahl sein")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as error:
        raise ValueError(f"{name} muss eine gültige Zahl sein") from error
    if not parsed.is_finite():
        raise ValueError(f"{name} muss endlich sein")
    return parsed


def _optional_decimal(value: Any) -> Decimal | None:
    if value in (None, "") or isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None
    return parsed if parsed.is_finite() else None


def _put_optional(
    target: dict[str, Any],
    key: str,
    value: Any,
    converter: Callable[[Any], Any] | None = None,
) -> None:
    if value is not None:
        target[key] = converter(value) if converter else value


def _require_side(value: Any) -> None:
    if not isinstance(value, ClobSide):
        raise TypeError("side muss ClobSide.BUY oder ClobSide.SELL sein")


def _require_text(value: Any, name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} darf nicht leer sein")


def _required_string(value: Any, name: str) -> str:
    _require_text(value, name)
    return str(value)


def _optional_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None
