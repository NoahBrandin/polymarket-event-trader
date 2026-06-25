"""Robuster asynchroner Client für den öffentlichen Polymarket-Market-Channel."""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import random
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass, field
from typing import Any, TypeAlias

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed, InvalidHandshake, InvalidURI

from .errors import PolymarketWebSocketError, WebSocketErrorCode
from .models import ConnectionState, MarketEventMessage, MarketEventType
from .orderbook import OrderBookStore
from .parser import MarketEventParser

# ============================================================================
# Verbindungsparameter und Callback-Typen
# ============================================================================
# ReconnectPolicy und MarketWebSocketConfig kapseln alle zeitlichen und
# protokollspezifischen Einstellungen des Clients.

MARKET_WEBSOCKET_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

EventHandler: TypeAlias = Callable[[MarketEventMessage], Awaitable[None] | None]
ErrorHandler: TypeAlias = Callable[[PolymarketWebSocketError], Awaitable[None] | None]
StateHandler: TypeAlias = Callable[[ConnectionState], Awaitable[None] | None]


@dataclass(slots=True, frozen=True)
class ReconnectPolicy:
    enabled: bool = True
    initial_delay: float = 1.0
    maximum_delay: float = 30.0
    multiplier: float = 2.0
    jitter_ratio: float = 0.20
    maximum_attempts: int | None = None
    stable_connection_seconds: float = 30.0

    def delay_for_attempt(self, attempt: int) -> float:
        base = min(
            self.maximum_delay,
            self.initial_delay * (self.multiplier ** max(attempt - 1, 0)),
        )
        jitter = base * self.jitter_ratio
        return max(0.0, base + random.uniform(-jitter, jitter))


@dataclass(slots=True, frozen=True)
class MarketWebSocketConfig:
    endpoint: str = MARKET_WEBSOCKET_URL
    custom_feature_enabled: bool = True
    heartbeat_interval: float = 10.0
    pong_timeout: float = 25.0
    open_timeout: float = 15.0
    close_timeout: float = 5.0
    maximum_message_size: int = 2**22
    network_queue_size: int = 32
    event_queue_size: int = 10_000
    reconnect: ReconnectPolicy = field(default_factory=ReconnectPolicy)

    def __post_init__(self) -> None:
        if self.heartbeat_interval <= 0:
            raise ValueError("heartbeat_interval muss positiv sein")
        if self.pong_timeout <= self.heartbeat_interval:
            raise ValueError("pong_timeout muss größer als heartbeat_interval sein")
        if self.event_queue_size < 0:
            raise ValueError("event_queue_size darf nicht negativ sein")


@dataclass(slots=True, frozen=True)
class _TerminalFailure:
    error: PolymarketWebSocketError


class _QueueClosed:
    pass


_QUEUE_CLOSED = _QueueClosed()


class MarketWebSocketClient:
    """
    Asynchroner Market-WebSocket-Client.

    Der Client stellt sowohl einen AsyncIterator als auch Callback-Handler bereit
    und hält einen lokalen OrderBookStore aktuell.

    Öffentlicher WebSocket-Client für Orderbooks, Preise und Markt-Lifecycle.

    Der Client unterstützt:
    - automatische Heartbeats (`PING` / `PONG`),
    - dynamisches Abonnieren und Abbestellen von Asset-IDs,
    - exponentielles Reconnect mit Jitter,
    - typisierte Events und Callback-Handler,
    - einen automatisch gepflegten lokalen Orderbook-Speicher,
    - asynchrone Iteration über alle eingehenden Ereignisse.
    """

    def __init__(
        self,
        asset_ids: Iterable[str],
        *,
        config: MarketWebSocketConfig | None = None,
        parser: MarketEventParser | None = None,
        order_books: OrderBookStore | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.config = config or MarketWebSocketConfig()
        self.parser = parser or MarketEventParser()
        self.order_books = order_books or OrderBookStore()
        self.logger = logger or logging.getLogger(__name__)

        self._subscriptions = _normalize_asset_ids(asset_ids)
        self._state = ConnectionState.DISCONNECTED
        self._websocket: ClientConnection | None = None
        self._runner_task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()
        self._connected_event = asyncio.Event()
        self._send_lock = asyncio.Lock()
        self._subscription_lock = asyncio.Lock()
        self._event_queue: asyncio.Queue[MarketEventMessage | _TerminalFailure | _QueueClosed] = (
            asyncio.Queue(maxsize=self.config.event_queue_size)
        )
        self._handlers: dict[MarketEventType | None, list[EventHandler]] = {}
        self._error_handlers: list[ErrorHandler] = []
        self._state_handlers: list[StateHandler] = []
        self._last_pong_monotonic = 0.0
        self._connection_started_at: float | None = None
        self._last_error: PolymarketWebSocketError | None = None

    async def __aenter__(self) -> MarketWebSocketClient:
        await self.start()
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.close()

    def __aiter__(self) -> MarketWebSocketClient:
        return self

    async def __anext__(self) -> MarketEventMessage:
        item = await self._event_queue.get()
        if item is _QUEUE_CLOSED:
            raise StopAsyncIteration
        if isinstance(item, _TerminalFailure):
            raise item.error
        return item

    @property
    def state(self) -> ConnectionState:
        return self._state

    @property
    def subscriptions(self) -> frozenset[str]:
        return frozenset(self._subscriptions)

    @property
    def is_connected(self) -> bool:
        return self._state is ConnectionState.SUBSCRIBED

    @property
    def last_error(self) -> PolymarketWebSocketError | None:
        return self._last_error

    def add_handler(
        self,
        handler: EventHandler,
        event_type: MarketEventType | None = None,
    ) -> Callable[[], None]:
        """Registriert einen Handler; `None` bedeutet: alle Events."""
        handlers = self._handlers.setdefault(event_type, [])
        handlers.append(handler)

        def remove() -> None:
            if handler in handlers:
                handlers.remove(handler)

        return remove

    def add_error_handler(self, handler: ErrorHandler) -> Callable[[], None]:
        self._error_handlers.append(handler)

        def remove() -> None:
            if handler in self._error_handlers:
                self._error_handlers.remove(handler)

        return remove

    def add_state_handler(self, handler: StateHandler) -> Callable[[], None]:
        self._state_handlers.append(handler)

        def remove() -> None:
            if handler in self._state_handlers:
                self._state_handlers.remove(handler)

        return remove

    async def start(self, *, timeout: float | None = 20.0) -> None:
        """Startet den Hintergrund-Runner und wartet auf die erste Subscription."""
        if self._runner_task is not None and not self._runner_task.done():
            return
        if not self._subscriptions:
            raise PolymarketWebSocketError(
                WebSocketErrorCode.INVALID_SUBSCRIPTION,
                "Mindestens eine Asset-/Token-ID ist für die Start-Subscription erforderlich",
            )

        self._stop_event.clear()
        self._connected_event.clear()
        self._runner_task = asyncio.create_task(self._run(), name="polymarket-market-websocket")

        connected_waiter = asyncio.create_task(self._connected_event.wait())
        try:
            done, _ = await asyncio.wait(
                {connected_waiter, self._runner_task},
                timeout=timeout,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if connected_waiter in done and connected_waiter.result():
                return

            if self._runner_task in done:
                error = self._last_error or PolymarketWebSocketError(
                    WebSocketErrorCode.CONNECTION_FAILED,
                    "WebSocket-Runner wurde vor der Subscription beendet",
                    retryable=True,
                )
                raise error

            last = self._last_error
            await self.close()
            message = "WebSocket konnte nicht innerhalb des Start-Timeouts abonnieren"
            if last is not None:
                message += f": {last}"
            raise PolymarketWebSocketError(
                WebSocketErrorCode.CONNECTION_FAILED,
                message,
                retryable=True,
            )
        finally:
            if not connected_waiter.done():
                connected_waiter.cancel()
                try:
                    await connected_waiter
                except asyncio.CancelledError:
                    pass

    async def wait_until_connected(self, timeout: float | None = None) -> None:
        waiter = self._connected_event.wait()
        if timeout is None:
            await waiter
        else:
            await asyncio.wait_for(waiter, timeout=timeout)

    async def get_event(self, *, timeout: float | None = None) -> MarketEventMessage:
        """Liest genau ein Event aus der internen Queue."""
        item = await self._event_queue.get() if timeout is None else await asyncio.wait_for(
            self._event_queue.get(), timeout=timeout
        )
        if item is _QUEUE_CLOSED:
            raise PolymarketWebSocketError(
                WebSocketErrorCode.CONNECTION_CLOSED,
                "WebSocket-Client wurde geschlossen",
            )
        if isinstance(item, _TerminalFailure):
            raise item.error
        return item

    async def subscribe(self, asset_ids: Iterable[str]) -> frozenset[str]:
        additions = _normalize_asset_ids(asset_ids)
        if not additions:
            return self.subscriptions

        async with self._subscription_lock:
            new_ids = additions - self._subscriptions
            self._subscriptions.update(additions)
            if new_ids and self.is_connected:
                await self._send_json(
                    {
                        "assets_ids": sorted(new_ids),
                        "operation": "subscribe",
                        "custom_feature_enabled": self.config.custom_feature_enabled,
                    }
                )
        return self.subscriptions

    async def unsubscribe(self, asset_ids: Iterable[str]) -> frozenset[str]:
        removals = _normalize_asset_ids(asset_ids)
        if not removals:
            return self.subscriptions

        async with self._subscription_lock:
            existing = removals & self._subscriptions
            if existing and self.is_connected:
                await self._send_json(
                    {
                        "assets_ids": sorted(existing),
                        "operation": "unsubscribe",
                    }
                )
            self._subscriptions.difference_update(existing)
            for asset_id in existing:
                self.order_books.remove(asset_id)
        return self.subscriptions

    async def replace_subscriptions(self, asset_ids: Iterable[str]) -> frozenset[str]:
        replacement = _normalize_asset_ids(asset_ids)
        if not replacement:
            raise PolymarketWebSocketError(
                WebSocketErrorCode.INVALID_SUBSCRIPTION,
                "Die Ersatz-Subscription darf nicht leer sein",
            )

        async with self._subscription_lock:
            old = set(self._subscriptions)
            removed = old - replacement
            added = replacement - old

            if self.is_connected and removed:
                await self._send_json({"assets_ids": sorted(removed), "operation": "unsubscribe"})
            if self.is_connected and added:
                await self._send_json(
                    {
                        "assets_ids": sorted(added),
                        "operation": "subscribe",
                        "custom_feature_enabled": self.config.custom_feature_enabled,
                    }
                )

            self._subscriptions = replacement
            for asset_id in removed:
                self.order_books.remove(asset_id)
        return self.subscriptions

    async def close(self) -> None:
        """Beendet Reconnect, Heartbeat und Netzwerkverbindung idempotent."""
        if self._state in {ConnectionState.CLOSED, ConnectionState.DISCONNECTED} and (
            self._runner_task is None or self._runner_task.done()
        ):
            self._set_state(ConnectionState.CLOSED)
            return

        self._set_state(ConnectionState.CLOSING)
        self._stop_event.set()
        websocket = self._websocket
        if websocket is not None:
            try:
                await websocket.close(code=1000, reason="Client shutdown")
            except Exception:
                pass

        task = self._runner_task
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        self._runner_task = None
        self._websocket = None
        self._connected_event.clear()
        self._set_state(ConnectionState.CLOSED)
        await self._put_queue(_QUEUE_CLOSED)

    async def _run(self) -> None:
        """
        Reconnect-Schleife: Jede Verbindung ist isoliert; bei retrybaren Fehlern wird
        mit exponentiellem Backoff und Jitter neu verbunden.
        """
        reconnect_attempt = 0

        while not self._stop_event.is_set():
            try:
                self._set_state(
                    ConnectionState.CONNECTING
                    if reconnect_attempt == 0
                    else ConnectionState.RECONNECTING
                )
                await self._run_connection()
                if self._stop_event.is_set():
                    break
                raise PolymarketWebSocketError(
                    WebSocketErrorCode.CONNECTION_CLOSED,
                    "WebSocket-Verbindung wurde vom Server beendet",
                    retryable=True,
                )
            except asyncio.CancelledError:
                raise
            except BaseException as error:
                normalized = self._normalize_error(error)
                self._last_error = normalized
                self._connected_event.clear()
                await self._notify_error(normalized)

                if self._stop_event.is_set():
                    break

                policy = self.config.reconnect
                if not policy.enabled:
                    await self._fail_terminal(normalized)
                    return

                if self._connection_started_at is not None:
                    lifetime = (
                        asyncio.get_running_loop().time() - self._connection_started_at
                    )
                    if lifetime >= policy.stable_connection_seconds:
                        reconnect_attempt = 0

                reconnect_attempt += 1
                if (
                    policy.maximum_attempts is not None
                    and reconnect_attempt > policy.maximum_attempts
                ):
                    await self._fail_terminal(normalized)
                    return

                self._set_state(ConnectionState.RECONNECTING)
                delay = policy.delay_for_attempt(reconnect_attempt)
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=delay)
                except TimeoutError:
                    pass
            finally:
                self._websocket = None
                self._connection_started_at = None

        if self._state is not ConnectionState.CLOSING:
            self._set_state(ConnectionState.CLOSED)

    async def _run_connection(self) -> float:
        loop = asyncio.get_running_loop()
        try:
            websocket = await connect(
                self.config.endpoint,
                open_timeout=self.config.open_timeout,
                close_timeout=self.config.close_timeout,
                ping_interval=None,
                max_size=self.config.maximum_message_size,
                max_queue=self.config.network_queue_size,
            )
        except (OSError, InvalidURI, InvalidHandshake, TimeoutError) as error:
            raise PolymarketWebSocketError(
                WebSocketErrorCode.CONNECTION_FAILED,
                f"Verbindung zu {self.config.endpoint} fehlgeschlagen: {error}",
                retryable=True,
                original_error=error,
            ) from error

        self._websocket = websocket
        connected_at = loop.time()
        self._connection_started_at = connected_at
        self._last_pong_monotonic = connected_at

        try:
            await self._send_initial_subscription()
            self._set_state(ConnectionState.SUBSCRIBED)
            self._connected_event.set()

            receiver = asyncio.create_task(self._receiver_loop(websocket), name="polymarket-receiver")
            heartbeat = asyncio.create_task(self._heartbeat_loop(websocket), name="polymarket-heartbeat")
            done, pending = await asyncio.wait(
                {receiver, heartbeat},
                return_when=asyncio.FIRST_COMPLETED,
            )

            for task in pending:
                task.cancel()
            for task in pending:
                try:
                    await task
                except asyncio.CancelledError:
                    pass

            for task in done:
                task.result()

            if not self._stop_event.is_set():
                raise PolymarketWebSocketError(
                    WebSocketErrorCode.CONNECTION_CLOSED,
                    "WebSocket-Receiver wurde ohne Abschlussfehler beendet",
                    retryable=True,
                )
            return connected_at
        finally:
            await websocket.close()

    async def _send_initial_subscription(self) -> None:
        async with self._subscription_lock:
            if not self._subscriptions:
                raise PolymarketWebSocketError(
                    WebSocketErrorCode.INVALID_SUBSCRIPTION,
                    "Keine Asset-IDs für die initiale Subscription vorhanden",
                )
            payload = {
                "assets_ids": sorted(self._subscriptions),
                "type": "market",
                "custom_feature_enabled": self.config.custom_feature_enabled,
            }
            await self._send_json(payload)

    async def _receiver_loop(self, websocket: ClientConnection) -> None:
        """
        Transport-Loops: Receiver parst und dispatcht Events, Heartbeat überwacht das
        PONG-Signal und erkennt stille beziehungsweise abgebrochene Verbindungen.
        """
        try:
            async for message in websocket:
                if isinstance(message, str):
                    control = message.strip().upper()
                    if control == "PONG":
                        self._last_pong_monotonic = asyncio.get_running_loop().time()
                        continue
                    if control == "PING":
                        await websocket.send("PONG")
                        continue

                try:
                    events = self.parser.parse_message(message)
                except PolymarketWebSocketError as error:
                    self._last_error = error
                    await self._notify_error(error)
                    continue

                for event in events:
                    self.order_books.apply(event)
                    await self._put_queue(event)
                    await self._dispatch_event(event)
        except ConnectionClosed as error:
            if self._stop_event.is_set():
                return
            raise PolymarketWebSocketError(
                WebSocketErrorCode.CONNECTION_CLOSED,
                f"WebSocket geschlossen: Code {error.code}, Grund {error.reason or 'unbekannt'}",
                retryable=True,
                original_error=error,
            ) from error

    async def _heartbeat_loop(self, websocket: ClientConnection) -> None:
        loop = asyncio.get_running_loop()
        while not self._stop_event.is_set():
            await asyncio.sleep(self.config.heartbeat_interval)
            if self._stop_event.is_set():
                return

            if loop.time() - self._last_pong_monotonic > self.config.pong_timeout:
                raise PolymarketWebSocketError(
                    WebSocketErrorCode.HEARTBEAT_TIMEOUT,
                    "Kein PONG innerhalb des konfigurierten Zeitfensters empfangen",
                    retryable=True,
                )

            try:
                await websocket.send("PING")
            except ConnectionClosed as error:
                raise PolymarketWebSocketError(
                    WebSocketErrorCode.SEND_FAILED,
                    "Heartbeat konnte nicht gesendet werden",
                    retryable=True,
                    original_error=error,
                ) from error

    async def _send_json(self, payload: dict[str, Any]) -> None:
        websocket = self._websocket
        if websocket is None:
            raise PolymarketWebSocketError(
                WebSocketErrorCode.SEND_FAILED,
                "Keine aktive WebSocket-Verbindung",
                retryable=True,
            )

        async with self._send_lock:
            try:
                await websocket.send(json.dumps(payload, separators=(",", ":")))
            except ConnectionClosed as error:
                raise PolymarketWebSocketError(
                    WebSocketErrorCode.SEND_FAILED,
                    "WebSocket-Nachricht konnte nicht gesendet werden",
                    retryable=True,
                    original_error=error,
                ) from error

    async def _dispatch_event(self, event: MarketEventMessage) -> None:
        """
        Event-Verteilung: Orderbook zuerst aktualisieren, danach Queue und registrierte
        Handler bedienen, damit Konsumenten bereits den neuen Zustand sehen.
        """
        handlers = [
            *self._handlers.get(None, ()),
            *self._handlers.get(event.event_type, ()),
        ]
        for handler in tuple(handlers):
            try:
                result = handler(event)
                if inspect.isawaitable(result):
                    await result
            except Exception as error:
                callback_error = PolymarketWebSocketError(
                    WebSocketErrorCode.CALLBACK_FAILED,
                    f"Event-Handler für {event.event_type.value} ist fehlgeschlagen: {error}",
                    retryable=False,
                    original_error=error,
                )
                await self._notify_error(callback_error)

    async def _notify_error(self, error: PolymarketWebSocketError) -> None:
        self.logger.warning("Polymarket WebSocket: %s", error)
        for handler in tuple(self._error_handlers):
            try:
                result = handler(error)
                if inspect.isawaitable(result):
                    await result
            except Exception:
                self.logger.exception("WebSocket-Error-Handler ist fehlgeschlagen")

    def _set_state(self, state: ConnectionState) -> None:
        if state is self._state:
            return
        self._state = state
        if state is not ConnectionState.SUBSCRIBED:
            self._connected_event.clear()
        for handler in tuple(self._state_handlers):
            try:
                result = handler(state)
                if inspect.isawaitable(result):
                    asyncio.create_task(result)
            except Exception:
                self.logger.exception("WebSocket-State-Handler ist fehlgeschlagen")

    async def _fail_terminal(self, error: PolymarketWebSocketError) -> None:
        self._set_state(ConnectionState.FAILED)
        await self._put_queue(_TerminalFailure(error))

    async def _put_queue(
        self,
        item: MarketEventMessage | _TerminalFailure | _QueueClosed,
    ) -> None:
        await self._event_queue.put(item)

    @staticmethod
    def _normalize_error(error: BaseException) -> PolymarketWebSocketError:
        if isinstance(error, PolymarketWebSocketError):
            return error
        return PolymarketWebSocketError(
            WebSocketErrorCode.INTERNAL_ERROR,
            f"Unerwarteter WebSocket-Fehler: {error}",
            retryable=True,
            original_error=error,
        )


def _normalize_asset_ids(asset_ids: Iterable[str]) -> set[str]:
    normalized = {str(asset_id).strip() for asset_id in asset_ids if str(asset_id).strip()}
    return normalized
