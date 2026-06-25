import asyncio
from collections.abc import Awaitable, Callable, Mapping
from enum import StrEnum
from typing import Any, Protocol, TypeVar

T = TypeVar("T")

class ErrorCode(StrEnum):
    """Definiert stabile Fehlercodes für Polymarket-API-Aufrufe."""
    NETWORK_ERROR = "NETWORK_ERROR"
    TIMEOUT = "TIMEOUT"
    RATE_LIMITED = "RATE_LIMITED"
    AUTH_FAILED = "AUTH_FAILED"
    MARKET_NOT_FOUND = "MARKET_NOT_FOUND"
    INVALID_RESPONSE = "INVALID_RESPONSE"
    API_ERROR = "API_ERROR"
    INTERNAL_ERROR = "INTERNAL_ERROR"


class PolymarketError(Exception):
    """Einheitlicher Fehler für HTTP-, Netzwerk- und Antwortprobleme."""

    def __init__(
        self,
        code: ErrorCode,
        message: str,
        *,
        retryable: bool = False,
        original_error: Exception | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.retryable = retryable
        self.original_error = original_error

    @classmethod
    def from_http_error(cls, status: int, body: Any = None) -> "PolymarketError":
        body_message = ""
        if isinstance(body, Mapping):
            for key in ("message", "error", "errorMsg", "error_msg"):
                value = body.get(key)
                if value not in (None, ""):
                    body_message = str(value)
                    break

        if status == 429:
            return cls(
                ErrorCode.RATE_LIMITED,
                body_message or "Rate limit erreicht",
                retryable=True,
            )
        if status in (401, 403):
            return cls(
                ErrorCode.AUTH_FAILED,
                body_message or "Authentifizierung fehlgeschlagen",
            )
        if status == 404:
            return cls(
                ErrorCode.MARKET_NOT_FOUND,
                body_message or "Ressource nicht gefunden",
            )
        if status == 400:
            return cls(
                ErrorCode.INVALID_RESPONSE,
                body_message or "Ungültige Anfrage",
            )

        return cls(
            ErrorCode.NETWORK_ERROR,
            body_message or f"HTTP-Fehler {status}",
            retryable=status >= 500,
        )


class ApiType(StrEnum):
    """Kennzeichnet den durch den RateLimiter gesteuerten API-Typ."""

    GAMMA_API = "gamma-api"
    CLOB_API = "clob-api"
    DATA_API = "data-api"



class UnifiedCache(Protocol):
    """
    Entspricht konzeptionell dem UnifiedCache des TypeScript-Projekts.

    Der ursprüngliche GammaApiClient speichert den Cache zwar im Konstruktor,
    verwendet ihn in seinen Methoden aber derzeit nicht.
    """

    async def get(self, key: str) -> Any | None:
        ...

    async def set(self, key: str, value: Any, ttl_ms: int) -> None:
        ...

    async def invalidate(self, pattern: str) -> None:
        ...

    def clear(self) -> None:
        ...



class RateLimiter:
    """
    Asynchroner Token-Bucket-Rate-Limiter.

    Standardmäßig sind kurzfristige Bursts bis 10 Requests möglich; langfristig
    werden höchstens 10 Requests pro Sekunde gestartet.
    """

    def __init__(
            self,
            requests_per_second: float = 10.0,
            burst_capacity: int = 10,
    ) -> None:
        if requests_per_second <= 0:
            raise ValueError("requests_per_second muss positiv sein")
        if burst_capacity <= 0:
            raise ValueError("burst_capacity muss positiv sein")

        self._rate = float(requests_per_second)
        self._capacity = float(burst_capacity)
        self._tokens = float(burst_capacity)
        self._last_refill: float | None = None
        self._lock = asyncio.Lock()

    async def _acquire(self) -> None:
        loop = asyncio.get_running_loop()

        while True:
            async with self._lock:
                now = loop.time()

                if self._last_refill is None:
                    self._last_refill = now

                elapsed = now - self._last_refill
                self._tokens = min(
                    self._capacity,
                    self._tokens + elapsed * self._rate,
                )
                self._last_refill = now

                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return

                wait_seconds = (1.0 - self._tokens) / self._rate

            await asyncio.sleep(wait_seconds)

    async def execute(
            self,
            api: ApiType,
            operation: Callable[[], Awaitable[T]],
    ) -> T:
        if not isinstance(api, ApiType):
            raise ValueError(f"Unbekannter API-Typ: {api}")

        await self._acquire()
        return await operation()