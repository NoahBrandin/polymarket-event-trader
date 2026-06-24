"""Fehlerklassen des Polymarket-WebSocket-Clients."""

from __future__ import annotations

from enum import Enum


class WebSocketErrorCode(str, Enum):
    """
    Normalisierte WebSocket-Fehler.

    Transport- und Protokollfehler werden auf stabile Fehlercodes abgebildet,
    damit Reconnect und Nutzer-Handler unabhängig von Library-Exceptions bleiben.
    """
    INVALID_SUBSCRIPTION = "INVALID_SUBSCRIPTION"
    CONNECTION_FAILED = "CONNECTION_FAILED"
    CONNECTION_CLOSED = "CONNECTION_CLOSED"
    HEARTBEAT_TIMEOUT = "HEARTBEAT_TIMEOUT"
    INVALID_MESSAGE = "INVALID_MESSAGE"
    SEND_FAILED = "SEND_FAILED"
    CALLBACK_FAILED = "CALLBACK_FAILED"
    AUTH_FAILED = "AUTH_FAILED"
    INTERNAL_ERROR = "INTERNAL_ERROR"


class PolymarketWebSocketError(Exception):
    """Einheitlicher Fehler für Verbindungs-, Protokoll- und Parsing-Probleme."""

    def __init__(
        self,
        code: WebSocketErrorCode,
        message: str,
        *,
        retryable: bool = False,
        original_error: BaseException | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.retryable = retryable
        self.original_error = original_error
