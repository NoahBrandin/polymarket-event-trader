import asyncio
import json
from typing import Any

import websockets

from pm_bot.configuration.logger_config import get_logger
from pm_bot.configuration.selection import SubscriptionSelection
from pm_bot.locel_types import ProducerDataType, ProducerName, SelectionType
from pm_bot.pipeline.events import ErrorPayload, EventEnvelope, HeartbeatPayload
from pm_bot.pipeline.queue import EventQueue
from pm_bot.producer.base import Producer, ProducerConfig
from pm_bot.producer.utils.websocket_normalizer import parse_market_ws_message
from polymarket_interfaces.gamma_api import GammaAPI

URL  = "wss://ws-subscriptions-clob.polymarket.com"

config = ProducerConfig(
    name=ProducerName.WEBSOCKET,
    type= ProducerDataType.WEBSOCKET,
    selection_type= SelectionType.MARKT_EVENT,
)

MARKET_CHANNEL = "market"

logger = get_logger()

class WebsocketFeed(Producer):
    """
    Verbindet den Bot mit Polymarket über eine WebSocket.
        - sendet Änderungen auf von abonnierten Märkten/Usern.
        - übersetzt raw_data zu FeedEvents
        - kann angehalten (stop) und neu verbunden (reconnect)
    """

    def __init__(self):
        super().__init__(config=config)

        self.url = URL.rstrip("/")

        self._subscription_selection = None #asset_ids

        self.verbose = False
        self.reconnect_backoff_s:float = 1.0
        self.max_backoff_s:int = 30

        self._ws = None

    async def _emit_event(self, payload: Any, queue: EventQueue) -> None:
        """Ersetzt deine _emit Methode, um die Daten an die Queue zu

        übergeben.
        """
        await queue.put(EventEnvelope(
            event_type=payload.event_type,
            timestamp=payload.timestamp,
            producer_name=ProducerName.WEBSOCKET,
            producer_type=ProducerDataType.WEBSOCKET,
            payload=payload,
            )
        )

    async def get_default_subscription_selection(self) -> SubscriptionSelection:
        trending_markets = await GammaAPI().get_trending_markets()
        asset_id_list = []
        for market in trending_markets:  # get trending markets
            asset_id_list.extend(market.clob_token_ids)
        return SubscriptionSelection(SelectionType.MARKT_EVENT, asset_id_list)

    async def set_subscription_selection(self, selection: SubscriptionSelection) -> None:
        self._subscription_selection = selection



    async def run(self, event_queue: EventQueue) -> None:
        """
        Das asynchrone Herzstück:

        Startet den Verbindungs-Loop mit Backoff.
        """
        backoff = self.reconnect_backoff_s
        furl = f"{self.url}/ws/market"

        while not self._stop_requested.is_set():
            try:
                logger.info(f"Websocket is connected whit: {furl}")

                # Verbindung asynchron öffnen
                async with websockets.connect(furl) as ws:
                    self._ws = ws
                    await self._on_open(ws)
                    # Reset Backoff bei erfolgreicher Verbindung
                    backoff = self.reconnect_backoff_s
                    async for message in ws:
                        if self._stop_requested.is_set():
                            break
                        await self._on_message(message, event_queue)

            except asyncio.CancelledError:
                logger.debug("WebSocket producer task cancelled")
                raise

            except websockets.exceptions.ConnectionClosed as error:
                logger.warning("WebSocket connection closed: %s", error)

            except Exception as error:
                logger.error(f"WebSocket producer failed whit: {error}")
                raise Exception(f"WebSocket producer failed: {error}")
            finally:
                self._ws = None

            # Falls Stop angefordert wurde, schleife sofort beenden
            if self._stop_requested.is_set():
                break

            # Asynchrones, non-blocking Warten vor dem Wiederverbinden
            logger.info(f"Websocket disconnected. Reconnecting in {backoff}s...")
            await self._emit_event(ErrorPayload(message=f"Websocket disconnected. Reconnecting in {backoff}s..."),
                                   queue=event_queue)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, self.max_backoff_s)


    async def _on_open(self, ws) -> None:
        """
        Wird aufgerufen, sobald die Verbindung steht.

        Sendet Subskriptions-Pakete.
        """
        await ws.send(json.dumps({"assets_ids": self._subscription_selection.selections, "type": MARKET_CHANNEL}))

    async def _on_message(self, message: str | bytes | dict , event_queue: EventQueue) -> None:
        """Verarbeitet jede eingehende Zeile vom Server."""
        try:
            if isinstance(message, bytes):
                message = message.decode("utf-8")

            if self.verbose:
                print("WS-Raw:", message)

            # JSON Parsing
            data = json.loads(message) if not isinstance(message, dict)  else message

            if isinstance(data, list):#erste Ausgabe ist immer list mit books von allen Märkten
                for d in data:
                    await self._on_message(d, event_queue)
                return

            parsed_payload = parse_market_ws_message(data)
            if parsed_payload is None:
                await self._emit_event(HeartbeatPayload(), queue=event_queue)
                return
            # Event in Queue pushen
            await self._emit_event(payload=parsed_payload, queue=event_queue)

        except Exception as e:
            await self._emit_event(ErrorPayload(details=e, message="Pushing Message into Queue failed"),
                                   queue=event_queue)

    async def _on_stop(self) -> None:
        """Setzt das Stopp-Signal."""
        logger.debug("Websocket-Feed stopping")

        # 1. Signal stop to other tasks
        self._stop_requested.set()

        if self._ws is not None:
            await self._ws.close(
                code=1000,
                reason="Application shutdown",
            )

        logger.debug(f"Websocket-Feed stopped set: ws={self._ws}")