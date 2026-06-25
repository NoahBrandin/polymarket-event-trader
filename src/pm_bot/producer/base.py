import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass


from pm_bot.configuration.logger_config import get_logger
from pm_bot.configuration.selection import SubscriptionSelection
from pm_bot.locel_types import SelectionType, ProducerName, ProducerDataType
from pm_bot.pipeline.queue import EventQueue

logger = get_logger()

@dataclass(frozen=True)
class ProducerConfig:
    name: ProducerName

    type: ProducerDataType
    selection_type: SelectionType


class Producer(ABC):
    def __init__(self, config: ProducerConfig):
        self.config = config

        self._stop_requested = asyncio.Event()
        self._subscription_selection = SubscriptionSelection(config.selection_type)
        self._selection_lock = asyncio.Lock()


        logger.debug("Producer init ok")

    @property
    def stop_requested(self) -> bool:
        logger.debug("Producer stop requested")
        return self._stop_requested.is_set()

    @property
    def subscription_selection(self) -> SubscriptionSelection:
        logger.debug("Producer subscription selection requested")
        return self._subscription_selection

    @subscription_selection.setter
    async def set_subscription_selection(self, selection: SubscriptionSelection) -> None:
        if not isinstance(selection.type, type(self.config.selection_type)):
            logger.error(f"Producer subscription selection type error: type=({selection.type}, expected={self.config.selection_type})")
            raise TypeError("selection muss eine MarketSelection sein")

        async with self._selection_lock:
            if selection == self._subscription_selection:
                return
            self._subscription_selection = selection
            await self._on_market_selection_changed(selection)
            logger.debug("Producer set subscription selection ok")

    @abstractmethod
    async def get_default_subscription_selection(self) -> SubscriptionSelection:
        logger.debug("Producer get default subscription selection")

    async def _on_market_selection_changed(self, selection: SubscriptionSelection,) -> None:
        """Optionaler Hook, beispielsweise für WebSocket-Subscriptions."""

    @abstractmethod
    async def run(self, event_queue: EventQueue) -> None:
        """
        Einheitlicher Lifecycle-Wrapper: Started/Failed/Stopped werden auch dann in
        die Queue geschrieben, wenn die konkrete Datenquelle fehlschlägt.

        Finaler Lifecycle-Wrapper für jeden Producer.
        """

    async def stop(self) -> None:
        """Fordert ein kooperatives Ende des Producers an."""
        logger.debug("Producer stop requested")
        self._stop_requested.set()
        logger.debug("Producer stop initiated")
        await self._on_stop()

    @abstractmethod
    async def _on_stop(self) -> None:
        """Optionaler Hook für Ressourcen wie WebSocket-Verbindungen."""