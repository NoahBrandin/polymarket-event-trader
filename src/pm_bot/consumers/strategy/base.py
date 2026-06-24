from abc import ABC, abstractmethod
from dataclasses import dataclass

from src.pm_bot.configuration.selection import SubscriptionSelection
from src.pm_bot.configuration.trading import StrategyDecision, ExecutionReport
from src.pm_bot.locel_types import StrategyName, StrategyType, ProducerDataType
from src.pm_bot.pipeline.events import EventEnvelope


@dataclass(frozen=True)
class StrategyConfig:
    strategy_name: StrategyName
    strategy_type: StrategyType
    producer_type: ProducerDataType

class Strategy(ABC):

    def __init__(self, config: StrategyConfig):
        self.config = config

    @abstractmethod
    def get_subscription_selection(self) -> SubscriptionSelection:
        pass

    @abstractmethod
    async def on_start(self) -> StrategyDecision | None:
        pass

    @abstractmethod
    async def on_event(self, envelope: EventEnvelope) -> StrategyDecision | None:
        """Verarbeitet ein ausgewähltes Marktevent und liefert Entscheidungen."""
        pass

    @abstractmethod
    async def on_execution(self, report: ExecutionReport) -> StrategyDecision | None:
        pass

    @abstractmethod
    async def on_stop(self) -> None:
        pass
