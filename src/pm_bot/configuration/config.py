from dataclasses import dataclass

from pm_bot.locel_types import ExecutionMode, LogMode, StrategyName, ProducerName


@dataclass(slots=True, frozen=True)
class BotConfig:
    name: str
    execution_mode: ExecutionMode
    log_mode: LogMode

    producer_name: ProducerName
    strategy_name: StrategyName