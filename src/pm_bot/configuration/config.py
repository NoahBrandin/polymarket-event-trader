from dataclasses import dataclass

from src.pm_bot.locel_types import RunMode, LogMode, SourceMode, StrategyName, ProducerName


@dataclass(slots=True, frozen=True)
class BotConfig:
    name: str
    run_mode: RunMode
    log_mode: LogMode
    source_mode: SourceMode

    producer_name: ProducerName
    strategy_name: StrategyName