from abc import ABC
from dataclasses import dataclass

from src.pm_bot.locel_types import ExecutionMode, NonNegativeDecimal, Probability


@dataclass(frozen=True, slots=True, kw_only=True)
class ExecutionConfig(ABC):
    execution_name: str
    mode: ExecutionMode

@dataclass(slots=True, frozen=True)
class PaperExecutionConfig(ExecutionConfig):
    initial_cash: NonNegativeDecimal = NonNegativeDecimal("10000")
    fee_bps: Probability = Probability("0")
    allow_short: bool = False