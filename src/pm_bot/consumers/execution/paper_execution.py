from dataclasses import dataclass, field
from decimal import Decimal

from src.pm_bot.configuration.trading import ExecutionReport
from src.pm_bot.consumers.execution.bass import Execution
from src.pm_bot.locel_types import RunMode, ExecutionStatus, NonNegativeDecimal, Probability

from datetime import datetime

@dataclass(slots=True, frozen=True)
class PaperExecutionConfig:
    initial_cash: NonNegativeDecimal = NonNegativeDecimal("10000")
    fee_bps: Probability = Probability("0")
    allow_short: bool = False

@dataclass
class PaperAccountSnapshot:
    cash: NonNegativeDecimal = NonNegativeDecimal("10000")
    positions: dict[str, Decimal] = field(default_factory=dict)
    open_orders: int = 0

class paper_execution(Execution):
    def __init__(self, run_mode: RunMode):
        super().__init__(run_mode)
        self.config = PaperExecutionConfig()
        self.state = PaperAccountSnapshot(cash=self.config.initial_cash)

    async def execute(self, order) -> ExecutionReport:
        return ExecutionReport(
            execution_name=self.creat_execution_name(order),
            order = order,
            status = ExecutionStatus.LIVE,
            timestamp=datetime.utcnow(),
        )


    async def start(self):
        pass

    async def stop(self):
        pass
