from abc import ABC, abstractmethod
from dataclasses import dataclass

from datetime import datetime
from decimal import Decimal

from pm_bot.consumers.execution.utils.account_interface import AccountInterface, PaperAccountInterface, Position
from pm_bot.consumers.execution.utils.config import ExecutionConfig
from pm_bot.configuration.trading import ExecutionReport, OrderIntent

@dataclass(frozen=True, slots=True, kw_only=True)
class FinalReport:
    available_cash: Decimal
    open_position: dict[str, Position]
    close_position: dict[str, Position]
    trade_volume: Decimal


class Execution(ABC):
    def __init__(self, config: ExecutionConfig):
        self.config = config
        self.account_interface: AccountInterface | None = None

    @abstractmethod
    async def execute(self, order: OrderIntent) -> ExecutionReport:
        pass

    @abstractmethod
    async def open(self, order: OrderIntent) -> ExecutionReport:
        pass

    @abstractmethod
    async def close(self, order: OrderIntent) -> ExecutionReport:
        pass

    @abstractmethod
    async def start(self):
        pass

    @abstractmethod
    async def stop(self):
        pass

    def report(self) -> FinalReport | None:
        return FinalReport(
            available_cash=self.account_interface.get_cash(),
            open_position=self.account_interface.get_open_positions(),
            close_position=self.account_interface.get_close_positions(),
            trade_volume=self.account_interface.get_trades_volume()
        ) if self.account_interface else None

    def creat_execution_name(self, order: OrderIntent) -> str:
        name = self.config.execution_name + "." + order.side.name + "." + order.asset_id + "." + datetime.now().isoformat()
        return name