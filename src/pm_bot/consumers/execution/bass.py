from abc import ABC, abstractmethod

from datetime import datetime

from src.pm_bot.configuration.trading import ExecutionReport, OrderIntent
from src.pm_bot.locel_types import RunMode


class Execution(ABC):
    def __init__(self, run_mode: RunMode):
        self.run_mode = run_mode

    @abstractmethod
    async def execute(self, order) -> ExecutionReport:
        pass

    @abstractmethod
    async def start(self):
        pass

    @abstractmethod
    async def stop(self):
        pass

    def creat_execution_name(self, order: OrderIntent) -> str:
        name = self.run_mode.name + "." + order.side.name + "." + order.asset_id + "." + datetime.isoformat()
        return name