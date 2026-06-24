from abc import ABC
from enum import Enum

from src.pm_bot.configuration.trading import ExecutionReport
from src.pm_bot.locel_types import RunMode


class Execution(ABC):
    def __init__(self, run_mode: RunMode):
        self.run_mode = run_mode

    async def submit_order(self, order) -> ExecutionReport:
        pass

    async def start(self):
        pass
    async def stop(self):
        pass
    def execute(self):
        pass