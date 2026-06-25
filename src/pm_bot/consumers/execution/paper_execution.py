from dataclasses import dataclass

from pm_bot.configuration import logger_config
from pm_bot.configuration.trading import OrderIntent

from pm_bot.consumers.execution.utils.account_interface import PaperAccountInterface, Position
from pm_bot.consumers.execution.utils.config import PaperExecutionConfig
from polymarket_interfaces import ClobMarketAPI
from src.pm_bot.configuration.trading import ExecutionReport
from src.pm_bot.consumers.execution.bass import Execution
from src.pm_bot.locel_types import ExecutionMode, ExecutionStatus, TradingSide

from datetime import datetime

logger = logger_config.get_logger()

class PaperExecution(Execution):
    def __init__(self):
        config = PaperExecutionConfig(
            execution_name="paper_execution",
            mode=ExecutionMode.PAPER,
        )
        super().__init__(config)

        self.account_interface: PaperAccountInterface = PaperAccountInterface(config)
        self._clob_market_api = ClobMarketAPI()

    async def execute(self, order:OrderIntent) -> ExecutionReport:

        if order.side == TradingSide.BUY:
            return await self.open(order)

        elif order.side == TradingSide.SELL and order.asset_id in self.account_interface.get_open_positions().keys():
            return await self.close(order)

        logger.warning(f"Executing paper execution failed: {order}")
        return ExecutionReport(
            execution_name=self.creat_execution_name(order),
            order = order,
            status = ExecutionStatus.FAILED,
            timestamp=datetime.utcnow(),
        )

    async def open(self, order:OrderIntent) -> ExecutionReport:
        asset_id = order.asset_id

        current = await self._clob_market_api.get_midpoint(asset_id)
        if asset_id in self.account_interface.get_open_positions().keys() :
            old_position = self.account_interface.get_open_positions()[asset_id]
            new_size = old_position.size + order.size
            new_price = (old_position.price*old_position.size + current*order.size)/(old_position.size*order.size)
            self.account_interface.get_open_positions()[asset_id] = Position(price=new_price, size=new_size)
        else:
            self.account_interface.get_open_positions()[asset_id] = Position(price=current, size=order.size)

        return ExecutionReport(
            execution_name=self.creat_execution_name(order),
            order=order,
            status=ExecutionStatus.LIVE,
            timestamp=datetime.utcnow(),
        )

    async def close(self, order:OrderIntent) -> ExecutionReport:
        asset_id = order.asset_id

        current = await self._clob_market_api.get_midpoint(asset_id)
        positions = self.account_interface.get_open_positions()[asset_id]

        _return = positions.size*(current/positions.price)
        positions.return_ = _return
        self.account_interface.set_cash(self.account_interface.get_cash() + _return)
        self.account_interface.get_close_positions()[asset_id] = positions

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
