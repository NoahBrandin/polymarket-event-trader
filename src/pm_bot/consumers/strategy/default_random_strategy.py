import random
from decimal import Decimal

from src.pm_bot.configuration.selection import SubscriptionSelection
from src.pm_bot.configuration.trading import StrategyDecision, ExecutionReport, OrderIntent
from src.pm_bot.consumers.strategy.base import Strategy, StrategyConfig
from src.pm_bot.locel_types import StrategyName, StrategyType, ProducerDataType, TradingSide
from src.pm_bot.pipeline.events import EventEnvelope, MarketUpdatePayload


class DefaultRandomStrategy(Strategy):
    """
    Test of Concept
    """
    def __init__(self) -> None:
        super().__init__(StrategyConfig(
            strategy_name=StrategyName.DEFAULT_RANDOM_STRATEGY,
            strategy_type=StrategyType.UPDATE_DRIVEN, #kann auch TICK_DRIVEN (eig alles muss nur auf tick angepasst werden)
            producer_type= ProducerDataType.WEBSOCKET, #kann auch DATA_API sein (eig alles)
        ))

    async def get_subscription_selection(self) -> SubscriptionSelection | None:
        return None # -> producer default selection

    async def on_start(self) -> StrategyDecision | None:
        return

    async def on_stop(self):
        pass

    async def on_event(self, envelope: EventEnvelope) -> StrategyDecision | None:
        random.randint(1,10)
        if not isinstance(envelope.payload, MarketUpdatePayload):
            return
        orders = []

        for asset in envelope.payload.asset_events:
            if asset.asset_id is None:
                continue

            decider  = random.randint(1, 20)
            if decider <= 18:
                continue
            size = random.randint(1, 10)
            orders.append(OrderIntent(
                strategy_name=self.config.strategy_name,
                asset_id=asset.asset_id,
                market_id=envelope.payload.market_id,
                side= TradingSide.BUY if decider == 19 else TradingSide.SELL,
                size=Decimal(size),
                limit_price= Decimal(float(random.randint(1, 10)/11)),
            ))
        if orders:
            return StrategyDecision(orders=orders)
        return None

    async def on_execution(self, report: ExecutionReport) -> StrategyDecision | None:
        return