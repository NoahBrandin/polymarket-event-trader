from pm_bot.configuration.bot_setup import setup
from pm_bot.locel_types import ExecutionMode, LogMode, ProducerName, StrategyName


def test_websocket_none_none():
    setup(log_mode=LogMode.ERROR, bot_name="test_websocket_none_none", producer=ProducerName.WEBSOCKET,
          strategy=StrategyName.NONE, execution_mode=ExecutionMode.NONE,
          testing_setup=True)

def test_websocket_default_none():
    setup(log_mode=LogMode.ERROR, bot_name="test_websocket_default_none", producer=ProducerName.WEBSOCKET,
          strategy=StrategyName.DEFAULT_RANDOM_STRATEGY, execution_mode=ExecutionMode.NONE,
          testing_setup=True)

def test_websocket_default_paper():
    setup(log_mode=LogMode.ERROR, bot_name="test_websocket_default_none", producer=ProducerName.WEBSOCKET,
          strategy=StrategyName.DEFAULT_RANDOM_STRATEGY, execution_mode=ExecutionMode.PAPER,
          testing_setup=True)