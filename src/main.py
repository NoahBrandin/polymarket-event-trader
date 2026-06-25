

from src.pm_bot.configuration.bot_setup import setup

from src.pm_bot.locel_types import StrategyName, ExecutionMode, LogMode, ProducerName


_BOT_NAME: str = "testerheld"

_LOG_MODE: LogMode = LogMode.INFO

_PRODUCER: ProducerName = ProducerName.WEBSOCKET
_STRATEGY: StrategyName = StrategyName.DEFAULT_RANDOM_STRATEGY
_EXECUTION_MODE: ExecutionMode = ExecutionMode.PAPER


_ENGINE_QUEUE_SIZE: int = 5000
_ENGINE_PRINT_EVENTS: bool = True

def main():
    setup(log_mode= _LOG_MODE, bot_name= _BOT_NAME, producer= _PRODUCER, strategy= _STRATEGY,
          execution_mode=_EXECUTION_MODE, engine_queue_size= _ENGINE_QUEUE_SIZE,
          engine_print_events = _ENGINE_PRINT_EVENTS)

if __name__ == '__main__':
    main()