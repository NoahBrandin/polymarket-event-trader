import asyncio
import logging
import signal

import src.pm_bot.configuration.logger_config as logger_config
from src.pm_bot.configuration import factory
from src.pm_bot.configuration.config import BotConfig
from src.pm_bot.consumers.execution.bass import Execution
from src.pm_bot.consumers.strategy.base import Strategy
from src.pm_bot.locel_types import StrategyName, ExecutionMode, LogMode, SourceMode, ProducerName
from src.pm_bot.pipeline.engine import EngineStats, Engine, EngineConfig
from src.pm_bot.producer.base import Producer

_BOT_NAME: str = "testerheld"

_EXECUTION_MODE: ExecutionMode = ExecutionMode.PAPER
_LOG_MODE: LogMode = LogMode.INFO
_SOURCE_MODE: SourceMode = SourceMode.LIVE

_PRODUCER: ProducerName = ProducerName.WEBSOCKET
_STRATEGY: StrategyName = StrategyName.DEFAULT_RANDOM_STRATEGY


_ENGINE_QUEUE_SIZE: int = 5000
_ENGINE_PRINT_EVENTS: bool = True

def main():
    """Startet die vollständig aus einer Bot-Config erzeugte Pipeline."""

    logger_config.setup_global_logger(_LOG_MODE)
    logger = logger_config.get_logger()
    logger.info("Logger setup")

    logger.debug("Setting up bot_config")
    bot_config: BotConfig = BotConfig(name=_BOT_NAME, execution_mode=_EXECUTION_MODE, log_mode=_LOG_MODE, source_mode=_SOURCE_MODE, producer_name=_PRODUCER, strategy_name=_STRATEGY)
    logger.info("Bot config setup")

    logger.debug("Setting up components")
    components = factory.get_components(bot_config)
    strategy = components["strategy"]
    producer = components["producer"]
    execution = components["execution"]
    logger.info("Components setup")

    logger.info("Starting Engine")
    try:
        stats = asyncio.run(
            _async_start(
                producer=producer,
                strategy=strategy,
                execution=execution,
                config=EngineConfig(
                    queue_size=_ENGINE_QUEUE_SIZE,
                    print_events=_ENGINE_PRINT_EVENTS,
                ),
            )
        )
        logger.debug(f"Engine run-methode stopped")
        logger.info(f"Engine run-methode {"failed" if _engine_run_failed(stats) else "completed"}")
    except KeyboardInterrupt:
        logger.warning(f"Bot shutdown by user")
        report = execution.report()
        if report is not None:
            logger.info(f"Execution report: available_cash={report.available_cash} "
                        f"open_position={str(report.open_position)} close_position={str(report.close_position)}, "
                        f"trade_volume={report.trade_volume}")
    except BaseException as error:
        logger.error(f"Bot_run failed with error {error}")
        raise BaseException("Bot-Run durch fehlgeschlagen")


async def _run_engine(engine: Engine) -> EngineStats:
    """Startet die Engine und behandelt SIGINT/SIGTERM kontrolliert."""

    logger = logger_config.get_logger()

    loop = asyncio.get_running_loop() # loop aller grade laufender async Events
    stop_requested = asyncio.Event() # Beendet alle async Events (steht auf False) True -> beendet alle async Tasks

    def request_stop() -> None:
        stop_requested.set()

    registered_signals: list[signal.Signals] = []
    for signum in (signal.SIGINT, signal.SIGTERM): # signal.SIGNT = Str +C, signal.SIGTERM = Sys ende
        try:
            logger.debug(f"Signal {signum} is being registered")
            loop.add_signal_handler(signum, request_stop)
            registered_signals.append(signum)
            logger.debug(f"Registered signal {signum} was registered")
        except (NotImplementedError, RuntimeError):
            pass

    engine_task = asyncio.create_task(engine.run(), name="pm-bot:engine") # Startet engine run-funktion
    stop_task = asyncio.create_task(stop_requested.wait(), name="pm-bot:stop-signal")

    try:
        logger.debug("Asyncio engine_task and stop_task going to be called")
        done, _ = await asyncio.wait( #wartet bis einer der Task fertig ist
            {engine_task, stop_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if stop_task in done and not engine_task.done():
            logger.info("Got stop signal. Stopping engine")
            await engine.stop()
        return await engine_task
    except asyncio.CancelledError:
        logger.info("User terminated Bot controlled or system was shutdown. Controlled shutdown initiated...")
        raise
    finally:
        stop_task.cancel() #Aufräumen
        await asyncio.gather(stop_task, return_exceptions=True)
        for signum in registered_signals:
            try:
                loop.remove_signal_handler(signum)
            except (NotImplementedError, RuntimeError):
                pass


async def _async_start(
    *,
    producer: Producer,
    strategy: Strategy,
    execution: Execution,
    config: EngineConfig,
) -> EngineStats:
    engine = Engine(
        producer=producer,
        strategy=strategy,
        execution=execution,
        config=config,
    )
    logging.getLogger(__name__).debug("Engine initialized")
    return await _run_engine(engine)

def _engine_failure_counts(stats: EngineStats) -> dict[str, int]:
    return {
        "producer_failures": stats.producer_failures,
        "strategy_failures": stats.strategy_failures,
        "execution_failures": stats.execution_failures,
    }


def _engine_run_failed(stats: EngineStats) -> bool:
    return any(_engine_failure_counts(stats).values())

if __name__ == '__main__':
    main()