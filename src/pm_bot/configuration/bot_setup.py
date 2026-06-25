import asyncio
import signal

from pm_bot.configuration import factory, logger_config
from pm_bot.configuration.config import BotConfig
from pm_bot.consumers.execution.bass import Execution
from pm_bot.consumers.strategy.base import Strategy
from pm_bot.locel_types import ExecutionMode, LogMode, ProducerName, StrategyName
from pm_bot.pipeline.engine import Engine, EngineConfig, EngineStats
from pm_bot.producer.base import Producer


def setup(log_mode: LogMode = LogMode.INFO,
          bot_name:str = "poly",
          execution_mode: ExecutionMode = ExecutionMode.PAPER,
          producer: ProducerName = ProducerName.WEBSOCKET,
          strategy: str = StrategyName.DEFAULT_RANDOM_STRATEGY,
          engine_queue_size: int = 5000,

          testing_setup: bool = False,
          ):
    """Startet die vollständig aus einer Bot-Config erzeugte Pipeline."""

    logger_config.setup_global_logger(log_mode)
    logger = logger_config.get_logger()
    logger.info("Logger setup")

    logger.debug("Setting up bot_config")
    bot_config: BotConfig = BotConfig(name=bot_name, execution_mode=execution_mode, log_mode=log_mode,
                                      producer_name=producer, strategy_name=strategy)
    logger.info("Bot config setup")

    logger.debug("Setting up components")
    components = factory.get_components(bot_config)
    strategy = components["strategy"]
    producer = components["producer"]
    execution = components["execution"]
    logger.info("Components setup")

    logger.info("Starting Engine")
    try:
        asyncio.run(
            _async_start(
                producer=producer,
                strategy=strategy,
                execution=execution,
                config=EngineConfig(
                    queue_size=engine_queue_size,
                    testing=testing_setup
                ),
            )
        )
        logger.debug("Engine run-methode stopped")
    except KeyboardInterrupt:
        logger.warning("Bot shutdown by user")
        report = execution.report()
        if report is not None:
            logger.info(f"Execution report: available_cash={report.available_cash} "
                        f"open_position={str(report.open_position)} close_position={str(report.close_position)}, "
                        f"trade_volume={report.trade_volume}")
    except Exception:
        logger.exception("Bot run failed")
        raise


async def _run_engine(engine: Engine) -> EngineStats:
    """Startet die Engine und behandelt SIGINT/SIGTERM kontrolliert."""

    logger = logger_config.get_logger()
    logger.debug("Engine initialized")


    loop = asyncio.get_running_loop()  # loop aller grade laufender async Events
    stop_requested = asyncio.Event()  # Beendet alle async Events (steht auf False) True -> beendet alle async Tasks

    def request_stop() -> None:
        stop_requested.set()

    registered_signals: list[signal.Signals] = []
    for signum in (signal.SIGINT, signal.SIGTERM):  # signal.SIGNT = Str +C, signal.SIGTERM = Sys ende
        try:
            logger.debug(f"Signal {signum} is being registered")
            loop.add_signal_handler(signum, request_stop)
            registered_signals.append(signum)
            logger.debug(f"Registered signal {signum} was registered")
        except (NotImplementedError, RuntimeError):
            pass

    engine_task = asyncio.create_task(engine.run(), name="pm-bot:engine")  # Startet engine run-funktion
    stop_task = asyncio.create_task(stop_requested.wait(), name="pm-bot:stop-signal")

    try:
        logger.warning("Bot started")
        done, _ = await asyncio.wait(  # wartet bis einer der Task fertig ist
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
        stop_task.cancel()  # Aufräumen
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
    return await _run_engine(engine)