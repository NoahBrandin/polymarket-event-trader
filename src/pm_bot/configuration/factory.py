import importlib
import re
from typing import Dict, TypeVar, Any


from src.pm_bot.configuration.config import BotConfig
from src.pm_bot.configuration.logger_config import get_logger
from src.pm_bot.consumers.execution.bass import Execution
from src.pm_bot.consumers.strategy.base import Strategy
from src.pm_bot.locel_types import RunMode, StrategyName, ProducerDataType, SourceMode, ProducerName, camel_to_snake
from src.pm_bot.producer.base import Producer

logger = get_logger()

T = TypeVar("T")

PRODUCER_PATH = "src/pm_bot/producer"
STRATEGY_PATH = "src/pm_bot/consumers/strategy"
EXECUTION_PATH = "src/pm_bot/consumers/execution"

def _load_class(parent_path: str, class_name) -> type[Any]:
    """
    Plugin-Auflösung.

    class_path lädt vertrauenswürdige externe Klassen und prüft anschließend die
    erwartete Mutterklasse.
    """
    # Producer Klasse muss so heissen wie Producer_file (nur anderer Case)
    parent_path = parent_path + "/" + camel_to_snake(class_name)
    try:
        module = importlib.import_module(parent_path.replace("/", "."))
    except ModuleNotFoundError:
        logger.error(f"Model {parent_path} not found in files")
        raise ModuleNotFoundError(f"Model {parent_path} not found in files")

    try:
        loaded = getattr(module, class_name)
    except AttributeError as error:
        logger.error(f"Class {class_name} not found in {parent_path}")
        raise ImportError(f"Class {class_name} not found in {parent_path} ") from error
    if not isinstance(loaded, type):
        logger.error(f"{class_name} not a class in {parent_path}")
        raise TypeError(f"class_path verweist nicht auf eine Klasse: {class_name}")
    return loaded


def _create_plugin(component_name:str, component_parent_path: str , expected_type: type[T]) -> T:
    """
    Läde Event(Producer, Strategy, etc) von ComponentSpec
    """
    logger.debug(f"Creating plugin {component_name}")

    cls = _load_class(component_parent_path, component_name)
    logger.debug(f"Class {component_name} loaded")

    if not issubclass(cls, expected_type):
        logger.error(f"Expected type {expected_type} not subclass of {expected_type}")
        raise TypeError(
            f"{component_name} muss von {expected_type.__module__}.{expected_type.__name__} erben"
        )
    return cls()

def _create_execution(run_mode) -> T:
    if isinstance(run_mode, RunMode):
        if run_mode == RunMode.NONE: return None
        return _create_plugin(run_mode, EXECUTION_PATH, Execution)
    else:
        logger.error(f"run_mode {run_mode} not supported")
        raise ValueError(f"run_mode {run_mode} not supported")

def _create_strategy(strategy_name) -> T:
    if isinstance(strategy_name, StrategyName):
        if strategy_name == StrategyName.NONE: return None
        return _create_plugin(strategy_name, STRATEGY_PATH, Strategy)
    else:
        logger.error(f"strategy_name {strategy_name} not supported")
        raise ValueError(f"strategy_name {strategy_name} not supported")

def _create_producer(producer_name) -> T:
    if isinstance(producer_name, ProducerName):
        return _create_plugin(producer_name, PRODUCER_PATH, Producer)
    else:
        logger.error(f"producer_name {producer_name} not supported")
        raise ValueError(f"producer_name {producer_name} not supported")

    if not isinstance(producer_name, SourceMode):
        logger.error(f"source_mode {source_mode} not supported")
        raise ValueError(f"source_mode {source_mode} not supported")

    if source_mode == SourceMode.LIVE:
        if (producer_type == ProducerDataType.WEBSOCKET):
            return _create_plugin("WebsocketApp", PRODUCER_PATH, Producer)
        elif (producer_type == ProducerDataType.DATA_API):
            return _create_plugin("DataAPIApp", PRODUCER_PATH, Producer)
    elif source_mode == SourceMode.BACKTEST:
        logger.error(f"source_mode {source_mode} not yet implemented")
        raise ValueError(f"source_mode {source_mode} not yet implemented")

    logger.error(f"producer_type:{producer_type} or source_mode:{source_mode} failed supported")
    raise ValueError(f"producer_type:{producer_type} or source_mode:{source_mode} failed supported")

def get_components(config: BotConfig) -> Dict:
    strategy = _create_strategy(config.strategy_name)
    producer = _create_producer(config.producer_name)
    execution = _create_execution(config.run_mode)

    return {"strategy": strategy, "producer": producer, "execution": execution,}
