import importlib
from typing import Any, TypeVar

from pm_bot.configuration.config import BotConfig
from pm_bot.configuration.logger_config import get_logger
from pm_bot.consumers.execution.bass import Execution
from pm_bot.consumers.strategy.base import Strategy
from pm_bot.locel_types import ExecutionMode, ProducerName, StrategyName, camel_to_snake
from pm_bot.producer.base import Producer

logger = get_logger()

T = TypeVar("T")

PRODUCER_PATH = "pm_bot/producer"
STRATEGY_PATH = "pm_bot/consumers/strategy"
EXECUTION_PATH = "pm_bot/consumers/execution"

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

def _create_execution(execution_mode) -> T:
    logger.debug(f"Execution_mode is {execution_mode}, type is {type(execution_mode)}, "
                 f"but isinstance(execution_mode, ExecutionMode)= {isinstance(execution_mode, ExecutionMode)}")
    if isinstance(execution_mode, ExecutionMode):
        if execution_mode == ExecutionMode.NONE: return None
        return _create_plugin(execution_mode, EXECUTION_PATH, Execution)
    else:
        logger.error(f"run_mode {execution_mode} not supported")
        raise ValueError(f"run_mode {execution_mode} not supported")

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


def get_components(config: BotConfig) -> dict:
    execution = _create_execution(config.execution_mode)
    strategy = _create_strategy(config.strategy_name)
    producer = _create_producer(config.producer_name)

    return {"strategy": strategy, "producer": producer, "execution": execution,}
