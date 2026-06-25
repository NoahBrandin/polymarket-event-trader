from pathlib import Path

import pytest

from pm_bot.configuration.runtime_config import (
    ConfigurationError,
    load_runtime_config,
)
from pm_bot.locel_types import (
    ExecutionMode,
    LogMode,
    ProducerName,
    StrategyName,
)


def _write_config(tmp_path: Path, content: str) -> Path:
    config_path = tmp_path / "bot.toml"
    config_path.write_text(content, encoding="utf-8")
    return config_path


def test_load_runtime_config_and_build_setup_kwargs(tmp_path: Path) -> None:
    config_path = _write_config(
        tmp_path,
        """
[bot]
name = "test-bot"
log_mode = "DEBUG"
producer = "WEBSOCKET"
strategy = "DEFAULT_RANDOM_STRATEGY"
execution_mode = "PAPER"

[engine]
queue_size = 123
print_events = false
testing = true
""",
    )

    config = load_runtime_config(config_path)

    assert config.as_setup_kwargs() == {
        "log_mode": LogMode.DEBUG,
        "bot_name": "test-bot",
        "execution_mode": ExecutionMode.PAPER,
        "producer": ProducerName.WEBSOCKET,
        "strategy": StrategyName.DEFAULT_RANDOM_STRATEGY,
        "engine_queue_size": 123,
        "engine_print_events": False,
        "testing_setup": True,
    }


def test_enum_values_are_accepted_in_addition_to_names(tmp_path: Path) -> None:
    config_path = _write_config(
        tmp_path,
        f"""
[bot]
name = "test-bot"
log_mode = "{LogMode.INFO.value}"
producer = "{ProducerName.WEBSOCKET.value}"
strategy = "{StrategyName.DEFAULT_RANDOM_STRATEGY.value}"
execution_mode = "{ExecutionMode.PAPER.value}"

[engine]
queue_size = 5000
print_events = true
""",
    )

    config = load_runtime_config(config_path)

    assert config.bot.log_mode is LogMode.INFO
    assert config.bot.producer is ProducerName.WEBSOCKET
    assert config.bot.strategy is StrategyName.DEFAULT_RANDOM_STRATEGY
    assert config.bot.execution_mode is ExecutionMode.PAPER
    assert config.engine.testing is False


@pytest.mark.parametrize("queue_size", [0, -1, True, "5000"])
def test_invalid_queue_size_is_rejected(
    tmp_path: Path,
    queue_size: object,
) -> None:
    rendered_value = (
        str(queue_size).lower()
        if isinstance(queue_size, bool)
        else f'"{queue_size}"'
        if isinstance(queue_size, str)
        else str(queue_size)
    )

    config_path = _write_config(
        tmp_path,
        f"""
[bot]
name = "test-bot"
log_mode = "INFO"
producer = "WEBSOCKET"
strategy = "DEFAULT_RANDOM_STRATEGY"
execution_mode = "PAPER"

[engine]
queue_size = {rendered_value}
print_events = true
""",
    )

    with pytest.raises(ConfigurationError, match="positive Ganzzahl"):
        load_runtime_config(config_path)


def test_unknown_enum_value_is_rejected(tmp_path: Path) -> None:
    config_path = _write_config(
        tmp_path,
        """
[bot]
name = "test-bot"
log_mode = "INVALID"
producer = "WEBSOCKET"
strategy = "DEFAULT_RANDOM_STRATEGY"
execution_mode = "PAPER"

[engine]
queue_size = 5000
print_events = true
""",
    )

    with pytest.raises(ConfigurationError, match="bot.log_mode"):
        load_runtime_config(config_path)
