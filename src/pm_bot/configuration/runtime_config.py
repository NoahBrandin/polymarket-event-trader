from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, TypeVar
import tomllib

from pm_bot.locel_types import (
    ExecutionMode,
    LogMode,
    ProducerName,
    StrategyName,
)


EnumType = TypeVar("EnumType", bound=Enum)


class ConfigurationError(ValueError):
    """Die Bot-Konfiguration ist syntaktisch oder semantisch ungültig."""


@dataclass(frozen=True, slots=True)
class BotRuntimeConfig:
    name: str
    log_mode: LogMode
    producer: ProducerName
    strategy: StrategyName
    execution_mode: ExecutionMode


@dataclass(frozen=True, slots=True)
class EngineRuntimeConfig:
    queue_size: int
    print_events: bool
    testing: bool


@dataclass(frozen=True, slots=True)
class RuntimeConfig:
    bot: BotRuntimeConfig
    engine: EngineRuntimeConfig
    source_path: Path

    def as_setup_kwargs(self) -> dict[str, Any]:
        """Liefert exakt die von ``bot_setup.setup`` erwarteten Argumente."""
        return {
            "log_mode": self.bot.log_mode,
            "bot_name": self.bot.name,
            "execution_mode": self.bot.execution_mode,
            "producer": self.bot.producer,
            "strategy": self.bot.strategy,
            "engine_queue_size": self.engine.queue_size,
            "engine_print_events": self.engine.print_events,
            "testing_setup": self.engine.testing,
        }


def load_runtime_config(path: str | Path) -> RuntimeConfig:
    config_path = Path(path).expanduser().resolve()

    if not config_path.is_file():
        raise FileNotFoundError(
            f"Konfigurationsdatei wurde nicht gefunden: {config_path}"
        )

    try:
        with config_path.open("rb") as config_file:
            raw_config = tomllib.load(config_file)
    except tomllib.TOMLDecodeError as error:
        raise ConfigurationError(
            f"Ungültige TOML-Syntax in {config_path}: {error}"
        ) from error

    bot_section = _require_section(raw_config, "bot")
    engine_section = _require_section(raw_config, "engine")

    return RuntimeConfig(
        bot=BotRuntimeConfig(
            name=_require_non_empty_string(bot_section, "name", section="bot"),
            log_mode=_parse_enum(
                LogMode,
                _require_non_empty_string(bot_section, "log_mode", section="bot"),
                field_name="bot.log_mode",
            ),
            producer=_parse_enum(
                ProducerName,
                _require_non_empty_string(bot_section, "producer", section="bot"),
                field_name="bot.producer",
            ),
            strategy=_parse_enum(
                StrategyName,
                _require_non_empty_string(bot_section, "strategy", section="bot"),
                field_name="bot.strategy",
            ),
            execution_mode=_parse_enum(
                ExecutionMode,
                _require_non_empty_string(
                    bot_section,
                    "execution_mode",
                    section="bot",
                ),
                field_name="bot.execution_mode",
            ),
        ),
        engine=EngineRuntimeConfig(
            queue_size=_require_positive_integer(
                engine_section,
                "queue_size",
                section="engine",
            ),
            print_events=_require_boolean(
                engine_section,
                "print_events",
                section="engine",
            ),
            testing=_optional_boolean(
                engine_section,
                "testing",
                default=False,
                section="engine",
            ),
        ),
        source_path=config_path,
    )


def _parse_enum(
    enum_type: type[EnumType],
    raw_value: str,
    *,
    field_name: str,
) -> EnumType:
    """Akzeptiert sowohl Enum-Namen als auch Enum-Werte."""
    normalized = raw_value.strip().casefold()

    for member in enum_type:
        if member.name.casefold() == normalized:
            return member
        if str(member.value).casefold() == normalized:
            return member

    allowed = sorted(
        {member.name for member in enum_type}
        | {str(member.value) for member in enum_type}
    )
    raise ConfigurationError(
        f"Ungültiger Wert für {field_name}: {raw_value!r}. "
        f"Erlaubt sind: {', '.join(allowed)}"
    )


def _require_section(
    config: dict[str, Any],
    section_name: str,
) -> dict[str, Any]:
    section = config.get(section_name)

    if not isinstance(section, dict):
        raise ConfigurationError(
            f"Die TOML-Sektion [{section_name}] fehlt oder ist ungültig."
        )

    return section


def _require_non_empty_string(
    section_data: dict[str, Any],
    key: str,
    *,
    section: str,
) -> str:
    value = section_data.get(key)

    if not isinstance(value, str) or not value.strip():
        raise ConfigurationError(
            f"{section}.{key} muss eine nicht leere Zeichenkette sein."
        )

    return value.strip()


def _require_positive_integer(
    section_data: dict[str, Any],
    key: str,
    *,
    section: str,
) -> int:
    value = section_data.get(key)

    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ConfigurationError(
            f"{section}.{key} muss eine positive Ganzzahl sein."
        )

    return value


def _require_boolean(
    section_data: dict[str, Any],
    key: str,
    *,
    section: str,
) -> bool:
    if key not in section_data:
        raise ConfigurationError(f"{section}.{key} fehlt.")

    value = section_data[key]

    if not isinstance(value, bool):
        raise ConfigurationError(
            f"{section}.{key} muss true oder false sein."
        )

    return value


def _optional_boolean(
    section_data: dict[str, Any],
    key: str,
    *,
    default: bool,
    section: str,
) -> bool:
    if key not in section_data:
        return default

    return _require_boolean(section_data, key, section=section)
