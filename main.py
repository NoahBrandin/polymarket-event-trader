from __future__ import annotations

import argparse
import os
from collections.abc import Sequence
from pathlib import Path

from pm_bot.configuration.bot_setup import setup
from pm_bot.configuration.runtime_config import load_runtime_config

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "bot.toml"
CONFIG_ENVIRONMENT_VARIABLE = "POLYMARKET_BOT_CONFIG"


def _parse_arguments(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Startet den Polymarket Event Trader.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help=(
            "Pfad zur TOML-Konfiguration. Alternativ kann "
            f"{CONFIG_ENVIRONMENT_VARIABLE} gesetzt werden. "
            f"Standard: {DEFAULT_CONFIG_PATH}"
        ),
    )
    return parser.parse_args(argv)


def _resolve_config_path(cli_path: Path | None) -> Path:
    if cli_path is not None:
        return cli_path.expanduser().resolve()

    environment_path = os.getenv(CONFIG_ENVIRONMENT_VARIABLE)
    if environment_path:
        return Path(environment_path).expanduser().resolve()

    return DEFAULT_CONFIG_PATH


def main(argv: Sequence[str] | None = None) -> None:
    arguments = _parse_arguments(argv)
    config_path = _resolve_config_path(arguments.config)
    runtime_config = load_runtime_config(config_path)

    setup(**runtime_config.as_setup_kwargs())


if __name__ == "__main__":
    main()
