import logging
import logging.handlers
from enum import Enum
from pm_bot.locel_types import LogMode

_logger_instance = None

class LogProfils(Enum):
    DEBUG = {
        "cli_level": logging.WARNING,     # Im Dev-Modus wollen wir ALLES im Terminal sehen
        "file_level": logging.DEBUG,
        "msg": "Entwicklungs-Modus: Volles Logging im Terminal und in Datei."
    }
    INFO = {
        "cli_level": logging.WARNING,   # In Prod nur Warnungen/Fehler im Terminal
        "file_level": logging.INFO,     # Aber detaillierte Infos in der Datei
        "msg": "Produktions-Modus: Terminal geschont, Datei loggt ab INFO."
    }
    ERROR = {
        "cli_level": logging.CRITICAL,  # Fast keine Ausgabe im Terminal
        "file_level": logging.WARNING,   # Nur wichtige Fehler in der Datei
        "msg": "Silent-Modus: Minimale Log-Einträge."
    }

    # Helper properties to make accessing keys cleaner
    @property
    def cli_level(self) -> int | str:
        return self.value["cli_level"]

    @property
    def file_level(self) -> int | str:
        return self.value["file_level"]

    @property
    def msg(self) -> int | str:
        return self.value["msg"]

def setup_global_logger(mode: LogMode):

    profile = LogProfils[mode.value]

    log_file = "trading_bot.log"

    # Formatierer
    file_formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] [%(filename)s:%(lineno)d] -> %(message)s')
    cli_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S')

    # Handler mit profil-spezifischen Levels
    file_handler = logging.FileHandler("trading_bot.log", mode="w", encoding="utf-8",)
    file_handler.setFormatter(file_formatter)
    file_handler.setLevel(logging.DEBUG)  # Schreibt alles ab DEBUG in die Datei

    # 5. Handler für die Konsolen-Ausgabe erstellen (damit du im Terminal siehst, was passiert)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(cli_formatter)
    console_handler.setLevel(profile.cli_level)  # Zeigt wichtige Infos auf der Konsole


    global _logger_instance
    # 6. Den Root-Logger (globalen Logger) konfigurieren
    root_logger = logging.getLogger("PolymarketBot")
    root_logger.setLevel(logging.DEBUG)  # Erlaubt das Verarbeiten aller Log-Stufen

    # Falls der Logger bereits Handler hat (z.B. durch Re-Initialisierung), löschen wir sie
    if root_logger.handlers:
        root_logger.handlers.clear()

    # Handler hinzufügen
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    _logger_instance = root_logger
    _logger_instance.info(f"► Globaler Logger initialisiert. Datei: {log_file}")

def get_logger():
    """Gibt den Logger zurück. Falls er noch nicht initialisiert wurde, mit Default-Werten."""
    global _logger_instance
    if _logger_instance is None:
        setup_global_logger(LogMode.DEBUG)
    return _logger_instance