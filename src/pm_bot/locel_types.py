import re
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import TypeVar

E = TypeVar("E", bound=StrEnum)

def camel_to_snake(text):
    # Fügt vor jedem Großbuchstaben einen Unterstrich ein,
    # es sei denn, er steht ganz am Anfang.
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", text)
    # Behandelt Fälle mit aufeinanderfolgenden Großbuchstaben (z.B. XMLParser -> xml_parser)
    s2 = re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1)
    # Macht am Ende alles kleingeschrieben
    return s2.lower()

def get_unix_time_millis_to_datetime(tms: float) -> datetime:
    return datetime.fromtimestamp(tms/1000.0, tz=UTC)

def get_str_enum_from_value(value: str, enum_class: type[E]) -> E:
    """Sucht ein Enum-Mitglied basierend auf seinem zugewiesenen Wert."""
    return enum_class(value)

# 1. NonNegativeDecimal (Muss >= 0 sein)
class NonNegativeDecimal(Decimal):
    def __new__(cls, value=0):
        decimal_value = Decimal(value)
        if decimal_value < 0:
            raise ValueError(
                f"NonNegativeDecimal darf nicht negativ sein (gegeben: {value})"
            )
        return super().__new__(cls, decimal_value)


# 2. Probability (Muss zwischen 0.0 und 1.0 liegen)
class Probability(Decimal):
    def __new__(cls, value=0):
        decimal_value = Decimal(value)
        if decimal_value < 0 or decimal_value > 1:
            raise ValueError(
                f"Probability muss zwischen 0 und 1 liegen (gegeben: {value})"
            )
        return super().__new__(cls, decimal_value)

class ExecutionMode(StrEnum):
    NONE = "None" #used for debugging and testing
    LIVE = "LiveExecution"
    PAPER = "PaperExecution"

class LogMode(StrEnum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    ERROR = "ERROR"

class StrategyName(StrEnum):
    NONE = "None" #used for debugging and testing
    DEFAULT_RANDOM_STRATEGY = "DefaultRandomStrategy"

class StrategyType(StrEnum):
    UPDATE_DRIVEN = "update_driver"
    TICK_DRIVEN = "tick_driver"

class ProducerName(StrEnum):
    WEBSOCKET = "WebsocketFeed"

class ProducerDataType(StrEnum): #Gibt den DatenTyp an den ein Producer ausgibt
    DEFAULT = "default"
    WEBSOCKET = "websocket"
    DATA_API = "data_api"

class SelectionType(StrEnum):
    MARKT_EVENT = "market_event" # -> asseat als ids
    API_DATA_EVENT = "api_data_event"


# --- Trading ---

class TradingSide(StrEnum):
    """Beschreibt die brokerunabhängige Kauf- oder Verkaufsrichtung einer Order."""
    BUY = "BUY"
    SELL = "SELL"


class TimeInForce(StrEnum):
    GTC = "GTC"
    GTD = "GTD"
    FOK = "FOK"
    FAK = "FAK"


class OrderStatus(StrEnum):
    LIVE = "LIVE"
    FILLED = "FILLED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    CANCELLED = "CANCELLED"
    EXPIRED = "EXPIRED"
    REJECTED = "REJECTED"
    FAILED = "FAILED"

    @property
    def terminal(self) -> bool:
        return self in {
            OrderStatus.FILLED,
            OrderStatus.CANCELLED,
            OrderStatus.EXPIRED,
            OrderStatus.REJECTED,
            OrderStatus.FAILED,
        }