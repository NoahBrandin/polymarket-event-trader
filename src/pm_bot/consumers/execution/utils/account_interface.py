from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from decimal import Decimal

from src.pm_bot.consumers.execution.utils.config import PaperExecutionConfig
from src.pm_bot.locel_types import NonNegativeDecimal


@dataclass
class AccountInterface(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def get_cash(self) -> Decimal:
        pass
    @abstractmethod
    def get_open_positions(self) -> dict[str, Position]:
        pass
    @abstractmethod
    def get_close_positions(self) -> dict[str, Position]:
        pass
    @abstractmethod
    def get_trades_volume(self) -> Decimal:
        pass

    @abstractmethod
    def set_cash(self, cash: Decimal):
        pass
    @abstractmethod
    def set_open_positions(self, open_positions: dict[str, Position]):
        pass
    @abstractmethod
    def set_close_positions(self, close_positions: dict[str, Position]):
        pass
    @abstractmethod
    def set_trades_volume(self, trades_volume: Decimal):
        pass

@dataclass
class Position:
    size: Decimal
    price: Decimal
    return_: Decimal = Decimal("0.00")

    def __str__(self):
        return f"Position: size={self.size} buyin_price={self.price} return={self.return_}"

@dataclass
class PaperAccount:
    cash: Decimal = NonNegativeDecimal("10000")
    open_positions: dict[str, Position] = field(default_factory=dict)
    close_positions: dict[str, Position] = field(default_factory=dict)
    trades_volume: Decimal = NonNegativeDecimal("0")

class PaperAccountInterface(AccountInterface):
    def __init__(self, config: PaperExecutionConfig):
        super().__init__()

        self.account: PaperAccount = (PaperAccount(cash=config.initial_cash))
        self.account.open_positions = {}
        self.account.close_positions = {}

    def get_cash(self) -> Decimal:
        return self.account.cash
    def get_open_positions(self) -> dict[str, Position]:
        return self.account.open_positions
    def get_close_positions(self) -> dict[str, Position]:
        return self.account.close_positions
    def get_trades_volume(self) -> Decimal:
        return self.account.trades_volume

    def set_cash(self, cash: Decimal):
        self.account.cash = cash
    def set_open_positions(self, open_positions: dict[str, Position]):
        self.account.open_positions = open_positions
    def set_close_positions(self, close_positions: dict[str, Position]):
        self.account.close_positions = close_positions
    def set_trades_volume(self, trades_volume: Decimal):
        self.account.trades_volume = trades_volume

