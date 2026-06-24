from dataclasses import dataclass

from src.pm_bot.locel_types import SelectionType


@dataclass(frozen=True)
class SubscriptionSelection:
    type: SelectionType
    ids: list[str] = list

    @property
    def is_empty(self) -> bool:
        return not self.ids
