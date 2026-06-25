from dataclasses import dataclass, field
from typing import Any

from pm_bot.locel_types import SelectionType


@dataclass(frozen=True)
class SubscriptionSelection:
    type: SelectionType
    selections: list[Any] = field(default_factory=list)

    @property
    def is_empty(self) -> bool:
        return not self.selections
