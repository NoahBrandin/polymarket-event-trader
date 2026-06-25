from dataclasses import dataclass, field

from pm_bot.locel_types import SelectionType


@dataclass(frozen=True)
class SubscriptionSelection:
    type: SelectionType
    ids: list[str] = field(default_factory=list)

    @property
    def is_empty(self) -> bool:
        return not self.ids
