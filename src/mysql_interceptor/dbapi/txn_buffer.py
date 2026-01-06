from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from ..events.models import SqlLogMessage


@dataclass
class TransactionBuffer:
    events: List[SqlLogMessage] = field(default_factory=list)

    def add(self, event: SqlLogMessage) -> None:
        self.events.append(event)

    def clear(self) -> None:
        self.events.clear()

    def drain(self) -> List[SqlLogMessage]:
        ev = list(self.events)
        self.events.clear()
        return ev
