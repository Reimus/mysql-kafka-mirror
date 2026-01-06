from __future__ import annotations

from typing import List, Protocol, runtime_checkable

from ..events.models import SqlLogMessage


@runtime_checkable
class Publisher(Protocol):
    def publish(self, event: SqlLogMessage) -> None: ...
    def publish_batch(self, events: List[SqlLogMessage]) -> None: ...
    def flush(self) -> None: ...
    def close(self) -> None: ...


class StdoutPublisher:
    def __init__(self, *, pretty: bool = False) -> None:
        self._pretty = pretty

    def publish(self, event: SqlLogMessage) -> None:
        import json
        payload = event.to_dict()
        if self._pretty:
            print(json.dumps(payload, indent=2, ensure_ascii=False, default=str))
        else:
            print(json.dumps(payload, ensure_ascii=False, default=str, separators=(",", ":")))

    def publish_batch(self, events: List[SqlLogMessage]) -> None:
        for e in events:
            self.publish(e)

    def flush(self) -> None:
        return None

    def close(self) -> None:
        return None
