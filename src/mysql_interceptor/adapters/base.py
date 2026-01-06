from __future__ import annotations

from typing import Any, Optional, Protocol, runtime_checkable

from ..dbapi.wrappers import DBAPIConnection


@runtime_checkable
class DriverAdapter(Protocol):
    name: str

    def connect(self, *args: Any, **kwargs: Any) -> DBAPIConnection: ...
    def database_name(self, conn: DBAPIConnection, connect_kwargs: dict[str, Any]) -> Optional[str]: ...
