from __future__ import annotations

import threading


class PoolCounter:
    """Process-wide count of open DBAPI connections ("sessions").

    This is intentionally "best effort":
    - increments on physical DBAPI connect (SQLAlchemy pool connect / direct connect wrapper)
    - decrements on physical close
    - never raises
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._count = 0

    def inc(self) -> int:
        with self._lock:
            self._count += 1
            return self._count

    def dec(self) -> int:
        with self._lock:
            if self._count > 0:
                self._count -= 1
            return self._count

    def get(self) -> int:
        with self._lock:
            return self._count


GLOBAL_POOL_COUNTER = PoolCounter()
