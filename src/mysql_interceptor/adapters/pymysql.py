from __future__ import annotations

from typing import Any, Callable, Optional

from .base import DriverAdapter

_CONNECT_FUNC: Optional[Callable[..., Any]] = None


def set_connect_func(func: Optional[Callable[..., Any]]) -> None:
    """Inject the real (unpatched) driver connect function."""
    global _CONNECT_FUNC
    _CONNECT_FUNC = func


class PyMySQLAdapter(DriverAdapter):
    """Driver adapter for PyMySQL.

    This adapter supports dependency injection of the underlying (unpatched)
    connect callable to prevent recursion when monkeypatching.
    """

    name = "pymysql"

    def connect(self, *args: Any, **kwargs: Any) -> Any:
        import pymysql  # type: ignore

        fn = _CONNECT_FUNC or pymysql.connect
        return fn(*args, **kwargs)

    def database_name(self, conn: Any, connect_kwargs: dict[str, Any]) -> Optional[str]:
        for k in ("db", "database"):
            v = connect_kwargs.get(k)
            if v:
                return str(v)
        for attr in ("db", "database", "_db", "_database"):
            v = getattr(conn, attr, None)
            if v:
                try:
                    return str(v)
                except Exception:
                    return None
        return None
