from __future__ import annotations

from typing import Any, Optional

from ..dbapi.wrappers import DBAPIConnection
from ..errors import DriverAdapterError


class PyMySQLAdapter:
    name = "pymysql"

    def connect(self, *args: Any, **kwargs: Any) -> DBAPIConnection:
        try:
            import pymysql  # type: ignore
        except Exception as e:
            raise DriverAdapterError("PyMySQL is not installed. Install mysql-interceptor[pymysql].") from e
        return pymysql.connect(*args, **kwargs)

    def database_name(self, conn: DBAPIConnection, connect_kwargs: dict[str, Any]) -> Optional[str]:
        for k in ("database", "db"):
            if k in connect_kwargs:
                return str(connect_kwargs[k])
        return getattr(conn, "db", None) or None
