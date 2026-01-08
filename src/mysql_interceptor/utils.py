from __future__ import annotations

import socket
from typing import Any, Optional, Tuple

from .dbapi.constants import PY_ERROR_DEFAULT_TZ


def hostname() -> str:
    try:
        return socket.gethostname()
    except Exception:
        return "unknown-host"


def _safe_str(v: Any) -> Optional[str]:
    try:
        return None if v is None else str(v)
    except Exception:
        return None


def _safe_int(v: Any) -> Optional[int]:
    try:
        return None if v is None else int(v)
    except Exception:
        return None


def _default_tz() -> Tuple[Optional[str], int]:
    try:
        import datetime

        return _safe_str(datetime.datetime.now().astimezone().tzinfo), 0
    except Exception:
        return None, PY_ERROR_DEFAULT_TZ


def _isolation_to_level(value: Optional[str]) -> Optional[int]:
    if not value:
        return None
    v = value.strip().upper().replace("-", " ").replace("_", " ")
    mapping = {
        "READ UNCOMMITTED": 1,
        "READ COMMITTED": 2,
        "REPEATABLE READ": 4,
        "SERIALIZABLE": 8,
    }
    return mapping.get(v)


def extract_server_info_best_effort(cursor: Any, conn: Any) -> Tuple[Optional[str], bool]:
    """Return (serverInfo, had_exception_during_extraction). Never raises."""
    had_error = False

    try:
        res = getattr(cursor, "_result", None)
        msg = getattr(res, "message", None) if res is not None else None
        s = _safe_str(msg)
        if s:
            return s, had_error
    except Exception:
        had_error = True

    for obj in (cursor, conn):
        for attr in ("info", "message", "messages"):
            try:
                v = getattr(obj, attr, None)
                if v is None:
                    continue
                if callable(v):
                    v = v()
                s = _safe_str(v)
                if s:
                    return s, had_error
            except Exception:
                had_error = True

    return None, had_error
