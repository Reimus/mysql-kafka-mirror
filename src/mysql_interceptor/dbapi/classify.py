from __future__ import annotations

import re
from typing import Optional

_WRITE = {"insert", "update", "delete", "replace"}
_DDL = {"create", "alter", "drop", "truncate", "rename"}
_CALL = {"call"}

_LEADING_COMMENTS = re.compile(r"^\s*(?:--[^\n]*\n|#[^\n]*\n|/\*.*?\*/\s*)*", re.DOTALL)
_FIRST_WORD = re.compile(r"^\s*([a-zA-Z]+)\b")


def _strip_leading_comments(sql: str) -> str:
    return _LEADING_COMMENTS.sub("", sql or "").strip()


def statement_kind(sql: str) -> Optional[str]:
    if not sql:
        return None
    s = _strip_leading_comments(sql)
    m = _FIRST_WORD.match(s)
    return m.group(1).lower() if m else None


def is_write(sql: str) -> bool:
    return statement_kind(sql) in _WRITE


def is_ddl(sql: str) -> bool:
    return statement_kind(sql) in _DDL


def is_call(sql: str) -> bool:
    return statement_kind(sql) in _CALL


def is_use(sql: str) -> bool:
    return statement_kind(sql) == "use"


def parse_use_db(sql: str) -> Optional[str]:
    try:
        s = (sql or "").strip()
        if s.startswith("/*"):
            idx = s.find("*/")
            if idx != -1:
                s = s[idx + 2 :].lstrip()
        m = re.match(r"(?is)^use\s+(`([^`]+)`|([a-zA-Z0-9_]+))\s*;?\s*$", s)
        if not m:
            return None
        return m.group(2) or m.group(3)
    except Exception:
        return None
