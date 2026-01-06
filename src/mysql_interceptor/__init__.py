"""mysql-interceptor."""

from .connect import connect

__all__ = ["connect"]

from .monkeypatch import patch_pymysql, patch_sqlalchemy
