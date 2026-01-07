"""mysql-interceptor.

Public API:
    - connect(...): wrap a DBAPI connection
    - patch_pymysql(...): monkeypatch pymysql.connect
    - patch_sqlalchemy(...): monkeypatch sqlalchemy.create_engine
"""

from .connect import connect
from .monkeypatch import patch_pymysql, patch_sqlalchemy

__all__ = ["connect", "patch_pymysql", "patch_sqlalchemy"]
