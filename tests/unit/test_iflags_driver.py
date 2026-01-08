from __future__ import annotations
from typing import Any
from mysql_interceptor.config.settings import Settings
from mysql_interceptor.dbapi.wrappers import ConnectionWrapper
from mysql_interceptor.dbapi.constants import IVER8, PY_DRIVER_PYMYSQL, PY_DRIVER_SQLALCHEMY
from mysql_interceptor.events.models import SqlLogMessage
from mysql_interceptor.kafka.publisher import Publisher

class _MemPublisher(Publisher):
    def __init__(self) -> None:
        self.events: list[SqlLogMessage] = []
    def publish(self, event: SqlLogMessage) -> None:
        self.events.append(event)
    def publish_batch(self, events: list[SqlLogMessage]) -> None:
        self.events.extend(events)
    def flush(self) -> None:
        pass
    def close(self) -> None:
        pass

class _Cur:
    def execute(self, sql, params=None): return 1
    def fetchone(self): return (1,)
    def close(self): pass

class _Conn:
    def cursor(self, *a, **k): return _Cur()
    def commit(self): pass
    def rollback(self): pass
    def close(self): pass
    def get_server_info(self): return "8.0.0"

def test_iflags_driver_bits() -> None:
    pub = _MemPublisher()
    settings = Settings(buffer_until_commit=False)
    
    # Test PyMySQL wrapper
    conn = ConnectionWrapper(conn=_Conn(), publisher=pub, settings=settings, driver_name="pymysql", database="test")
    cur = conn.cursor()
    cur.execute("SELECT 1")
    
    assert len(pub.events) == 1
    # iFlags should contain IVER8 and PY_DRIVER_PYMYSQL
    # and possibly others if they were successfully computed, but at least these two.
    assert (pub.events[0].iFlags & IVER8) != 0
    assert (pub.events[0].iFlags & PY_DRIVER_PYMYSQL) != 0
    assert (pub.events[0].iFlags & PY_DRIVER_SQLALCHEMY) == 0

def test_sqlalchemy_iflags_driver_bit() -> None:
    # We can't easily unit test sqlalchemy_interceptor without sqlalchemy installed
    # but we can try to mock it enough to call _build_state
    from mysql_interceptor.sqlalchemy_interceptor import _build_state
    
    pub = _MemPublisher()
    settings = Settings()
    
    class MockUrl:
        database = "test"
        username = "user"
        host = "localhost"
        port = 3306

    state = _build_state(dbapi_conn=_Conn(), engine_url=MockUrl(), publisher=pub, settings=settings)
    assert (state.base_iflags & IVER8) != 0
    assert (state.base_iflags & PY_DRIVER_SQLALCHEMY) != 0
    assert (state.base_iflags & PY_DRIVER_PYMYSQL) == 0
