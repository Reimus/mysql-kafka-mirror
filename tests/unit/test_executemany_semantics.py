from __future__ import annotations

from mysql_interceptor.config.settings import Settings
from mysql_interceptor.dbapi.wrappers import ConnectionWrapper
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
        return


class _Cur:
    def __init__(self) -> None:
        self.rowcount = 0
        self._result = type("R", (), {"message": "Rows matched: 0  Changed: 0  Warnings: 0"})()

    def execute(self, operation: str, params=None):
        self.rowcount = 1
        return 1

    def executemany(self, operation: str, seq_of_params):
        n = 0
        for _ in seq_of_params:
            n += 1
        self.rowcount = n
        return n

    def callproc(self, procname: str, params=None):
        self.rowcount = 0
        return 0

    def close(self):
        return


class _Conn:
    user = "root"
    host = "localhost"
    port = 3306
    client_flag = 0
    server_status = 2

    def cursor(self, *a, **k):
        return _Cur()

    def commit(self):
        return

    def rollback(self):
        return

    def close(self):
        return

    def get_server_info(self):
        return "8.0.x"


def test_executemany_emits_one_record_per_param_and_only_last_has_duration_updatecount_serverinfo() -> None:
    pub = _MemPublisher()
    s = Settings(kafka_bootstrap_servers=None, buffer_until_commit=False, capture_all=True)
    conn = ConnectionWrapper(conn=_Conn(), publisher=pub, settings=s, driver_name="pymysql", database="test")
    cur = conn.cursor()

    cur.executemany("INSERT INTO t (a) VALUES (%s)", [(1,), (2,), (3,)])

    assert len(pub.events) == 3
    assert pub.events[0].durationNs is None and pub.events[0].updateCount is None and pub.events[0].serverInfo is None
    assert pub.events[1].durationNs is None and pub.events[1].updateCount is None and pub.events[1].serverInfo is None
    assert isinstance(pub.events[2].durationNs, int)
    assert pub.events[2].updateCount == 3
    assert isinstance(pub.events[2].serverInfo, str)
