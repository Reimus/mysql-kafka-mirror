from __future__ import annotations

import os
import uuid

import pytest

from mysql_interceptor.monkeypatch import patch_pymysql
from tests.integration._helpers import consume_json_messages, wait_for_port


@pytest.mark.integration
def test_pymysql_end_to_end_mysql_kafka_docker() -> None:
    wait_for_port("localhost", 3306, timeout_s=120.0)
    wait_for_port("localhost", 9092, timeout_s=120.0)

    topic = f"MYSQL_EVENTS_TEST_{uuid.uuid4().hex[:8]}"
    db = f"testdb_{uuid.uuid4().hex[:8]}"

    os.environ["INTERCEPTOR_KAFKA_BOOTSTRAP_SERVERS"] = "localhost:9092"
    os.environ["INTERCEPTOR_KAFKA_TOPIC"] = topic
    os.environ["INTERCEPTOR_CAPTURE_ALL"] = "true"
    os.environ["INTERCEPTOR_BUFFER_UNTIL_COMMIT"] = "false"
    os.environ["DEBUGQUERYINTERCEPTOR_DEBUG"] = "it-pymysql"

    unpatch = patch_pymysql()
    try:
        import pymysql  # type: ignore

        conn = pymysql.connect(host="127.0.0.1", user="root", password="root", port=3306, autocommit=True)
        cur = conn.cursor()

        cur.execute(f"CREATE DATABASE {db}")
        cur.execute(f"USE {db}")
        cur.execute("CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(32))")

        cur.execute("INSERT INTO t (name) VALUES (%s)", ("a",))
        cur.executemany("INSERT INTO t (name) VALUES (%s)", [("b",), ("c",)])

        cur.execute("SELECT * FROM t")
        cur.execute("UPDATE t SET name=%s WHERE name=%s", ("z", "a"))
        cur.execute("DELETE FROM t WHERE name=%s", ("b",))

        cur.execute("DROP TABLE t")
        cur.execute(f"DROP DATABASE {db}")

        cur.close()
        conn.close()
    finally:
        unpatch()

    msgs = consume_json_messages(topic=topic, timeout_s=30.0, max_messages=5000)
    sqls = [m.get("sql") for m in msgs if m.get("sql")]
    joined = "\n".join(sqls).lower()

    for needle in [
        f"create database {db}".lower(),
        f"use {db}".lower(),
        "create table t".lower(),
        "insert into t".lower(),
        "select * from t".lower(),
        "update t set".lower(),
        "delete from t".lower(),
        "drop table t".lower(),
        f"drop database {db}".lower(),
    ]:
        assert needle in joined

    dml = [m for m in msgs if isinstance(m.get("sql"), str) and m["sql"].strip().lower().startswith(("insert", "update", "delete"))]
    assert any(isinstance(m.get("serverInfo"), str) and m["serverInfo"] for m in dml)
