from __future__ import annotations

import os
import uuid

import pytest

from mysql_interceptor.monkeypatch import patch_sqlalchemy
from tests.integration._helpers import consume_json_messages, wait_for_port


@pytest.mark.integration
def test_sqlalchemy_end_to_end_mysql_kafka_docker() -> None:
    wait_for_port("localhost", 3306, timeout_s=120.0)
    wait_for_port("localhost", 9092, timeout_s=120.0)

    topic = f"MYSQL_EVENTS_TEST_{uuid.uuid4().hex[:8]}"
    db = f"testdb_{uuid.uuid4().hex[:8]}"

    os.environ["INTERCEPTOR_KAFKA_BOOTSTRAP_SERVERS"] = "localhost:9092"
    os.environ["INTERCEPTOR_KAFKA_TOPIC"] = topic
    os.environ["INTERCEPTOR_CAPTURE_ALL"] = "true"
    os.environ["INTERCEPTOR_BUFFER_UNTIL_COMMIT"] = "false"
    os.environ["DEBUGQUERYINTERCEPTOR_DEBUG"] = "it-sqlalchemy"

    unpatch = patch_sqlalchemy()
    try:
        from sqlalchemy import create_engine, text

        engine = create_engine("mysql+pymysql://root:root@127.0.0.1:3306/mysql")
        with engine.begin() as conn:
            conn.execute(text(f"CREATE DATABASE {db}"))
            conn.execute(text(f"USE {db}"))
            conn.execute(text("CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(32))"))

            conn.execute(text("INSERT INTO t (name) VALUES (:n)"), {"n": "a"})
            conn.execute(text("INSERT INTO t (name) VALUES (:n)"), [{"n": "b"}, {"n": "c"}])

            conn.execute(text("SELECT * FROM t"))
            conn.execute(text("UPDATE t SET name=:n WHERE name=:w"), {"n": "z", "w": "a"})
            conn.execute(text("DELETE FROM t WHERE name=:n"), {"n": "b"})

            conn.execute(text("DROP TABLE t"))
            conn.execute(text(f"DROP DATABASE {db}"))
    finally:
        unpatch()

    msgs = consume_json_messages(topic=topic, timeout_s=30.0, max_messages=8000)
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
