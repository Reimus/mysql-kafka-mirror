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

    topic = "MYSQL_EVENTS"
    db = f"testdb_{uuid.uuid4().hex[:8]}"
    db2 = f"testdb2_{uuid.uuid4().hex[:8]}"

    os.environ["INTERCEPTOR_KAFKA_BOOTSTRAP_SERVERS"] = "localhost:9092"
    os.environ["INTERCEPTOR_KAFKA_TOPIC"] = topic
    os.environ["INTERCEPTOR_CAPTURE_ALL"] = "true"
    os.environ["INTERCEPTOR_BUFFER_UNTIL_COMMIT"] = "false"
    os.environ["INTERCEPTOR_CAPTURE_CALLPROC"] = "true"
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

        # BAD SQL: syntax error
        try:
            cur.execute("SELEC 1")
        except Exception:
            pass

        # BAD SQL: valid syntax, missing column
        try:
            cur.execute("SELECT does_not_exist FROM t")
        except Exception:
            pass

        cur.execute("UPDATE t SET name=%s WHERE name=%s", ("z", "a"))
        cur.execute("DELETE FROM t WHERE name=%s", ("b",))


        # Create a second database and switch via USE (session db tracking)
        cur.execute(f"CREATE DATABASE {db2}")
        cur.execute(f"USE {db2}")
        cur.execute("CREATE TABLE t_sw (id INT PRIMARY KEY AUTO_INCREMENT, v INT)")
        cur.execute("INSERT INTO t_sw (v) VALUES (42)")
        cur.execute("DROP TABLE t_sw")

        # Switch back to original db
        cur.execute(f"USE {db}")

        # Create a view
        cur.execute("CREATE VIEW v_t AS SELECT id, name FROM t")
        cur.execute("SELECT * FROM v_t")
        cur.execute("DROP VIEW v_t")

        # Stored procedure (simple)
        cur.execute("DROP PROCEDURE IF EXISTS p_add")
        cur.execute("CREATE PROCEDURE p_add(IN a INT, IN b INT) BEGIN SELECT a + b AS s; END")
        cur.callproc("p_add", (1, 2))
        cur.execute("DROP PROCEDURE p_add")

        cur.execute("DROP TABLE t")

        # Second connection with database set at connect-time (dbName should be populated)
        conn2 = pymysql.connect(host="127.0.0.1", user="root", password="root", port=3306, db=db, autocommit=True)
        cur2 = conn2.cursor()
        cur2.execute("SELECT 9001")
        cur2.execute("CREATE TABLE t2 (id INT PRIMARY KEY AUTO_INCREMENT, v INT)")
        # Cross-db session switch: dbName should remain `db`, stmtDbName should become `db2`
        cur2.execute(f"USE {db2}")
        cur2.execute("CREATE TABLE t_cross (id INT PRIMARY KEY AUTO_INCREMENT, v INT)")
        cur2.execute("INSERT INTO t_cross (v) VALUES (%s)", (9,))
        cur2.execute("SELECT * FROM t_cross")
        cur2.execute("DROP TABLE t_cross")
        cur2.execute(f"USE {db}")

        cur2.execute("INSERT INTO t2 (v) VALUES (%s)", (1,))
        cur2.executemany("INSERT INTO t2 (v) VALUES (%s)", [(2,), (3,)])
        cur2.execute("SELECT * FROM t2")
        cur2.execute("DROP TABLE t2")
        cur2.close()
        conn2.close()

        # totalPoolCount should decrement after closing conn2 (conn1 still alive)
        cur.execute(f"USE {db}")
        cur.execute("SELECT 4242")

        cur.execute(f"DROP DATABASE {db2}")
        cur.execute(f"DROP DATABASE {db}")

        cur.close()
        conn.close()
    finally:
        unpatch()

    msgs = consume_json_messages(topic=topic, timeout_s=30.0, max_messages=200000)
    msgs = [m for m in msgs if (
        (m.get("stmtDbName") in {db, db2}) or
        (isinstance(m.get("sql"), str) and any(m["sql"].lower().strip().startswith(x) for x in [
            f"create database {db}".lower(), f"use {db}".lower(), f"drop database {db}".lower(),
            f"create database {db2}".lower(), f"use {db2}".lower(), f"drop database {db2}".lower(),
        ]))
    )]
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

    dml = [m for m in msgs if m.get("stmtDbName") == db and isinstance(m.get("sql"), str) and m["sql"].strip().lower().startswith(("insert", "update", "delete"))]
    assert any(isinstance(m.get("serverInfo"), str) and m["serverInfo"] for m in dml)
    # For statements executed on conn2 (connected with db=db), dbName should be populated
    t2_msgs = [m for m in msgs if isinstance(m.get("sql"), str) and " t2 " in (" " + m["sql"].lower() + " ")]
    assert any(m.get("dbName") == db for m in t2_msgs)
    t_cross_msgs = [m for m in msgs if isinstance(m.get("sql"), str) and " t_cross " in (" " + m["sql"].lower() + " ")]
    assert any(m.get("dbName") == db and m.get("stmtDbName") == db2 for m in t_cross_msgs)
    err_sqls = [m for m in msgs if m.get("errorMessage") and isinstance(m.get("sql"), str)]
    assert any("selec 1" in m["sql"].lower() for m in err_sqls)
    assert any("does_not_exist" in m["sql"].lower() for m in err_sqls)


    pool_counts = [m.get("totalPoolCount") for m in msgs if isinstance(m.get("totalPoolCount"), int)]
    assert pool_counts, "expected integer totalPoolCount in at least one message"
    assert min(pool_counts) >= 1

    hi = [m for m in msgs if isinstance(m.get("sql"), str) and m["sql"].lower().strip().startswith("select 9001")]
    assert any(isinstance(m.get("totalPoolCount"), int) and m["totalPoolCount"] >= 2 for m in hi)

    lo = [m for m in msgs if isinstance(m.get("sql"), str) and m["sql"].lower().strip().startswith("select 4242")]
    assert any(m.get("totalPoolCount") == 1 for m in lo)

