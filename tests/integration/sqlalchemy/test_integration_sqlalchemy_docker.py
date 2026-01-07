from __future__ import annotations
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column
from sqlalchemy import String



import os
import uuid

import pytest

from mysql_interceptor.monkeypatch import patch_sqlalchemy
from tests.integration._helpers import consume_json_messages, wait_for_port


@pytest.mark.integration
def test_sqlalchemy_end_to_end_mysql_kafka_docker() -> None:
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

            # BAD SQL: syntax error
            try:
                conn.execute(text("SELEC 1"))
            except Exception:
                pass

            # BAD SQL: valid syntax, missing column
            try:
                conn.execute(text("SELECT does_not_exist FROM t"))
            except Exception:
                pass

            conn.execute(text("UPDATE t SET name=:n WHERE name=:w"), {"n": "z", "w": "a"})
            conn.execute(text("DELETE FROM t WHERE name=:n"), {"n": "b"})


            # Second database + USE switching
            conn.execute(text(f"CREATE DATABASE {db2}"))
            conn.execute(text(f"USE {db2}"))
            conn.execute(text("CREATE TABLE t_sw (id INT PRIMARY KEY AUTO_INCREMENT, v INT)"))
            conn.execute(text("INSERT INTO t_sw (v) VALUES (42)"))
            conn.execute(text("DROP TABLE t_sw"))
            conn.execute(text(f"USE {db}"))

            # View
            conn.execute(text("CREATE VIEW v_t AS SELECT id, name FROM t"))
            conn.execute(text("SELECT * FROM v_t"))
            conn.execute(text("DROP VIEW v_t"))

            # Stored procedure + call
            conn.execute(text("DROP PROCEDURE IF EXISTS p_add"))
            conn.execute(text("CREATE PROCEDURE p_add(IN a INT, IN b INT) BEGIN SELECT a + b AS s; END"))
            conn.execute(text("CALL p_add(1, 2)"))
            conn.execute(text("DROP PROCEDURE p_add"))

            conn.execute(text("DROP TABLE t"))

        # Second engine with database set at connect-time (dbName should be populated)
        engine2 = create_engine(f"mysql+pymysql://root:root@127.0.0.1:3306/{db}")
        with engine2.begin() as conn2:
            conn2.execute(text("CREATE TABLE t2 (id INT PRIMARY KEY AUTO_INCREMENT, v INT)"))
            conn2.execute(text("SELECT 9001"))
            # Cross-db session switch: dbName should remain `db`, stmtDbName should become `db2`
            conn2.execute(text(f"USE {db2}"))
            conn2.execute(text("CREATE TABLE t_cross (id INT PRIMARY KEY AUTO_INCREMENT, v INT)"))
            conn2.execute(text("INSERT INTO t_cross (v) VALUES (9)"))
            conn2.execute(text("SELECT * FROM t_cross"))
            conn2.execute(text("DROP TABLE t_cross"))
            conn2.execute(text(f"USE {db}"))
            conn2.execute(text("INSERT INTO t2 (v) VALUES (:v)"), {"v": 1})
            conn2.execute(text("INSERT INTO t2 (v) VALUES (:v)"), [{"v": 2}, {"v": 3}])
            conn2.execute(text("SELECT * FROM t2"))
            conn2.execute(text("DROP TABLE t2"))

        # ORM smoke test (engine2 is connected to db at connect-time)

        class Base(DeclarativeBase):
            pass

        class Employee(Base):
            __tablename__ = "employees_orm"
            id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
            name: Mapped[str] = mapped_column(String(64))
        Base.metadata.create_all(engine2)
        with Session(engine2) as sess:
            sess.add_all([Employee(name="a"), Employee(name="b")])
            sess.commit()
            rows = sess.query(Employee).filter(Employee.name.in_(["a", "b"])).all()
            assert len(rows) == 2
            sess.query(Employee).filter(Employee.name == "a").update({Employee.name: "z"})
            sess.commit()
            sess.query(Employee).filter(Employee.name == "b").delete()
            sess.commit()

        Base.metadata.drop_all(engine2)
        engine2.dispose()

        # totalPoolCount should decrement after disposing engine2 (engine1 still alive)
        with engine.begin() as conn:
            conn.execute(text(f"USE {db}"))
            conn.execute(text("SELECT 4242"))


        # Drop databases (ensure these statements are also intercepted)
        with engine.begin() as conn:
            conn.execute(text(f"DROP DATABASE IF EXISTS {db2}"))
            conn.execute(text(f"DROP DATABASE IF EXISTS {db}"))

    finally:
        unpatch()

    msgs = consume_json_messages(topic=topic, timeout_s=30.0, max_messages=200000)
    msgs = [m for m in msgs if (
        (m.get("stmtDbName") in {db, db2}) or
        (isinstance(m.get("sql"), str) and any(m["sql"].lower().strip().startswith(x) for x in [
            f"create database {db}".lower(), f"use {db}".lower(), f"drop database if exists {db}".lower(),
            f"create database {db2}".lower(), f"use {db2}".lower(), f"drop database if exists {db2}".lower(),
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
        f"drop database if exists {db}".lower(),
    ]:
        assert needle in joined

    dml = [m for m in msgs if m.get("stmtDbName") == db and isinstance(m.get("sql"), str) and m["sql"].strip().lower().startswith(("insert", "update", "delete"))]
    assert any(isinstance(m.get("serverInfo"), str) and m["serverInfo"] for m in dml)
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
