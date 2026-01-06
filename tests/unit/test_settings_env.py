from __future__ import annotations

import importlib

import pytest

from mysql_interceptor.config.settings import ENV_SPECS, Settings


def _clear_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for spec in ENV_SPECS:
        monkeypatch.delenv(spec.env, raising=False)


def test_from_env_defaults_match_base(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_env(monkeypatch)
    assert Settings.from_env() == Settings()


def test_from_env_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_env(monkeypatch)

    monkeypatch.setenv("INTERCEPTOR_KAFKA_BOOTSTRAP_SERVERS", "k1:9092,k2:9092")
    monkeypatch.setenv("INTERCEPTOR_KAFKA_TOPIC", "MYSQL_EVENTS_X")
    monkeypatch.setenv("INTERCEPTOR_KAFKA_ACKS", "1")
    monkeypatch.setenv("INTERCEPTOR_KAFKA_RETRIES", "9")
    monkeypatch.setenv("INTERCEPTOR_KAFKA_LINGER_MS", "50")
    monkeypatch.setenv("INTERCEPTOR_KAFKA_BATCH_SIZE", "777")
    monkeypatch.setenv("INTERCEPTOR_KAFKA_BUFFER_MEMORY", "123456")
    monkeypatch.setenv("INTERCEPTOR_KAFKA_ADAPTIVE_PARTITIONING_ENABLED", "false")

    monkeypatch.setenv("INTERCEPTOR_CAPTURE_ALL", "false")
    monkeypatch.setenv("INTERCEPTOR_BUFFER_UNTIL_COMMIT", "false")
    monkeypatch.setenv("INTERCEPTOR_CAPTURE_DDL", "true")
    monkeypatch.setenv("INTERCEPTOR_CAPTURE_CALLPROC", "false")

    monkeypatch.setenv("INTERCEPTOR_INCLUDE_SQL", "false")
    monkeypatch.setenv("INTERCEPTOR_INCLUDE_PARAMS", "false")

    monkeypatch.setenv("INTERCEPTOR_REDACT_KEYS", "pwd,token,api_key")
    monkeypatch.setenv("INTERCEPTOR_REDACT_VALUE", "REDACTED")
    monkeypatch.setenv("INTERCEPTOR_MAX_PARAM_LENGTH", "12")

    monkeypatch.setenv("INTERCEPTOR_ENABLE_QUEUEING_PUBLISHER", "true")
    monkeypatch.setenv("INTERCEPTOR_PUBLISH_QUEUE_MAXSIZE", "5")
    monkeypatch.setenv("INTERCEPTOR_PUBLISH_BATCH_SIZE", "2")
    monkeypatch.setenv("INTERCEPTOR_PUBLISH_FLUSH_INTERVAL_S", "0.1")
    monkeypatch.setenv("INTERCEPTOR_BACKPRESSURE", "drop")

    monkeypatch.setenv("DEBUGQUERYINTERCEPTOR_STATEMENTLOGGING", "false")
    monkeypatch.setenv("DEBUGQUERYINTERCEPTOR_INLINEDEBUG", "true")
    monkeypatch.setenv("DEBUGQUERYINTERCEPTOR_DEBUG", "job-123")

    monkeypatch.setenv("SERVICE_NAME", "svc")

    s = Settings.from_env()

    assert s.kafka_bootstrap_servers == "k1:9092,k2:9092"
    assert s.kafka_topic == "MYSQL_EVENTS_X"
    assert s.kafka_acks == "1"
    assert s.kafka_retries == 9
    assert s.kafka_linger_ms == 50
    assert s.kafka_batch_size == 777
    assert s.kafka_buffer_memory == 123456
    assert s.kafka_adaptive_partitioning_enabled is False

    assert s.capture_all is False
    assert s.buffer_until_commit is False
    assert s.capture_ddl is True
    assert s.capture_callproc is False

    assert s.include_sql is False
    assert s.include_params is False

    assert s.redact_keys == ["pwd", "token", "api_key"]
    assert s.redact_value == "REDACTED"
    assert s.max_param_length == 12

    assert s.enable_queueing_publisher is True
    assert s.publish_queue_maxsize == 5
    assert s.publish_batch_size == 2
    assert s.publish_flush_interval_s == 0.1
    assert s.backpressure == "drop"

    assert s.statement_logging_allowed is False
    assert s.inline_debug is True
    assert s.inline_debug_value == "job-123"

    assert s.service_name == "svc"


def test_import_does_not_require_optional_deps(monkeypatch: pytest.MonkeyPatch) -> None:
    """Importing mysql_interceptor must not import pymysql/sqlalchemy."""
    real_import = __import__

    def blocked_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name.startswith("pymysql") or name.startswith("sqlalchemy"):
            raise ImportError("blocked")
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr("builtins.__import__", blocked_import)

    import mysql_interceptor  # noqa: F401
    importlib.reload(mysql_interceptor)

    from mysql_interceptor.monkeypatch import patch_pymysql, patch_sqlalchemy

    with pytest.raises(RuntimeError):
        patch_pymysql()

    with pytest.raises(RuntimeError):
        patch_sqlalchemy()
