from __future__ import annotations

import os
from dataclasses import dataclass, field, replace
from typing import List, Optional


def _env(name: str) -> Optional[str]:
    v = os.getenv(name)
    return v if v not in (None, "") else None


def _env_bool(name: str, default: bool) -> bool:
    v = _env(name)
    if v is None:
        return default
    return v.strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def _env_int(name: str, default: int) -> int:
    v = _env(name)
    if v is None:
        return default
    try:
        return int(v.strip())
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    v = _env(name)
    if v is None:
        return default
    try:
        return float(v.strip())
    except ValueError:
        return default


def _env_csv(name: str, default: List[str]) -> List[str]:
    v = _env(name)
    if v is None:
        return default
    parts = [p.strip() for p in v.split(",")]
    return [p for p in parts if p]


@dataclass(frozen=True)
class EnvSpec:
    env: str
    field: str
    kind: str  # "opt_str" | "str" | "bool" | "int" | "float" | "csv"


def _coerce_env(spec: EnvSpec, *, default: object) -> object:
    if spec.kind == "opt_str":
        v = _env(spec.env)
        return default if v is None else v
    if spec.kind == "str":
        v = _env(spec.env)
        return default if v is None else v
    if spec.kind == "bool":
        return _env_bool(spec.env, bool(default))
    if spec.kind == "int":
        return _env_int(spec.env, int(default))
    if spec.kind == "float":
        return _env_float(spec.env, float(default))
    if spec.kind == "csv":
        if not isinstance(default, list):
            return default
        return _env_csv(spec.env, default)
    return default


@dataclass(frozen=True)
class Settings:
    """Static configuration loaded from environment (once)."""

    # Kafka (Java-compatible env vars)
    kafka_bootstrap_servers: Optional[str] = None
    kafka_topic: str = "MYSQL_EVENTS"
    kafka_acks: str = "all"
    kafka_retries: int = 3
    kafka_linger_ms: int = 1000
    kafka_batch_size: int = 16384
    kafka_buffer_memory: int = 33_554_432
    kafka_adaptive_partitioning_enabled: bool = True

    # Capture policy
    buffer_until_commit: bool = True
    capture_all: bool = True  # log SELECTs and everything else
    capture_ddl: bool = False  # used only when capture_all=false
    capture_callproc: bool = True  # used only when capture_all=false

    # Payload toggles
    include_sql: bool = True
    include_params: bool = True

    # Redaction
    redact_keys: List[str] = field(default_factory=lambda: ["password", "passwd", "secret", "token"])
    redact_value: str = "***"
    max_param_length: int = 2048

    # Async publishing
    enable_queueing_publisher: bool = False
    publish_queue_maxsize: int = 10_000
    publish_batch_size: int = 500
    publish_flush_interval_s: float = 0.5
    backpressure: str = "block"  # "block" or "drop"

    # Inline debug / statement logging knobs (Java naming)
    statement_logging_allowed: bool = True  # DEBUGQUERYINTERCEPTOR_STATEMENTLOGGING
    inline_debug: bool = False  # DEBUGQUERYINTERCEPTOR_INLINEDEBUG
    inline_debug_value: Optional[str] = None  # DEBUGQUERYINTERCEPTOR_DEBUG

    service_name: str = "unknown-service"


    @classmethod
    def from_env(cls) -> "Settings":
        base = cls()
        overrides: dict[str, object] = {}
        for spec in ENV_SPECS:
            overrides[spec.field] = _coerce_env(spec, default=getattr(base, spec.field))
        return replace(base, **overrides)


ENV_SPECS: List[EnvSpec] = [
    # Kafka (Java-compatible)
    EnvSpec("INTERCEPTOR_KAFKA_BOOTSTRAP_SERVERS", "kafka_bootstrap_servers", "opt_str"),
    EnvSpec("INTERCEPTOR_KAFKA_TOPIC", "kafka_topic", "str"),
    EnvSpec("INTERCEPTOR_KAFKA_ACKS", "kafka_acks", "str"),
    EnvSpec("INTERCEPTOR_KAFKA_RETRIES", "kafka_retries", "int"),
    EnvSpec("INTERCEPTOR_KAFKA_LINGER_MS", "kafka_linger_ms", "int"),
    EnvSpec("INTERCEPTOR_KAFKA_BATCH_SIZE", "kafka_batch_size", "int"),
    EnvSpec("INTERCEPTOR_KAFKA_BUFFER_MEMORY", "kafka_buffer_memory", "int"),
    EnvSpec("INTERCEPTOR_KAFKA_ADAPTIVE_PARTITIONING_ENABLED", "kafka_adaptive_partitioning_enabled", "bool"),

    # Capture policy
    EnvSpec("INTERCEPTOR_BUFFER_UNTIL_COMMIT", "buffer_until_commit", "bool"),
    EnvSpec("INTERCEPTOR_CAPTURE_ALL", "capture_all", "bool"),
    EnvSpec("INTERCEPTOR_CAPTURE_DDL", "capture_ddl", "bool"),
    EnvSpec("INTERCEPTOR_CAPTURE_CALLPROC", "capture_callproc", "bool"),

    # Payload toggles
    EnvSpec("INTERCEPTOR_INCLUDE_SQL", "include_sql", "bool"),
    EnvSpec("INTERCEPTOR_INCLUDE_PARAMS", "include_params", "bool"),

    # Redaction
    EnvSpec("INTERCEPTOR_REDACT_KEYS", "redact_keys", "csv"),
    EnvSpec("INTERCEPTOR_REDACT_VALUE", "redact_value", "str"),
    EnvSpec("INTERCEPTOR_MAX_PARAM_LENGTH", "max_param_length", "int"),

    # Async publishing
    EnvSpec("INTERCEPTOR_ENABLE_QUEUEING_PUBLISHER", "enable_queueing_publisher", "bool"),
    EnvSpec("INTERCEPTOR_PUBLISH_QUEUE_MAXSIZE", "publish_queue_maxsize", "int"),
    EnvSpec("INTERCEPTOR_PUBLISH_BATCH_SIZE", "publish_batch_size", "int"),
    EnvSpec("INTERCEPTOR_PUBLISH_FLUSH_INTERVAL_S", "publish_flush_interval_s", "float"),
    EnvSpec("INTERCEPTOR_BACKPRESSURE", "backpressure", "str"),

    # Java-compatible debug knobs
    EnvSpec("DEBUGQUERYINTERCEPTOR_STATEMENTLOGGING", "statement_logging_allowed", "bool"),
    EnvSpec("DEBUGQUERYINTERCEPTOR_INLINEDEBUG", "inline_debug", "bool"),
    EnvSpec("DEBUGQUERYINTERCEPTOR_DEBUG", "inline_debug_value", "opt_str"),

    # Generic
    EnvSpec("SERVICE_NAME", "service_name", "str"),
]
