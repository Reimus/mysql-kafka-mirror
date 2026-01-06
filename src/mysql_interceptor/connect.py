from __future__ import annotations

from typing import Any, Optional

from .adapters.pymysql import PyMySQLAdapter
from .config.settings import Settings
from .dbapi.wrappers import ConnectionWrapper
from .kafka.batching import QueueingPublisher
from .kafka.confluent import ConfluentKafkaPublisher
from .kafka.kafka_python import KafkaPythonPublisher
from .kafka.publisher import Publisher, StdoutPublisher


_ADAPTERS = {
    "pymysql": PyMySQLAdapter,
}


def _build_default_kafka_publisher(settings: Settings) -> Publisher:
    last_err: Exception | None = None

    try:
        return ConfluentKafkaPublisher(
            bootstrap_servers=settings.kafka_bootstrap_servers or "",
            topic=settings.kafka_topic,
            acks=settings.kafka_acks,
            retries=settings.kafka_retries,
            linger_ms=settings.kafka_linger_ms,
            batch_size=settings.kafka_batch_size,
            buffer_memory=settings.kafka_buffer_memory,
            adaptive_partitioning_enabled=settings.kafka_adaptive_partitioning_enabled,
        )
    except Exception as e:
        last_err = e

    try:
        return KafkaPythonPublisher(
            bootstrap_servers=settings.kafka_bootstrap_servers or "",
            topic=settings.kafka_topic,
            acks=settings.kafka_acks,
            retries=settings.kafka_retries,
            linger_ms=settings.kafka_linger_ms,
            batch_size=settings.kafka_batch_size,
            buffer_memory=settings.kafka_buffer_memory,
        )
    except Exception as e:
        raise e from last_err


def connect(
    *,
    driver: str = "pymysql",
    publisher: Optional[Publisher] = None,
    settings: Optional[Settings] = None,
    **connect_kwargs: Any,
) -> ConnectionWrapper:
    adapter_cls = _ADAPTERS.get(driver)
    if adapter_cls is None:
        raise ValueError(f"Unsupported driver '{driver}'. Supported: {sorted(_ADAPTERS)}")

    adapter = adapter_cls()
    settings = settings or Settings.from_env()

    if publisher is None and settings.kafka_bootstrap_servers:
        publisher = _build_default_kafka_publisher(settings)

    publisher = publisher or StdoutPublisher()

    if settings.enable_queueing_publisher and not isinstance(publisher, QueueingPublisher):
        publisher = QueueingPublisher(inner=publisher, settings=settings)

    conn = adapter.connect(**connect_kwargs)
    db = adapter.database_name(conn, connect_kwargs)

    return ConnectionWrapper(
        conn=conn,
        publisher=publisher,
        settings=settings,
        driver_name=adapter.name,
        database=db,
    )
