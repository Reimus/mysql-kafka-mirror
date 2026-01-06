from __future__ import annotations

from typing import Any, Optional

from .config.settings import Settings
from .dbapi.wrappers import ConnectionWrapper
from .errors import DriverAdapterError
from .kafka.batching import QueueingPublisher
from .kafka.publisher import Publisher, StdoutPublisher


def _get_adapter(driver: str):
    if driver == "pymysql":
        from .adapters.pymysql import PyMySQLAdapter
        return PyMySQLAdapter()
    raise DriverAdapterError(f"Unsupported driver: {driver!r}")


def _build_default_kafka_publisher(settings: Settings) -> Publisher:
    if not settings.kafka_bootstrap_servers:
        return StdoutPublisher()

    try:
        from .kafka.confluent import ConfluentKafkaPublisher
        return ConfluentKafkaPublisher(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            topic=settings.kafka_topic,
            acks=settings.kafka_acks,
            retries=settings.kafka_retries,
            linger_ms=settings.kafka_linger_ms,
            batch_size=settings.kafka_batch_size,
            buffer_memory=settings.kafka_buffer_memory,
            adaptive_partitioning_enabled=settings.kafka_adaptive_partitioning_enabled,
        )
    except Exception:
        pass

    try:
        from .kafka.kafka_python import KafkaPythonPublisher
        return KafkaPythonPublisher(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            topic=settings.kafka_topic,
            acks=settings.kafka_acks,
            retries=settings.kafka_retries,
            linger_ms=settings.kafka_linger_ms,
            batch_size=settings.kafka_batch_size,
            buffer_memory=settings.kafka_buffer_memory,
        )
    except Exception:
        pass

    return StdoutPublisher()


def connect(
    *,
    driver: str = "pymysql",
    publisher: Optional[Publisher] = None,
    settings: Optional[Settings] = None,
    **connect_kwargs: Any,
):
    settings = settings or Settings.from_env()
    adapter = _get_adapter(driver)
    conn = adapter.connect(**connect_kwargs)

    if publisher is None:
        publisher = _build_default_kafka_publisher(settings)

    if settings.enable_queueing_publisher:
        publisher = QueueingPublisher(inner=publisher, settings=settings)

    db_name = adapter.database_name(conn, connect_kwargs)
    return ConnectionWrapper(conn=conn, publisher=publisher, settings=settings, driver_name=driver, database=db_name)
