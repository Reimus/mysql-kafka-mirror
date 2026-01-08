from __future__ import annotations

import logging
from typing import Callable, List, Optional

from ..errors import PublisherError
from ..events.models import SqlLogMessage

logger = logging.getLogger(__name__)


def _json_serializer(event: SqlLogMessage) -> bytes:
    import json
    return json.dumps(event.to_dict(), ensure_ascii=False, default=str, separators=(",", ":")).encode("utf-8")


class ConfluentKafkaPublisher:
    def __init__(
        self,
        *,
        bootstrap_servers: str,
        topic: str,
        client_id: str = "mysql-interceptor",
        acks: str = "all",
        retries: int = 3,
        linger_ms: int = 1000,
        batch_size: int = 16384,
        buffer_memory: int = 33_554_432,
        adaptive_partitioning_enabled: bool = True,
    ) -> None:
        try:
            from confluent_kafka import Producer  # type: ignore
        except Exception as e:
            raise PublisherError("confluent-kafka is not installed. Install mysql-interceptor[confluent].") from e

        self._topic = topic
        conf = {
            "bootstrap.servers": bootstrap_servers,
            "client.id": client_id,
            "acks": acks,
            "retries": retries,
            "linger.ms": linger_ms,
            "batch.size": batch_size,
            "queue.buffering.max.kbytes": max(1, buffer_memory // 1024),
        }
        if adaptive_partitioning_enabled is False:
            conf["debug"] = conf.get("debug", "")
        self._producer = Producer(conf)

    def publish(self, event: SqlLogMessage) -> None:
        try:
            self._producer.produce(
                self._topic,
                key=str(event.connectionId or ""),
                value=_json_serializer(event),
                on_delivery=self._on_delivery,
            )
            self._producer.poll(0)
        except Exception:
            logger.exception("Failed to produce message to Kafka")

    def _on_delivery(self, err, msg):
        if err is not None:
            logger.error("Failed to deliver message to Kafka: %s", err)

    def publish_batch(self, events: List[SqlLogMessage]) -> None:
        for e in events:
            self.publish(e)

    def flush(self) -> None:
        self._producer.flush()

    def close(self) -> None:
        self.flush()
