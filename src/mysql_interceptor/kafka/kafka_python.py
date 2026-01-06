from __future__ import annotations

from typing import List

from ..errors import PublisherError
from ..events.models import SqlLogMessage


def _json_serializer(event: SqlLogMessage) -> bytes:
    import json
    return json.dumps(event.to_dict(), ensure_ascii=False, default=str, separators=(",", ":")).encode("utf-8")


class KafkaPythonPublisher:
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
    ) -> None:
        try:
            from kafka import KafkaProducer  # type: ignore
        except Exception as e:
            raise PublisherError("kafka-python is not installed. Install mysql-interceptor[kafka-python].") from e

        self._topic = topic
        self._producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            client_id=client_id,
            acks=acks,
            retries=retries,
            linger_ms=linger_ms,
            batch_size=batch_size,
            buffer_memory=buffer_memory,
            key_serializer=lambda s: s.encode("utf-8"),
            value_serializer=lambda b: b,
        )

    def publish(self, event: SqlLogMessage) -> None:
        self._producer.send(self._topic, key=str(event.connectionId or ""), value=_json_serializer(event))

    def publish_batch(self, events: List[SqlLogMessage]) -> None:
        for e in events:
            self.publish(e)

    def flush(self) -> None:
        self._producer.flush()

    def close(self) -> None:
        self.flush()
        self._producer.close()
