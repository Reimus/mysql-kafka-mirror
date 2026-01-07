from __future__ import annotations

import json
from typing import Any, Optional

from .publisher import Publisher


class KafkaPythonPublisher(Publisher):
    """Publisher implementation based on the `kafka-python` package.

    This module is optional; it is imported dynamically in connect._build_default_kafka_publisher.
    """

    def __init__(
        self,
        *,
        bootstrap_servers: str,
        topic: str,
        acks: str,
        retries: int,
        linger_ms: int,
        batch_size: int,
        buffer_memory: int,
    ) -> None:
        try:
            from kafka import KafkaProducer  # type: ignore
        except Exception as e:  # pragma: no cover
            raise ImportError("kafka-python is not installed") from e

        self._topic = topic
        self._producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers.split(",") if isinstance(bootstrap_servers, str) else bootstrap_servers,
            acks=acks,
            retries=retries,
            linger_ms=linger_ms,
            batch_size=batch_size,
            buffer_memory=buffer_memory,
            value_serializer=lambda v: json.dumps(v, separators=(",", ":")).encode("utf-8"),
        )

    def publish(self, msg: Any) -> None:
        # Fire-and-forget; errors are handled by the interceptor's best-effort wrappers.
        self._producer.send(self._topic, msg.model_dump(by_alias=True))

    def publish_batch(self, msgs: list[Any]) -> None:
        for m in msgs:
            self.publish(m)

    def flush(self, timeout_s: Optional[float] = None) -> None:
        self._producer.flush(timeout=timeout_s)

    def close(self) -> None:
        try:
            self._producer.flush()
        finally:
            self._producer.close()
