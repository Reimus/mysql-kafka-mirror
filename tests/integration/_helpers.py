import json
import socket
import time
from typing import Any, Dict, List, Optional

from confluent_kafka import Consumer


def wait_for_port(host: str, port: int, timeout_s: float = 30.0) -> None:
    deadline = time.time() + timeout_s
    last: Optional[Exception] = None
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1.0):
                return
        except Exception as e:
            last = e
            time.sleep(0.5)
    raise TimeoutError(f"Port {host}:{port} not reachable within {timeout_s}s. Last error: {last!r}")


def consume_json_messages(
    *,
    bootstrap: str = "localhost:9092",
    topic: str,
    timeout_s: float = 10.0,
    max_messages: int = 10000,
    group_id: str = "mi-it-consumer",
) -> List[Dict[str, Any]]:
    cfg = {
        "bootstrap.servers": bootstrap,
        "group.id": f"{group_id}-{int(time.time() * 1000)}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    c = Consumer(cfg)
    out: List[Dict[str, Any]] = []
    try:
        c.subscribe([topic])
        deadline = time.time() + timeout_s
        while time.time() < deadline and len(out) < max_messages:
            msg = c.poll(0.5)
            if msg is None:
                continue
            if msg.error():
                # ignore transient errors
                continue
            try:
                out.append(json.loads(msg.value().decode("utf-8")))
            except Exception:
                # best effort: skip malformed
                continue
    finally:
        try:
            c.close()
        except Exception:
            pass
    return out
