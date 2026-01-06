#!/usr/bin/env python3
"""Read and print MySQL interceptor events from Kafka.

Defaults:
- bootstrap: localhost:9092
- topic: MYSQL_EVENTS

Usage:
  python scripts/inspect_kafka.py
  python scripts/inspect_kafka.py --bootstrap localhost:9092
  python scripts/inspect_kafka.py --topic MYSQL_EVENTS
  python scripts/inspect_kafka.py --pattern 'MYSQL_EVENTS_TEST_.*'
  python scripts/inspect_kafka.py --list-topics

Notes:
- Requires `kafka-python` installed in the active environment.
- Prints one JSON object per line (the Kafka record value payload).
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from typing import Any, Dict, Iterable, List, Optional

try:
    from kafka import KafkaConsumer  # type: ignore
    from kafka.admin import KafkaAdminClient  # type: ignore
except Exception as e:  # pragma: no cover
    raise SystemExit("kafka-python is required. Install: pip install kafka-python") from e


DEFAULT_TOPIC = "MYSQL_EVENTS"


def list_topics(bootstrap: str) -> List[str]:
    admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id=f"mi-inspect-{time.time_ns()}")
    try:
        return sorted(admin.list_topics())
    finally:
        admin.close()


def iter_messages(
    *,
    bootstrap: str,
    group_id: str,
    topic: str,
    pattern: Optional[str],
    timeout_s: float,
    max_messages: int,
) -> Iterable[Dict[str, Any]]:
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=group_id,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    try:
        if pattern:
            consumer.subscribe(pattern=pattern)
        else:
            consumer.subscribe([topic])

        deadline = time.time() + timeout_s
        yielded = 0
        while time.time() < deadline and yielded < max_messages:
            records = consumer.poll(timeout_ms=500)
            any_record = False
            for _, msgs in records.items():
                for msg in msgs:
                    any_record = True
                    yielded += 1
                    yield msg.value
                    if yielded >= max_messages:
                        return
            if not any_record:
                time.sleep(0.2)
    finally:
        consumer.close()


def main(argv: List[str]) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default=DEFAULT_TOPIC)
    ap.add_argument("--pattern", default=None, help="Topic subscription pattern (consumer regex). Overrides --topic.")
    ap.add_argument("--timeout-s", type=float, default=10.0)
    ap.add_argument("--max-messages", type=int, default=100000)
    ap.add_argument("--list-topics", action="store_true")
    args = ap.parse_args(argv)

    if args.list_topics:
        for t in list_topics(args.bootstrap):
            print(t)
        return 0

    group_id = f"mi-inspect-{time.time_ns()}"
    for obj in iter_messages(
        bootstrap=args.bootstrap,
        group_id=group_id,
        topic=args.topic,
        pattern=args.pattern,
        timeout_s=args.timeout_s,
        max_messages=args.max_messages,
    ):
        sys.stdout.write(json.dumps(obj, separators=(",", ":")) + "\n")
        sys.stdout.flush()

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main(sys.argv[1:]))
