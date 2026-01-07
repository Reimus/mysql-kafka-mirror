#!/usr/bin/env python3
"""
Read and print MySQL interceptor events from Kafka.

Defaults:
- bootstrap: localhost:9092
- topic: MYSQL_EVENTS

Usage:
  python scripts/inspect_kafka.py
  python scripts/inspect_kafka.py --topic MYSQL_EVENTS
  python scripts/inspect_kafka.py --debug it-pymysql

Notes:
- Requires `confluent-kafka` installed in the active environment.
- Prints one JSON object per line (Kafka record value payload).
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from typing import Any, Dict, Iterable, List, Optional

from confluent_kafka import Consumer, KafkaException, admin


DEFAULT_TOPIC = "MYSQL_EVENTS"


def list_topics(bootstrap: str) -> List[str]:
    a = admin.AdminClient({"bootstrap.servers": bootstrap})
    md = a.list_topics(timeout=10)
    return sorted(list(md.topics.keys()))


def iter_messages(
    *,
    bootstrap: str,
    topic: str,
    pattern: Optional[str],
    timeout_s: float,
    max_messages: int,
) -> Iterable[Dict[str, Any]]:
    cfg = {
        "bootstrap.servers": bootstrap,
        "group.id": f"mi-inspect-{int(time.time() * 1000)}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    c = Consumer(cfg)
    try:
        if pattern:
            # confluent-kafka doesn't natively do regex subscribe; we emulate by listing topics then subscribing.
            topics = [t for t in list_topics(bootstrap) if __import__("re").match(pattern, t)]
            c.subscribe(topics)
        else:
            c.subscribe([topic])

        deadline = time.time() + timeout_s
        yielded = 0
        while time.time() < deadline and yielded < max_messages:
            msg = c.poll(0.5)
            if msg is None:
                continue
            if msg.error():
                continue
            try:
                obj = json.loads(msg.value().decode("utf-8"))
            except Exception:
                continue
            yielded += 1
            yield obj
    finally:
        try:
            c.close()
        except Exception:
            pass


def main(argv: List[str]) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default=DEFAULT_TOPIC)
    ap.add_argument("--pattern", default=None, help="Python regex over topic names. Overrides --topic.")
    ap.add_argument("--timeout-s", type=float, default=10.0)
    ap.add_argument("--max-messages", type=int, default=100000)
    ap.add_argument("--list-topics", action="store_true")
    ap.add_argument("--debug", default=None, help="Filter by message debug field (exact match).")
    args = ap.parse_args(argv)

    if args.list_topics:
        for t in list_topics(args.bootstrap):
            print(t)
        return 0

    for obj in iter_messages(
        bootstrap=args.bootstrap,
        topic=args.topic,
        pattern=args.pattern,
        timeout_s=args.timeout_s,
        max_messages=args.max_messages,
    ):
        if args.debug is not None and obj.get("debug") != args.debug:
            continue
        sys.stdout.write(json.dumps(obj, separators=(",", ":")) + "\n")
        sys.stdout.flush()

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main(sys.argv[1:]))
