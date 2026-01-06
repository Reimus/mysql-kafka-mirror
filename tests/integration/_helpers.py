import json, socket, time
from kafka import KafkaConsumer

def wait_for_port(host: str, port: int, timeout_s: float = 60.0) -> None:
    end = time.time() + timeout_s
    last = None
    while time.time() < end:
        try:
            with socket.create_connection((host, port), timeout=2):
                return
        except OSError as e:
            last = e
            time.sleep(0.5)
    raise RuntimeError(f"port not ready: {host}:{port} ({last})")

def consume_json_messages(topic: str, bootstrap: str = "localhost:9092", timeout_s: float = 20.0, max_messages: int = 10000):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=f"mysql-interceptor-tests-{time.time_ns()}",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    out=[]
    end=time.time()+timeout_s
    try:
        while time.time()<end and len(out)<max_messages:
            records=consumer.poll(timeout_ms=250)
            for _, msgs in records.items():
                for m in msgs:
                    out.append(m.value)
            if out:
                time.sleep(0.25)
    finally:
        consumer.close()
    return out
