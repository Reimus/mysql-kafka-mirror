# mysql-interceptor

Drop-in interception for MySQL writes that mirrors events to Kafka using a Java-compatible JSON schema.

## Enable Kafka via env vars (Java-compatible)

Kafka export is enabled when `INTERCEPTOR_KAFKA_BOOTSTRAP_SERVERS` is set and you don't pass `publisher=...`.

```bash
export INTERCEPTOR_KAFKA_BOOTSTRAP_SERVERS="server1:9092,server2:9092"
export INTERCEPTOR_KAFKA_TOPIC="MYSQL_EVENTS"
export INTERCEPTOR_KAFKA_ACKS="all"
export INTERCEPTOR_KAFKA_RETRIES="3"
export INTERCEPTOR_KAFKA_LINGER_MS="1000"
export INTERCEPTOR_KAFKA_BATCH_SIZE="16384"
export INTERCEPTOR_KAFKA_BUFFER_MEMORY="33554432"
export INTERCEPTOR_KAFKA_ADAPTIVE_PARTITIONING_ENABLED="true"
```

## Output schema

Kafka messages use the same field names as the Java interceptor's `SqlLogMessage`
(see `src/mysql_interceptor/events/models.py`).

Note: `timestamp` is **epoch milliseconds (wall clock)** and represents the statement start time estimate:
`end_ms - (durationNs/1e6)` (Java-compatible).

## Drop-in options

### PyMySQL

```python
from mysql_interceptor.monkeypatch import patch_pymysql

unpatch = patch_pymysql()
# your code that calls pymysql.connect(...)
unpatch()
```

### SQLAlchemy

```python
from mysql_interceptor.monkeypatch import patch_sqlalchemy

unpatch = patch_sqlalchemy()
# your code that calls sqlalchemy.create_engine(...)
unpatch()
```

## executemany behavior

`executemany(...)` emits **one Kafka record per parameter set**.
Only the **last** record contains `durationNs` and `updateCount` for the whole batch; earlier records set them to null.


See docs/ENV_VARS.md for the full env var list.
