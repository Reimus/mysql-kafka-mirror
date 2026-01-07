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



## iFlags and isolation levels

See `docs/IFLAGS.md`.
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


## Makefile

From repo root:

- `make up` (starts MySQL+Kafka)
- `make pymysql-test / make alchemysql-test` (unit tests)
- `make pymysql-test / make alchemysql-test-integration` (runs both suites; install deps per `tests/integration/*/requirements.txt`)
- `make down`



### Inspect Kafka output

Print interceptor events from Kafka (one JSON object per line):

```bash
make inspect-results
```

By default it reads topic `MYSQL_EVENTS` (the library default). Override if needed:

```bash
make inspect-results TOPIC=MYSQL_EVENTS
make inspect-results PATTERN='MYSQL_EVENTS_.*'
make inspect-results DEBUG=it-pymysql
```

List topics:

```bash
source tests/integration/pymysql/.venv/bin/activate
python scripts/inspect_kafka.py --bootstrap localhost:9092 --list-topics
```



Integration tests publish to `MYSQL_EVENTS` and validate by filtering Kafka messages by the test start timestamp (to avoid interference from previous runs).


Integration tests publish to `MYSQL_EVENTS` and validate by filtering Kafka messages using `stmtDbName == <test db>` (plus CREATE/USE/DROP DATABASE statements).


Integration tests cover both `USE <db>` (dbName may be null) and connecting with `db=<db>` / URL `/<db>` (dbName should be populated).


Repo includes a `.gitignore` to avoid committing `.venv`, `*.egg-info`, and `__pycache__`.


Integration tests additionally cover views, stored procedures, and switching databases via `USE`.



## Running locally

### Install docker-compose (if missing)

```bash
./scripts/install-docker-compose.sh
```

### Start MySQL + Kafka

```bash
make up
make ps
```

### Run integration tests

PyMySQL:

```bash
make pymysql-test
```

SQLAlchemy + ORM:

```bash
make alchemysql-test
```

### Inspect Kafka output

```bash
make inspect-results
```

Optional:

```bash
make inspect-results TOPIC=MYSQL_EVENTS
make inspect-results PATTERN='MYSQL_EVENTS_.*'
make inspect-results DEBUG=it-pymysql
```

### Stop everything

```bash
make down
```




## Dependencies

- Kafka client: **confluent-kafka** is a required dependency of this package (installed automatically with `pip install mysql-interceptor`).
- MySQL drivers are optional:
  - PyMySQL is only needed if you use `patch_pymysql()` or `mysql_interceptor.connect(..., driver="pymysql")`.
  - SQLAlchemy is only needed if you use `patch_sqlalchemy()`.

If you want optional installs:

```bash
pip install mysql-interceptor[pymysql]
pip install mysql-interceptor[sqlalchemy]
```

## License

MIT. See `LICENSE`.
