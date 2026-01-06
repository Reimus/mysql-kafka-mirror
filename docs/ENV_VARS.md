# Environment variables

This file is generated from `Settings` + `ENV_SPECS`.

| Env var | Field | Type | Default |
|---|---|---|---|
| `INTERCEPTOR_KAFKA_BOOTSTRAP_SERVERS` | `kafka_bootstrap_servers` | `opt_str` | `` |
| `INTERCEPTOR_KAFKA_TOPIC` | `kafka_topic` | `str` | `MYSQL_EVENTS` |
| `INTERCEPTOR_KAFKA_ACKS` | `kafka_acks` | `str` | `all` |
| `INTERCEPTOR_KAFKA_RETRIES` | `kafka_retries` | `int` | `3` |
| `INTERCEPTOR_KAFKA_LINGER_MS` | `kafka_linger_ms` | `int` | `1000` |
| `INTERCEPTOR_KAFKA_BATCH_SIZE` | `kafka_batch_size` | `int` | `16384` |
| `INTERCEPTOR_KAFKA_BUFFER_MEMORY` | `kafka_buffer_memory` | `int` | `33554432` |
| `INTERCEPTOR_KAFKA_ADAPTIVE_PARTITIONING_ENABLED` | `kafka_adaptive_partitioning_enabled` | `bool` | `True` |
| `INTERCEPTOR_BUFFER_UNTIL_COMMIT` | `buffer_until_commit` | `bool` | `True` |
| `INTERCEPTOR_CAPTURE_ALL` | `capture_all` | `bool` | `True` |
| `INTERCEPTOR_CAPTURE_DDL` | `capture_ddl` | `bool` | `False` |
| `INTERCEPTOR_CAPTURE_CALLPROC` | `capture_callproc` | `bool` | `True` |
| `INTERCEPTOR_INCLUDE_SQL` | `include_sql` | `bool` | `True` |
| `INTERCEPTOR_INCLUDE_PARAMS` | `include_params` | `bool` | `True` |
| `INTERCEPTOR_REDACT_KEYS` | `redact_keys` | `csv` | `['password', 'passwd', 'secret', 'token']` |
| `INTERCEPTOR_REDACT_VALUE` | `redact_value` | `str` | `***` |
| `INTERCEPTOR_MAX_PARAM_LENGTH` | `max_param_length` | `int` | `2048` |
| `INTERCEPTOR_ENABLE_QUEUEING_PUBLISHER` | `enable_queueing_publisher` | `bool` | `False` |
| `INTERCEPTOR_PUBLISH_QUEUE_MAXSIZE` | `publish_queue_maxsize` | `int` | `10000` |
| `INTERCEPTOR_PUBLISH_BATCH_SIZE` | `publish_batch_size` | `int` | `500` |
| `INTERCEPTOR_PUBLISH_FLUSH_INTERVAL_S` | `publish_flush_interval_s` | `float` | `0.5` |
| `INTERCEPTOR_BACKPRESSURE` | `backpressure` | `str` | `block` |
| `DEBUGQUERYINTERCEPTOR_STATEMENTLOGGING` | `statement_logging_allowed` | `bool` | `True` |
| `DEBUGQUERYINTERCEPTOR_INLINEDEBUG` | `inline_debug` | `bool` | `False` |
| `DEBUGQUERYINTERCEPTOR_DEBUG` | `inline_debug_value` | `opt_str` | `` |
| `SERVICE_NAME` | `service_name` | `str` | `unknown-service` |
