from __future__ import annotations

import re
import time
from typing import Any, List, Optional, Protocol, TypeVar, runtime_checkable

from ..config.redaction import params_to_query_params
from ..config.settings import Settings
from ..dbapi.classify import is_call, is_ddl, is_use, is_write
from ..dbapi.txn_buffer import TransactionBuffer
from ..events.models import SqlLogMessage
from ..kafka.publisher import Publisher
from ..utils import extract_server_info_best_effort, hostname
from ..pool_counter import GLOBAL_POOL_COUNTER

IVER8 = 1
PY_DRIVER = 2

# Python-only iFlag bits (avoid collision with Java bits)
PY_ERROR_CONNECTION_ID = 1 << 21
PY_ERROR_PREPROCESS_BATCHED_ARGS = 1 << 22
PY_ERROR_POSTPROCESS_BATCHED_ARGS = 1 << 23
PY_ERROR_DEFAULT_TZ = 1 << 24
PY_ERROR_SERVER_TZ = 1 << 25
PY_ERROR_ISOLATION = 1 << 26
PY_ERROR_CLIENT_FLAGS = 1 << 27
PY_ERROR_SERVER_FLAGS = 1 << 28
PY_ERROR_SERVER_VERSION = 1 << 29
PY_ERROR_SERVER_HOST = 1 << 30
PY_ERROR_SERVER_INFO = 1 << 31


@runtime_checkable
class DBAPICursor(Protocol):
    rowcount: int

    def execute(self, operation: str, params: Any = ...) -> Any: ...
    def executemany(self, operation: str, seq_of_params: Any) -> Any: ...
    def callproc(self, procname: str, params: Any = ...) -> Any: ...
    def close(self) -> Any: ...


@runtime_checkable
class DBAPIConnection(Protocol):
    def cursor(self, *args: Any, **kwargs: Any) -> DBAPICursor: ...
    def commit(self) -> Any: ...
    def rollback(self) -> Any: ...
    def close(self) -> Any: ...


TConn = TypeVar("TConn", bound=DBAPIConnection)


def _safe_str(v: Any) -> Optional[str]:
    try:
        return None if v is None else str(v)
    except Exception:
        return None


def _safe_int(v: Any) -> Optional[int]:
    try:
        return None if v is None else int(v)
    except Exception:
        return None


def _default_tz() -> tuple[Optional[str], int]:
    try:
        import datetime
        return _safe_str(datetime.datetime.now().astimezone().tzinfo), 0
    except Exception:
        return None, PY_ERROR_DEFAULT_TZ


def _isolation_to_level(value: Optional[str]) -> Optional[int]:
    if not value:
        return None
    v = value.strip().upper().replace("-", " ").replace("_", " ")
    mapping = {
        "READ UNCOMMITTED": 1,
        "READ COMMITTED": 2,
        "REPEATABLE READ": 4,
        "SERIALIZABLE": 8,
    }
    return mapping.get(v)


class _RecordingParamsIterable:
    def __init__(self, seq: Any, settings: Settings) -> None:
        self._seq = seq
        self._settings = settings
        self.recorded: List[Optional[List[str]]] = []
        self.iflags: int = 0

    def __iter__(self):
        try:
            for item in self._seq:
                try:
                    self.recorded.append(params_to_query_params(item, self._settings))
                except Exception:
                    self.iflags |= PY_ERROR_PREPROCESS_BATCHED_ARGS
                    self.recorded.append(None)
                yield item
        except Exception:
            self.iflags |= PY_ERROR_PREPROCESS_BATCHED_ARGS
            raise


class CursorWrapper:
    def __init__(self, *, cursor: DBAPICursor, parent: "ConnectionWrapper") -> None:
        self._cursor = cursor
        self._parent = parent

    def __getattr__(self, name: str) -> Any:
        return getattr(self._cursor, name)

    def execute(self, operation: str, params: Any = None) -> Any:
        operation = self._parent._maybe_apply_inline_debug(operation)
        t0 = time.perf_counter_ns()
        err: Optional[BaseException] = None
        try:
            return self._cursor.execute(operation, params)
        except BaseException as e:
            err = e
            raise
        finally:
            duration_ns = time.perf_counter_ns() - t0
            end_ms = time.time_ns() // 1_000_000
            timestamp_ms = end_ms - (duration_ns // 1_000_000)

            server_info, had_err = extract_server_info_best_effort(self._cursor, self._parent._conn)
            extra_iflags = PY_ERROR_SERVER_INFO if had_err else 0

            self._parent._after_statement(
                sql=operation,
                params=params,
                timestamp_ms=timestamp_ms,
                duration_ns=duration_ns,
                update_count=_safe_int(getattr(self._cursor, "rowcount", None)),
                server_info=server_info,
                error=err,
                extra_iflags=extra_iflags,
            )

    def executemany(self, operation: str, seq_of_params: Any) -> Any:
        operation = self._parent._maybe_apply_inline_debug(operation)
        recorder = _RecordingParamsIterable(seq_of_params, self._parent._settings)
        t0 = time.perf_counter_ns()
        err: Optional[BaseException] = None
        try:
            return self._cursor.executemany(operation, recorder)
        except BaseException as e:
            err = e
            raise
        finally:
            duration_ns = time.perf_counter_ns() - t0
            end_ms = time.time_ns() // 1_000_000
            timestamp_ms = end_ms - (duration_ns // 1_000_000)

            server_info, had_err = extract_server_info_best_effort(self._cursor, self._parent._conn)
            extra_iflags = (PY_ERROR_SERVER_INFO if had_err else 0) | recorder.iflags

            self._parent._after_executemany(
                sql=operation,
                recorded_query_params=recorder.recorded,
                timestamp_ms=timestamp_ms,
                duration_ns=duration_ns,
                total_update_count=_safe_int(getattr(self._cursor, "rowcount", None)),
                server_info=server_info,
                error=err,
                extra_iflags=extra_iflags,
            )

    def callproc(self, procname: str, params: Any = None) -> Any:
        sql = self._parent._maybe_apply_inline_debug(f"CALL {procname}")
        t0 = time.perf_counter_ns()
        err: Optional[BaseException] = None
        try:
            return self._cursor.callproc(procname, params)
        except BaseException as e:
            err = e
            raise
        finally:
            duration_ns = time.perf_counter_ns() - t0
            end_ms = time.time_ns() // 1_000_000
            timestamp_ms = end_ms - (duration_ns // 1_000_000)

            server_info, had_err = extract_server_info_best_effort(self._cursor, self._parent._conn)
            extra_iflags = PY_ERROR_SERVER_INFO if had_err else 0

            self._parent._after_statement(
                sql=sql,
                params=params,
                timestamp_ms=timestamp_ms,
                duration_ns=duration_ns,
                update_count=_safe_int(getattr(self._cursor, "rowcount", None)),
                server_info=server_info,
                error=err,
                force_call=True,
                extra_iflags=extra_iflags,
            )

    def close(self) -> Any:
        return self._cursor.close()


class ConnectionWrapper:
    def __init__(
        self,
        *,
        conn: TConn,
        publisher: Publisher,
        settings: Settings,
        driver_name: str,
        database: Optional[str],
    ) -> None:
        self._conn = conn
        try:
            GLOBAL_POOL_COUNTER.inc()
        except Exception:
            pass
        self._publisher = publisher
        self._settings = settings
        self._driver_name = driver_name

        self._db_name = database
        self._stmt_db_name = database

        self._buffer = TransactionBuffer()
        self._execution_count = 0

        self._client = hostname()
        self._user = _safe_str(getattr(conn, "user", None)) or _safe_str(getattr(conn, "_user", None))

        self._server_host, self._server_host_iflags = self._compute_server_host()
        self._server_version, self._server_version_iflags = self._compute_server_version()
        self._connection_id, self._connid_iflags = self._compute_connection_id()

        self._default_tz, self._default_tz_iflags = _default_tz()
        self._server_tz, self._server_tz_iflags = self._compute_server_tz()
        self._isolation_lvl, self._isolation_iflags = self._compute_isolation_lvl()
        self._client_flags, self._client_flags_iflags = self._compute_client_flags()

        self._cached_server_flags: Optional[int] = None

    def __getattr__(self, name: str) -> Any:
        return getattr(self._conn, name)

    def cursor(self, *args: Any, **kwargs: Any) -> CursorWrapper:
        return CursorWrapper(cursor=self._conn.cursor(*args, **kwargs), parent=self)

    def commit(self) -> Any:
        out = self._conn.commit()
        self._flush_on_commit()
        return out

    def rollback(self) -> Any:
        out = self._conn.rollback()
        self._drop_on_rollback()
        return out

    def close(self) -> Any:
        if not self._settings.buffer_until_commit:
            try:
                self._publisher.flush()
            except Exception:
                pass
        try:
            GLOBAL_POOL_COUNTER.dec()
        except Exception:
            pass
        return self._conn.close()

    def _maybe_apply_inline_debug(self, sql: str) -> str:
        if not self._settings.inline_debug:
            return sql
        debug_value = self._settings.inline_debug_value or self._settings.service_name
        return (
            f"/* Id [{self._connection_id}] User [{self._user}] Client [{self._client}] "
            f"Count [{self._execution_count + 1}] Debug [{debug_value}] */\n"
            + sql
        )

    def _base_iflags(self) -> int:
        iflags = IVER8 | PY_DRIVER
        iflags |= self._server_host_iflags
        iflags |= self._server_version_iflags
        iflags |= self._default_tz_iflags
        iflags |= self._server_tz_iflags
        iflags |= self._isolation_iflags
        iflags |= self._client_flags_iflags
        iflags |= self._connid_iflags

        server_flags, sf_if = self._compute_server_flags()
        iflags |= sf_if
        self._cached_server_flags = server_flags
        return iflags

    def _should_capture(self, sql: str, *, force_call: bool = False) -> bool:
        if self._settings.capture_all:
            return True
        if is_use(sql):
            return True
        return (
            is_write(sql)
            or (is_call(sql) and self._settings.capture_callproc)
            or (is_ddl(sql) and self._settings.capture_ddl)
            or force_call
        )

    def _track_stmt_db_name(self, sql: str) -> None:
        if not is_use(sql):
            return
        db = _parse_use_db(sql)
        if db:
            self._stmt_db_name = db

    def _after_statement(
        self,
        *,
        sql: str,
        params: Any,
        timestamp_ms: int,
        duration_ns: int,
        update_count: Optional[int],
        server_info: Optional[str],
        error: Optional[BaseException],
        force_call: bool = False,
        extra_iflags: int = 0,
    ) -> None:
        self._execution_count += 1

        self._track_stmt_db_name(sql)
        if not self._should_capture(sql, force_call=force_call):
            return

        iflags = self._base_iflags() | extra_iflags

        query_params: Optional[List[str]] = None
        try:
            query_params = params_to_query_params(params, self._settings)
        except Exception:
            iflags |= PY_ERROR_POSTPROCESS_BATCHED_ARGS

        msg = SqlLogMessage(
            timestamp=timestamp_ms,
            serverHost=self._server_host,
            serverVersion=self._server_version,
            user=self._user,
            client=self._client,
            dbName=self._db_name,
            stmtDbName=self._stmt_db_name or self._db_name,
            debug=self._settings.inline_debug_value,
            connectionId=self._connection_id,
            totalPoolCount=_safe_int(GLOBAL_POOL_COUNTER.get()),
            executionCount=self._execution_count,
            durationNs=duration_ns,
            serverFlags=self._cached_server_flags,
            clientFlags=self._client_flags,
            iFlags=iflags,
            defaultTZ=self._default_tz,
            serverTZ=self._server_tz,
            isolationLvl=self._isolation_lvl,
            updateCount=update_count,
            sql=(sql if self._settings.include_sql else None),
            queryParams=(query_params if self._settings.include_params else None),
            errorMessage=_safe_str(error),
            serverInfo=server_info,
        )

        if error is not None:
            self._publish_best_effort(msg)
            return

        if self._settings.buffer_until_commit:
            self._buffer.add(msg)
        else:
            self._publish_best_effort(msg)

    def _after_executemany(
        self,
        *,
        sql: str,
        recorded_query_params: List[Optional[List[str]]],
        timestamp_ms: int,
        duration_ns: int,
        total_update_count: Optional[int],
        server_info: Optional[str],
        error: Optional[BaseException],
        extra_iflags: int = 0,
    ) -> None:
        self._track_stmt_db_name(sql)

        n = len(recorded_query_params)
        if not self._should_capture(sql):
            self._execution_count += n
            return

        base_iflags = self._base_iflags() | extra_iflags

        for i in range(n):
            self._execution_count += 1
            is_last = i == (n - 1)

            msg = SqlLogMessage(
                timestamp=timestamp_ms,
                serverHost=self._server_host,
                serverVersion=self._server_version,
                user=self._user,
                client=self._client,
                dbName=self._db_name,
                stmtDbName=self._stmt_db_name or self._db_name,
                debug=self._settings.inline_debug_value,
                connectionId=self._connection_id,
                totalPoolCount=_safe_int(GLOBAL_POOL_COUNTER.get()),
                executionCount=self._execution_count,
                durationNs=(duration_ns if is_last else None),
                serverFlags=self._cached_server_flags,
                clientFlags=self._client_flags,
                iFlags=base_iflags,
                defaultTZ=self._default_tz,
                serverTZ=self._server_tz,
                isolationLvl=self._isolation_lvl,
                updateCount=(total_update_count if is_last else None),
                sql=(sql if self._settings.include_sql else None),
                queryParams=(recorded_query_params[i] if self._settings.include_params else None),
                errorMessage=_safe_str(error),
                serverInfo=(server_info if is_last else None),
            )

            if error is not None:
                self._publish_best_effort(msg)
            elif self._settings.buffer_until_commit:
                self._buffer.add(msg)
            else:
                self._publish_best_effort(msg)

    def _publish_best_effort(self, msg: SqlLogMessage) -> None:
        try:
            self._publisher.publish(msg)
        except Exception:
            return

    def _flush_on_commit(self) -> None:
        events = self._buffer.drain()
        if not events:
            return
        try:
            self._publisher.publish_batch(events)
        except Exception:
            for e in events:
                self._publish_best_effort(e)

    def _drop_on_rollback(self) -> None:
        self._buffer.clear()

    def _compute_server_host(self) -> tuple[Optional[str], int]:
        try:
            host = getattr(self._conn, "host", None) or getattr(self._conn, "_host", None)
            port = getattr(self._conn, "port", None) or getattr(self._conn, "_port", None)
            if host and port:
                return f"{host}:{int(port)}", 0
            if host:
                return _safe_str(host), 0
            return None, PY_ERROR_SERVER_HOST
        except Exception:
            return None, PY_ERROR_SERVER_HOST

    def _compute_server_version(self) -> tuple[Optional[str], int]:
        try:
            if hasattr(self._conn, "get_server_info"):
                return _safe_str(self._conn.get_server_info()), 0  # type: ignore[attr-defined]
            cur = self._conn.cursor()
            try:
                cur.execute("SELECT VERSION()")
                row = cur.fetchone()  # type: ignore[attr-defined]
                if isinstance(row, (list, tuple)) and row:
                    return _safe_str(row[0]), 0
                return _safe_str(row), 0
            finally:
                try:
                    cur.close()
                except Exception:
                    pass
        except Exception:
            return None, PY_ERROR_SERVER_VERSION

    def _compute_connection_id(self) -> tuple[Optional[int], int]:
        try:
            cur = self._conn.cursor()
            try:
                cur.execute("SELECT CONNECTION_ID()")
                row = cur.fetchone()  # type: ignore[attr-defined]
                if row is None:
                    return None, 0
                if isinstance(row, (list, tuple)):
                    return _safe_int(row[0]), 0
                return _safe_int(row), 0
            finally:
                try:
                    cur.close()
                except Exception:
                    pass
        except Exception:
            return None, PY_ERROR_CONNECTION_ID

    def _compute_server_tz(self) -> tuple[Optional[str], int]:
        try:
            cur = self._conn.cursor()
            try:
                cur.execute("SELECT @@session.time_zone")
                row = cur.fetchone()  # type: ignore[attr-defined]
                if isinstance(row, (list, tuple)) and row:
                    return _safe_str(row[0]), 0
                return _safe_str(row), 0
            finally:
                try:
                    cur.close()
                except Exception:
                    pass
        except Exception:
            return None, PY_ERROR_SERVER_TZ

    def _compute_isolation_lvl(self) -> tuple[Optional[int], int]:
        try:
            cur = self._conn.cursor()
            try:
                cur.execute("SELECT @@transaction_isolation")
                row = cur.fetchone()  # type: ignore[attr-defined]
                iso = _safe_str(row[0]) if isinstance(row, (list, tuple)) and row else _safe_str(row)
                return _isolation_to_level(iso), 0
            finally:
                try:
                    cur.close()
                except Exception:
                    pass
        except Exception:
            return None, PY_ERROR_ISOLATION

    def _compute_client_flags(self) -> tuple[Optional[int], int]:
        try:
            v = getattr(self._conn, "client_flag", None) or getattr(self._conn, "_client_flag", None)
            return _safe_int(v), 0
        except Exception:
            return None, PY_ERROR_CLIENT_FLAGS

    def _compute_server_flags(self) -> tuple[Optional[int], int]:
        try:
            v = getattr(self._conn, "server_status", None) or getattr(self._conn, "_server_status", None)
            return _safe_int(v), 0
        except Exception:
            return None, PY_ERROR_SERVER_FLAGS


def _parse_use_db(sql: str) -> Optional[str]:
    try:
        s = (sql or "").strip()
        if s.startswith("/*"):
            idx = s.find("*/")
            if idx != -1:
                s = s[idx + 2 :].lstrip()
        m = re.match(r"(?is)^use\s+(`([^`]+)`|([a-zA-Z0-9_]+))\s*;?\s*$", s)
        if not m:
            return None
        return m.group(2) or m.group(3)
    except Exception:
        return None
