from __future__ import annotations

import dataclasses
import re
import time
from typing import Any, List, Optional, Tuple

from .config.redaction import params_to_query_params
from .config.settings import Settings
from .dbapi.classify import is_call, is_ddl, is_use, is_write
from .events.models import SqlLogMessage
from .kafka.publisher import Publisher
from .utils import extract_server_info_best_effort, hostname

IVER8 = 1
PY_DRIVER = 2

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


def _default_tz() -> Tuple[Optional[str], int]:
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


@dataclasses.dataclass
class _SAState:
    publisher: Publisher
    settings: Settings

    db_name: Optional[str]
    stmt_db_name: Optional[str]

    user: Optional[str]
    client: str

    server_host: Optional[str]
    server_version: Optional[str]
    connection_id: Optional[int]

    default_tz: Optional[str]
    server_tz: Optional[str]
    isolation_lvl: Optional[int]

    client_flags: Optional[int]
    cached_server_flags: Optional[int] = None

    base_iflags: int = IVER8 | PY_DRIVER
    execution_count: int = 0
    buffer: List[SqlLogMessage] = dataclasses.field(default_factory=list)


def _get_dbapi_conn_from_sa_connection(sa_conn: Any) -> Any:
    """Return underlying DBAPI connection for a SQLAlchemy Connection (best effort).

    SQLAlchemy 2.x deprecates ConnectionFairy.connection; prefer driver_connection.
    """
    try:
        fairy = getattr(sa_conn, "connection", None)
        if fairy is None:
            return None
        v = getattr(fairy, "driver_connection", None)
        if v is not None:
            return v
        v = getattr(fairy, "connection", None)
        if v is not None:
            return v
        return fairy
    except Exception:
        return None


def _try_query_scalar(dbapi_conn: Any, sql: str) -> Any:
    cur = dbapi_conn.cursor()
    try:
        cur.execute(sql)
        row = cur.fetchone()
        if isinstance(row, (list, tuple)):
            return row[0] if row else None
        return row
    finally:
        try:
            cur.close()
        except Exception:
            pass


def _build_state(*, dbapi_conn: Any, engine_url: Any, publisher: Publisher, settings: Settings) -> _SAState:
    iflags = IVER8 | PY_DRIVER

    db_name = _safe_str(getattr(engine_url, "database", None))
    stmt_db_name = db_name
    user = _safe_str(getattr(engine_url, "username", None))
    client = hostname()

    server_host = None
    try:
        host = getattr(engine_url, "host", None)
        port = getattr(engine_url, "port", None)
        if host and port:
            server_host = f"{host}:{int(port)}"
        elif host:
            server_host = _safe_str(host)
        else:
            iflags |= PY_ERROR_SERVER_HOST
    except Exception:
        iflags |= PY_ERROR_SERVER_HOST

    server_version = None
    try:
        if hasattr(dbapi_conn, "get_server_info"):
            server_version = _safe_str(dbapi_conn.get_server_info())
        else:
            server_version = _safe_str(_try_query_scalar(dbapi_conn, "SELECT VERSION()"))
    except Exception:
        iflags |= PY_ERROR_SERVER_VERSION

    connection_id = None
    try:
        connection_id = _safe_int(_try_query_scalar(dbapi_conn, "SELECT CONNECTION_ID()"))
    except Exception:
        iflags |= PY_ERROR_CONNECTION_ID

    default_tz, tz_if = _default_tz()
    iflags |= tz_if

    server_tz = None
    try:
        server_tz = _safe_str(_try_query_scalar(dbapi_conn, "SELECT @@session.time_zone"))
    except Exception:
        iflags |= PY_ERROR_SERVER_TZ

    isolation_lvl = None
    try:
        iso = _safe_str(_try_query_scalar(dbapi_conn, "SELECT @@transaction_isolation"))
        isolation_lvl = _isolation_to_level(iso)
    except Exception:
        iflags |= PY_ERROR_ISOLATION

    client_flags = None
    try:
        client_flags = _safe_int(getattr(dbapi_conn, "client_flag", None) or getattr(dbapi_conn, "_client_flag", None))
    except Exception:
        iflags |= PY_ERROR_CLIENT_FLAGS

    return _SAState(
        publisher=publisher,
        settings=settings,
        db_name=db_name,
        stmt_db_name=stmt_db_name,
        user=user,
        client=client,
        server_host=server_host,
        server_version=server_version,
        connection_id=connection_id,
        default_tz=default_tz,
        server_tz=server_tz,
        isolation_lvl=isolation_lvl,
        client_flags=client_flags,
        base_iflags=iflags,
    )


def instrument_engine(*, engine: Any, publisher: Publisher, settings: Settings) -> None:
    from sqlalchemy import event  # type: ignore

    @event.listens_for(engine, "connect")
    def _on_connect(dbapi_conn: Any, connection_record: Any) -> None:
        connection_record.info["mysql_interceptor_state"] = _build_state(
            dbapi_conn=dbapi_conn, engine_url=engine.url, publisher=publisher, settings=settings
        )

    @event.listens_for(engine, "commit")
    def _on_commit(sa_conn: Any) -> None:
        st: Optional[_SAState] = getattr(sa_conn, "info", {}).get("mysql_interceptor_state")  # type: ignore[attr-defined]
        if not st or not st.settings.buffer_until_commit or not st.buffer:
            return
        batch = list(st.buffer)
        st.buffer.clear()
        try:
            st.publisher.publish_batch(batch)
        except Exception:
            for e in batch:
                _publish_best_effort(st, e)

    @event.listens_for(engine, "rollback")
    def _on_rollback(sa_conn: Any) -> None:
        st: Optional[_SAState] = getattr(sa_conn, "info", {}).get("mysql_interceptor_state")  # type: ignore[attr-defined]
        if st and st.settings.buffer_until_commit:
            st.buffer.clear()

    @event.listens_for(engine, "before_cursor_execute")
    def _before_cursor_execute(sa_conn: Any, cursor: Any, statement: str, parameters: Any, context: Any, executemany: bool) -> None:
        context._mi_t0 = time.perf_counter_ns()

    @event.listens_for(engine, "after_cursor_execute")
    def _after_cursor_execute(sa_conn: Any, cursor: Any, statement: str, parameters: Any, context: Any, executemany: bool) -> None:
        t0 = getattr(context, "_mi_t0", None)
        if t0 is None:
            return

        duration_ns = time.perf_counter_ns() - t0
        end_ms = time.time_ns() // 1_000_000
        timestamp_ms = end_ms - (duration_ns // 1_000_000)

        dbapi_conn = _get_dbapi_conn_from_sa_connection(sa_conn)

        server_info, had_err = extract_server_info_best_effort(cursor, dbapi_conn)
        iflags_extra = PY_ERROR_SERVER_INFO if had_err else 0

        st: Optional[_SAState] = getattr(sa_conn, "info", {}).get("mysql_interceptor_state")  # type: ignore[attr-defined]
        if not st:
            return

        _track_stmt_db_name(st, statement)

        if not _should_capture(st, statement, force_call=False):
            if executemany:
                try:
                    st.execution_count += len(parameters)
                except Exception:
                    st.execution_count += 1
            else:
                st.execution_count += 1
            return

        server_flags, _ = _compute_server_flags(sa_conn)
        st.cached_server_flags = server_flags
        iflags = st.base_iflags | iflags_extra

        if not executemany:
            st.execution_count += 1
            msg = _build_message(
                st=st,
                timestamp_ms=timestamp_ms,
                duration_ns=duration_ns,
                update_count=_safe_int(getattr(cursor, "rowcount", None)),
                sql=statement,
                query_params=_params_or_none(st, parameters),
                server_info=server_info,
                iflags=iflags,
                error=None,
            )
            _buffer_or_publish(st, msg)
            return

        try:
            param_sets = list(parameters) if not isinstance(parameters, list) else parameters
        except Exception:
            param_sets = [parameters]
            iflags |= PY_ERROR_PREPROCESS_BATCHED_ARGS

        n = len(param_sets)
        total_update = _safe_int(getattr(cursor, "rowcount", None))
        for i in range(n):
            st.execution_count += 1
            is_last = i == (n - 1)

            qps = None
            if st.settings.include_params:
                try:
                    qps = params_to_query_params(param_sets[i], st.settings)
                except Exception:
                    iflags |= PY_ERROR_POSTPROCESS_BATCHED_ARGS
                    qps = None

            msg = _build_message(
                st=st,
                timestamp_ms=timestamp_ms,
                duration_ns=(duration_ns if is_last else None),
                update_count=(total_update if is_last else None),
                sql=statement,
                query_params=qps,
                server_info=(server_info if is_last else None),
                iflags=iflags,
                error=None,
            )
            _buffer_or_publish(st, msg)



    @event.listens_for(engine, "handle_error")
    def _on_handle_error(exception_context: Any) -> None:
        """Capture DBAPI errors (after_cursor_execute does not run on exceptions)."""
        try:
            sa_conn = getattr(exception_context, "connection", None)
            st: Optional[_SAState] = getattr(sa_conn, "info", {}).get("mysql_interceptor_state") if sa_conn else None  # type: ignore[attr-defined]
            if not st:
                return

            statement = getattr(exception_context, "statement", None)
            if statement is None:
                return
            sql = str(statement)

            _track_stmt_db_name(st, sql)

            if not _should_capture(st, sql, force_call=False):
                st.execution_count += 1
                return

            exec_ctx = getattr(exception_context, "execution_context", None)
            t0 = getattr(exec_ctx, "_mi_t0", None)
            duration_ns: Optional[int] = (time.perf_counter_ns() - t0) if isinstance(t0, int) else None

            end_ms = time.time_ns() // 1_000_000
            timestamp_ms = end_ms - (duration_ns // 1_000_000) if duration_ns is not None else end_ms

            cursor = getattr(exception_context, "cursor", None)
            dbapi_conn = _get_dbapi_conn_from_sa_connection(sa_conn) if sa_conn is not None else None
            server_info, had_err = extract_server_info_best_effort(cursor, dbapi_conn)
            iflags_extra = PY_ERROR_SERVER_INFO if had_err else 0

            server_flags, _ = _compute_server_flags(sa_conn)
            st.cached_server_flags = server_flags
            iflags = st.base_iflags | iflags_extra

            query_params = _params_or_none(st, getattr(exception_context, "parameters", None))

            original = getattr(exception_context, "original_exception", None)
            if original is None:
                original = getattr(exception_context, "original", None)

            st.execution_count += 1
            msg = _build_message(
                st=st,
                timestamp_ms=timestamp_ms,
                duration_ns=duration_ns,
                update_count=None,
                sql=sql,
                query_params=query_params,
                server_info=server_info,
                iflags=iflags,
                error=original,
            )

            # Errors are easy to lose with buffering (rollback clears the buffer).
            _publish_best_effort(st, msg)
        except Exception:
            return

def _compute_server_flags(sa_conn: Any) -> Tuple[Optional[int], int]:
    try:
        dbapi_conn = _get_dbapi_conn_from_sa_connection(sa_conn)
        v = getattr(dbapi_conn, "server_status", None) or getattr(dbapi_conn, "_server_status", None)
        return _safe_int(v), 0
    except Exception:
        return None, PY_ERROR_SERVER_FLAGS


def _track_stmt_db_name(st: _SAState, sql: str) -> None:
    if not is_use(sql):
        return
    db = _parse_use_db(sql)
    if db:
        st.stmt_db_name = db


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


def _should_capture(st: _SAState, sql: str, *, force_call: bool) -> bool:
    if st.settings.capture_all:
        return True
    if is_use(sql):
        return True
    return (
        is_write(sql)
        or (is_call(sql) and st.settings.capture_callproc)
        or (is_ddl(sql) and st.settings.capture_ddl)
        or force_call
    )


def _params_or_none(st: _SAState, params: Any) -> Optional[List[str]]:
    if not st.settings.include_params:
        return None
    try:
        return params_to_query_params(params, st.settings)
    except Exception:
        return None


def _build_message(
    *,
    st: _SAState,
    timestamp_ms: int,
    duration_ns: Optional[int],
    update_count: Optional[int],
    sql: str,
    query_params: Optional[List[str]],
    server_info: Optional[str],
    iflags: int,
    error: Optional[BaseException],
) -> SqlLogMessage:
    return SqlLogMessage(
        timestamp=timestamp_ms,
        serverHost=st.server_host,
        serverVersion=st.server_version,
        user=st.user,
        client=st.client,
        dbName=st.db_name,
        stmtDbName=st.stmt_db_name or st.db_name,
        debug=st.settings.inline_debug_value,
        connectionId=st.connection_id,
        totalPoolCount=None,
        executionCount=st.execution_count,
        serverFlags=st.cached_server_flags,
        clientFlags=st.client_flags,
        iFlags=iflags,
        defaultTZ=st.default_tz,
        serverTZ=st.server_tz,
        isolationLvl=st.isolation_lvl,
        durationNs=duration_ns,
        updateCount=update_count,
        sql=(sql if st.settings.include_sql else None),
        queryParams=query_params,
        errorMessage=_safe_str(error),
        serverInfo=server_info,
    )


def _publish_best_effort(st: _SAState, msg: SqlLogMessage) -> None:
    try:
        st.publisher.publish(msg)
    except Exception:
        return


def _buffer_or_publish(st: _SAState, msg: SqlLogMessage) -> None:
    if st.settings.buffer_until_commit:
        st.buffer.append(msg)
        return
    _publish_best_effort(st, msg)


def handle_error(exception_context: Any) -> None:
    """SQLAlchemy event hook for DBAPI errors.

    Safe to register directly: pulls state from
    exception_context.connection.info["mysql_interceptor_state"].
    """
    try:
        sa_conn = getattr(exception_context, "connection", None)
        st: Optional[_SAState] = getattr(sa_conn, "info", {}).get("mysql_interceptor_state") if sa_conn else None  # type: ignore[attr-defined]
        if not st:
            return

        statement = getattr(exception_context, "statement", None)
        if statement is None:
            return
        sql = str(statement)

        _track_stmt_db_name(st, sql)

        if not _should_capture(st, sql, force_call=False):
            st.execution_count += 1
            return

        exec_ctx = getattr(exception_context, "execution_context", None)
        t0 = getattr(exec_ctx, "_mi_t0", None)
        duration_ns: Optional[int] = (time.perf_counter_ns() - t0) if isinstance(t0, int) else None

        end_ms = time.time_ns() // 1_000_000
        timestamp_ms = end_ms - (duration_ns // 1_000_000) if duration_ns is not None else end_ms

        cursor = getattr(exception_context, "cursor", None)
        dbapi_conn = _get_dbapi_conn_from_sa_connection(sa_conn) if sa_conn is not None else None
        server_info, had_err = extract_server_info_best_effort(cursor, dbapi_conn)
        iflags_extra = PY_ERROR_SERVER_INFO if had_err else 0

        server_flags, _ = _compute_server_flags(sa_conn)
        st.cached_server_flags = server_flags
        iflags = st.base_iflags | iflags_extra

        query_params = _params_or_none(st, getattr(exception_context, "parameters", None))

        original = getattr(exception_context, "original_exception", None)
        if original is None:
            original = getattr(exception_context, "original", None)

        st.execution_count += 1
        msg = _build_message(
            st=st,
            timestamp_ms=timestamp_ms,
            duration_ns=duration_ns,
            update_count=None,
            sql=sql,
            query_params=query_params,
            server_info=server_info,
            iflags=iflags,
            error=original,
        )
        _publish_best_effort(st, msg)
    except Exception:
        return

