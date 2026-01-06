from __future__ import annotations

import inspect
from typing import Any, Callable, Dict, Optional

from .config.settings import Settings
from .connect import connect, _build_default_kafka_publisher
from .kafka.publisher import Publisher, StdoutPublisher
from .sqlalchemy_interceptor import instrument_engine


def patch_pymysql(*, publisher: Optional[Publisher] = None, settings: Optional[Settings] = None) -> Callable[[], None]:
try:
    import pymysql  # type: ignore
except Exception as e:
    raise RuntimeError("pymysql is not installed. Install: pip install mysql-interceptor[pymysql]") from e

    settings = settings or Settings.from_env()
    original = pymysql.connect

    def _bind_connect_args(original_connect: Any, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Dict[str, Any]:
        sig = inspect.signature(original_connect)
        bound = sig.bind_partial(*args, **kwargs)
        bound.apply_defaults()
        out: Dict[str, Any] = {}
        for k, v in bound.arguments.items():
            if k == "kwargs" and isinstance(v, dict):
                out.update(v)
            else:
                out[k] = v
        return out

    def _patched_connect(*args: Any, **kwargs: Any):
        conn_kwargs = _bind_connect_args(original, args, kwargs)
        return connect(driver="pymysql", publisher=publisher, settings=settings, **conn_kwargs)

    pymysql.connect = _patched_connect  # type: ignore[attr-defined]

    def unpatch() -> None:
        pymysql.connect = original  # type: ignore[attr-defined]

    return unpatch


def patch_sqlalchemy(*, publisher: Optional[Publisher] = None, settings: Optional[Settings] = None) -> Callable[[], None]:
    """Monkeypatch sqlalchemy.create_engine() to auto-instrument created engines."""
    try:
        import sqlalchemy  # type: ignore
    except Exception as e:
        raise RuntimeError("sqlalchemy is not installed. Install mysql-interceptor[sqlalchemy].") from e

    settings = settings or Settings.from_env()

    if publisher is None and settings.kafka_bootstrap_servers:
        publisher = _build_default_kafka_publisher(settings)

    publisher = publisher or StdoutPublisher()

    original = sqlalchemy.create_engine

    def _patched_create_engine(*args: Any, **kwargs: Any):
        engine = original(*args, **kwargs)
        instrument_engine(engine=engine, publisher=publisher, settings=settings)
        return engine

    sqlalchemy.create_engine = _patched_create_engine  # type: ignore[attr-defined]

    def unpatch() -> None:
        sqlalchemy.create_engine = original  # type: ignore[attr-defined]

    return unpatch
