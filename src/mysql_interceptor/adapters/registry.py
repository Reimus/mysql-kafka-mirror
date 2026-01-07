from __future__ import annotations

from typing import Dict

from .base import DriverAdapter
from ..errors import DriverAdapterError


_ADAPTERS: Dict[str, DriverAdapter] = {}


def register_adapter(adapter: DriverAdapter) -> None:
    """Register a DB driver adapter.

    This is intentionally simple (a module-level registry) so new drivers can be
    added without changing the public API.
    """

    name = getattr(adapter, "name", None)
    if not isinstance(name, str) or not name:
        raise DriverAdapterError("Adapter must define a non-empty 'name' attribute")
    _ADAPTERS[name] = adapter


def get_adapter(driver: str) -> DriverAdapter:
    try:
        return _ADAPTERS[driver]
    except KeyError as e:
        raise DriverAdapterError(f"Unsupported driver: {driver!r}") from e


def _ensure_default_adapters_registered() -> None:
    """Register built-in adapters once."""
    if _ADAPTERS:
        return
    from .pymysql import PyMySQLAdapter

    register_adapter(PyMySQLAdapter())


def get_adapter_with_defaults(driver: str) -> DriverAdapter:
    _ensure_default_adapters_registered()
    return get_adapter(driver)
