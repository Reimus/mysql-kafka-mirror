from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional

from .settings import Settings


def _truncate(value: str, max_len: int) -> str:
    if len(value) <= max_len:
        return value
    return value[: max(0, max_len - 3)] + "..."


def redact_params(params: Any, settings: Settings) -> Any:
    if params is None:
        return None

    if isinstance(params, Mapping):
        out: Dict[str, Any] = {}
        for k, v in params.items():
            key = str(k).lower()
            if any(rk in key for rk in settings.redact_keys):
                out[str(k)] = settings.redact_value
            else:
                out[str(k)] = redact_params(v, settings)
        return out

    if isinstance(params, (list, tuple)):
        return [redact_params(v, settings) for v in params]

    if isinstance(params, (bytes, bytearray, memoryview)):
        return f"<bytes:{len(params)}>"

    if isinstance(params, str):
        return _truncate(params, settings.max_param_length)

    return params


def params_to_query_params(params: Any, settings: Settings) -> Optional[List[str]]:
    redacted = redact_params(params, settings)
    if redacted is None:
        return None

    if isinstance(redacted, Mapping):
        return [_truncate(f"{k}={v}", settings.max_param_length) for k, v in redacted.items()]

    if isinstance(redacted, (list, tuple)):
        return [_truncate(str(v), settings.max_param_length) for v in redacted]

    return [_truncate(str(redacted), settings.max_param_length)]
