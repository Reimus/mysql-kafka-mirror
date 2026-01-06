from __future__ import annotations

import socket


def hostname() -> str:
    try:
        return socket.gethostname()
    except Exception:
        return "unknown-host"
