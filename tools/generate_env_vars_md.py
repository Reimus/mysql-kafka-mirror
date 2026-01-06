#!/usr/bin/env python3
"""Generate docs/ENV_VARS.md from mysql_interceptor.config.settings.

Usage:
    python tools/generate_env_vars_md.py
"""

from __future__ import annotations

from pathlib import Path

from mysql_interceptor.config.settings import ENV_SPECS, Settings


def main() -> int:
    base = Settings()
    lines: list[str] = []
    lines.append("# Environment variables")
    lines.append("")
    lines.append("This file is generated from `Settings` + `ENV_SPECS`.")
    lines.append("")
    lines.append("| Env var | Field | Type | Default |")
    lines.append("|---|---|---|---|")

    for spec in ENV_SPECS:
        default = getattr(base, spec.field)
        default_str = "" if default is None else str(default)
        lines.append(f"| `{spec.env}` | `{spec.field}` | `{spec.kind}` | `{default_str}` |")

    out = Path(__file__).resolve().parents[1] / "docs" / "ENV_VARS.md"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
