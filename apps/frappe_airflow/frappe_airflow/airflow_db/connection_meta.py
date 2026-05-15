"""Serialize marketplace metadata in Airflow connection.extra JSON."""
from __future__ import annotations

import json
from typing import Any


def pack_extra(platform: str = "", slug: str = "", display_name: str = "", **kwargs: Any) -> str:
    payload = {k: v for k, v in {
        "platform": platform or "",
        "slug": slug or "",
        "display_name": display_name or "",
        **kwargs,
    }.items() if v}
    return json.dumps(payload, ensure_ascii=False) if payload else ""


def unpack_extra(raw: str | None) -> dict[str, str]:
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        return {}
    if not isinstance(data, dict):
        return {}
    return {k: str(v) for k, v in data.items() if v is not None}
