"""Serialize marketplace metadata in Airflow connection.extra JSON."""
from __future__ import annotations

import json
from typing import Any


_META_KEYS = ("platform", "slug", "display_name", "target_db_connection", "client")


def pack_extra(
    platform: str = "",
    slug: str = "",
    display_name: str = "",
    target_db_connection: str = "",
    existing_extra: str | None = None,
    **kwargs: Any,
) -> str:
    """Build extra JSON, merging with existing_extra when provided."""
    base: dict[str, Any] = {}
    if existing_extra:
        base.update(unpack_extra(existing_extra))

    updates = {
        "platform": platform,
        "slug": slug,
        "display_name": display_name,
        "target_db_connection": target_db_connection,
        **kwargs,
    }
    for key, value in updates.items():
        if value is not None and value != "":
            base[key] = value
        elif key in base and value == "":
            base.pop(key, None)

    if not base:
        return ""
    return json.dumps(base, ensure_ascii=False)


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
