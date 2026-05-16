"""DAG platform inference and connection-to-DAG matching rules."""
from __future__ import annotations

CONN_ID_TEMPLATES: dict[str, str] = {
    "wb": "wb_api_token_{slug}",
    "oz_seller": "oz_api_token_{slug}",
    "oz_perf": "oz_client_perf_id_{slug}",
    "ms": "ms_api_token_{slug}",
    "ym": "ym_api_token_{slug}",
    "amo": "amocrm_api_token_{slug}",
    "bitrix": "bitrix_{slug}",
    "iiko": "iiko_{slug}",
}

CONN_TYPE_BY_PLATFORM: dict[str, tuple[str, ...]] = {
    "wb": ("wb",),
    "oz": ("oz_seller", "oz_perf"),
    "ms": ("ms",),
    "ym": ("ym",),
    "amo": ("amo",),
    "bitrix": ("bitrix",),
    "iiko": ("iiko",),
}

PLATFORM_BY_CONN_TYPE: dict[str, str] = {
    "wb": "wb",
    "oz_seller": "oz",
    "oz_perf": "oz",
    "ms": "ms",
    "ym": "ym",
    "amo": "amo",
    "bitrix": "bitrix",
    "iiko": "iiko",
    "other": "ym",
}

MARKETPLACE_CONN_TYPES = frozenset(CONN_ID_TEMPLATES.keys())

LEGACY_CONN_TYPE_MAP: dict[str, str] = {
    "wb_token": "wb",
    "oz_token": "oz_seller",
    "oz_performance": "oz_perf",
    "ms_token": "ms",
    "other": "ym",
}

CONN_ID_TYPE_PREFIXES: tuple[tuple[str, str], ...] = (
    ("wb_api_token_", "wb"),
    ("oz_api_token_", "oz_seller"),
    ("oz_client_perf_id_", "oz_perf"),
    ("ms_api_token_", "ms"),
    ("ym_api_token_", "ym"),
    ("amocrm_api_token_", "amo"),
    ("bitrix_", "bitrix"),
    ("iiko_", "iiko"),
)

COMPANION_CONN_PREFIXES: tuple[str, ...] = (
    "oz_client_seller_id_",
    "oz_client_perf_secret_",
)


def is_perf_dag(dag_id: str) -> bool:
    return dag_id.startswith("oz_adv_")


def infer_dag_platform(dag_id: str) -> str | None:
    if dag_id.startswith("wb_"):
        return "wb"
    if dag_id.startswith("ms_"):
        return "ms"
    if dag_id.startswith("oz_"):
        return "oz"
    if dag_id.startswith("amo_"):
        return "amo"
    if dag_id.startswith("ym_"):
        return "ym"
    if dag_id.startswith("bitrix_"):
        return "bitrix"
    if dag_id.startswith("iiko_"):
        return "iiko"
    return None


def build_conn_id(conn_type: str, slug: str) -> str:
    template = CONN_ID_TEMPLATES.get(conn_type)
    if not template:
        return slug
    return template.format(slug=slug)


def companion_conn_ids(conn_type: str, slug: str) -> list[str]:
    if conn_type == "oz_seller":
        return [f"oz_client_seller_id_{slug}"]
    if conn_type == "oz_perf":
        return [f"oz_client_perf_secret_{slug}"]
    return []


def is_companion_conn_id(conn_id: str, extra_meta: dict | None = None) -> bool:
    if extra_meta and extra_meta.get("is_companion"):
        return True
    return any(conn_id.startswith(prefix) for prefix in COMPANION_CONN_PREFIXES)


def infer_conn_type_from_conn_id(conn_id: str) -> str | None:
    for prefix, conn_type in CONN_ID_TYPE_PREFIXES:
        if conn_id.startswith(prefix):
            return conn_type
    if conn_id.startswith("wb_"):
        return "wb"
    if conn_id.startswith("oz_client_perf_secret_"):
        return "oz_perf"
    if conn_id.startswith("oz_client_seller_id_"):
        return "oz_seller"
    if conn_id.startswith("oz_"):
        return "oz_seller"
    if conn_id.startswith("ms_"):
        return "ms"
    if conn_id.startswith("amocrm_"):
        return "amo"
    return None


def infer_slug_from_conn_id(conn_id: str, conn_type: str) -> str:
    for prefix, _ in CONN_ID_TYPE_PREFIXES:
        if conn_id.startswith(prefix):
            return conn_id[len(prefix) :]
    for prefix in COMPANION_CONN_PREFIXES:
        if conn_id.startswith(prefix):
            return conn_id[len(prefix) :]
    platform = PLATFORM_BY_CONN_TYPE.get(conn_type)
    if platform and conn_id.startswith(f"{platform}_"):
        return conn_id[len(platform) + 1 :]
    return ""


def normalize_conn_type(conn_id: str, conn_type: str) -> str:
    ct = (conn_type or "").strip()
    if ct in MARKETPLACE_CONN_TYPES:
        return ct
    legacy = LEGACY_CONN_TYPE_MAP.get(ct)
    if legacy:
        return legacy
    inferred = infer_conn_type_from_conn_id(conn_id)
    if inferred:
        return inferred
    return ct or "ym"


def infer_connection_profile(
    conn_id: str,
    conn_type: str = "",
    extra_meta: dict | None = None,
) -> dict[str, str | bool]:
    meta = extra_meta or {}
    if is_companion_conn_id(conn_id, meta):
        return {"is_companion": True, "conn_type": "", "platform": "", "slug": ""}

    normalized = normalize_conn_type(conn_id, conn_type)
    platform = meta.get("platform") or PLATFORM_BY_CONN_TYPE.get(normalized, "")
    slug = meta.get("slug") or infer_slug_from_conn_id(conn_id, normalized)
    return {
        "is_companion": False,
        "conn_type": normalized,
        "platform": platform,
        "slug": slug,
    }


def conn_matches_dag(conn_type: str, dag_platform: str | None, dag_id: str, conn_id: str = "") -> bool:
    if not dag_platform:
        return False

    effective_type = normalize_conn_type(conn_id, conn_type) if conn_id else conn_type
    conn_plat = PLATFORM_BY_CONN_TYPE.get(effective_type)
    if not conn_plat or conn_plat != dag_platform:
        return False

    if effective_type == "oz_perf":
        return is_perf_dag(dag_id)

    if effective_type == "oz_seller" and is_perf_dag(dag_id):
        return False

    return True


def default_conn_type_for_platform(platform: str) -> str:
    types = CONN_TYPE_BY_PLATFORM.get(platform, ())
    return types[0] if types else "ym"
