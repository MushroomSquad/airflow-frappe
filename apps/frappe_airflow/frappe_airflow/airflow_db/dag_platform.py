"""DAG platform inference and connection-to-DAG matching rules."""
from __future__ import annotations

CONN_ID_TEMPLATES: dict[str, str] = {
    "wb": "wb_api_token_{slug}",
    "oz_seller": "oz_api_token_{slug}",
    "oz_perf": "oz_client_perf_id_{slug}",
    "ms": "ms_api_token_{slug}",
}

CONN_TYPE_BY_PLATFORM: dict[str, tuple[str, ...]] = {
    "wb": ("wb",),
    "oz": ("oz_seller", "oz_perf"),
    "ms": ("ms",),
    "ym": ("other",),
}

PLATFORM_BY_CONN_TYPE: dict[str, str] = {
    "wb": "wb",
    "oz_seller": "oz",
    "oz_perf": "oz",
    "ms": "ms",
    "other": "ym",
}

MARKETPLACE_CONN_TYPES = frozenset(CONN_ID_TEMPLATES.keys())


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


def conn_matches_dag(conn_type: str, dag_platform: str | None, dag_id: str) -> bool:
    if not dag_platform or dag_platform == "amo":
        return False

    conn_plat = PLATFORM_BY_CONN_TYPE.get(conn_type)
    if not conn_plat or conn_plat != dag_platform:
        return False

    if conn_type == "oz_perf":
        return is_perf_dag(dag_id)

    if conn_type == "oz_seller" and is_perf_dag(dag_id):
        return False

    return True


def default_conn_type_for_platform(platform: str) -> str:
    types = CONN_TYPE_BY_PLATFORM.get(platform, ())
    return types[0] if types else "other"
