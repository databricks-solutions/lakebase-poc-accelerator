"""Actual Lakebase spend from ``system.billing.usage``.

Lakebase bills two SKU families under ``billing_origin_product = 'LAKEBASE'``:
compute (``%_DATABASE_SERVERLESS_COMPUTE_%``, in DBUs) and storage
(``%_DATABRICKS_STORAGE_%``, in DSUs). Storage further breaks down by
``product_features.lakebase.storage_type`` into logical branch data, PITR history,
and expiring-branch change storage. Joining ``system.billing.list_prices`` (the row
with ``price_end_time IS NULL`` is the current list price) turns usage into dollars.

Usage is keyed on ``usage_metadata.project_id``, which is the project's physical uid
(see ``lakebase_service.resolve_project_uid``). The query runs on a SQL warehouse via
``statement_execution`` using only the ``sql`` scope; results are still governed by the
caller's access to the billing system tables.

Note: costs are computed from *list* prices — they do not reflect account-level
discounts or the Lakebase compute promotion, so treat them as an upper-bound estimate.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from databricks.sdk import WorkspaceClient

# A project uid is a UUID; validated before interpolation into the billing query.
_UID_RE = re.compile(r"^[0-9a-fA-F-]{8,64}$")


@dataclass
class CostDay:
    usage_date: str
    compute_dbus: float
    compute_cost: float
    branch_storage_dsu: float
    pitr_storage_dsu: float
    expiring_storage_dsu: float
    storage_dsu: float
    storage_cost: float
    total_cost: float


@dataclass
class CostReport:
    project_uid: str
    days: int
    rows: list[CostDay] = field(default_factory=list)
    compute_cost: float = 0.0
    storage_cost: float = 0.0
    total_cost: float = 0.0


def _cost_query(project_uid: str, days: int) -> str:
    # project_uid is regex-validated and days is an int, so both are safe to
    # interpolate. Cost = SUM(usage * list_price) so multiple regional SKUs (each
    # with its own price) roll up correctly into a single daily figure.
    return f"""
WITH compute_costs AS (
  SELECT u.usage_date AS d,
         ROUND(SUM(u.usage_quantity), 4) AS compute_dbus,
         ROUND(SUM(u.usage_quantity * p.pricing.default), 2) AS compute_cost
  FROM system.billing.usage u
  JOIN system.billing.list_prices p
    ON u.sku_name = p.sku_name AND p.price_end_time IS NULL
  WHERE u.billing_origin_product = 'LAKEBASE'
    AND u.sku_name ILIKE '%_DATABASE_SERVERLESS_COMPUTE_%'
    AND u.usage_metadata.project_id = '{project_uid}'
    AND u.usage_date >= current_date() - INTERVAL {days} DAYS
  GROUP BY u.usage_date
),
storage_costs AS (
  SELECT u.usage_date AS d,
         ROUND(SUM(CASE WHEN u.product_features.lakebase.storage_type = 'BRANCH_DATA_STORAGE' THEN u.usage_quantity ELSE 0 END), 2) AS branch_dsu,
         ROUND(SUM(CASE WHEN u.product_features.lakebase.storage_type = 'BRANCH_HISTORY_STORAGE' THEN u.usage_quantity ELSE 0 END), 2) AS pitr_dsu,
         ROUND(SUM(CASE WHEN u.product_features.lakebase.storage_type = 'BRANCH_CHANGE_STORAGE' THEN u.usage_quantity ELSE 0 END), 4) AS expiring_dsu,
         ROUND(SUM(u.usage_quantity), 2) AS storage_dsu,
         ROUND(SUM(u.usage_quantity * p.pricing.default), 2) AS storage_cost
  FROM system.billing.usage u
  JOIN system.billing.list_prices p
    ON u.sku_name = p.sku_name AND p.price_end_time IS NULL
  WHERE u.billing_origin_product = 'LAKEBASE'
    AND u.sku_name ILIKE '%_DATABRICKS_STORAGE_%'
    AND u.usage_metadata.project_id = '{project_uid}'
    AND u.usage_date >= current_date() - INTERVAL {days} DAYS
  GROUP BY u.usage_date
)
SELECT CAST(COALESCE(c.d, s.d) AS STRING) AS usage_date,
       COALESCE(c.compute_dbus, 0) AS compute_dbus,
       COALESCE(c.compute_cost, 0) AS compute_cost,
       COALESCE(s.branch_dsu, 0) AS branch_storage_dsu,
       COALESCE(s.pitr_dsu, 0) AS pitr_storage_dsu,
       COALESCE(s.expiring_dsu, 0) AS expiring_storage_dsu,
       COALESCE(s.storage_dsu, 0) AS storage_dsu,
       COALESCE(s.storage_cost, 0) AS storage_cost,
       ROUND(COALESCE(c.compute_cost, 0) + COALESCE(s.storage_cost, 0), 2) AS total_cost
FROM compute_costs c
FULL OUTER JOIN storage_costs s ON c.d = s.d
ORDER BY usage_date
""".strip()


def _f(v: object) -> float:
    # statement_execution returns cells as strings; str() also neutralizes None.
    try:
        return float(str(v))
    except (TypeError, ValueError):
        return 0.0


def get_lakebase_cost(
    ws: WorkspaceClient, project_uid: str, warehouse_id: str, days: int = 30
) -> CostReport:
    """Return per-day Lakebase compute + storage cost for a project over ``days``.

    ``project_uid`` must be the project's physical uid (resolve via
    ``lakebase_service.resolve_project_uid``). Raises on an invalid uid or a failed
    statement; the router degrades those into an error field for the UI.
    """
    if not _UID_RE.match(project_uid):
        raise ValueError(f"'{project_uid}' is not a valid project uid.")
    days = max(1, min(int(days), 365))
    if not warehouse_id:
        raise ValueError("A SQL warehouse is required to query billing usage.")

    resp = ws.statement_execution.execute_statement(
        statement=_cost_query(project_uid, days),
        warehouse_id=warehouse_id,
        wait_timeout="50s",
    )
    state = str(resp.status.state) if resp.status and resp.status.state else "UNKNOWN"
    if "SUCCEEDED" not in state:
        err = resp.status.error.message if resp.status and resp.status.error else state
        raise RuntimeError(f"Billing query failed ({state}): {err}")

    report = CostReport(project_uid=project_uid, days=days)
    for row in (resp.result.data_array if resp.result else None) or []:
        day = CostDay(
            usage_date=str(row[0]) if row[0] is not None else "",
            compute_dbus=_f(row[1]),
            compute_cost=_f(row[2]),
            branch_storage_dsu=_f(row[3]),
            pitr_storage_dsu=_f(row[4]),
            expiring_storage_dsu=_f(row[5]),
            storage_dsu=_f(row[6]),
            storage_cost=_f(row[7]),
            total_cost=_f(row[8]),
        )
        report.rows.append(day)
        report.compute_cost += day.compute_cost
        report.storage_cost += day.storage_cost
        report.total_cost += day.total_cost

    report.compute_cost = round(report.compute_cost, 2)
    report.storage_cost = round(report.storage_cost, 2)
    report.total_cost = round(report.total_cost, 2)
    return report
