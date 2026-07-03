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
# ISO-8601 instant as produced by JS Date.toISOString(), validated before interpolation.
_ISO_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$")

# 1 CU running for 1 hour bills 0.213 DBU (see the Lakebase pricing FAQ).
DBU_PER_CU_HOUR = 0.213
# List price fallback ($/CU-hr, AWS us-east-1) when a project has no compute usage
# history yet to read a region-specific price from.
DEFAULT_PRICE_PER_CU_HOUR = 0.111


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


def _run_sql(ws: WorkspaceClient, statement: str, warehouse_id: str) -> list[list]:
    """Run a statement on a warehouse and return its data rows (or raise)."""
    resp = ws.statement_execution.execute_statement(
        statement=statement, warehouse_id=warehouse_id, wait_timeout="50s"
    )
    state = str(resp.status.state) if resp.status and resp.status.state else "UNKNOWN"
    if "SUCCEEDED" not in state:
        err = resp.status.error.message if resp.status and resp.status.error else state
        raise RuntimeError(f"Billing query failed ({state}): {err}")
    return (resp.result.data_array if resp.result else None) or []


# --- Per-benchmark-run cost attribution -------------------------------------
#
# Two ways to price a benchmark run, both Lakebase compute only (storage is a
# function of data size, not the run, and bills daily — not run-attributable):
#   * modeled — CU x $/CU-hr x duration; deterministic and available immediately.
#   * reconciled — the run's share of actual billed compute, proportionally
#     allocated across the 10-minute usage buckets that overlap the run window.
#     Accurate but lags (usage lands minutes-to-hours later) and is coarse.


@dataclass
class RunCostEstimate:
    price_per_cu_hour: float
    price_source: str  # "list_prices" | "default"
    cu: float
    duration_seconds: float
    total_queries: int
    discount: float
    cost: float
    cost_per_million_queries: float | None
    queries_per_dollar: float | None


@dataclass
class RunCostReconcile:
    available: bool
    allocated_dbu: float
    cu_hours: float
    effective_avg_cu: float | None
    cost: float
    cost_per_million_queries: float | None
    queries_per_dollar: float | None
    buckets: int
    note: str


def _normalized(cost: float, total_queries: int) -> tuple[float | None, float | None]:
    """Return (cost per 1M queries, queries per dollar) — the cross-system metrics."""
    if cost <= 0:
        return None, None
    cpm = cost / (total_queries / 1_000_000) if total_queries > 0 else None
    qpd = total_queries / cost
    return (round(cpm, 4) if cpm is not None else None, round(qpd, 0))


def compute_price_per_cu_hour(
    ws: WorkspaceClient, project_uid: str, warehouse_id: str
) -> tuple[float, str]:
    """Look up the project's current compute list price, as $/CU-hr.

    Reads the most recent compute usage row for the project to pick the right
    regional SKU, joined to its current list price. Falls back to the AWS us-east-1
    list price when the project has no compute history yet.
    """
    stmt = f"""
SELECT p.pricing.default AS price_per_dbu
FROM system.billing.usage u
JOIN system.billing.list_prices p
  ON u.sku_name = p.sku_name AND p.price_end_time IS NULL
WHERE u.billing_origin_product = 'LAKEBASE'
  AND u.usage_metadata.project_id = '{project_uid}'
  AND u.sku_name ILIKE '%_DATABASE_SERVERLESS_COMPUTE_%'
ORDER BY u.usage_date DESC
LIMIT 1
""".strip()
    rows = _run_sql(ws, stmt, warehouse_id)
    if rows and rows[0] and rows[0][0] is not None:
        return round(_f(rows[0][0]) * DBU_PER_CU_HOUR, 6), "list_prices"
    return DEFAULT_PRICE_PER_CU_HOUR, "default"


def estimate_run_cost(
    *,
    price_per_cu_hour: float,
    price_source: str,
    cu: float,
    duration_seconds: float,
    total_queries: int,
    discount: float = 0.0,
) -> RunCostEstimate:
    """Modeled Lakebase compute cost for a run: CU x $/CU-hr x duration."""
    hours = max(duration_seconds, 0.0) / 3600.0
    cost = round(cu * price_per_cu_hour * hours * (1.0 - discount), 6)
    cpm, qpd = _normalized(cost, total_queries)
    return RunCostEstimate(
        price_per_cu_hour=price_per_cu_hour,
        price_source=price_source,
        cu=cu,
        duration_seconds=duration_seconds,
        total_queries=total_queries,
        discount=discount,
        cost=cost,
        cost_per_million_queries=cpm,
        queries_per_dollar=qpd,
    )


def reconcile_run_cost(
    ws: WorkspaceClient,
    *,
    project_uid: str,
    warehouse_id: str,
    start_iso: str,
    end_iso: str,
    total_queries: int,
    price_per_cu_hour: float,
    duration_seconds: float,
    discount: float = 0.0,
) -> RunCostReconcile:
    """The run's share of *actual* billed compute over its [start, end] window.

    Each 10-minute usage bucket that overlaps the run is allocated proportionally
    to the fraction of the bucket that falls inside the window, so a short run isn't
    charged for a full bucket's idle time.
    """
    if not (_ISO_RE.match(start_iso) and _ISO_RE.match(end_iso)):
        raise ValueError("start/end must be ISO-8601 instants (…Z).")
    price_per_dbu = price_per_cu_hour / DBU_PER_CU_HOUR
    stmt = f"""
SELECT
  ROUND(SUM(u.usage_quantity *
    GREATEST(0, unix_timestamp(LEAST(u.usage_end_time, TIMESTAMP'{end_iso}'))
                - unix_timestamp(GREATEST(u.usage_start_time, TIMESTAMP'{start_iso}')))
    / NULLIF(unix_timestamp(u.usage_end_time) - unix_timestamp(u.usage_start_time), 0)
  ), 6) AS allocated_dbu,
  COUNT(*) AS buckets
FROM system.billing.usage u
WHERE u.billing_origin_product = 'LAKEBASE'
  AND u.sku_name ILIKE '%_DATABASE_SERVERLESS_COMPUTE_%'
  AND u.usage_metadata.project_id = '{project_uid}'
  AND u.usage_start_time < TIMESTAMP'{end_iso}'
  AND u.usage_end_time > TIMESTAMP'{start_iso}'
""".strip()
    rows = _run_sql(ws, stmt, warehouse_id)
    allocated_dbu = _f(rows[0][0]) if rows and rows[0] else 0.0
    buckets = int(_f(rows[0][1])) if rows and rows[0] and len(rows[0]) > 1 else 0

    if buckets == 0 or allocated_dbu <= 0:
        return RunCostReconcile(
            available=False, allocated_dbu=0.0, cu_hours=0.0, effective_avg_cu=None,
            cost=0.0, cost_per_million_queries=None, queries_per_dollar=None, buckets=0,
            note=(
                "No billed compute found for this window yet. Usage lands minutes-to-hours "
                "after a run — try reconciling again later."
            ),
        )

    cost = round(allocated_dbu * price_per_dbu * (1.0 - discount), 6)
    cu_hours = round(allocated_dbu / DBU_PER_CU_HOUR, 6)
    hours = max(duration_seconds, 0.0) / 3600.0
    eff_cu = round(cu_hours / hours, 3) if hours > 0 else None
    cpm, qpd = _normalized(cost, total_queries)
    return RunCostReconcile(
        available=True, allocated_dbu=round(allocated_dbu, 6), cu_hours=cu_hours,
        effective_avg_cu=eff_cu, cost=cost, cost_per_million_queries=cpm,
        queries_per_dollar=qpd, buckets=buckets,
        note=(
            f"Proportionally allocated across {buckets} ten-minute billing bucket(s). "
            "Coarse for short runs and may include adjacent activity on the same instance."
        ),
    )


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
