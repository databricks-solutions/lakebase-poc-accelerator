"""Lakebase cost endpoints: actual spend from ``system.billing.usage``.

Powers the Cost route, which shows a project's daily compute + storage cost over a
window. Degrades gracefully (empty rows + error string) so the UI can explain missing
access to the billing system tables rather than erroring out.
"""

from __future__ import annotations

from pydantic import BaseModel, Field

from ..core import create_router, logger
from ..deps import EffectiveClient
from ..services import cost, lakebase_service

router = create_router()


class CostDayOut(BaseModel):
    usage_date: str
    compute_dbus: float
    compute_cost: float
    branch_storage_dsu: float
    pitr_storage_dsu: float
    expiring_storage_dsu: float
    storage_dsu: float
    storage_cost: float
    total_cost: float


class CostUsageIn(BaseModel):
    # A project handle (id / display name); resolved server-side to the billing uid.
    project: str
    warehouse_id: str
    days: int = Field(default=30, ge=1, le=365)


class CostUsageOut(BaseModel):
    project_uid: str | None = None
    days: int
    rows: list[CostDayOut] = []
    compute_cost: float = 0.0
    storage_cost: float = 0.0
    total_cost: float = 0.0
    error: str | None = None


class RunCostIn(BaseModel):
    project: str
    warehouse_id: str
    cu: float = Field(gt=0)              # CU the benchmark ran at (pin min=max for accuracy)
    duration_seconds: float = Field(gt=0)
    total_queries: int = Field(ge=0)
    discount: float = Field(default=0.0, ge=0.0, le=1.0)  # e.g. 0.5 for the compute promo
    # Optional run window (ISO-8601 …Z) enabling billing reconciliation.
    start: str | None = None
    end: str | None = None


class RunCostEstimateOut(BaseModel):
    price_per_cu_hour: float
    price_source: str
    cu: float
    duration_seconds: float
    total_queries: int
    discount: float
    cost: float
    cost_per_million_queries: float | None = None
    queries_per_dollar: float | None = None


class RunCostReconcileOut(BaseModel):
    available: bool
    allocated_dbu: float
    cu_hours: float
    effective_avg_cu: float | None = None
    cost: float
    cost_per_million_queries: float | None = None
    queries_per_dollar: float | None = None
    buckets: int
    note: str


class RunCostOut(BaseModel):
    estimate: RunCostEstimateOut | None = None
    reconcile: RunCostReconcileOut | None = None
    error: str | None = None


@router.post("/cost/run", response_model=RunCostOut, operation_id="getRunCost")
def get_run_cost(req: RunCostIn, ws: EffectiveClient) -> RunCostOut:
    """Attribute Lakebase compute cost to a single benchmark run (modeled + reconciled)."""
    if not req.project.strip() or not req.warehouse_id.strip():
        return RunCostOut(error="project and warehouse_id are required")
    try:
        uid = lakebase_service.resolve_project_uid(ws, req.project)
        price_per_cu_hour, price_source = cost.compute_price_per_cu_hour(ws, uid, req.warehouse_id)
        est = cost.estimate_run_cost(
            price_per_cu_hour=price_per_cu_hour,
            price_source=price_source,
            cu=req.cu,
            duration_seconds=req.duration_seconds,
            total_queries=req.total_queries,
            discount=req.discount,
        )
        out = RunCostOut(estimate=RunCostEstimateOut(**est.__dict__))
        if req.start and req.end:
            rec = cost.reconcile_run_cost(
                ws,
                project_uid=uid,
                warehouse_id=req.warehouse_id,
                start_iso=req.start,
                end_iso=req.end,
                total_queries=req.total_queries,
                price_per_cu_hour=price_per_cu_hour,
                duration_seconds=req.duration_seconds,
                discount=req.discount,
            )
            out.reconcile = RunCostReconcileOut(**rec.__dict__)
        return out
    except Exception as e:  # noqa: BLE001
        logger.info(f"get_run_cost failed for {req.project}: {e}")
        return RunCostOut(error=str(e))


@router.post("/cost/usage", response_model=CostUsageOut, operation_id="getLakebaseCost")
def get_lakebase_cost(req: CostUsageIn, ws: EffectiveClient) -> CostUsageOut:
    """Return a project's daily Lakebase compute + storage cost (from list prices)."""
    if not req.project.strip():
        return CostUsageOut(days=req.days, error="project is required")
    if not req.warehouse_id.strip():
        return CostUsageOut(days=req.days, error="A SQL warehouse is required.")
    try:
        uid = lakebase_service.resolve_project_uid(ws, req.project)
        report = cost.get_lakebase_cost(ws, uid, req.warehouse_id, req.days)
        return CostUsageOut(
            project_uid=report.project_uid,
            days=report.days,
            rows=[
                CostDayOut(
                    usage_date=d.usage_date,
                    compute_dbus=d.compute_dbus,
                    compute_cost=d.compute_cost,
                    branch_storage_dsu=d.branch_storage_dsu,
                    pitr_storage_dsu=d.pitr_storage_dsu,
                    expiring_storage_dsu=d.expiring_storage_dsu,
                    storage_dsu=d.storage_dsu,
                    storage_cost=d.storage_cost,
                    total_cost=d.total_cost,
                )
                for d in report.rows
            ],
            compute_cost=report.compute_cost,
            storage_cost=report.storage_cost,
            total_cost=report.total_cost,
        )
    except Exception as e:  # noqa: BLE001
        logger.info(f"get_lakebase_cost failed for {req.project}: {e}")
        return CostUsageOut(days=req.days, error=str(e))
