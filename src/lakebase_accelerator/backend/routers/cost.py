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
