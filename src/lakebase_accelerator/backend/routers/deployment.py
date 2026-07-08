"""Deployment endpoints: autoscaling sizing recommendation + (next) project /
synced-table provisioning via ``w.postgres``.

Currently implements the pure-computation sizing recommendation. Project/branch/
endpoint creation and synced-table provisioning are added next (they create real,
billable resources, so they are built and verified deliberately).
"""

from __future__ import annotations

from pydantic import BaseModel, Field

from ..core import create_router, logger
from ..deps import EffectiveClient
from ..services import deployment, sizing

router = create_router()


class SizingIn(BaseModel):
    bulk_writes_per_second: float = Field(default=0, ge=0)
    continuous_writes_per_second: float = Field(default=0, ge=0)
    reads_per_second: float = Field(default=0, ge=0)


class SizingOut(BaseModel):
    bulk_cu: float
    continuous_cu: float
    read_cu: float
    total_cu: float
    recommended_min_cu: float
    recommended_max_cu: float
    rationale: str


@router.post(
    "/deployment/recommend-size",
    response_model=SizingOut,
    operation_id="recommendSize",
)
def recommend_size(req: SizingIn) -> SizingOut:
    """Recommend an autoscaling min/max CU range for the described workload."""
    r = sizing.recommend_cu(
        bulk_writes_per_second=req.bulk_writes_per_second,
        continuous_writes_per_second=req.continuous_writes_per_second,
        reads_per_second=req.reads_per_second,
    )
    return SizingOut(
        bulk_cu=r.bulk_cu,
        continuous_cu=r.continuous_cu,
        read_cu=r.read_cu,
        total_cu=r.total_cu,
        recommended_min_cu=r.recommended_min_cu,
        recommended_max_cu=r.recommended_max_cu,
        rationale=r.rationale,
    )


# --- New-project creation ----------------------------------------------------


class CreateProjectIn(BaseModel):
    project_id: str = Field(min_length=1, max_length=63)
    display_name: str = Field(min_length=1)
    min_cu: float = Field(ge=0.5, le=32)
    max_cu: float = Field(ge=0.5, le=32)
    pg_version: int = 16


class CreateProjectOut(BaseModel):
    ok: bool
    name: str | None = None
    detail: str


@router.post(
    "/deployment/create-project",
    response_model=CreateProjectOut,
    operation_id="createProject",
)
def create_project(req: CreateProjectIn, ws: EffectiveClient) -> CreateProjectOut:
    """Create a new Lakebase Autoscaling project (real, billable resource)."""
    if req.max_cu < req.min_cu:
        return CreateProjectOut(ok=False, detail="max_cu must be ≥ min_cu")
    if req.max_cu - req.min_cu > 8:
        return CreateProjectOut(ok=False, detail="Autoscale range (max − min) cannot exceed 8 CU")
    try:
        name = deployment.create_project(
            ws,
            project_id=req.project_id,
            display_name=req.display_name,
            min_cu=req.min_cu,
            max_cu=req.max_cu,
            pg_version=req.pg_version,
        )
        return CreateProjectOut(ok=True, name=name, detail=f"Created project {name} ({req.min_cu}–{req.max_cu} CU)")
    except Exception as e:  # noqa: BLE001
        logger.info(f"create_project failed: {e}")
        return CreateProjectOut(ok=False, detail=str(e))


# --- Sync requirement check --------------------------------------------------


class WarehouseOut(BaseModel):
    id: str
    name: str
    state: str | None = None


class WarehouseListOut(BaseModel):
    warehouses: list[WarehouseOut] = []
    error: str | None = None


@router.get(
    "/deployment/warehouses",
    response_model=WarehouseListOut,
    operation_id="listWarehouses",
)
def list_warehouses(ws: EffectiveClient) -> WarehouseListOut:
    """List SQL warehouses the caller can access (for the CDF requirement check)."""
    try:
        return WarehouseListOut(
            warehouses=[
                WarehouseOut(id=w.id, name=w.name, state=w.state)
                for w in deployment.list_warehouses(ws)
            ]
        )
    except Exception as e:  # noqa: BLE001
        logger.info(f"Could not list warehouses: {e}")
        return WarehouseListOut(warehouses=[], error=str(e))


class SyncCheckIn(BaseModel):
    source_table_full_name: str
    scheduling_policy: str = "SNAPSHOT"
    warehouse_id: str | None = None


class SyncCheckOut(BaseModel):
    ok: bool
    verified: bool
    cdf_enabled: bool
    table_exists: bool
    message: str
    enable_cdf_sql: str | None = None


@router.post(
    "/deployment/check-sync",
    response_model=SyncCheckOut,
    operation_id="checkSyncRequirements",
)
def check_sync_requirements(req: SyncCheckIn, ws: EffectiveClient) -> SyncCheckOut:
    """Check whether a source Delta table meets the chosen sync mode's requirements."""
    r = deployment.check_sync_requirements(
        ws, req.source_table_full_name, req.scheduling_policy, req.warehouse_id
    )
    return SyncCheckOut(
        ok=r.ok, verified=r.verified, cdf_enabled=r.cdf_enabled, table_exists=r.table_exists,
        message=r.message, enable_cdf_sql=r.enable_cdf_sql,
    )


class TableSizeIn(BaseModel):
    table_full_name: str
    warehouse_id: str


class TableSizeOut(BaseModel):
    ok: bool
    uncompressed_bytes: int
    size_mb: float
    message: str


@router.post(
    "/deployment/table-size",
    response_model=TableSizeOut,
    operation_id="getTableSize",
)
def get_table_size(req: TableSizeIn, ws: EffectiveClient) -> TableSizeOut:
    """Estimate a source Delta table's uncompressed size (for Lakebase storage sizing)."""
    try:
        r = deployment.get_table_uncompressed_size(ws, req.table_full_name, req.warehouse_id)
        return TableSizeOut(
            ok=r.ok, uncompressed_bytes=r.uncompressed_bytes, size_mb=r.size_mb, message=r.message,
        )
    except Exception as e:  # noqa: BLE001
        logger.info(f"get_table_size failed for {req.table_full_name}: {e}")
        return TableSizeOut(ok=False, uncompressed_bytes=0, size_mb=0.0, message=str(e))


# --- Existing-project inspection + provisioning ------------------------------


class EndpointInfoOut(BaseModel):
    name: str
    endpoint_type: str | None = None
    host: str | None = None
    min_cu: float | None = None
    max_cu: float | None = None
    state: str | None = None


class ProjectInfoOut(BaseModel):
    name: str
    branch: str | None = None
    endpoints: list[EndpointInfoOut] = []
    error: str | None = None


@router.get(
    "/deployment/project-info",
    response_model=ProjectInfoOut,
    operation_id="getProjectInfo",
)
def get_project_info(ws: EffectiveClient, project: str) -> ProjectInfoOut:
    """Inspect an existing autoscaling project: branch, endpoints, and current CU range."""
    if not project.strip():
        return ProjectInfoOut(name="", error="project is required")
    try:
        info = deployment.get_project_info(ws, project)
        return ProjectInfoOut(
            name=info.name,
            branch=info.branch,
            endpoints=[
                EndpointInfoOut(
                    name=e.name, endpoint_type=e.endpoint_type, host=e.host,
                    min_cu=e.min_cu, max_cu=e.max_cu, state=e.state,
                )
                for e in info.endpoints
            ],
        )
    except Exception as e:  # noqa: BLE001
        logger.info(f"get_project_info failed for {project}: {e}")
        return ProjectInfoOut(name=project, error=str(e))


class SetCuIn(BaseModel):
    endpoint_name: str
    min_cu: float = Field(ge=0.5)
    max_cu: float = Field(ge=0.5)


class OpResultOut(BaseModel):
    ok: bool
    detail: str


@router.post("/deployment/set-cu", response_model=OpResultOut, operation_id="setEndpointCu")
def set_endpoint_cu(req: SetCuIn, ws: EffectiveClient) -> OpResultOut:
    """Update an existing endpoint's autoscaling min/max CU range."""
    try:
        deployment.set_endpoint_cu(ws, req.endpoint_name, req.min_cu, req.max_cu)
        return OpResultOut(ok=True, detail=f"Updated {req.endpoint_name} to {req.min_cu}-{req.max_cu} CU")
    except Exception as e:  # noqa: BLE001
        logger.info(f"set_endpoint_cu failed: {e}")
        return OpResultOut(ok=False, detail=str(e))


class SyncTableIn(BaseModel):
    target_uc_name: str            # "{catalog}.{schema}.{table}" in the Lakebase catalog
    source_table_full_name: str    # source Delta "{catalog}.{schema}.{table}"
    primary_key_columns: list[str]
    scheduling_policy: str = "SNAPSHOT"
    # Optional for Lakebase Autoscaling — the platform auto-manages staging storage
    # when these are omitted. Only set to pin a new pipeline's staging location.
    storage_catalog: str | None = None
    storage_schema: str | None = None
    branch: str | None = None
    # Omit when targeting a database catalog (inferred); required for a standard catalog.
    database: str | None = None


@router.post("/deployment/sync", response_model=OpResultOut, operation_id="createSyncedTable")
def create_synced_table(req: SyncTableIn, ws: EffectiveClient) -> OpResultOut:
    """Create a synced (reverse-ETL) table from a Delta source into an existing project."""
    try:
        name = deployment.create_synced_table(
            ws,
            target_uc_name=req.target_uc_name,
            source_table_full_name=req.source_table_full_name,
            primary_key_columns=req.primary_key_columns,
            scheduling_policy=req.scheduling_policy,
            storage_catalog=req.storage_catalog,
            storage_schema=req.storage_schema,
            branch=req.branch,
            database=req.database,
        )
        return OpResultOut(ok=True, detail=f"Synced table created: {name}")
    except Exception as e:  # noqa: BLE001
        logger.info(f"create_synced_table failed: {e}")
        return OpResultOut(ok=False, detail=str(e))


class SyncStatusIn(BaseModel):
    # Three-part UC name (the target_uc_name / synced_table_id used at creation).
    target_uc_name: str


class SyncStatusOut(BaseModel):
    ok: bool
    name: str
    exists: bool = False
    detailed_state: str | None = None
    kind: str = "unknown"  # ok | syncing | failed | unknown
    pipeline_id: str | None = None
    last_sync_time: str | None = None
    message: str | None = None
    error: str | None = None


@router.post(
    "/deployment/sync-status",
    response_model=SyncStatusOut,
    operation_id="getSyncedTableStatus",
)
def get_synced_table_status(req: SyncStatusIn, ws: EffectiveClient) -> SyncStatusOut:
    """Read a synced table's live replication status (pipeline id, state, last sync)."""
    r = deployment.get_synced_table_status(ws, req.target_uc_name)
    return SyncStatusOut(
        ok=r.ok, name=r.name, exists=r.exists, detailed_state=r.detailed_state,
        kind=r.kind, pipeline_id=r.pipeline_id, last_sync_time=r.last_sync_time,
        message=r.message, error=r.error,
    )
