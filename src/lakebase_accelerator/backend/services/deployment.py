"""Deployment service for Lakebase Autoscaling via ``w.postgres``.

Scope (per user decision): reuse existing projects. Provides read-only project
inspection, endpoint autoscaling-CU updates, and synced-table (reverse-ETL)
creation. New-project creation is included for completeness but is not the
verified path.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import postgres as pg

from .lakebase_service import _resolve_project_name

SchedulingPolicy = pg.SyncedTableSyncedTableSpecSyncedTableSchedulingPolicy

# A three-part UC name: catalog.schema.table, each part [A-Za-z0-9_]+.
_TABLE_NAME_RE = re.compile(r"^[A-Za-z0-9_]+\.[A-Za-z0-9_]+\.[A-Za-z0-9_]+$")


@dataclass
class WarehouseInfo:
    id: str
    name: str
    state: Optional[str]


def list_warehouses(ws: WorkspaceClient) -> list[WarehouseInfo]:
    """List SQL warehouses the caller can access (used to run the CDF check)."""
    out: list[WarehouseInfo] = []
    for w in ws.warehouses.list():
        if w.id:
            out.append(WarehouseInfo(id=w.id, name=w.name or w.id, state=str(w.state) if w.state else None))
    return out


def _cdf_enabled_via_warehouse(ws: WorkspaceClient, warehouse_id: str, table: str) -> bool:
    """Read ``delta.enableChangeDataFeed`` via SHOW TBLPROPERTIES on a SQL warehouse.

    Uses only the ``sql`` OAuth scope (no Unity Catalog metadata scope needed); the
    read is still governed by the user's UC permissions. ``table`` must already be a
    validated three-part name to keep it safe to interpolate.
    """
    resp = ws.statement_execution.execute_statement(
        statement=f"SHOW TBLPROPERTIES {table}",
        warehouse_id=warehouse_id,
        wait_timeout="30s",
    )
    state = str(resp.status.state) if resp.status and resp.status.state else "UNKNOWN"
    if "SUCCEEDED" not in state:
        err = resp.status.error.message if resp.status and resp.status.error else state
        raise RuntimeError(f"SHOW TBLPROPERTIES failed ({state}): {err}")
    rows = (resp.result.data_array if resp.result else None) or []
    for row in rows:
        if row and len(row) >= 2 and row[0] == "delta.enableChangeDataFeed":
            return str(row[1]).strip().lower() == "true"
    return False


@dataclass
class EndpointInfo:
    name: str
    endpoint_type: Optional[str]
    host: Optional[str]
    min_cu: Optional[float]
    max_cu: Optional[float]
    state: Optional[str]


@dataclass
class ProjectInfo:
    name: str
    branch: Optional[str]
    endpoints: list[EndpointInfo]


def create_project(
    ws: WorkspaceClient,
    *,
    project_id: str,
    display_name: str,
    min_cu: float,
    max_cu: float,
    pg_version: int = 16,
) -> str:
    """Create a new Lakebase Autoscaling project.

    ``create_project`` provisions the project plus its default production branch and
    primary read-write endpoint, using the supplied autoscaling CU range as the
    default endpoint settings. Returns the new project's resource name.

    This creates a real, billable resource — only call it on explicit user action.
    """
    spec = pg.ProjectSpec(
        display_name=display_name,
        pg_version=pg_version,
        default_endpoint_settings=pg.ProjectDefaultEndpointSettings(
            autoscaling_limit_min_cu=min_cu,
            autoscaling_limit_max_cu=max_cu,
        ),
    )
    op = ws.postgres.create_project(project=pg.Project(spec=spec), project_id=project_id)
    result = op.wait()
    return result.name or project_id


@dataclass
class SyncRequirementCheck:
    ok: bool            # safe to proceed (requirement met, or no requirement)
    verified: bool      # whether we could actually read the table's CDF property
    cdf_enabled: bool
    table_exists: bool
    message: str
    enable_cdf_sql: Optional[str] = None


def check_sync_requirements(
    ws: WorkspaceClient,
    source_table_full_name: str,
    scheduling_policy: str,
    warehouse_id: Optional[str] = None,
) -> SyncRequirementCheck:
    """Verify a source Delta table meets the requirements for the chosen sync mode.

    SNAPSHOT has no special requirement. TRIGGERED and CONTINUOUS require Change
    Data Feed (``delta.enableChangeDataFeed=true``) on the source table.

    CDF is read with ``SHOW TBLPROPERTIES`` on the supplied SQL warehouse, using only
    the ``sql`` scope (still governed by the user's UC permissions). Without a
    warehouse the check degrades to *unverified*: it surfaces the requirement and the
    enabling SQL but does not block the sync.
    """
    policy = scheduling_policy.upper()
    requires_cdf = policy in ("TRIGGERED", "CONTINUOUS")
    enable_sql = (
        f"ALTER TABLE {source_table_full_name} "
        "SET TBLPROPERTIES (delta.enableChangeDataFeed = true);"
    )

    if not requires_cdf:
        return SyncRequirementCheck(
            ok=True, verified=True, cdf_enabled=False, table_exists=True,
            message=f"Snapshot mode has no Change Data Feed requirement for '{source_table_full_name}'.",
        )

    if not _TABLE_NAME_RE.match(source_table_full_name):
        return SyncRequirementCheck(
            ok=False, verified=False, cdf_enabled=False, table_exists=False,
            message=(
                f"'{source_table_full_name}' is not a valid three-part table name "
                "(catalog.schema.table)."
            ),
        )

    if not warehouse_id:
        return SyncRequirementCheck(
            ok=True, verified=False, cdf_enabled=False, table_exists=False,
            message=(
                f"Select a SQL warehouse to verify Change Data Feed. {policy} sync REQUIRES "
                "CDF — make sure it is enabled (SQL below), then re-check."
            ),
            enable_cdf_sql=enable_sql,
        )

    try:
        cdf_enabled = _cdf_enabled_via_warehouse(ws, warehouse_id, source_table_full_name)
    except Exception as e:  # noqa: BLE001
        return SyncRequirementCheck(
            ok=True, verified=False, cdf_enabled=False, table_exists=False,
            message=(
                f"Could not auto-verify Change Data Feed for '{source_table_full_name}' "
                f"({e}). {policy} sync REQUIRES CDF: make sure it is enabled (SQL below), then proceed."
            ),
            enable_cdf_sql=enable_sql,
        )

    if cdf_enabled:
        return SyncRequirementCheck(
            ok=True, verified=True, cdf_enabled=True, table_exists=True,
            message=f"Change Data Feed is enabled — '{source_table_full_name}' supports {policy} sync.",
        )
    return SyncRequirementCheck(
        ok=False, verified=True, cdf_enabled=False, table_exists=True,
        message=(
            f"{policy} sync requires Change Data Feed on '{source_table_full_name}', "
            "which is not enabled. Run the SQL below (change history is only captured "
            "after CDF is turned on), then re-check."
        ),
        enable_cdf_sql=enable_sql,
    )


def get_project_info(ws: WorkspaceClient, project: str) -> ProjectInfo:
    """Inspect an existing project: its primary branch + endpoints and current CU range."""
    pg_api = ws.postgres
    project_name = _resolve_project_name(ws, project)

    branches = list(pg_api.list_branches(parent=project_name))
    if not branches or not branches[0].name:
        return ProjectInfo(name=project_name, branch=None, endpoints=[])
    branch_name: str = branches[0].name

    endpoints: list[EndpointInfo] = []
    for ep in pg_api.list_endpoints(parent=branch_name):
        spec = getattr(ep, "spec", None)
        status = getattr(ep, "status", None)
        hosts = getattr(status, "hosts", None) if status else None
        endpoints.append(
            EndpointInfo(
                name=ep.name or "",
                endpoint_type=str(getattr(spec, "endpoint_type", None)) if spec else None,
                host=getattr(hosts, "host", None) if hosts else None,
                min_cu=getattr(spec, "autoscaling_limit_min_cu", None) if spec else None,
                max_cu=getattr(spec, "autoscaling_limit_max_cu", None) if spec else None,
                state=str(getattr(status, "current_state", None)) if status else None,
            )
        )
    return ProjectInfo(name=project_name, branch=branch_name, endpoints=endpoints)


def set_endpoint_cu(
    ws: WorkspaceClient, endpoint_name: str, min_cu: float, max_cu: float
) -> None:
    """Update an existing endpoint's autoscaling CU range."""
    ws.postgres.update_endpoint(
        name=endpoint_name,
        endpoint=pg.Endpoint(
            name=endpoint_name,
            spec=pg.EndpointSpec(
                endpoint_type=pg.EndpointType.ENDPOINT_TYPE_READ_WRITE,
                autoscaling_limit_min_cu=min_cu,
                autoscaling_limit_max_cu=max_cu,
            ),
        ),
        update_mask=pg.FieldMask(
            field_mask=["spec.autoscaling_limit_min_cu", "spec.autoscaling_limit_max_cu"]
        ),
    )


def create_synced_table(
    ws: WorkspaceClient,
    *,
    target_uc_name: str,           # "{catalog}.{schema}.{table}" in the Lakebase catalog
    source_table_full_name: str,   # source Delta table "{catalog}.{schema}.{table}"
    primary_key_columns: list[str],
    scheduling_policy: str,        # SNAPSHOT | TRIGGERED | CONTINUOUS
    storage_catalog: Optional[str] = None,
    storage_schema: Optional[str] = None,
    branch: Optional[str] = None,
    database: Optional[str] = None,
) -> str:
    """Create a synced (reverse-ETL) table from a Delta source into Lakebase.

    Returns the synced table's resource name. ``scheduling_policy`` must be one of
    SNAPSHOT / TRIGGERED / CONTINUOUS.

    ``storage_catalog`` / ``storage_schema`` are optional for Lakebase Autoscaling:
    when omitted, the platform auto-manages the sync pipeline's staging storage. They
    only need to be set to pin the staging location of a newly-created pipeline (a
    holdover from the Provisioned ``w.database`` API).
    """
    policy = getattr(SchedulingPolicy, scheduling_policy.upper())
    new_pipeline_spec = None
    if storage_catalog or storage_schema:
        new_pipeline_spec = pg.NewPipelineSpec(
            storage_catalog=storage_catalog or None,
            storage_schema=storage_schema or None,
        )
    spec = pg.SyncedTableSyncedTableSpec(
        source_table_full_name=source_table_full_name,
        primary_key_columns=primary_key_columns,
        scheduling_policy=policy,
        branch=branch,
        # Omit for a database catalog (inferred); set for a standard catalog (required).
        postgres_database=database or None,
        create_database_objects_if_missing=True,
        new_pipeline_spec=new_pipeline_spec,
    )
    op = ws.postgres.create_synced_table(
        synced_table=pg.SyncedTable(spec=spec),
        synced_table_id=target_uc_name,
    )
    result = op.wait()
    return result.name or target_uc_name
