"""Run-history endpoints (Lakebase destination).

The hybrid identity model: the *workload* runs under the end user (OBO), but the
history table is read/written under the app **service principal** so the table is
SP-owned and shared across all users. The end user's identity is captured from the
OBO client and stored in ``created_by`` for attribution.

Browser (localStorage) history needs no backend — these endpoints exist only for
the opt-in, consent-gated Lakebase destination.
"""

from __future__ import annotations

import psycopg
from fastapi import Request
from pydantic import BaseModel, Field

from ..core import create_router, logger
from ..deps import EffectiveClient
from ..services import auth, history
from .testing import QueryIn

router = create_router()


class HistoryConnIn(BaseModel):
    auth_method: auth.AuthMethod = "identity"
    project: str | None = None
    database: str | None = None
    # dev OAuth fallback
    endpoint_host: str | None = None
    access_token: str | None = None
    postgres_user_name: str | None = None
    # target schema (dedicated, SP-owned) + table name for the history table
    schema_name: str = history.DEFAULT_SCHEMA
    table_name: str = history.DEFAULT_TABLE


class HistoryRunIn(BaseModel):
    """One session run to archive — engine-agnostic (psycopg or pgbench)."""
    # Stable client-generated id; keying on it makes re-archiving idempotent.
    id: str | None = None
    engine: str = "psycopg"
    label: str | None = None
    project: str | None = None
    created_at: str | None = None  # preserved from the original run time when present
    config: dict = Field(default_factory=dict)
    queries: list[QueryIn] = Field(default_factory=list)
    baseline_report: dict | None = None
    optimized_report: dict | None = None
    index_ddls: list[str] = Field(default_factory=list)


class HistoryArchiveIn(HistoryConnIn):
    runs: list[HistoryRunIn] = Field(default_factory=list)


class HistoryEnableOut(BaseModel):
    ok: bool
    table: str | None = None
    message: str
    grant_sql: str | None = None
    ddl: str | None = None
    error: str | None = None


class HistoryArchiveOut(BaseModel):
    ok: bool
    inserted: int = 0
    error: str | None = None


class HistoryTablesOut(BaseModel):
    tables: list[str] = Field(default_factory=list)
    error: str | None = None


class HistoryRunOut(BaseModel):
    id: str
    created_at: str | None = None
    engine: str = "psycopg"
    label: str | None = None
    project: str | None = None
    config: dict = Field(default_factory=dict)
    created_by: str | None = None
    queries: list[dict] = Field(default_factory=list)
    baseline_report: dict | None = None
    optimized_report: dict | None = None
    index_ddls: list[str] = Field(default_factory=list)


class HistoryListOut(BaseModel):
    runs: list[HistoryRunOut] = Field(default_factory=list)
    error: str | None = None


def _sp_creds(req: HistoryConnIn, request: Request):
    """Resolve Lakebase credentials under the app service principal (not the OBO user).

    For ``identity`` the SP WorkspaceClient mints the token; ``oauth`` (dev) uses the
    pasted token.
    """
    sp_ws = request.app.state.workspace_client
    return auth.resolve(
        sp_ws,
        auth_method=req.auth_method,
        project=req.project,
        database=req.database,
        endpoint_host=req.endpoint_host,
        access_token=req.access_token,
        postgres_user_name=req.postgres_user_name,
    )


def _current_user(ws: EffectiveClient) -> str | None:
    try:
        me = ws.current_user.me()
        return me.user_name or getattr(me, "application_id", None) or me.id
    except Exception as e:  # noqa: BLE001
        logger.info(f"history: could not resolve OBO identity: {e}")
        return None


def _sp_identity(request: Request) -> str:
    """The app service principal's identity (used as its Postgres role name)."""
    try:
        me = request.app.state.workspace_client.current_user.me()
        return getattr(me, "application_id", None) or me.user_name or me.id or "<app-service-principal>"
    except Exception as e:  # noqa: BLE001
        logger.info(f"history: could not resolve SP identity: {e}")
        return "<app-service-principal>"


@router.post("/history/lakebase/enable", response_model=HistoryEnableOut, operation_id="enableLakebaseHistory")
def enable_lakebase_history(req: HistoryConnIn, request: Request) -> HistoryEnableOut:
    """Preflight the SP's privileges and create the named history table (consent step)."""
    # Only available when deployed (behind the Databricks Apps proxy, which forwards the
    # user token): there the app-level client is the service principal, so the history
    # objects end up SP-owned. In local dev the app-level client is the *developer's*
    # identity, so we short-circuit — otherwise provisioning would create/guide objects
    # owned by the wrong Postgres role (e.g. CREATE SCHEMA ... AUTHORIZATION "you@…").
    if "X-Forwarded-Access-Token" not in request.headers:
        return HistoryEnableOut(
            ok=False,
            message=(
                "Shared Lakebase history is provisioned only in the deployed app, where it runs as the "
                "app's service principal (so the table is SP-owned). In local dev the app uses your own "
                "identity, so enabling is disabled here to avoid creating mis-owned objects. Deploy the app "
                "to enable it — history is still kept in this browser locally."
            ),
            ddl=_safe_ddl(req.schema_name, req.table_name),
        )

    try:
        creds = _sp_creds(req, request)
    except Exception as e:  # noqa: BLE001
        return HistoryEnableOut(ok=False, message=f"Could not resolve connection: {e}", error=str(e), ddl=_safe_ddl(req.schema_name, req.table_name))

    try:
        result = history.ensure_table(creds, req.schema_name, req.table_name)
        return HistoryEnableOut(**result, ddl=_safe_ddl(req.schema_name, req.table_name))
    except psycopg.OperationalError as e:
        # Layer 1: the SP has no role in this project (or can't connect) — emit the
        # role-creation + owned-schema setup so a project owner can provision it.
        logger.info(f"history enable: SP cannot connect to project: {e}")
        sp = _sp_identity(request)
        return HistoryEnableOut(
            ok=False,
            message=(
                f"The app service principal ({sp}) has no role in this project, so it can't connect. "
                f"A project owner must provision a least-privilege role + owned schema (below), or keep "
                f"runs in this browser, which needs no setup."
            ),
            grant_sql=history.provisioning_sql(sp, req.schema_name, creds.database, include_role=True),
            ddl=_safe_ddl(req.schema_name, req.table_name),
            error=str(e),
        )
    except Exception as e:  # noqa: BLE001
        logger.info(f"history enable failed: {e}")
        return HistoryEnableOut(ok=False, message="Could not enable Lakebase history", error=str(e), ddl=_safe_ddl(req.schema_name, req.table_name))


@router.post("/history/lakebase/tables", response_model=HistoryTablesOut, operation_id="listLakebaseHistoryTables")
def list_lakebase_history_tables(req: HistoryConnIn, request: Request) -> HistoryTablesOut:
    """List existing tables in the SP-owned schema, for the 'archive to existing table' picker."""
    try:
        creds = _sp_creds(req, request)
        return HistoryTablesOut(tables=history.list_tables(creds, req.schema_name))
    except Exception as e:  # noqa: BLE001
        logger.info(f"history tables list failed: {e}")
        return HistoryTablesOut(error=str(e))


@router.post("/history/lakebase/archive", response_model=HistoryArchiveOut, operation_id="archiveLakebaseHistory")
def archive_lakebase_history(req: HistoryArchiveIn, request: Request, ws: EffectiveClient) -> HistoryArchiveOut:
    """Bulk-archive session runs into the named table under the SP, attributed to the OBO user."""
    if not req.runs:
        return HistoryArchiveOut(ok=False, error="No runs to archive.")
    try:
        creds = _sp_creds(req, request)
        inserted = history.save_runs(
            creds,
            req.schema_name,
            req.table_name,
            created_by=_current_user(ws),
            runs=[r.model_dump() for r in req.runs],
        )
        return HistoryArchiveOut(ok=True, inserted=inserted)
    except Exception as e:  # noqa: BLE001
        logger.info(f"history archive failed: {e}")
        return HistoryArchiveOut(ok=False, error=str(e))


@router.post("/history/lakebase/list", response_model=HistoryListOut, operation_id="listLakebaseHistory")
def list_lakebase_history(req: HistoryConnIn, request: Request) -> HistoryListOut:
    """List recent runs from the named Lakebase history table (shared, newest first)."""
    try:
        creds = _sp_creds(req, request)
        runs = history.list_runs(creds, req.schema_name, req.table_name)
        return HistoryListOut(runs=[HistoryRunOut(**r) for r in runs])
    except Exception as e:  # noqa: BLE001
        logger.info(f"history list failed: {e}")
        return HistoryListOut(error=str(e))


def _safe_ddl(schema_name: str, table_name: str) -> str | None:
    try:
        return history.ddl(schema_name, table_name)
    except Exception:  # noqa: BLE001
        return None


# --------------------------------------------------------------------------- #
# Saved query sets ("query library") — reusable named query mixes, same SP-owned
# store + created_by attribution as run history.
# --------------------------------------------------------------------------- #
class SavedQuerySaveIn(HistoryConnIn):
    name: str
    engine: str | None = None
    shared: bool = False
    queries: list[QueryIn] = Field(default_factory=list)


class SavedQueryListIn(HistoryConnIn):
    include_shared: bool = False


class SavedQueryDeleteIn(HistoryConnIn):
    id: str


class SavedQuerySetOut(BaseModel):
    id: str
    name: str
    engine: str | None = None
    shared: bool = False
    created_by: str | None = None
    is_mine: bool = False
    created_at: str | None = None
    updated_at: str | None = None
    queries: list[dict] = Field(default_factory=list)


class SavedQuerySaveOut(BaseModel):
    ok: bool
    id: str | None = None
    message: str | None = None
    grant_sql: str | None = None
    error: str | None = None


class SavedQueryListOut(BaseModel):
    sets: list[SavedQuerySetOut] = Field(default_factory=list)
    error: str | None = None


class SavedQueryDeleteOut(BaseModel):
    ok: bool
    deleted: int = 0
    error: str | None = None


@router.post("/library/lakebase/save", response_model=SavedQuerySaveOut, operation_id="saveLakebaseQuerySet")
def save_lakebase_query_set(req: SavedQuerySaveIn, request: Request, ws: EffectiveClient) -> SavedQuerySaveOut:
    """Save (upsert by name) the current query mix under the OBO user, into the SP-owned table."""
    if not req.name.strip():
        return SavedQuerySaveOut(ok=False, error="A name is required.")
    if not req.queries:
        return SavedQuerySaveOut(ok=False, error="No queries to save.")
    try:
        creds = _sp_creds(req, request)
    except Exception as e:  # noqa: BLE001
        return SavedQuerySaveOut(ok=False, error=str(e))
    try:
        ensured = history.ensure_saved_queries_table(creds, req.schema_name)
        if not ensured.get("ok"):
            return SavedQuerySaveOut(
                ok=False, message=ensured.get("message"), grant_sql=ensured.get("grant_sql")
            )
        set_id = history.save_query_set(
            creds,
            req.schema_name,
            name=req.name,
            engine=req.engine,
            queries=[q.model_dump() for q in req.queries],
            shared=req.shared,
            created_by=_current_user(ws),
        )
        return SavedQuerySaveOut(ok=True, id=set_id)
    except Exception as e:  # noqa: BLE001
        logger.info(f"save query set failed: {e}")
        return SavedQuerySaveOut(ok=False, error=str(e))


@router.post("/library/lakebase/list", response_model=SavedQueryListOut, operation_id="listLakebaseQuerySets")
def list_lakebase_query_sets(req: SavedQueryListIn, request: Request, ws: EffectiveClient) -> SavedQueryListOut:
    """List saved query sets visible to the OBO user: their own, plus shared when requested."""
    try:
        creds = _sp_creds(req, request)
        sets = history.list_query_sets(
            creds,
            req.schema_name,
            current_user=_current_user(ws),
            include_shared=req.include_shared,
        )
        return SavedQueryListOut(sets=[SavedQuerySetOut(**s) for s in sets])
    except Exception as e:  # noqa: BLE001
        logger.info(f"list query sets failed: {e}")
        return SavedQueryListOut(error=str(e))


@router.post("/library/lakebase/delete", response_model=SavedQueryDeleteOut, operation_id="deleteLakebaseQuerySet")
def delete_lakebase_query_set(req: SavedQueryDeleteIn, request: Request, ws: EffectiveClient) -> SavedQueryDeleteOut:
    """Delete one of the OBO user's own saved sets (never someone else's shared set)."""
    try:
        creds = _sp_creds(req, request)
        deleted = history.delete_query_set(
            creds, req.schema_name, set_id=req.id, created_by=_current_user(ws)
        )
        return SavedQueryDeleteOut(ok=True, deleted=deleted)
    except Exception as e:  # noqa: BLE001
        logger.info(f"delete query set failed: {e}")
        return SavedQueryDeleteOut(ok=False, error=str(e))
