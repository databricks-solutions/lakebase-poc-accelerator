"""Optimize endpoint: ready-to-run index DDL + live tuning findings for Lakebase.

Combines static query-parse (candidate indexes from the tested SQL) with optional
live introspection (detection SQL from the OLTP Technical Guide). Auth uses the same
identity / dev-OAuth model as testing.
"""

from __future__ import annotations

from pydantic import BaseModel

from ..core import create_router, logger
from ..deps import EffectiveClient
from ..services import auth, optimize

router = create_router()


class OptimizeQueryIn(BaseModel):
    identifier: str
    content: str


class OptimizeIn(BaseModel):
    auth_method: auth.AuthMethod = "identity"
    project: str | None = None
    database: str | None = None
    endpoint_host: str | None = None
    access_token: str | None = None
    postgres_user_name: str | None = None
    queries: list[OptimizeQueryIn] = []
    run_live: bool = True


class IndexSuggestionOut(BaseModel):
    table: str
    columns: list[str]
    rationale: str
    ddl: str


class FindingOut(BaseModel):
    severity: str
    category: str
    title: str
    detail: str
    actions: list[str]


class OptimizeOut(BaseModel):
    index_suggestions: list[IndexSuggestionOut]
    findings: list[FindingOut]
    stats: dict
    live_ran: bool
    error: str | None = None


class ApplyIndexIn(BaseModel):
    auth_method: auth.AuthMethod = "identity"
    project: str | None = None
    database: str | None = None
    endpoint_host: str | None = None
    access_token: str | None = None
    postgres_user_name: str | None = None
    ddls: list[str] = []


class ApplyResultOut(BaseModel):
    ddl: str
    ok: bool
    detail: str


class ApplyIndexesOut(BaseModel):
    results: list[ApplyResultOut] = []
    error: str | None = None


@router.post("/optimize/apply-indexes", response_model=ApplyIndexesOut, operation_id="applyIndexes")
def apply_indexes(req: ApplyIndexIn, ws: EffectiveClient) -> ApplyIndexesOut:
    """Apply CREATE INDEX (etc.) DDL to Lakebase, so the user can re-run the test
    and see the before/after impact without leaving the app."""
    if not req.ddls:
        return ApplyIndexesOut(error="No DDL provided")
    try:
        creds = auth.resolve(
            ws,
            auth_method=req.auth_method,
            project=req.project,
            database=req.database,
            endpoint_host=req.endpoint_host,
            access_token=req.access_token,
            postgres_user_name=req.postgres_user_name,
        )
        results = optimize.apply_indexes(creds, req.ddls)
        return ApplyIndexesOut(results=[ApplyResultOut(**r) for r in results])
    except Exception as e:  # noqa: BLE001
        logger.info(f"apply_indexes failed: {e}")
        return ApplyIndexesOut(error=str(e))


@router.post("/optimize/analyze", response_model=OptimizeOut, operation_id="optimizeAnalyze")
def optimize_analyze(req: OptimizeIn, ws: EffectiveClient) -> OptimizeOut:
    """Parse the tested queries for candidate indexes and (optionally) run live
    introspection against Lakebase for data-driven tuning findings."""
    query_pairs = [(q.identifier, q.content) for q in req.queries]
    candidates = optimize.parse_candidate_indexes(query_pairs)
    focus_tables = optimize.tables_in_queries(query_pairs)

    findings: list[FindingOut] = []
    stats: dict = {}
    live_ran = False
    error: str | None = None

    if req.run_live:
        try:
            creds = auth.resolve(
                ws,
                auth_method=req.auth_method,
                project=req.project,
                database=req.database,
                endpoint_host=req.endpoint_host,
                access_token=req.access_token,
                postgres_user_name=req.postgres_user_name,
            )
            raw_findings, stats = optimize.live_introspection(creds, focus_tables)
            findings = [
                FindingOut(
                    severity=f.severity, category=f.category, title=f.title,
                    detail=f.detail, actions=f.actions,
                )
                for f in raw_findings
            ]
            # Drop suggestions that an existing index already covers (so re-running
            # after applying indexes stops re-suggesting the same ones).
            kept, dropped = optimize.filter_existing_indexes(
                candidates, stats.get("existing_indexes")
            )
            candidates = kept
            if dropped:
                names = ", ".join(f"{s.table}({', '.join(s.columns)})" for s in dropped)
                findings.insert(0, FindingOut(
                    severity="low", category="index",
                    title=f"{len(dropped)} candidate index(es) already exist",
                    detail=f"Skipped because a matching index is already present: {names}.",
                    actions=["No action needed — these predicates are already indexed."],
                ))
            live_ran = True
        except Exception as e:  # noqa: BLE001
            logger.info(f"optimize live introspection failed: {e}")
            error = f"Live introspection skipped: {e}"

    suggestions = [
        IndexSuggestionOut(table=s.table, columns=s.columns, rationale=s.rationale, ddl=s.ddl)
        for s in candidates
    ]

    return OptimizeOut(
        index_suggestions=suggestions,
        findings=findings,
        stats=stats,
        live_ran=live_ran,
        error=error,
    )
