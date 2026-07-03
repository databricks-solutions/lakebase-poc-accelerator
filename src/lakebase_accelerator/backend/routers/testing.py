"""Unified concurrency-testing endpoints.

psycopg: connects on-demand from the backend and runs the uploaded query mix at a
target concurrency level, returning latency/throughput metrics. pgbench: submits a
Databricks Job that runs ``pgbench`` on a single-node cluster (the cluster installs
``postgresql-client`` via an init script) and polls the run for results. Auth is
identity (primary) or pasted OAuth (dev) — never static username & password.
"""

from __future__ import annotations

from fastapi import HTTPException, Request
from pydantic import BaseModel, Field

from ..core import create_router, logger
from ..deps import EffectiveClient
from ..services import auth, lakebase_service, pgbench_job, pgbench_local, query_format
from ..services.connection import ConnectionPool

router = create_router()


def _app_sp(request: Request):
    """The app service principal client (M2M), used to orchestrate the pgbench
    Databricks Job. The Jobs/Clusters API needs workspace scopes the forwarded user
    (OBO) token doesn't carry, so the SP *owns* the job while the workload still
    connects to Lakebase as the user (their creds are passed into the job)."""
    return request.app.state.workspace_client


class QueryIn(BaseModel):
    identifier: str
    content: str


class PsycopgTestIn(BaseModel):
    auth_method: auth.AuthMethod = "identity"
    project: str | None = None
    database: str | None = None
    # Default schema (search_path) so unqualified table names in the queries resolve
    # to the chosen (e.g. synced) schema without needing fully-qualified names.
    db_schema: str | None = None
    # dev OAuth fallback
    endpoint_host: str | None = None
    access_token: str | None = None
    postgres_user_name: str | None = None
    # workload
    concurrency_level: int = Field(default=10, ge=1, le=1000)
    total_executions: int = Field(default=100, ge=1, le=100000)
    queries: list[QueryIn]


class QueryStat(BaseModel):
    query_identifier: str
    calls: int
    avg_time_ms: float
    total_time_ms: float
    p95_time_ms: float | None = None
    p99_time_ms: float | None = None


class TestReportOut(BaseModel):
    concurrency_level: int
    total_queries_executed: int
    successful_queries: int
    failed_queries: int
    success_rate: float
    average_execution_time_ms: float
    p50_execution_time_ms: float
    p95_execution_time_ms: float
    p99_execution_time_ms: float
    throughput_queries_per_second: float
    total_duration_seconds: float
    cache_hit_pct: float | None = None
    connection_pool_metrics: dict
    per_query: list[QueryStat] = Field(default_factory=list)
    monitoring_url: str | None = None
    error: str | None = None


@router.post(
    "/testing/psycopg/run",
    response_model=TestReportOut,
    operation_id="runPsycopgTest",
)
async def run_psycopg_test(req: PsycopgTestIn, ws: EffectiveClient) -> TestReportOut:
    """Run the uploaded query mix against Lakebase via psycopg at the target concurrency."""
    if not req.queries:
        return _empty_report(req.concurrency_level, "No queries provided.")

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
    except Exception as e:  # noqa: BLE001
        logger.info(f"psycopg auth resolution failed: {e}")
        return _empty_report(req.concurrency_level, f"Authentication failed: {e}")

    try:
        parsed = [query_format.parse_query(q.identifier, q.content) for q in req.queries]
        execution_queries = query_format.to_execution_queries(parsed)
    except ValueError as e:
        return _empty_report(req.concurrency_level, str(e))

    pool = ConnectionPool()
    try:
        pool.initialize(
            host=creds.host,
            port=creds.port,
            database=creds.database,
            user=creds.user,
            password=creds.password,
            ssl_mode=creds.ssl_mode,
            # Persistent pool sized to the concurrency level so each worker reuses a
            # warm connection instead of paying a TCP+TLS+auth handshake per query
            # (QueuePool closes *overflow* connections on return). A small overflow
            # absorbs the cache-counter probes that run alongside the workload.
            base_pool_size=req.concurrency_level,
            max_overflow=max(2, req.concurrency_level // 4),
            schema=req.db_schema,
        )
    except ValueError as e:
        return _empty_report(req.concurrency_level, str(e))
    except Exception as e:  # noqa: BLE001
        logger.info(f"psycopg pool init failed: {e}")
        return _empty_report(req.concurrency_level, f"Connection failed: {e}")

    try:
        report = await pool.run_concurrent(
            execution_queries, req.concurrency_level, req.total_executions
        )
        monitoring_url = lakebase_service.build_monitoring_url(ws, creds)
        return TestReportOut(**report, monitoring_url=monitoring_url)
    finally:
        pool.close()


def _empty_report(concurrency: int, error: str) -> TestReportOut:
    return TestReportOut(
        concurrency_level=concurrency,
        total_queries_executed=0,
        successful_queries=0,
        failed_queries=0,
        success_rate=0.0,
        average_execution_time_ms=0.0,
        p50_execution_time_ms=0.0,
        p95_execution_time_ms=0.0,
        p99_execution_time_ms=0.0,
        throughput_queries_per_second=0.0,
        total_duration_seconds=0.0,
        connection_pool_metrics={"status": "not_initialized"},
        error=error,
    )


# --------------------------------------------------------------------------- #
# pgbench (Databricks Job)
# --------------------------------------------------------------------------- #
class PgbenchConfigIn(BaseModel):
    clients: int = Field(default=8, ge=1, le=1000)
    jobs: int = Field(default=8, ge=1, le=100)
    duration_seconds: int = Field(default=30, ge=1, le=3600)
    progress_interval: int = Field(default=5, ge=1, le=60)
    protocol: str = Field(default="prepared")
    per_statement_latency: bool = True
    detailed_logging: bool = True
    connect_per_transaction: bool = False


class PgbenchSubmitIn(BaseModel):
    auth_method: auth.AuthMethod = "identity"
    project: str | None = None
    database: str | None = None
    # Default schema (search_path); same purpose as PsycopgTestIn.db_schema.
    db_schema: str | None = None
    # dev OAuth fallback
    endpoint_host: str | None = None
    access_token: str | None = None
    postgres_user_name: str | None = None
    # workload — same unified query format as psycopg (QueryIn)
    config: PgbenchConfigIn = Field(default_factory=PgbenchConfigIn)
    queries: list[QueryIn] = Field(default_factory=list)
    cluster_id: str | None = None


class PgbenchSubmitOut(BaseModel):
    job_id: str | None = None
    run_id: str | None = None
    job_name: str | None = None
    status: str
    job_run_url: str | None = None
    job_url: str | None = None
    monitoring_url: str | None = None
    error: str | None = None


class PgbenchStatusOut(BaseModel):
    run_id: str
    status: str
    message: str
    progress: int
    pgbench_results: dict | None = None
    error: str | None = None


class ClusterOut(BaseModel):
    cluster_id: str
    cluster_name: str
    state: str
    node_type_id: str | None = None


class ClusterListOut(BaseModel):
    clusters: list[ClusterOut] = Field(default_factory=list)
    error: str | None = None


@router.post(
    "/testing/pgbench/submit",
    response_model=PgbenchSubmitOut,
    operation_id="submitPgbenchJob",
)
def submit_pgbench_job(req: PgbenchSubmitIn, ws: EffectiveClient, request: Request) -> PgbenchSubmitOut:
    """Submit a pgbench run as a Databricks Job and return its run identifiers.

    Hybrid identity: the job is created/owned by the app service principal (the Jobs
    API needs workspace scopes the OBO user token lacks), but Lakebase credentials are
    resolved as the *user* (OBO) and passed into the job, so the benchmark connects to
    Postgres as the user.
    """
    if not req.queries:
        return PgbenchSubmitOut(status="error", error="No queries provided.")

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
    except Exception as e:  # noqa: BLE001
        logger.info(f"pgbench auth resolution failed: {e}")
        return PgbenchSubmitOut(status="error", error=f"Authentication failed: {e}")

    # Translate the unified query format into native pgbench scripts (\set + :name)
    # so the same query body the user wrote for psycopg drives pgbench unchanged.
    try:
        parsed = [query_format.parse_query(q.identifier, q.content) for q in req.queries]
        pgbench_queries = query_format.to_pgbench_queries(parsed)
    except ValueError as e:
        return PgbenchSubmitOut(status="error", error=str(e))

    try:
        # SP orchestrates the job; user creds (above) run the workload.
        result = pgbench_job.submit(
            _app_sp(request),
            creds=creds,
            config=req.config.model_dump(),
            queries=pgbench_queries,
            cluster_id=req.cluster_id,
            schema=req.db_schema,
        )
        monitoring_url = lakebase_service.build_monitoring_url(ws, creds)
        return PgbenchSubmitOut(**result, monitoring_url=monitoring_url)
    except Exception as e:  # noqa: BLE001
        logger.info(f"pgbench job submission failed: {e}")
        return PgbenchSubmitOut(status="error", error=str(e))


@router.get(
    "/testing/pgbench/status/{run_id}",
    response_model=PgbenchStatusOut,
    operation_id="getPgbenchRunStatus",
)
def get_pgbench_run_status(run_id: str, request: Request) -> PgbenchStatusOut:
    """Poll a submitted pgbench job run for its status and parsed metrics.

    Read under the app SP that owns the job (same identity that submitted it)."""
    try:
        return PgbenchStatusOut(**pgbench_job.run_status(_app_sp(request), run_id))
    except Exception as e:  # noqa: BLE001
        logger.info(f"pgbench status lookup failed: {e}")
        return PgbenchStatusOut(
            run_id=run_id, status="failed", message=str(e), progress=0, error=str(e)
        )


@router.get(
    "/testing/clusters",
    response_model=ClusterListOut,
    operation_id="listClusters",
)
def list_clusters(request: Request) -> ClusterListOut:
    """List interactive clusters available to attach the pgbench job to (optional).

    Listed under the app SP that owns/runs the job (Clusters API needs workspace scopes
    the OBO user token lacks)."""
    try:
        return ClusterListOut(clusters=[ClusterOut(**c) for c in pgbench_job.list_clusters(_app_sp(request))])
    except Exception as e:  # noqa: BLE001
        logger.info(f"cluster listing failed: {e}")
        return ClusterListOut(error=str(e))


# --------------------------------------------------------------------------- #
# Capabilities + local pgbench (dev-only fallback)
# --------------------------------------------------------------------------- #
class CapabilitiesOut(BaseModel):
    # True only when running locally (not a deployed Databricks App) and the pgbench
    # binary is present. The frontend uses this to show the "Local (dev)" run mode,
    # which is meant for serverless-only workspaces where the job cluster can't run.
    pgbench_local_available: bool = False


class PgbenchLocalSubmitOut(BaseModel):
    run_id: str | None = None
    status: str
    monitoring_url: str | None = None
    error: str | None = None


@router.get(
    "/testing/capabilities",
    response_model=CapabilitiesOut,
    operation_id="getTestingCapabilities",
)
def get_testing_capabilities() -> CapabilitiesOut:
    """Report optional, environment-gated testing capabilities."""
    return CapabilitiesOut(pgbench_local_available=pgbench_local.local_available())


@router.post(
    "/testing/pgbench/local/submit",
    response_model=PgbenchLocalSubmitOut,
    operation_id="submitLocalPgbench",
)
def submit_local_pgbench(req: PgbenchSubmitIn, ws: EffectiveClient) -> PgbenchLocalSubmitOut:
    """Run pgbench locally (dev-only) against Lakebase, returning a pollable run id."""
    if not pgbench_local.local_available():
        # Defense in depth: refuse even if the UI somehow surfaces this in production.
        raise HTTPException(status_code=403, detail="Local pgbench is not available in this environment.")
    if not req.queries:
        return PgbenchLocalSubmitOut(status="error", error="No queries provided.")

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
    except Exception as e:  # noqa: BLE001
        logger.info(f"local pgbench auth resolution failed: {e}")
        return PgbenchLocalSubmitOut(status="error", error=f"Authentication failed: {e}")

    try:
        parsed = [query_format.parse_query(q.identifier, q.content) for q in req.queries]
        pgbench_queries = query_format.to_pgbench_queries(parsed)
    except ValueError as e:
        return PgbenchLocalSubmitOut(status="error", error=str(e))

    try:
        result = pgbench_local.submit(
            creds=creds,
            config=req.config.model_dump(),
            queries=pgbench_queries,
            schema=req.db_schema,
        )
        monitoring_url = lakebase_service.build_monitoring_url(ws, creds)
        return PgbenchLocalSubmitOut(**result, monitoring_url=monitoring_url)
    except Exception as e:  # noqa: BLE001
        logger.info(f"local pgbench submission failed: {e}")
        return PgbenchLocalSubmitOut(status="error", error=str(e))


@router.get(
    "/testing/pgbench/local/status/{run_id}",
    response_model=PgbenchStatusOut,
    operation_id="getLocalPgbenchStatus",
)
def get_local_pgbench_status(run_id: str) -> PgbenchStatusOut:
    """Poll a local pgbench run for its status and parsed metrics."""
    if not pgbench_local.local_available():
        raise HTTPException(status_code=403, detail="Local pgbench is not available in this environment.")
    return PgbenchStatusOut(**pgbench_local.run_status(run_id))
