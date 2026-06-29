"""Unified concurrency-testing endpoints.

psycopg: connects on-demand from the backend and runs the uploaded query mix at a
target concurrency level, returning latency/throughput metrics. pgbench: submits a
Databricks Job that runs ``pgbench`` on a single-node cluster (the cluster installs
``postgresql-client`` via an init script) and polls the run for results. Auth is
identity / app-resource (primary) or pasted OAuth (dev) — never static username &
password.
"""

from __future__ import annotations

from pydantic import BaseModel, Field

from ..core import create_router, logger
from ..deps import EffectiveClient
from ..services import auth, pgbench_job, query_format
from ..services.connection import ConnectionPool

router = create_router()


class QueryIn(BaseModel):
    identifier: str
    content: str


class PsycopgTestIn(BaseModel):
    auth_method: auth.AuthMethod = "identity"
    project: str | None = None
    database: str | None = None
    # dev OAuth fallback
    endpoint_host: str | None = None
    access_token: str | None = None
    postgres_user_name: str | None = None
    # workload
    concurrency_level: int = Field(default=10, ge=1, le=1000)
    queries: list[QueryIn]


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
    connection_pool_metrics: dict
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

    parsed = [query_format.parse_query(q.identifier, q.content) for q in req.queries]
    execution_queries = query_format.to_execution_queries(parsed)

    pool = ConnectionPool()
    try:
        pool.initialize(
            host=creds.host,
            port=creds.port,
            database=creds.database,
            user=creds.user,
            password=creds.password,
            ssl_mode=creds.ssl_mode,
            base_pool_size=max(1, req.concurrency_level // 4),
            max_overflow=req.concurrency_level,
        )
    except Exception as e:  # noqa: BLE001
        logger.info(f"psycopg pool init failed: {e}")
        return _empty_report(req.concurrency_level, f"Connection failed: {e}")

    try:
        report = await pool.run_concurrent(execution_queries, req.concurrency_level)
        return TestReportOut(**report)
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
def submit_pgbench_job(req: PgbenchSubmitIn, ws: EffectiveClient) -> PgbenchSubmitOut:
    """Submit a pgbench run as a Databricks Job and return its run identifiers."""
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
    parsed = [query_format.parse_query(q.identifier, q.content) for q in req.queries]
    pgbench_queries = query_format.to_pgbench_queries(parsed)

    try:
        result = pgbench_job.submit(
            ws,
            creds=creds,
            config=req.config.model_dump(),
            queries=pgbench_queries,
            cluster_id=req.cluster_id,
        )
        return PgbenchSubmitOut(**result)
    except Exception as e:  # noqa: BLE001
        logger.info(f"pgbench job submission failed: {e}")
        return PgbenchSubmitOut(status="error", error=str(e))


@router.get(
    "/testing/pgbench/status/{run_id}",
    response_model=PgbenchStatusOut,
    operation_id="getPgbenchRunStatus",
)
def get_pgbench_run_status(run_id: str, ws: EffectiveClient) -> PgbenchStatusOut:
    """Poll a submitted pgbench job run for its status and parsed metrics."""
    try:
        return PgbenchStatusOut(**pgbench_job.run_status(ws, run_id))
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
def list_clusters(ws: EffectiveClient) -> ClusterListOut:
    """List interactive clusters available to attach the pgbench job to (optional)."""
    try:
        return ClusterListOut(clusters=[ClusterOut(**c) for c in pgbench_job.list_clusters(ws)])
    except Exception as e:  # noqa: BLE001
        logger.info(f"cluster listing failed: {e}")
        return ClusterListOut(error=str(e))
