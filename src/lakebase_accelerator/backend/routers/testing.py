"""Unified concurrency-testing endpoints.

psycopg: connects on-demand from the backend and runs the uploaded query mix at a
target concurrency level, returning latency/throughput metrics. pgbench (Databricks
job) is added separately. Auth is identity / app-resource (primary) or pasted OAuth
(dev) — never static username & password.
"""

from __future__ import annotations

from pydantic import BaseModel, Field

from ..core import create_router, logger
from ..deps import EffectiveClient
from ..services import auth, query_format
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
