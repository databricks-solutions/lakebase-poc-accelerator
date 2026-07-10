"""On-demand PostgreSQL connection pool + concurrent query runner for Lakebase.

Ported from the legacy ``autoscaling_connection_service.py`` and adapted to
psycopg3 (the driver shipped with the apx project). Used by the testing and
optimize routers. Connects with already-resolved credentials (host/user/token),
so it is agnostic to how those credentials were obtained (identity/OBO or the dev
OAuth-paste fallback).
"""

from __future__ import annotations

import asyncio
import re
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from typing import Any, Optional
from urllib.parse import quote_plus

import psycopg
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.pool import QueuePool

from . import query_format
from .lakebase_service import PgCredentials
from .stats import percentile

# A schema is interpolated into the libpq ``search_path`` connection option, so it
# must be a plain identifier (no quoting/escaping games).
_IDENT_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def search_path_option(schema: Optional[str]) -> Optional[str]:
    """Build the libpq ``options`` value that sets the connection ``search_path`` to
    ``<schema>, public``, so unqualified table names in the workload resolve to the
    chosen (e.g. synced) schema. Returns None when no valid schema is given.

    Shared by the psycopg pool and the pgbench runners (which pass it via ``PGOPTIONS``).
    """
    schema = (schema or "").strip()
    if not schema:
        return None
    if not _IDENT_RE.match(schema):
        raise ValueError(f"Invalid schema name: {schema!r}")
    return f"-c search_path={schema},public"


class ConnectionPool:
    """A SQLAlchemy engine (psycopg3) with a sized pool and a concurrent runner."""

    def __init__(self) -> None:
        self._engine: Optional[Engine] = None
        self._host: str = ""
        self._database: str = ""
        self._user: str = ""

    def initialize(
        self,
        *,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        ssl_mode: str = "require",
        base_pool_size: int = 5,
        max_overflow: int = 10,
        pool_timeout: int = 30,
        command_timeout: int = 30,
        connect_timeout: int = 10,
        schema: Optional[str] = None,
    ) -> None:
        self._host, self._database, self._user = host, database, user

        # Set the connection default schema so unqualified table names resolve to the
        # chosen (e.g. synced) schema, alongside the per-statement timeout.
        options = f"-c statement_timeout={command_timeout * 1000}"
        sp = search_path_option(schema)
        if sp:
            options = f"{options} {sp}"

        url = (
            f"postgresql+psycopg://{quote_plus(user)}:{quote_plus(password)}"
            f"@{host}:{port}/{quote_plus(database)}?sslmode={ssl_mode}"
        )
        self._engine = create_engine(
            url,
            poolclass=QueuePool,
            pool_size=base_pool_size,
            max_overflow=max_overflow,
            pool_timeout=pool_timeout,
            pool_recycle=3600,
            # pre_ping adds a "SELECT 1" round-trip on every checkout, which both
            # halves throughput and inflates measured query latency under load.
            # Off for the benchmark; pool_recycle still guards stale connections.
            pool_pre_ping=False,
            echo=False,
            connect_args={
                # Fail fast on an unreachable host / DNS hang instead of blocking forever.
                "connect_timeout": connect_timeout,
                "options": options,
            },
        )
        # Validate connectivity up front so failures surface as a clear error.
        with self._engine.connect() as conn:
            conn.execute(text("SELECT 1")).fetchone()

    @contextmanager
    def _connection(self):
        if not self._engine:
            raise RuntimeError("Connection pool not initialized")
        conn = self._engine.connect()
        try:
            yield conn
        finally:
            conn.close()

    def _execute_sync(self, query: str, parameters: Optional[Any]) -> dict[str, Any]:
        start = time.time()
        try:
            with self._connection() as conn:
                raw = conn.connection  # psycopg3 DBAPI connection
                cur = raw.cursor()
                try:
                    cur.execute(query, parameters or None)
                    try:
                        rows = cur.fetchall()
                    except psycopg.ProgrammingError:
                        rows = []  # non-SELECT (INSERT/UPDATE/DDL)
                    return {
                        "success": True,
                        "duration_ms": (time.time() - start) * 1000,
                        "rows_returned": len(rows),
                        "error_message": None,
                        "error_type": None,
                    }
                finally:
                    cur.close()
        except Exception as e:  # noqa: BLE001
            return {
                "success": False,
                "duration_ms": (time.time() - start) * 1000,
                "rows_returned": 0,
                "error_message": str(e),
                "error_type": type(e).__name__,
            }

    async def run_concurrent(
        self, queries: list[dict[str, Any]], concurrency_level: int, total_executions: int
    ) -> dict[str, Any]:
        """Distribute ``total_executions`` across the queries by weight, expand each
        into that many executions (drawing a fresh random parameter dict per
        execution), run them with a concurrency cap, and aggregate metrics."""
        counts = query_format.distribute_executions(
            [int(qc.get("weight", 1)) for qc in queries], total_executions
        )
        tasks: list[dict[str, Any]] = []
        for qc, count in zip(queries, counts):
            sql_lines = [
                ln for ln in qc["query_content"].split("\n")
                if not ln.strip().startswith("--")
            ]
            clean_sql = "\n".join(sql_lines).strip()
            param_specs = qc.get("param_specs") or []
            for _ in range(count):
                tasks.append(
                    {
                        "query_identifier": qc["query_identifier"],
                        "query": clean_sql,
                        "parameters": query_format.draw_from_specs(param_specs) or None,
                    }
                )

        # A dedicated executor sized to the concurrency level — asyncio's default
        # executor caps at min(32, cpus+4) threads, which would silently throttle the
        # run far below the requested concurrency (e.g. only ~8 in flight on a 4-core
        # box). The semaphore then just bounds how many tasks are queued at once.
        concurrency = max(1, concurrency_level)
        loop = asyncio.get_event_loop()
        executor = ThreadPoolExecutor(max_workers=concurrency, thread_name_prefix="lakebench")
        semaphore = asyncio.Semaphore(concurrency)

        async def run_one(task: dict[str, Any]) -> dict[str, Any]:
            async with semaphore:
                res = await loop.run_in_executor(
                    executor, self._execute_sync, task["query"], task["parameters"]
                )
                res["query_identifier"] = task["query_identifier"]
                return res

        # Snapshot DB cache counters around the run so we can report cache hit % for
        # THIS run (a delta), instead of the misleading lifetime ratio that never
        # recovers from the initial bulk load.
        cache_before = self._cache_counters()
        start = time.time()
        try:
            results = await asyncio.gather(
                *[run_one(t) for t in tasks], return_exceptions=True
            )
        finally:
            executor.shutdown(wait=False)
        total_duration = time.time() - start
        cache_after = self._cache_counters()
        cache_hit_pct = self._cache_hit_delta(cache_before, cache_after)

        successful = 0
        failed = 0
        latencies: list[float] = []
        # Keep the first failure's message so the UI can show *why* a run failed
        # instead of just a 0% success rate (otherwise failures are silent).
        sample_error: str | None = None
        for r in results:
            if isinstance(r, BaseException):
                failed += 1
                if sample_error is None:
                    sample_error = f"{type(r).__name__}: {r}"
            elif r.get("success"):
                successful += 1
                latencies.append(r["duration_ms"])
            else:
                failed += 1
                if sample_error is None:
                    etype = r.get("error_type")
                    emsg = r.get("error_message") or "query failed"
                    qid = r.get("query_identifier")
                    prefix = f"[{qid}] " if qid else ""
                    sample_error = f"{prefix}{etype + ': ' if etype else ''}{emsg}"

        n = len(results)
        success_rate = successful / n if n else 0.0
        avg = sum(latencies) / len(latencies) if latencies else 0.0
        throughput = n / total_duration if total_duration > 0 else 0.0
        latencies.sort()

        def pct(p: float, vals: list[float]) -> float:
            return percentile(vals, p) if vals else 0.0

        return {
            "concurrency_level": concurrency_level,
            "total_queries_executed": n,
            "successful_queries": successful,
            "failed_queries": failed,
            "success_rate": success_rate,
            "sample_error": sample_error,
            "average_execution_time_ms": avg,
            "p50_execution_time_ms": pct(50, latencies),
            "p95_execution_time_ms": pct(95, latencies),
            "p99_execution_time_ms": pct(99, latencies),
            "throughput_queries_per_second": throughput,
            "total_duration_seconds": total_duration,
            "cache_hit_pct": cache_hit_pct,
            "connection_pool_metrics": self.pool_status(),
            "per_query": self._per_query_breakdown(results, pct),
        }

    @staticmethod
    def _per_query_breakdown(results: list[Any], pct: Any) -> list[dict[str, Any]]:
        """Per-query (by identifier) calls/avg/total/p95/p99, like Lakebase's query
        performance view. Sorted by total time descending."""
        from collections import defaultdict

        groups: dict[str, dict[str, Any]] = defaultdict(lambda: {"calls": 0, "lat": []})
        for r in results:
            if isinstance(r, BaseException):
                continue
            g = groups[r.get("query_identifier", "?")]
            g["calls"] += 1
            if r.get("success"):
                g["lat"].append(r["duration_ms"])

        out: list[dict[str, Any]] = []
        for q, g in groups.items():
            lat = sorted(g["lat"])
            out.append(
                {
                    "query_identifier": q,
                    "calls": g["calls"],
                    "avg_time_ms": sum(lat) / len(lat) if lat else 0.0,
                    "total_time_ms": sum(lat),
                    "p95_time_ms": pct(95, lat),
                    "p99_time_ms": pct(99, lat),
                }
            )
        out.sort(key=lambda d: d["total_time_ms"], reverse=True)
        return out

    def _cache_counters(self) -> Optional[tuple[int, int]]:
        """(blks_hit, blks_read) for the current database, or None if unavailable."""
        try:
            with self._connection() as conn:
                row = conn.execute(
                    text(
                        "SELECT blks_hit, blks_read FROM pg_stat_database "
                        "WHERE datname = current_database()"
                    )
                ).fetchone()
                if row is not None:
                    return int(row[0] or 0), int(row[1] or 0)
        except Exception:  # noqa: BLE001 - best-effort metric, never fail the test
            return None
        return None

    @staticmethod
    def _cache_hit_delta(
        before: Optional[tuple[int, int]], after: Optional[tuple[int, int]]
    ) -> Optional[float]:
        return cache_hit_delta(before, after)

    def pool_status(self) -> dict[str, Any]:
        if not self._engine:
            return {"status": "not_initialized"}
        pool = self._engine.pool

        def _metric(name: str) -> Any:
            fn = getattr(pool, name, None)
            return fn() if callable(fn) else None

        return {
            "pool_size": _metric("size"),
            "checked_in_connections": _metric("checkedin"),
            "checked_out_connections": _metric("checkedout"),
            "overflow": _metric("overflow"),
            "status": "active",
        }

    def close(self) -> None:
        if self._engine:
            self._engine.dispose()
            self._engine = None


# --------------------------------------------------------------------------- #
# Shared cache-hit helpers (psycopg pool uses the methods above; pgbench, which
# doesn't use the pool, uses these standalone functions).
# --------------------------------------------------------------------------- #
def read_cache_counters(creds: PgCredentials) -> Optional[tuple[int, int]]:
    """(blks_hit, blks_read) for the run's database via a fresh psycopg connection.
    Best-effort — returns None on any failure so it never breaks a run."""
    try:
        with psycopg.connect(
            host=creds.host,
            port=creds.port,
            dbname=creds.database,
            user=creds.user,
            password=creds.password,
            sslmode=creds.ssl_mode,
            connect_timeout=10,
        ) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT blks_hit, blks_read FROM pg_stat_database "
                "WHERE datname = current_database()"
            )
            row = cur.fetchone()
            if row is not None:
                return int(row[0] or 0), int(row[1] or 0)
    except Exception:  # noqa: BLE001 - best-effort metric
        return None
    return None


def cache_hit_delta(
    before: Optional[tuple[int, int]], after: Optional[tuple[int, int]]
) -> Optional[float]:
    """Cache hit % attributable to a run: delta(hit) / delta(hit + read)."""
    if not before or not after:
        return None
    d_hit = max(0, after[0] - before[0])
    d_read = max(0, after[1] - before[1])
    total = d_hit + d_read
    if total <= 0:
        return None
    return round(d_hit * 100.0 / total, 2)
