"""On-demand PostgreSQL connection pool + concurrent query runner for Lakebase.

Ported from the legacy ``autoscaling_connection_service.py`` and adapted to
psycopg3 (the driver shipped with the apx project). Used by the testing and
optimize routers. Connects with already-resolved credentials (host/user/token),
so it is agnostic to how those credentials were obtained (identity/OBO or the dev
OAuth-paste fallback).
"""

from __future__ import annotations

import asyncio
import time
from contextlib import contextmanager
from typing import Any, Optional
from urllib.parse import quote_plus

import psycopg
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.pool import QueuePool

from . import query_format


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
    ) -> None:
        self._host, self._database, self._user = host, database, user

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
            pool_pre_ping=True,
            echo=False,
            connect_args={
                # Fail fast on an unreachable host / DNS hang instead of blocking forever.
                "connect_timeout": connect_timeout,
                "options": f"-c statement_timeout={command_timeout * 1000}",
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

    async def _execute(self, query: str, parameters: Optional[Any]) -> dict[str, Any]:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._execute_sync, query, parameters)

    async def run_concurrent(
        self, queries: list[dict[str, Any]], concurrency_level: int
    ) -> dict[str, Any]:
        """Expand each query into ``execution_count`` individual executions (drawing a
        fresh random parameter dict per execution), run them with a concurrency cap,
        and aggregate latency/throughput metrics."""
        tasks: list[dict[str, Any]] = []
        for qc in queries:
            sql_lines = [
                ln for ln in qc["query_content"].split("\n")
                if not ln.strip().startswith("--")
            ]
            clean_sql = "\n".join(sql_lines).strip()
            param_specs = qc.get("param_specs") or []
            for _ in range(qc.get("execution_count", 1)):
                tasks.append(
                    {
                        "query_identifier": qc["query_identifier"],
                        "query": clean_sql,
                        "parameters": query_format.draw_from_specs(param_specs) or None,
                    }
                )

        semaphore = asyncio.Semaphore(max(1, concurrency_level))

        async def run_one(task: dict[str, Any]) -> dict[str, Any]:
            async with semaphore:
                res = await self._execute(task["query"], task["parameters"])
                res["query_identifier"] = task["query_identifier"]
                return res

        start = time.time()
        results = await asyncio.gather(
            *[run_one(t) for t in tasks], return_exceptions=True
        )
        total_duration = time.time() - start

        successful = 0
        failed = 0
        latencies: list[float] = []
        for r in results:
            if isinstance(r, BaseException):
                failed += 1
            elif r.get("success"):
                successful += 1
                latencies.append(r["duration_ms"])
            else:
                failed += 1

        n = len(results)
        success_rate = successful / n if n else 0.0
        avg = sum(latencies) / len(latencies) if latencies else 0.0
        throughput = n / total_duration if total_duration > 0 else 0.0
        latencies.sort()

        def pct(p: float) -> float:
            if not latencies:
                return 0.0
            idx = min(int(len(latencies) * p), len(latencies) - 1)
            return latencies[idx]

        return {
            "concurrency_level": concurrency_level,
            "total_queries_executed": n,
            "successful_queries": successful,
            "failed_queries": failed,
            "success_rate": success_rate,
            "average_execution_time_ms": avg,
            "p50_execution_time_ms": pct(0.50),
            "p95_execution_time_ms": pct(0.95),
            "p99_execution_time_ms": pct(0.99),
            "throughput_queries_per_second": throughput,
            "total_duration_seconds": total_duration,
            "connection_pool_metrics": self.pool_status(),
        }

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
