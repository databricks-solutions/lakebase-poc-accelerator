"""Local (dev-only) pgbench runner.

Runs the ``pgbench`` binary as a subprocess on the machine hosting the backend and
connects straight to Lakebase — no Databricks Job, no cluster. This is the fallback
for serverless-only workspaces where the single-node job cluster can't be created.

It is strictly gated to local development: :func:`local_available` is false (and the
endpoints refuse) whenever the backend runs as a deployed Databricks App
(``DATABRICKS_APP_NAME`` / ``DATABRICKS_APP_PORT`` set) or the ``pgbench`` binary is
not on ``PATH`` — so this path is never reachable in production.

The same unified query format and stdout parser as the Databricks-job runner are
reused, so the metrics returned are identical. Runs execute in a background thread and
are polled by ``run_id`` to mirror the job-runner UX.
"""

from __future__ import annotations

import glob
import os
import shutil
import subprocess
import tempfile
import threading
import time
import uuid
from typing import Any

from ..core import logger
from .connection import cache_hit_delta, read_cache_counters
from .lakebase_service import PgCredentials
from .pgbench_job import parse_pgbench_stdout
from .stats import percentile

# Env vars Databricks Apps inject into the running container; presence ⇒ production.
_APP_ENV_MARKERS = ("DATABRICKS_APP_NAME", "DATABRICKS_APP_PORT")

# Hard ceiling so a wedged subprocess can't run forever (duration + this grace).
_GRACE_SECONDS = 120


def _is_databricks_app() -> bool:
    return any(os.environ.get(k) for k in _APP_ENV_MARKERS)


def local_available() -> bool:
    """True only off-Databricks-App *and* when the ``pgbench`` binary is on PATH."""
    return (not _is_databricks_app()) and shutil.which("pgbench") is not None


# --------------------------------------------------------------------------- #
# Run registry: run_id -> mutable status dict, guarded by _LOCK.
# --------------------------------------------------------------------------- #
_LOCK = threading.Lock()
_RUNS: dict[str, dict[str, Any]] = {}


def _set(run_id: str, **patch: Any) -> None:
    with _LOCK:
        _RUNS.setdefault(run_id, {}).update(patch)


def run_status(run_id: str) -> dict[str, Any]:
    """Return the latest {run_id, status, message, progress, pgbench_results} snapshot."""
    with _LOCK:
        run = _RUNS.get(run_id)
        if run is None:
            return {
                "run_id": run_id,
                "status": "failed",
                "message": "Unknown run id (local runs are in-memory and reset on restart).",
                "progress": 100,
                "pgbench_results": None,
                "error": "unknown run id",
            }
        return {
            "run_id": run_id,
            "status": run.get("status", "unknown"),
            "message": run.get("message", ""),
            "progress": run.get("progress", 0),
            "pgbench_results": run.get("pgbench_results"),
            "error": run.get("error"),
        }


def submit(
    *,
    creds: PgCredentials,
    config: dict[str, Any],
    queries: list[dict[str, Any]],
) -> dict[str, Any]:
    """Start a local pgbench run in a background thread and return its run id."""
    run_id = uuid.uuid4().hex
    _set(
        run_id,
        status="pending",
        message="Starting local pgbench…",
        progress=0,
        pgbench_results=None,
        error=None,
    )
    thread = threading.Thread(
        target=_run,
        args=(run_id, creds, dict(config), list(queries)),
        daemon=True,
    )
    thread.start()
    return {"run_id": run_id, "status": "submitted"}


def _latency_percentiles(workdir: str) -> dict[str, float]:
    """Compute p50/p95/p99 (ms) from pgbench's per-transaction logs (the ``-l`` flag).

    pgbench log line: ``client_id transaction_no time script_no time_epoch time_us``;
    column index 2 (``time``) is the transaction latency in microseconds. Returns {}
    when detailed logging is off or no log lines are present.
    """
    latencies: list[float] = []
    for path in glob.glob(os.path.join(workdir, "pgbench_log.*")):
        try:
            with open(path) as f:
                for line in f:
                    parts = line.split()
                    if len(parts) >= 3:
                        try:
                            latencies.append(float(parts[2]) / 1000.0)
                        except ValueError:
                            continue
        except OSError:
            continue
    if not latencies:
        return {}
    latencies.sort()
    return {
        "latency_p50_ms": round(percentile(latencies, 50), 3),
        "latency_p95_ms": round(percentile(latencies, 95), 3),
        "latency_p99_ms": round(percentile(latencies, 99), 3),
    }


def _per_query_from_logs(workdir: str, query_names: list[str]) -> list[dict[str, Any]]:
    """Per-query (per pgbench script) calls/avg/total/p95/p99 from the ``-l`` logs.

    The log's column index 3 is ``script_no`` (0-based, in the order scripts were
    passed with ``-f``), so it maps back to ``query_names``. Sorted by total time
    descending, like Lakebase's query performance view. Returns [] when detailed
    logging is off or no log lines are present.
    """
    from collections import defaultdict

    groups: dict[int, list[float]] = defaultdict(list)
    for path in glob.glob(os.path.join(workdir, "pgbench_log.*")):
        try:
            with open(path) as f:
                for line in f:
                    parts = line.split()
                    if len(parts) >= 4:
                        try:
                            groups[int(parts[3])].append(float(parts[2]) / 1000.0)
                        except ValueError:
                            continue
        except OSError:
            continue

    out: list[dict[str, Any]] = []
    for script_no, lat in groups.items():
        lat.sort()
        name = query_names[script_no] if script_no < len(query_names) else f"script {script_no}"
        out.append(
            {
                "query_identifier": name,
                "calls": len(lat),
                "avg_time_ms": round(sum(lat) / len(lat), 3),
                "total_time_ms": round(sum(lat), 3),
                "p95_time_ms": round(percentile(lat, 95), 3),
                "p99_time_ms": round(percentile(lat, 99), 3),
            }
        )
    out.sort(key=lambda d: d["total_time_ms"], reverse=True)
    return out


def _build_cmd(config: dict[str, Any], query_files: list[tuple[str, Any]]) -> list[str]:
    """Mirror the Databricks-job notebook's pgbench invocation."""
    cmd = [
        "pgbench",
        "-n",  # skip vacuum (custom scripts)
        "-c", str(config.get("clients", 8)),
        "-j", str(config.get("jobs", 8)),
        "-T", str(config.get("duration_seconds", 30)),
        "-P", str(config.get("progress_interval", 5)),
        "-M", str(config.get("protocol", "prepared")),
    ]
    if config.get("per_statement_latency", True):
        cmd.append("-r")
    if config.get("detailed_logging", True):
        cmd.append("-l")
    if config.get("connect_per_transaction", False):
        cmd.append("-C")
    for path, weight in query_files:
        cmd.extend(["-f", f"{path}@{int(weight)}"])
    return cmd


def _run(
    run_id: str,
    creds: PgCredentials,
    config: dict[str, Any],
    queries: list[dict[str, Any]],
) -> None:
    if not queries:
        _set(run_id, status="failed", message="No queries provided.", progress=100,
             error="No queries provided.")
        return

    tmpdir = tempfile.mkdtemp(prefix="pgbench_local_")
    try:
        query_files: list[tuple[str, Any]] = []
        for q in queries:
            name = q.get("name", "query")
            path = os.path.join(tmpdir, f"{name}.sql")
            with open(path, "w") as f:
                f.write((q.get("content", "") or "").strip() + "\n")
            query_files.append((path, q.get("weight", 1)))

        env = os.environ.copy()
        env.update(
            {
                "PGHOST": creds.host,
                "PGPORT": str(creds.port),
                "PGDATABASE": creds.database,
                "PGUSER": creds.user,
                "PGPASSWORD": creds.password,
                "PGSSLMODE": creds.ssl_mode,
            }
        )

        cmd = _build_cmd(config, query_files)
        # Password lives only in env, so logging the command is safe.
        logger.info(f"pgbench local [{run_id}]: {' '.join(cmd)}")
        _set(run_id, status="running", message="Running local pgbench…", progress=5)

        # Snapshot DB cache counters around the run for a per-run cache hit %, the
        # same delta the psycopg runner reports.
        cache_before = read_cache_counters(creds)

        proc = subprocess.Popen(
            cmd, env=env, cwd=tmpdir,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )

        # Drain stdout in a helper thread (so the pipe never fills) while we tick progress.
        output: dict[str, str] = {}

        def _drain() -> None:
            out, _ = proc.communicate()
            output["out"] = out or ""

        reader = threading.Thread(target=_drain, daemon=True)
        reader.start()

        duration = max(int(config.get("duration_seconds", 30)), 1)
        deadline = time.time() + duration + _GRACE_SECONDS
        start = time.time()
        while reader.is_alive():
            elapsed = time.time() - start
            _set(run_id, progress=min(95, 5 + int(elapsed / duration * 90)))
            if time.time() > deadline:
                proc.kill()
                reader.join(timeout=5)
                _set(run_id, status="failed", progress=100,
                     message="Local pgbench timed out.", error="timed out")
                return
            reader.join(timeout=1)

        raw_output = output.get("out", "")
        if proc.returncode != 0:
            tail = raw_output.strip()[-2000:]
            _set(run_id, status="failed", progress=100,
                 message=f"pgbench exited with code {proc.returncode}.", error=tail)
            return

        summary = parse_pgbench_stdout(raw_output)
        if not summary:
            tail = raw_output.strip()[-2000:]
            _set(run_id, status="failed", progress=100,
                 message="pgbench produced no parseable results.", error=tail)
            return

        # pgbench's summary stdout has no percentiles; derive them (and the per-query
        # breakdown) from the -l log, read before the tmpdir is cleaned up below.
        summary.update(_latency_percentiles(tmpdir))
        summary["per_query"] = _per_query_from_logs(tmpdir, [q.get("name", "") for q in queries])
        summary["cache_hit_pct"] = cache_hit_delta(cache_before, read_cache_counters(creds))

        _set(run_id, status="completed", progress=100,
             message="Local pgbench completed successfully.", pgbench_results=summary)
    except Exception as e:  # noqa: BLE001
        logger.info(f"pgbench local [{run_id}] failed: {e}")
        _set(run_id, status="failed", progress=100, message=str(e), error=str(e))
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
