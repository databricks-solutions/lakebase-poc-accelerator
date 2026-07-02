"""pgbench-as-a-Databricks-Job runner.

Submits ``pgbench`` as a Databricks Job that runs on a single-node cluster, lets the
job's init script install ``postgresql-client`` (so we never need the pgbench binary
inside the App container), and polls the run for parsed throughput/latency results.

Connection credentials come from :mod:`..services.auth` (identity / oauth) — never
static username & password. The job is reused across runs (one job per
app, looked up by name) and only re-created when the chosen cluster configuration
changes. All Databricks calls use the injected (OBO / SP) ``WorkspaceClient`` so the
job runs under the caller's identity.
"""

from __future__ import annotations

import base64
import json
import re
import time
from pathlib import Path
from typing import Any, Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat

from ..core import logger
from .lakebase_service import PgCredentials

# Bundled job payload (shipped as package data, see resources/pgbench/).
_RESOURCES = Path(__file__).resolve().parent.parent / "resources" / "pgbench"
_NOTEBOOK_LOCAL = _RESOURCES / "pgbench_parameterized.ipynb"
_INIT_SCRIPT_LOCAL = _RESOURCES / "init.sh"

# Reusable workspace locations.
_WS_DIR = "/Shared/pgbench_resources"
_NOTEBOOK_WS_PATH = f"{_WS_DIR}/pgbench_parameterized"
_INIT_SCRIPT_WS_PATH = f"{_WS_DIR}/init.sh"
_JOB_NAME = "lakebase_accelerator_pgbench_job"

_SPARK_VERSION = "14.3.x-scala2.12"
_TIMEOUT_SECONDS = 3600
# Job notebook_params are size-limited; keep the inline query payload well under it.
_INLINE_QUERY_LIMIT = 8192


# --------------------------------------------------------------------------- #
# Cloud / node sizing
# --------------------------------------------------------------------------- #
def _detect_cloud(ws: WorkspaceClient) -> str:
    """Best-effort cloud detection from the workspace host ('aws' | 'azure' | 'gcp')."""
    host = (getattr(ws.config, "host", "") or "").lower()
    if "azuredatabricks.net" in host or "azure" in host:
        return "azure"
    if "gcp.databricks.com" in host:
        return "gcp"
    return "aws"


_INSTANCE_MAP = {
    "aws": {"small": "m6i.xlarge", "medium": "m6i.2xlarge", "large": "m6i.4xlarge",
            "xlarge": "m6i.8xlarge", "2xlarge": "m6i.16xlarge"},
    "azure": {"small": "Standard_E4s_v4", "medium": "Standard_E8s_v3", "large": "Standard_E16s_v3",
              "xlarge": "Standard_E32s_v3", "2xlarge": "Standard_E64s_v3"},
    "gcp": {"small": "n2-highmem-4", "medium": "n2-highmem-8", "large": "n2-highmem-16",
            "xlarge": "n2-highmem-32", "2xlarge": "n2-highmem-64"},
}
# (tier, max_threads, memory_gb)
_TIERS = [("small", 4, 16), ("medium", 8, 32), ("large", 16, 64),
          ("xlarge", 32, 128), ("2xlarge", 64, 256)]


def _node_type_for_workload(ws: WorkspaceClient, threads: int, clients: int) -> str:
    """Pick a single-node instance sized to the pgbench worker threads and clients.

    Primary criterion is worker threads → cores (capped at 64); the tier is then
    upgraded if the client count needs more memory (~200 MB per client).
    """
    cloud = _detect_cloud(ws)
    threads = min(max(threads, 1), 64)
    required_mem_gb = (clients * 200) / 1024

    tier, tier_mem = "2xlarge", 256
    for name, max_threads, mem in _TIERS:
        if threads <= max_threads:
            tier, tier_mem = name, mem
            break
    if required_mem_gb > tier_mem:
        for name, _max_threads, mem in _TIERS:
            if required_mem_gb <= mem:
                tier, tier_mem = name, mem
                break
        else:
            tier, tier_mem = "2xlarge", 256

    node_type = _INSTANCE_MAP.get(cloud, _INSTANCE_MAP["aws"])[tier]
    logger.info(f"pgbench job: node_type={node_type} (tier {tier}) for {threads}t/{clients}c on {cloud}")
    return node_type


def _new_cluster_config(ws: WorkspaceClient, node_type: str, single_user: str, init_path: str) -> dict:
    cfg: dict[str, Any] = {
        "spark_version": _SPARK_VERSION,
        "node_type_id": node_type,
        "num_workers": 0,
        "spark_conf": {
            "spark.databricks.cluster.profile": "singleNode",
            "spark.master": "local[*]",
        },
        "custom_tags": {"ResourceClass": "SingleNode", "pgbench_job": "true"},
        "data_security_mode": "SINGLE_USER",
        "single_user_name": single_user,
        "init_scripts": [{"workspace": {"destination": init_path}}],
    }
    # AWS 6th-gen Intel families have no local storage and need an EBS volume.
    if _detect_cloud(ws) == "aws" and any(f in node_type for f in ("m6i", "r6i", "c6i", "m6a", "r6a", "c6a")):
        cfg["aws_attributes"] = {
            "ebs_volume_type": "GENERAL_PURPOSE_SSD",
            "ebs_volume_count": 1,
            "ebs_volume_size": 100,
        }
    return cfg


# --------------------------------------------------------------------------- #
# Workspace asset upload
# --------------------------------------------------------------------------- #
def _ensure_dir(ws: WorkspaceClient, path: str) -> None:
    try:
        ws.workspace.get_status(path)
    except Exception:  # noqa: BLE001 - get_status raises when missing
        ws.workspace.mkdirs(path)


def _upload_notebook(ws: WorkspaceClient) -> str:
    """Upload the bundled pgbench notebook (overwrite) and return its workspace path."""
    _ensure_dir(ws, _WS_DIR)
    ws.workspace.upload(
        path=_NOTEBOOK_WS_PATH,
        content=_NOTEBOOK_LOCAL.read_bytes(),
        format=ImportFormat.JUPYTER,
        overwrite=True,
    )
    return _NOTEBOOK_WS_PATH


def _upload_init_script(ws: WorkspaceClient) -> str:
    """Upload the bundled cluster init script (overwrite) and return its workspace path."""
    _ensure_dir(ws, _WS_DIR)
    content_b64 = base64.b64encode(_INIT_SCRIPT_LOCAL.read_bytes()).decode("utf-8")
    # AUTO format so Databricks stores it as a plain file, not a notebook.
    ws.api_client.do(
        "POST",
        "/api/2.0/workspace/import",
        body={"path": _INIT_SCRIPT_WS_PATH, "format": "AUTO", "content": content_b64, "overwrite": True},
    )
    return _INIT_SCRIPT_WS_PATH


def _current_identity(ws: WorkspaceClient) -> str:
    me = ws.current_user.me()
    return getattr(me, "application_id", None) or me.user_name or (me.id or "")


def _workspace_url(ws: WorkspaceClient) -> str:
    return (getattr(ws.config, "host", "") or "").rstrip("/")


# --------------------------------------------------------------------------- #
# Job lifecycle
# --------------------------------------------------------------------------- #
def _find_job_id(ws: WorkspaceClient, name: str) -> Optional[int]:
    for job in ws.jobs.list():
        if job.settings and job.settings.name == name:
            return job.job_id
    return None


def _get_or_create_job(ws: WorkspaceClient, cluster_id: Optional[str], node_type: str) -> str:
    """Return the reusable pgbench job, creating or re-pointing its cluster as needed."""
    notebook_path = _upload_notebook(ws)
    task: dict[str, Any] = {
        "task_key": "pgbench_test",
        "notebook_task": {"notebook_path": notebook_path, "base_parameters": {}},
        "timeout_seconds": _TIMEOUT_SECONDS,
    }
    if cluster_id and cluster_id.strip():
        task["existing_cluster_id"] = cluster_id.strip()
    else:
        task["new_cluster"] = _new_cluster_config(
            ws, node_type, _current_identity(ws), _upload_init_script(ws)
        )

    settings = {
        "name": _JOB_NAME,
        "tasks": [task],
        "max_concurrent_runs": 1,
        "timeout_seconds": _TIMEOUT_SECONDS,
    }

    existing_id = _find_job_id(ws, _JOB_NAME)
    if existing_id is None:
        resp = ws.api_client.do("POST", "/api/2.1/jobs/create", body=settings)
        job_id = str(resp.get("job_id")) if isinstance(resp, dict) else ""
        logger.info(f"pgbench job: created job {job_id}")
        return job_id

    # Reset so the cluster (existing vs job cluster) always matches this request.
    ws.api_client.do(
        "POST", "/api/2.1/jobs/reset", body={"job_id": existing_id, "new_settings": settings}
    )
    logger.info(f"pgbench job: reused/updated job {existing_id}")
    return str(existing_id)


def submit(
    ws: WorkspaceClient,
    *,
    creds: PgCredentials,
    config: dict[str, Any],
    queries: list[dict[str, Any]],
    cluster_id: Optional[str] = None,
) -> dict[str, Any]:
    """Submit a pgbench run and return job/run identifiers and workspace links."""
    query_json = json.dumps(queries)
    if len(query_json) > _INLINE_QUERY_LIMIT:
        raise ValueError(
            "Query payload is too large to pass inline to the job. "
            "Reduce the number/size of queries."
        )

    node_type = _node_type_for_workload(
        ws, int(config.get("jobs", 8)), int(config.get("clients", 8))
    )
    job_id = _get_or_create_job(ws, cluster_id, node_type)

    params = {
        "lakebase_instance_name": creds.host,
        "database_name": creds.database,
        "pghost": creds.host,
        "pgport": str(creds.port),
        "pgdatabase": creds.database,
        "pguser": creds.user,
        "pgpassword": creds.password,
        "pgsslmode": creds.ssl_mode,
        "pgbench_clients": str(config.get("clients", 8)),
        "pgbench_jobs": str(config.get("jobs", 8)),
        "pgbench_duration": str(config.get("duration_seconds", 30)),
        "pgbench_progress_interval": str(config.get("progress_interval", 5)),
        "pgbench_protocol": config.get("protocol", "prepared"),
        "pgbench_per_statement_latency": str(config.get("per_statement_latency", True)),
        "pgbench_detailed_logging": str(config.get("detailed_logging", True)),
        "pgbench_connect_per_transaction": str(config.get("connect_per_transaction", False)),
        "query_config": query_json,
    }

    run = ws.jobs.run_now(job_id=int(job_id), notebook_params=params)
    run_id = str(run.run_id)

    base = _workspace_url(ws)
    return {
        "job_id": job_id,
        "run_id": run_id,
        "job_name": _JOB_NAME,
        "status": "submitted",
        "job_run_url": f"{base}#job/{job_id}/run/{run_id}" if base else None,
        "job_url": f"{base}#job/{job_id}" if base else None,
    }


_STATE_MAP = {
    "PENDING": "pending",
    "RUNNING": "running",
    "TERMINATING": "running",
    "TERMINATED": "completed",
    "SKIPPED": "failed",
    "INTERNAL_ERROR": "failed",
}


def run_status(ws: WorkspaceClient, run_id: str) -> dict[str, Any]:
    """Map a job run to {status, message, progress, pgbench_results}."""
    run = ws.jobs.get_run(int(run_id))
    state = run.state
    life = state.life_cycle_state.value if state and state.life_cycle_state else "UNKNOWN"
    result = state.result_state.value if state and state.result_state else None

    status = _STATE_MAP.get(life, "unknown")
    if life == "TERMINATED":
        status = "completed" if result == "SUCCESS" else "failed"
    progress = {"pending": 0, "running": 50, "completed": 100, "failed": 100}.get(status, 0)

    out: dict[str, Any] = {
        "run_id": run_id,
        "status": status,
        "message": _status_message(life, result),
        "progress": progress,
        "pgbench_results": None,
    }

    if status == "completed":
        try:
            out["pgbench_results"] = _fetch_pgbench_results(ws, run)
        except Exception as e:  # noqa: BLE001
            logger.info(f"pgbench job: could not read run output: {e}")
    return out


def _status_message(life: str, result: Optional[str]) -> str:
    if life == "PENDING":
        return "Job is pending execution"
    if life in ("RUNNING", "TERMINATING"):
        return "Job is running the pgbench test"
    if life == "TERMINATED":
        return "pgbench test completed successfully" if result == "SUCCESS" else f"Job failed: {result}"
    return f"Job status: {life}"


def _fetch_pgbench_results(ws: WorkspaceClient, run: Any) -> Optional[dict[str, Any]]:
    """Pull the pgbench task's notebook output and parse the summary stats."""
    task_run_id = None
    for task in run.tasks or []:
        if task.task_key == "pgbench_test":
            task_run_id = task.run_id
            break
    if task_run_id is None:
        return None

    output = ws.jobs.get_run_output(task_run_id)
    if not output or not output.notebook_output or not output.notebook_output.result:
        return None

    parsed = _parse_notebook_result(output.notebook_output.result)
    if not parsed:
        return None
    summary = parse_pgbench_stdout(parsed.get("raw_output", "")) or {}
    # Prefer the notebook's own computed metrics where present.
    metrics = parsed.get("performance_metrics") or {}
    for key in ("tps", "latency_p50_ms", "latency_p95_ms", "latency_p99_ms",
                "total_transactions", "per_query", "cache_hit_pct"):
        if metrics.get(key) is not None:
            summary[key] = metrics[key]
    return summary or None


def _parse_notebook_result(raw: str) -> Optional[dict[str, Any]]:
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return None


def parse_pgbench_stdout(raw_output: str) -> Optional[dict[str, Any]]:
    """Parse pgbench summary statistics from its stdout.

    Shared by the Databricks-job runner and the local (dev) runner so both surface
    identical metrics.
    """
    if not raw_output:
        return None
    patterns = {
        "transaction_type": r"transaction type:\s*(.+)",
        "scaling_factor": r"scaling factor:\s*(\d+)",
        "query_mode": r"query mode:\s*(\w+)",
        "num_clients": r"number of clients:\s*(\d+)",
        "num_threads": r"number of threads:\s*(\d+)",
        "duration": r"duration:\s*(\d+)\s*s",
        "total_transactions": r"number of transactions actually processed:\s*(\d+)",
        "failed_transactions": r"number of failed transactions:\s*(\d+)",
        "latency_avg_ms": r"latency average\s*=\s*([\d.]+)\s*ms",
        "latency_stddev_ms": r"latency stddev\s*=\s*([\d.]+)\s*ms",
        "initial_connection_time_ms": r"initial connection time\s*=\s*([\d.]+)\s*ms",
        "tps": r"tps\s*=\s*([\d.]+)",
    }
    int_keys = {"scaling_factor", "num_clients", "num_threads", "duration",
                "total_transactions", "failed_transactions"}
    float_keys = {"latency_avg_ms", "latency_stddev_ms", "initial_connection_time_ms", "tps"}

    results: dict[str, Any] = {}
    for key, pattern in patterns.items():
        m = re.search(pattern, raw_output)
        if not m:
            continue
        val = m.group(1)
        if key in int_keys:
            results[key] = int(val)
        elif key in float_keys:
            results[key] = float(val)
        else:
            results[key] = val.strip()

    total = results.get("total_transactions")
    failed = results.get("failed_transactions")
    if total and total > 0:
        results["success_rate"] = round((total - (failed or 0)) / total * 100, 2)

    return results if "tps" in results else None


# --------------------------------------------------------------------------- #
# Clusters (optional picker)
# --------------------------------------------------------------------------- #
def list_clusters(ws: WorkspaceClient) -> list[dict[str, Any]]:
    """List interactive clusters the caller can attach the pgbench job to."""
    out: list[dict[str, Any]] = []
    for c in ws.clusters.list():
        out.append(
            {
                "cluster_id": c.cluster_id or "",
                "cluster_name": c.cluster_name or (c.cluster_id or ""),
                "state": c.state.value if c.state else "UNKNOWN",
                "node_type_id": c.node_type_id,
            }
        )
    return out
