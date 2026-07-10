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
import os
import re
import time
from pathlib import Path
from typing import Any, Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat

from ..core import logger
from .connection import search_path_option
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
# App-owned dedicated benchmark cluster (one per workspace, looked up by name). Owning a
# cluster the app SP creates makes the app self-sufficient in any workspace instead of
# depending on a pre-existing cluster, and reusing it keeps a stable egress IP for the
# cluster's lifetime (far fewer workspace IP-ACL edits than an ephemeral job cluster).
# Secret scope/key the app SP writes the (OBO-user-minted) DB token to, so the notebook
# reads it via ``dbutils.secrets.get`` (redacted) instead of receiving it as a job
# parameter. Runs are serialized (max_concurrent_runs=1) and there is one app deployment
# per workspace (shared job/cluster names already assume this), so a fixed scope+key
# overwritten per run is safe; the token is short-lived and self-expires.
_SECRET_SCOPE = "lakebase-pgbench"
_SECRET_KEY = "db_token"
_CLUSTER_NAME = "lakebase_accelerator_pgbench_cluster"
# Auto-termination window (minutes) for the app-owned cluster, deployment-configurable via
# PGBENCH_CLUSTER_AUTOTERMINATION_MIN. Lower = less idle cost but the egress IP is more
# likely to rotate on restart (→ another IP-ACL allow); 0 disables auto-termination for a
# stable "allow the IP once" setup at the cost of an always-on cluster. Databricks requires
# either 0 or >= 10 minutes.
_CLUSTER_AUTOTERMINATION_ENV = "PGBENCH_CLUSTER_AUTOTERMINATION_MIN"
_CLUSTER_AUTOTERMINATION_DEFAULT_MIN = 30


def _cluster_autotermination_min() -> int:
    """Resolve the configured auto-termination window, clamped to Databricks' rules
    (0 = never, otherwise a minimum of 10 minutes)."""
    raw = os.environ.get(_CLUSTER_AUTOTERMINATION_ENV)
    if raw is None or not raw.strip():
        return _CLUSTER_AUTOTERMINATION_DEFAULT_MIN
    try:
        minutes = int(raw)
    except ValueError:
        logger.info(
            f"pgbench cluster: invalid {_CLUSTER_AUTOTERMINATION_ENV}={raw!r}; "
            f"using default {_CLUSTER_AUTOTERMINATION_DEFAULT_MIN}"
        )
        return _CLUSTER_AUTOTERMINATION_DEFAULT_MIN
    if minutes <= 0:
        return 0
    return max(minutes, 10)

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
# Fixed at the largest single-node tier (64 vCPU / 256 GB). The shared benchmark cluster
# is one fixed size for *all* runs so per-run concurrency (clients/threads) never reshapes
# it — that removes the resize→restart→egress-IP-rotation conflict between concurrent
# users, and guarantees the single-process pgbench load generator is never the bottleneck
# (up to the UI's 1000-client / 100-thread ceiling) so measurements reflect Lakebase.
_BENCHMARK_TIER = "2xlarge"


def _benchmark_node_type(ws: WorkspaceClient) -> str:
    """Return the fixed max-tier single-node instance type for the detected cloud."""
    cloud = _detect_cloud(ws)
    node_type = _INSTANCE_MAP.get(cloud, _INSTANCE_MAP["aws"])[_BENCHMARK_TIER]
    logger.info(f"pgbench cluster: node_type={node_type} (fixed {_BENCHMARK_TIER}) on {cloud}")
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
# App-owned benchmark cluster
# --------------------------------------------------------------------------- #
def _benchmark_cluster_config(ws: WorkspaceClient, node_type: str) -> dict:
    """All-purpose single-node cluster spec for the app-owned pgbench cluster.

    Reuses the job-cluster builder (single-node, init script that installs
    ``postgresql-client``, sizing) and adds the bits an interactive cluster needs: a
    stable name, auto-termination, and a lookup tag.
    """
    cfg = _new_cluster_config(ws, node_type, _current_identity(ws), _upload_init_script(ws))
    cfg["cluster_name"] = _CLUSTER_NAME
    cfg["autotermination_minutes"] = _cluster_autotermination_min()
    cfg["custom_tags"] = {**cfg.get("custom_tags", {}), "pgbench_cluster": "true"}
    return cfg


def _find_benchmark_cluster(ws: WorkspaceClient) -> Optional[Any]:
    for c in ws.clusters.list():
        if c.cluster_name == _CLUSTER_NAME:
            return c
    return None


def _get_or_create_benchmark_cluster(ws: WorkspaceClient) -> str:
    """Return the app-owned dedicated pgbench cluster, creating it if absent.

    The app service principal owns one fixed-size single-node cluster per workspace
    (looked up by name), so the app works in every workspace with no pre-provisioning and
    every run reuses the same warm cluster — no per-run resize, so no restart and a stable
    egress IP. A terminated cluster is auto-started when the job attaches. If the fixed
    tier changed (a redeploy) the existing cluster is edited to match.
    """
    node_type = _benchmark_node_type(ws)
    existing = _find_benchmark_cluster(ws)
    cfg = _benchmark_cluster_config(ws, node_type)
    if existing is None:
        resp = ws.api_client.do("POST", "/api/2.0/clusters/create", body=cfg)
        cluster_id = str(resp.get("cluster_id")) if isinstance(resp, dict) else ""
        logger.info(f"pgbench cluster: created {cluster_id} (node_type={node_type})")
        return cluster_id

    cluster_id = existing.cluster_id or ""
    if (existing.node_type_id or "") != node_type:
        ws.api_client.do("POST", "/api/2.0/clusters/edit", body={**cfg, "cluster_id": cluster_id})
        logger.info(f"pgbench cluster: reconciled {cluster_id} to node_type={node_type}")
    else:
        logger.info(f"pgbench cluster: reusing {cluster_id} (node_type={node_type})")
    return cluster_id


# --------------------------------------------------------------------------- #
# Job lifecycle
# --------------------------------------------------------------------------- #
def _find_job_id(ws: WorkspaceClient, name: str) -> Optional[int]:
    # Filter by name server-side so we don't page through every job in the workspace.
    for job in ws.jobs.list(name=name):
        if job.settings and job.settings.name == name:
            return job.job_id
    return None


def _get_or_create_job(ws: WorkspaceClient, cluster_id: str) -> str:
    """Return the reusable pgbench job, pointed at the app-owned benchmark cluster."""
    notebook_path = _upload_notebook(ws)
    task: dict[str, Any] = {
        "task_key": "pgbench_test",
        "notebook_task": {"notebook_path": notebook_path, "base_parameters": {}},
        "timeout_seconds": _TIMEOUT_SECONDS,
        "existing_cluster_id": cluster_id,
    }

    settings = {
        "name": _JOB_NAME,
        "tasks": [task],
        # Serialize execution (one benchmark at a time on the shared cluster) but *queue*
        # excess runs instead of dropping them: without queueing, a submit that exceeds
        # max_concurrent_runs is SKIPPED (which surfaces as a failed run).
        "max_concurrent_runs": 1,
        "queue": {"enabled": True},
        "timeout_seconds": _TIMEOUT_SECONDS,
    }

    existing_id = _find_job_id(ws, _JOB_NAME)
    if existing_id is None:
        resp = ws.api_client.do("POST", "/api/2.1/jobs/create", body=settings)
        job_id = str(resp.get("job_id")) if isinstance(resp, dict) else ""
        logger.info(f"pgbench job: created job {job_id}")
        return job_id

    # Reset so the job always points at the current benchmark cluster.
    ws.api_client.do(
        "POST", "/api/2.1/jobs/reset", body={"job_id": existing_id, "new_settings": settings}
    )
    logger.info(f"pgbench job: reused/updated job {existing_id}")
    return str(existing_id)


def _ensure_token_secret(ws: WorkspaceClient, token: str) -> None:
    """Stash the DB token in a secret scope owned by the app SP.

    The token is minted in the backend as the OBO user (see ``resolve_credentials``), so
    the benchmark still connects to Postgres as the user; writing it to a secret and
    reading it via ``dbutils.secrets.get`` in the notebook keeps it out of the job's run
    parameters/logs. Idempotent: the scope is created once, the key overwritten per run.
    """
    try:
        ws.secrets.create_scope(scope=_SECRET_SCOPE)
    except Exception as e:  # noqa: BLE001 - already-exists is expected on reruns
        if "RESOURCE_ALREADY_EXISTS" not in str(e):
            logger.info(f"pgbench: secret scope create note: {e}")
    try:
        ws.secrets.put_secret(scope=_SECRET_SCOPE, key=_SECRET_KEY, string_value=token)
    except Exception as e:  # noqa: BLE001
        sp = _current_identity(ws)
        raise ValueError(
            f"Could not store the database token in secret scope '{_SECRET_SCOPE}'. The "
            f"app service principal ({sp}) needs permission to create/write secret scopes "
            f"in this workspace. Ask a workspace admin to grant it, then retry. ({e})"
        ) from e


# Per-process cache of the (stable) benchmark cluster + job ids. The first submit of each
# worker does the full, slow setup (list workspace jobs/clusters, upload the notebook +
# init script, create/reset the job); later submits reuse these and just run_now, so they
# don't hang for seconds. A redeploy starts a fresh process, so a changed notebook/config
# is still picked up. Invalidated (force=True) after a run_now failure in case the job or
# cluster was deleted out from under us.
_cached_cluster_id: Optional[str] = None
_cached_job_id: Optional[str] = None


def _resolve_job(ws: WorkspaceClient, *, force: bool = False) -> str:
    """Return the pgbench job id, provisioning the cluster + job on first use and caching
    both for the life of the worker process."""
    global _cached_cluster_id, _cached_job_id
    if force:
        _cached_cluster_id = None
        _cached_job_id = None
    if _cached_cluster_id is None:
        _cached_cluster_id = _get_or_create_benchmark_cluster(ws)
    if _cached_job_id is None:
        _cached_job_id = _get_or_create_job(ws, _cached_cluster_id)
    return _cached_job_id


def submit(
    ws: WorkspaceClient,
    *,
    creds: PgCredentials,
    config: dict[str, Any],
    queries: list[dict[str, Any]],
    schema: Optional[str] = None,
) -> dict[str, Any]:
    """Submit a pgbench run and return job/run identifiers and workspace links."""
    # Validate the schema up front (raises ValueError) and build the PGOPTIONS the
    # notebook exports so unqualified table names resolve to the chosen schema.
    pgoptions = search_path_option(schema) or ""
    query_json = json.dumps(queries)
    if len(query_json) > _INLINE_QUERY_LIMIT:
        raise ValueError(
            "Query payload is too large to pass inline to the job. "
            "Reduce the number/size of queries."
        )

    # Stash the DB token in a secret scope; the notebook reads it via dbutils.secrets.get
    # (redacted) so the token never appears in the job run parameters. The username is
    # not sensitive and is passed directly.
    _ensure_token_secret(ws, creds.password)
    params = {
        "lakebase_instance_name": creds.host,
        "database_name": creds.database,
        "pghost": creds.host,
        "pgport": str(creds.port),
        "pgdatabase": creds.database,
        "pguser": creds.user,
        "pgsslmode": creds.ssl_mode,
        "pgoptions": pgoptions,
        "secret_scope": _SECRET_SCOPE,
        "secret_key": _SECRET_KEY,
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

    # Resolve the job/cluster from the per-process cache (fast after the first submit);
    # rebuild once if the cached job was deleted and run_now fails.
    job_id = _resolve_job(ws)
    try:
        run = ws.jobs.run_now(job_id=int(job_id), notebook_params=params)
    except Exception:  # noqa: BLE001 - stale cache (job/cluster removed) → rebuild once
        job_id = _resolve_job(ws, force=True)
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
    "QUEUED": "pending",
    "PENDING": "pending",
    "BLOCKED": "pending",
    "WAITING_FOR_RETRY": "pending",
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
    elif status == "failed":
        out["error"] = _failure_detail(ws, run)
    return out


def _failure_detail(ws: WorkspaceClient, run: Any) -> Optional[str]:
    """Extract the underlying failure cause from the run and, when it's a recognizable
    permission problem, append an actionable suggestion for the user."""
    causes: list[str] = []
    state = run.state
    if state and getattr(state, "state_message", None):
        causes.append(state.state_message)
    for task in run.tasks or []:
        ts = getattr(task, "state", None)
        msg = getattr(ts, "state_message", None) if ts else None
        if msg and msg not in causes:
            causes.append(msg)
        # The pgbench stderr (e.g. an IP-ACL rejection) surfaces in the task's run
        # output error, not the state message, so pull that too.
        tr_id = getattr(task, "run_id", None)
        if tr_id:
            try:
                out = ws.jobs.get_run_output(tr_id)
                err = getattr(out, "error", None) if out else None
                if err and err not in causes:
                    causes.append(err)
            except Exception:  # noqa: BLE001
                pass
    cause = "\n".join(causes).strip()

    suggestion = _permission_suggestion(ws, cause) or _ip_acl_suggestion(ws, cause)
    if suggestion:
        return f"{cause}\n\n{suggestion}" if cause else suggestion
    return cause or None


def _permission_suggestion(ws: WorkspaceClient, cause: str) -> Optional[str]:
    """If the failure is a cluster-create permission denial, explain the fix.

    The pgbench job cluster is created by the app *service principal*, so the fix is to
    grant that SP the cluster-create entitlement (or reuse an existing cluster)."""
    low = (cause or "").lower()
    is_cluster_perm = "not authorized to create clusters" in low or (
        "permission_denied" in low and "cluster" in low
    )
    if not is_cluster_perm:
        return None
    try:
        sp = _current_identity(ws)
    except Exception:  # noqa: BLE001
        sp = "the app service principal"
    return (
        f"The pgbench job cluster is created by this app's service principal ({sp}), "
        f"which is not authorized to create clusters in this workspace. To fix this, ask "
        f"a workspace admin to grant that service principal the \"Allow unrestricted "
        f"cluster creation\" entitlement (Settings → Identity and access → Service "
        f"principals → select the SP → Entitlements), or select an existing cluster in "
        f"the Cluster field before submitting so no new cluster needs to be created."
    )


def _ip_acl_suggestion(ws: WorkspaceClient, cause: Optional[str]) -> Optional[str]:
    """If the failure is a workspace IP-ACL rejection, extract the blocked source IP and
    explain the one-time allow-list fix — naming the exact workspace and targeting the
    CLI command at it (so an admin with multiple profiles doesn't hit the wrong one).

    Lakebase connections traverse the public endpoint and are gated by the *workspace*
    IP access list. A benchmark cluster's egress IP that isn't on the allow-list is
    dropped at the edge with ``FATAL: External authorization failed`` before auth."""
    low = (cause or "").lower()
    if "external authorization failed" not in low and "blocked by databricks ip acl" not in low:
        return None
    m = re.search(r"source ip address:\s*([0-9a-fA-F:.]+)", cause or "", re.IGNORECASE)
    ip = m.group(1).rstrip(".") if m else None
    m_ws = re.search(r"workspace:\s*(\d+)", cause or "", re.IGNORECASE)
    ws_id = m_ws.group(1) if m_ws else None
    host = _workspace_url(ws)  # the workspace this app (and its cluster) run in

    where = ""
    if host:
        where = f" of workspace {host}" + (f" (id {ws_id})" if ws_id else "")
    elif ws_id:
        where = f" of workspace id {ws_id}"

    msg = (
        f"The pgbench cluster's egress IP{f' ({ip})' if ip else ''} is blocked by the IP "
        f"access list{where}, so it can't reach the Lakebase endpoint. A workspace admin "
        f"must add the source IP to that workspace's IP access list "
        f"(Settings → Security → IP access list, or the CLI below)."
    )
    if ip:
        # One stable-label entry that is created once and *updated* when the cluster's
        # egress IP rotates — avoids accumulating a new single-IP allow entry per IP.
        label = "lakebase-pgbench-egress"
        json_body = (
            "'{\"label\":\"" + label + "\",\"list_type\":\"ALLOW\",\"enabled\":true,"
            + "\"ip_addresses\":[\"" + ip + "/32\"]}'"
        )
        create_cmd = f"databricks ip-access-lists create -p <profile> --json {json_body}"
        # Return just the list_id for the stable-label entry, so the user can paste it
        # straight into <list_id> below instead of eyeballing the full list output.
        find_id_cmd = (
            "databricks ip-access-lists list -p <profile> -o json | "
            f"jq -r '.[] | select(.label==\"{label}\") | .list_id'"
        )
        update_cmd = (
            f"databricks ip-access-lists update <list_id> -p <profile> --json {json_body}"
        )
        msg += (
            "\n\nRun the Databricks CLI on your machine (not in the app), authenticated "
            "as an admin of that workspace:"
            f"\n\n• First time — add the IP:\n  {create_cmd}"
            "\n\n• If it already exists (the egress IP changed) — update that entry in "
            f"place. First get its id (the `{label}` row):\n  {find_id_cmd}\n  then paste "
            f"that id in place of <list_id>:\n  {update_cmd}"
        )
        hint = f"whose host is {host}" if host else "pointing at this workspace"
        msg += (
            f"\n\nReplace `<profile>` with the ~/.databrickscfg profile {hint} (run "
            f"`databricks auth profiles`). The CLI has no `--host` flag; without `-p` it "
            f"uses your DEFAULT profile, which may be a different workspace."
        )
    return msg


def _status_message(life: str, result: Optional[str]) -> str:
    if life == "QUEUED":
        return "Run is queued behind another pgbench run"
    if life in ("PENDING", "BLOCKED", "WAITING_FOR_RETRY"):
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
