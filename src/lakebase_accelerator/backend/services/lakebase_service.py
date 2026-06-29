"""Lakebase Autoscaling service: project discovery, credential generation, and
database listing via the Databricks SDK ``w.postgres`` API.

Autoscaling-only (projects → branches → endpoints). Provisioned (``w.database``)
is intentionally not supported. Ported and slimmed from the legacy
``oauth_service.py`` autoscaling SDK path; the REST fallback was dropped because the
pinned SDK (>=0.74) always exposes ``w.postgres``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from databricks.sdk import WorkspaceClient


def _is_not_found(e: Exception) -> bool:
    msg = (getattr(e, "message", None) or str(e)).lower()
    return "not found" in msg


@dataclass
class PgCredentials:
    host: str
    port: int
    database: str
    user: str
    password: str
    ssl_mode: str = "require"


@dataclass
class ProjectInfo:
    name: str          # display name when present, else project id (use as the handle)
    id: Optional[str]
    display_name: Optional[str]
    state: Optional[str]


def list_projects(ws: WorkspaceClient) -> list[ProjectInfo]:
    """List Lakebase Autoscaling projects the caller (OBO or SP) can access."""
    projects: list[ProjectInfo] = []
    for p in ws.postgres.list_projects():
        spec = getattr(p, "spec", None)
        display_name = getattr(spec, "display_name", None) if spec else None
        raw_name = getattr(p, "name", None)  # "projects/<id>"
        project_id = raw_name.replace("projects/", "", 1) if raw_name else None
        status = getattr(p, "status", None)
        state = getattr(status, "state", None) if status else getattr(p, "state", None)
        handle = display_name or project_id
        if handle:
            projects.append(
                ProjectInfo(
                    name=handle,
                    id=project_id,
                    display_name=display_name,
                    state=str(state) if state is not None else None,
                )
            )
    return projects


def _resolve_project_name(ws: WorkspaceClient, project: str) -> str:
    """Resolve a project handle (id, ``projects/<id>``, or display name) to its
    full resource name ``projects/<id>``."""
    full = project if project.startswith("projects/") else f"projects/{project}"
    try:
        resolved = ws.postgres.get_project(name=full)
        if resolved and resolved.name:
            return resolved.name
    except Exception as e:
        if not _is_not_found(e):
            raise
    # Fall back to matching by display name
    for p in ws.postgres.list_projects():
        spec = getattr(p, "spec", None)
        if spec and getattr(spec, "display_name", None) == project:
            if p.name:
                return p.name
    raise ValueError(
        f"Lakebase project not found: '{project}'. "
        "Use the project ID from the URL or the project's display name."
    )


def resolve_credentials(
    ws: WorkspaceClient, project: str, database: Optional[str] = None
) -> PgCredentials:
    """Resolve connection credentials for a project's primary endpoint:
    pick an active/idle read-write endpoint, read its host, and mint a short-lived
    OAuth token via ``generate_database_credential``."""
    pg = ws.postgres
    project_name = _resolve_project_name(ws, project)

    branches = list(pg.list_branches(parent=project_name))
    if not branches or not branches[0].name:
        raise ValueError(f"No branches found in project {project_name}")
    branch_name: str = branches[0].name

    endpoints = list(pg.list_endpoints(parent=branch_name))
    if not endpoints:
        raise ValueError(f"No endpoints found in branch {branch_name}")

    endpoint = None
    for ep in endpoints:
        status = getattr(ep, "status", None)
        hosts = getattr(status, "hosts", None) if status else None
        if hosts and getattr(hosts, "host", None):
            state = getattr(status, "current_state", None)
            if state and str(state).upper() in ("ACTIVE", "IDLE"):
                endpoint = ep
                break
    if endpoint is None:
        endpoint = endpoints[0]

    host = None
    if endpoint.status and endpoint.status.hosts:
        host = getattr(endpoint.status.hosts, "host", None) or getattr(
            endpoint.status.hosts, "read_only_host", None
        )
    if not host:
        raise ValueError(
            f"Endpoint {endpoint.name} has no host; it may still be initializing."
        )
    if not endpoint.name:
        raise ValueError("Endpoint has no resource name.")

    cred = pg.generate_database_credential(endpoint=endpoint.name)
    token = getattr(cred, "token", None)
    if not token:
        raise ValueError("Failed to obtain OAuth token for Lakebase Autoscaling.")

    user_name = ws.current_user.me().user_name
    if not user_name:
        raise ValueError("Could not resolve the current user's Postgres role name.")
    return PgCredentials(
        host=host,
        port=5432,
        database=database or "databricks_postgres",
        user=user_name,
        password=token,
        ssl_mode="require",
    )


def list_databases(ws: WorkspaceClient, project: str) -> list[str]:
    """List non-template databases in a project by connecting and querying
    ``pg_database`` with freshly-resolved credentials."""
    import psycopg

    creds = resolve_credentials(ws, project)
    with psycopg.connect(
        host=creds.host,
        port=creds.port,
        dbname=creds.database,
        user=creds.user,
        password=creds.password,
        sslmode=creds.ssl_mode,
        connect_timeout=10,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT datname FROM pg_database "
                "WHERE datistemplate = false ORDER BY datname"
            )
            return [row[0] for row in cur.fetchall()]
