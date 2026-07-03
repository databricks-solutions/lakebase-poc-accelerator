"""Shared Lakebase Autoscaling endpoints: project + database discovery.

These power the project/database pickers used across the Deployment, Testing, and
Quickstart routes. They degrade gracefully (return an empty list + error string)
so the UI dropdowns can fall back to manual entry — e.g. locally without an OBO
token, or before the user has access to any project.
"""

from __future__ import annotations

import base64
import binascii
import json

from fastapi import Request
from pydantic import BaseModel

from ..core import create_router, logger
from ..deps import EffectiveClient
from ..services import lakebase_service

router = create_router()


class WorkspaceInfoOut(BaseModel):
    host: str | None = None


@router.get(
    "/workspace-info",
    response_model=WorkspaceInfoOut,
    operation_id="getWorkspaceInfo",
)
def get_workspace_info(ws: EffectiveClient) -> WorkspaceInfoOut:
    """Return the workspace URL, used to deep-link into Catalog Explorer / native dialogs."""
    host = getattr(ws.config, "host", None)
    return WorkspaceInfoOut(host=host.rstrip("/") if host else None)


class ProjectOut(BaseModel):
    name: str
    id: str | None = None
    display_name: str | None = None
    state: str | None = None


class ProjectListOut(BaseModel):
    projects: list[ProjectOut]
    error: str | None = None


class DatabaseListOut(BaseModel):
    databases: list[str]
    error: str | None = None


class SchemaListOut(BaseModel):
    schemas: list[str]
    error: str | None = None


class TokenScopesOut(BaseModel):
    has_obo_token: bool
    scopes: list[str] = []
    has_postgres_scope: bool = False
    note: str | None = None


@router.get(
    "/lakebase/token-scopes",
    response_model=TokenScopesOut,
    operation_id="getTokenScopes",
)
def get_token_scopes(request: Request) -> TokenScopesOut:
    """Diagnostic: decode the forwarded user (OBO) token and report its OAuth scopes.

    Reads the unverified JWT payload of ``X-Forwarded-Access-Token`` to confirm
    whether the ``postgres`` scope was granted after re-authorization.
    """
    token = request.headers.get("X-Forwarded-Access-Token")
    if not token:
        return TokenScopesOut(
            has_obo_token=False,
            note="No X-Forwarded-Access-Token header (running locally or not via the deployed app).",
        )
    try:
        payload_b64 = token.split(".")[1]
        payload_b64 += "=" * (-len(payload_b64) % 4)  # pad base64
        claims = json.loads(base64.urlsafe_b64decode(payload_b64))
    except (IndexError, ValueError, binascii.Error) as e:
        return TokenScopesOut(has_obo_token=True, note=f"Could not decode token payload: {e}")
    raw = claims.get("scope") or claims.get("scp") or ""
    scopes = raw.split() if isinstance(raw, str) else list(raw)
    return TokenScopesOut(
        has_obo_token=True,
        scopes=sorted(scopes),
        has_postgres_scope="postgres" in scopes,
    )


@router.get(
    "/lakebase/projects",
    response_model=ProjectListOut,
    operation_id="listLakebaseProjects",
)
def list_lakebase_projects(ws: EffectiveClient) -> ProjectListOut:
    """List Lakebase Autoscaling projects the caller can access (OBO when deployed)."""
    try:
        projects = [
            ProjectOut(name=p.name, id=p.id, display_name=p.display_name, state=p.state)
            for p in lakebase_service.list_projects(ws)
        ]
        return ProjectListOut(projects=projects)
    except Exception as e:  # noqa: BLE001 - surface as soft error for the dropdown
        logger.info(f"Could not list Lakebase projects: {e}")
        return ProjectListOut(projects=[], error=str(e))


@router.get(
    "/lakebase/databases",
    response_model=DatabaseListOut,
    operation_id="listLakebaseDatabases",
)
def list_lakebase_databases(ws: EffectiveClient, project: str) -> DatabaseListOut:
    """List databases inside a Lakebase Autoscaling project."""
    if not project.strip():
        return DatabaseListOut(databases=[], error="project is required")
    try:
        return DatabaseListOut(databases=lakebase_service.list_databases(ws, project))
    except Exception as e:  # noqa: BLE001
        logger.info(f"Could not list databases for {project}: {e}")
        return DatabaseListOut(databases=[], error=str(e))


@router.get(
    "/lakebase/schemas",
    response_model=SchemaListOut,
    operation_id="listLakebaseSchemas",
)
def list_lakebase_schemas(
    ws: EffectiveClient, project: str, database: str | None = None
) -> SchemaListOut:
    """List schemas in a project's database, to populate the default-schema picker."""
    if not project.strip():
        return SchemaListOut(schemas=[], error="project is required")
    try:
        return SchemaListOut(schemas=lakebase_service.list_schemas(ws, project, database))
    except Exception as e:  # noqa: BLE001
        logger.info(f"Could not list schemas for {project}/{database}: {e}")
        return SchemaListOut(schemas=[], error=str(e))
