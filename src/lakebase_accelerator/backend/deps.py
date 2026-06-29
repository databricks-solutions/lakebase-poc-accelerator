"""Shared FastAPI dependencies for the Lakebase accelerator.

Deploy-primary + dev-fallback auth model:
- When deployed as a Databricks App, requests carry an ``X-Forwarded-Access-Token``
  header → we build an on-behalf-of-user (OBO) WorkspaceClient so Lakebase
  operations are scoped to the logged-in user's identity.
- Locally (no OBO header), we fall back to the app-level service-principal client
  built at startup (which uses the pinned ``DATABRICKS_CONFIG_PROFILE``). This keeps
  ``apx dev`` usable without a forwarded token.
"""

from __future__ import annotations

from typing import Annotated, TypeAlias

from databricks.sdk import WorkspaceClient
from fastapi import Depends, Request


def get_effective_ws(request: Request) -> WorkspaceClient:
    """Return an OBO WorkspaceClient when a forwarded user token is present,
    otherwise the service-principal client created at app startup."""
    token = request.headers.get("X-Forwarded-Access-Token")
    if token:
        # auth_type=pat to avoid the SDK trying SP/CLI auth alongside the token
        return WorkspaceClient(token=token, auth_type="pat")
    return request.app.state.workspace_client


EffectiveClient: TypeAlias = Annotated[WorkspaceClient, Depends(get_effective_ws)]
