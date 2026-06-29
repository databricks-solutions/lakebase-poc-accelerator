"""Resolve PostgreSQL connection credentials from the supported auth methods.

Deploy-primary + dev-fallback:
- ``identity``     — OBO/SP client resolves the project's endpoint + mints a token (primary).
- ``app_resource`` — read PG* env vars injected by an attached Lakebase app resource (primary).
- ``oauth``        — caller pastes endpoint host + OAuth token + Postgres user (dev fallback).
"""

from __future__ import annotations

import os
from typing import Literal, Optional

from databricks.sdk import WorkspaceClient

from .lakebase_service import PgCredentials, resolve_credentials

AuthMethod = Literal["identity", "app_resource", "oauth"]


def _env(name: str) -> Optional[str]:
    v = os.environ.get(name)
    return v.strip() if v and v.strip() else None


def app_resource_configured() -> bool:
    """True when a Lakebase app resource injected the standard libpq env vars."""
    return bool(_env("PGHOST") and _env("PGUSER") and _env("PGPASSWORD"))


def resolve(
    ws: WorkspaceClient,
    *,
    auth_method: AuthMethod,
    project: Optional[str] = None,
    database: Optional[str] = None,
    endpoint_host: Optional[str] = None,
    access_token: Optional[str] = None,
    postgres_user_name: Optional[str] = None,
) -> PgCredentials:
    if auth_method == "identity":
        proj = (project or "").strip()
        if not proj:
            raise ValueError("A Lakebase project is required for identity auth.")
        return resolve_credentials(ws, proj, database)

    if auth_method == "app_resource":
        if not app_resource_configured():
            raise ValueError(
                "No Lakebase app resource is attached (PGHOST/PGUSER/PGPASSWORD not set)."
            )
        return PgCredentials(
            host=_env("PGHOST") or "",
            port=int(_env("PGPORT") or "5432"),
            database=database or _env("PGDATABASE") or "databricks_postgres",
            user=_env("PGUSER") or "",
            password=_env("PGPASSWORD") or "",
            ssl_mode=_env("PGSSLMODE") or "require",
        )

    if auth_method == "oauth":
        token = (access_token or "").strip()
        host = (endpoint_host or "").strip()
        user = (postgres_user_name or "").strip()
        if not token:
            raise ValueError("Postgres OAuth token is required (Lakebase Connect → Copy OAuth token).")
        if not host:
            raise ValueError("Endpoint host is required (from the Lakebase Connect dialog).")
        if not user:
            raise ValueError("Postgres user (your Databricks email/username) is required for OAuth.")
        return PgCredentials(
            host=host,
            port=5432,
            database=database or "databricks_postgres",
            user=user,
            password=token,
            ssl_mode="require",
        )

    raise ValueError(f"Unsupported auth method: {auth_method}")
