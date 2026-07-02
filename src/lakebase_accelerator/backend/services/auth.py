"""Resolve PostgreSQL connection credentials from the supported auth methods.

Deploy-primary + dev-fallback:
- ``identity`` — OBO/SP client resolves the project's endpoint + mints a token (primary).
- ``oauth``    — caller pastes endpoint host + OAuth token + Postgres user (dev fallback).
"""

from __future__ import annotations

from typing import Literal, Optional

from databricks.sdk import WorkspaceClient

from .lakebase_service import PgCredentials, resolve_credentials

AuthMethod = Literal["identity", "oauth"]


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
