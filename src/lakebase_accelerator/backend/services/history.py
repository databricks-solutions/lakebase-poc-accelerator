"""Persist before/after test-run history into a Lakebase Postgres table.

This is the *app-owned* store: it is always read/written under the app's service
principal (so the table is SP-owned and shared across all app users), while the
real end-user identity is recorded in ``created_by`` for attribution. Browser
(localStorage) persistence is handled entirely on the frontend; this module only
covers the Lakebase destination.

Writes are best-effort from the caller's perspective — a failure here must never
break a test run.
"""

from __future__ import annotations

import re
from typing import Any, Optional

import psycopg
from psycopg.types.json import Json

from .lakebase_service import PgCredentials

TABLE = "_accelerator_run_history"
# Least-privilege default: a dedicated schema the app service principal OWNS, so it
# can manage its history table but has no access to the project's other data.
DEFAULT_SCHEMA = "accelerator_history"

# Schema names are interpolated into DDL/queries, so they must be plain identifiers.
_IDENT_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _validate_schema(schema: str) -> str:
    schema = (schema or DEFAULT_SCHEMA).strip()
    if not _IDENT_RE.match(schema):
        raise ValueError(f"Invalid schema name: {schema!r}")
    return schema


def provisioning_sql(sp_role: str, schema: str, database: str, *, include_role: bool) -> str:
    """One-time setup SQL a project owner runs to give the SP a least-privilege home.

    ``include_role`` is True for the Layer-1 case (the SP can't even connect, so its
    role doesn't exist yet); False once we've connected and only the owned schema is
    missing.
    """
    schema = _validate_schema(schema)
    lines: list[str] = []
    if include_role:
        lines += [
            "-- The app service principal has no role in this project yet.",
            f'CREATE ROLE "{sp_role}" WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT;',
            f'GRANT CONNECT ON DATABASE {database} TO "{sp_role}";',
        ]
    lines.append(
        f"-- Dedicated schema the SP owns — its entire sandbox; no access to your other data."
    )
    lines.append(f'CREATE SCHEMA IF NOT EXISTS {schema} AUTHORIZATION "{sp_role}";')
    return "\n".join(lines)


def ddl(schema: str) -> str:
    """The CREATE TABLE statement for the history table (shown to the user as consent)."""
    schema = _validate_schema(schema)
    return (
        f"CREATE TABLE IF NOT EXISTS {schema}.{TABLE} (\n"
        "  id                uuid PRIMARY KEY DEFAULT gen_random_uuid(),\n"
        "  created_at        timestamptz NOT NULL DEFAULT now(),\n"
        "  label             text,\n"
        "  project           text,\n"
        "  concurrency_level int,\n"
        "  queries           jsonb,\n"
        "  baseline_report   jsonb,\n"
        "  optimized_report  jsonb,\n"
        "  index_ddls        jsonb,\n"
        "  created_by        text\n"
        ");"
    )


def _connect(creds: PgCredentials, *, autocommit: bool = False):
    return psycopg.connect(
        host=creds.host,
        port=creds.port,
        dbname=creds.database,
        user=creds.user,
        password=creds.password,
        sslmode=creds.ssl_mode,
        connect_timeout=10,
        autocommit=autocommit,
    )


def ensure_table(creds: PgCredentials, schema: str) -> dict[str, Any]:
    """Preflight the service-principal's privileges and create the table if absent.

    Returns ``{ok, table, message, grant_sql}``. ``ok`` is False (with ``grant_sql``)
    for each least-privilege gap: the SP's owned schema is missing, the schema exists
    but the SP doesn't own/can't create in it, or the table exists but the SP lacks
    INSERT. ``grant_sql`` is the exact statement a project owner runs to fix it.

    This assumes the SP can already CONNECT (it has a role). The no-role / can't-connect
    case (Layer 1) surfaces as a connection error to the caller, which builds the
    role-creation SQL from the SP identity.
    """
    schema = _validate_schema(schema)
    qualified = f"{schema}.{TABLE}"

    with _connect(creds, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("SELECT current_user")
        sp_role = cur.fetchone()[0]

        # Does the SP's dedicated schema exist at all?
        cur.execute("SELECT to_regnamespace(%s)", (schema,))
        schema_exists = cur.fetchone()[0] is not None
        if not schema_exists:
            return {
                "ok": False,
                "table": qualified,
                "message": (
                    f'Schema "{schema}" does not exist. A project owner must create it and '
                    f'make the app service principal ({sp_role}) its owner, so the app is '
                    f"confined to this schema and cannot touch your other data."
                ),
                "grant_sql": provisioning_sql(sp_role, schema, creds.database, include_role=False),
            }

        cur.execute("SELECT to_regclass(%s)", (qualified,))
        table_exists = cur.fetchone()[0] is not None

        if table_exists:
            cur.execute("SELECT has_table_privilege(current_user, %s, 'INSERT')", (qualified,))
            can_insert = bool(cur.fetchone()[0])
            if can_insert:
                return {"ok": True, "table": qualified, "message": f"Using existing {qualified}.", "grant_sql": None}
            return {
                "ok": False,
                "table": qualified,
                "message": f"{qualified} exists but {sp_role} lacks INSERT. Ask a project owner to run the grant below.",
                "grant_sql": f'GRANT INSERT, SELECT ON {qualified} TO "{sp_role}";',
            }

        cur.execute("SELECT has_schema_privilege(current_user, %s, 'CREATE')", (schema,))
        can_create = bool(cur.fetchone()[0])
        if not can_create:
            return {
                "ok": False,
                "table": qualified,
                "message": (
                    f'{sp_role} cannot create objects in schema "{schema}" (it does not own it). '
                    f"Ask a project owner to make the SP the schema owner so it stays confined to this schema."
                ),
                "grant_sql": f'ALTER SCHEMA {schema} OWNER TO "{sp_role}";',
            }

        cur.execute(ddl(schema))  # ty: ignore[no-matching-overload]
        return {"ok": True, "table": qualified, "message": f"Created {qualified} (owned by {sp_role}).", "grant_sql": None}


def save_run(
    creds: PgCredentials,
    schema: str,
    *,
    created_by: Optional[str],
    label: Optional[str],
    project: Optional[str],
    concurrency_level: Optional[int],
    queries: list[dict[str, Any]],
    baseline_report: Optional[dict[str, Any]],
    optimized_report: Optional[dict[str, Any]],
    index_ddls: list[str],
) -> str:
    """Insert one run row and return its id. Assumes the table already exists."""
    schema = _validate_schema(schema)
    qualified = f"{schema}.{TABLE}"
    with _connect(creds, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO {qualified} "  # ty: ignore[no-matching-overload]
            "(label, project, concurrency_level, queries, baseline_report, optimized_report, index_ddls, created_by) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id",
            (
                label,
                project,
                concurrency_level,
                Json(queries),
                Json(baseline_report) if baseline_report is not None else None,
                Json(optimized_report) if optimized_report is not None else None,
                Json(index_ddls),
                created_by,
            ),
        )
        return str(cur.fetchone()[0])


def list_runs(creds: PgCredentials, schema: str, limit: int = 50) -> list[dict[str, Any]]:
    """Return the most recent runs (newest first)."""
    schema = _validate_schema(schema)
    qualified = f"{schema}.{TABLE}"
    limit = max(1, min(limit, 500))
    with _connect(creds) as conn, conn.cursor() as cur:
        cur.execute(
            f"SELECT id, created_at, label, project, concurrency_level, queries, "  # ty: ignore[no-matching-overload]
            f"baseline_report, optimized_report, index_ddls, created_by "
            f"FROM {qualified} ORDER BY created_at DESC LIMIT {limit}"
        )
        rows = cur.fetchall()
    return [
        {
            "id": str(r[0]),
            "created_at": r[1].isoformat() if r[1] else None,
            "label": r[2],
            "project": r[3],
            "concurrency_level": r[4],
            "queries": r[5] or [],
            "baseline_report": r[6],
            "optimized_report": r[7],
            "index_ddls": r[8] or [],
            "created_by": r[9],
        }
        for r in rows
    ]
