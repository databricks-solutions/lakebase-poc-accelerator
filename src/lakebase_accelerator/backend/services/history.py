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
import uuid
from typing import Any, Optional

import psycopg
from psycopg.types.json import Json

from .lakebase_service import PgCredentials

DEFAULT_TABLE = "_accelerator_run_history"
# Least-privilege default: a dedicated schema the app service principal OWNS, so it
# can manage its history table but has no access to the project's other data.
DEFAULT_SCHEMA = "accelerator_history"

# Schema/table names are interpolated into DDL/queries, so they must be plain identifiers.
_IDENT_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _validate_schema(schema: str) -> str:
    schema = (schema or DEFAULT_SCHEMA).strip()
    if not _IDENT_RE.match(schema):
        raise ValueError(f"Invalid schema name: {schema!r}")
    return schema


def _validate_table(table: str) -> str:
    table = (table or DEFAULT_TABLE).strip()
    if not _IDENT_RE.match(table):
        raise ValueError(f"Invalid table name: {table!r}")
    return table


def _qualified(schema: str, table: str) -> str:
    return f"{_validate_schema(schema)}.{_validate_table(table)}"


def _uuid_or_none(value: Any) -> Optional[str]:
    """Return a canonical UUID string if ``value`` is a valid UUID, else None.

    The client supplies a stable ``id`` per run (used to make archiving idempotent),
    but older/fallback ids may not be valid UUIDs — those get a server-generated id.
    """
    try:
        return str(uuid.UUID(str(value)))
    except (ValueError, TypeError, AttributeError):
        return None


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


def ddl(schema: str, table: str = DEFAULT_TABLE) -> str:
    """The CREATE TABLE statement for a history table (shown to the user as consent).

    Engine-agnostic: ``engine`` discriminates psycopg vs pgbench and ``config`` holds
    the engine-specific run parameters (psycopg concurrency, pgbench clients/jobs/…).
    """
    qualified = _qualified(schema, table)
    return (
        f"CREATE TABLE IF NOT EXISTS {qualified} (\n"
        "  id                uuid PRIMARY KEY DEFAULT gen_random_uuid(),\n"
        "  created_at        timestamptz NOT NULL DEFAULT now(),\n"
        "  engine            text,\n"
        "  label             text,\n"
        "  project           text,\n"
        "  config            jsonb,\n"
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


def _migrate(cur, qualified: str) -> None:
    """Bring an older history table up to the engine-aware shape (idempotent)."""
    cur.execute(f"ALTER TABLE {qualified} ADD COLUMN IF NOT EXISTS engine text")  # noqa: S608
    cur.execute(f"ALTER TABLE {qualified} ADD COLUMN IF NOT EXISTS config jsonb")  # noqa: S608


def ensure_table(creds: PgCredentials, schema: str, table: str = DEFAULT_TABLE) -> dict[str, Any]:
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
    table = _validate_table(table)
    qualified = f"{schema}.{table}"

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
            if not can_insert:
                return {
                    "ok": False,
                    "table": qualified,
                    "message": f"{qualified} exists but {sp_role} lacks INSERT. Ask a project owner to run the grant below.",
                    "grant_sql": f'GRANT INSERT, SELECT ON {qualified} TO "{sp_role}";',
                }
            _migrate(cur, qualified)
            return {"ok": True, "table": qualified, "message": f"Using existing {qualified}.", "grant_sql": None}

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

        cur.execute(ddl(schema, table))
        return {"ok": True, "table": qualified, "message": f"Created {qualified} (owned by {sp_role}).", "grant_sql": None}


def list_tables(creds: PgCredentials, schema: str) -> list[str]:
    """List existing tables in the SP-owned schema, so the user can archive to an
    existing one. Best-effort: returns [] if the schema is missing/unreadable."""
    schema = _validate_schema(schema)
    try:
        with _connect(creds) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT tablename FROM pg_tables WHERE schemaname = %s ORDER BY tablename",
                (schema,),
            )
            return [r[0] for r in cur.fetchall()]
    except Exception:  # noqa: BLE001 - best-effort picker
        return []


def save_runs(
    creds: PgCredentials,
    schema: str,
    table: str,
    *,
    created_by: Optional[str],
    runs: list[dict[str, Any]],
) -> int:
    """Idempotently upsert session runs into the table; return the number written.

    Each run dict carries a stable client-generated ``id`` plus ``engine``/``label``/
    ``project``/``config``/``queries``/``baseline_report``/``optimized_report``/
    ``index_ddls`` and optionally a client ``created_at`` (preserved so archived rows
    keep their original run time).

    Keying on the run's ``id`` makes re-archiving the same session idempotent: hitting
    "Archive" twice (or archiving after a new run) updates the existing rows in place
    rather than inserting duplicates. Runs without a valid UUID ``id`` fall back to a
    server-generated id (plain insert, no dedup).
    """
    qualified = _qualified(schema, table)
    written = 0
    with _connect(creds, autocommit=True) as conn, conn.cursor() as cur:
        for run in runs:
            cur.execute(
                f"INSERT INTO {qualified} "  # noqa: S608
                "(id, created_at, engine, label, project, config, queries, "
                "baseline_report, optimized_report, index_ddls, created_by) "
                "VALUES (COALESCE(%s::uuid, gen_random_uuid()), COALESCE(%s, now()), "
                "%s, %s, %s, %s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (id) DO UPDATE SET "
                "created_at = EXCLUDED.created_at, engine = EXCLUDED.engine, "
                "label = EXCLUDED.label, project = EXCLUDED.project, "
                "config = EXCLUDED.config, queries = EXCLUDED.queries, "
                "baseline_report = EXCLUDED.baseline_report, "
                "optimized_report = EXCLUDED.optimized_report, "
                "index_ddls = EXCLUDED.index_ddls, created_by = EXCLUDED.created_by",
                (
                    _uuid_or_none(run.get("id")),
                    run.get("created_at"),
                    run.get("engine"),
                    run.get("label"),
                    run.get("project"),
                    Json(run.get("config") or {}),
                    Json(run.get("queries") or []),
                    Json(run["baseline_report"]) if run.get("baseline_report") is not None else None,
                    Json(run["optimized_report"]) if run.get("optimized_report") is not None else None,
                    Json(run.get("index_ddls") or []),
                    created_by,
                ),
            )
            written += 1
    return written


def list_runs(creds: PgCredentials, schema: str, table: str = DEFAULT_TABLE, limit: int = 200) -> list[dict[str, Any]]:
    """Return the most recent runs (newest first). Tolerates older rows that predate
    the engine/config columns by defaulting engine to 'psycopg' and folding any
    legacy ``concurrency_level`` into ``config``."""
    qualified = _qualified(schema, table)
    limit = max(1, min(limit, 500))
    with _connect(creds) as conn, conn.cursor() as cur:
        # Detect legacy concurrency_level column for back-compat.
        cur.execute(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s AND column_name = 'concurrency_level'",
            (_validate_schema(schema), _validate_table(table)),
        )
        has_legacy = cur.fetchone() is not None
        legacy_col = ", concurrency_level" if has_legacy else ""
        cur.execute(
            f"SELECT id, created_at, engine, label, project, config, queries, "  # noqa: S608
            f"baseline_report, optimized_report, index_ddls, created_by{legacy_col} "
            f"FROM {qualified} ORDER BY created_at DESC LIMIT {limit}"
        )
        rows = cur.fetchall()

    out: list[dict[str, Any]] = []
    for r in rows:
        config = r[5] or {}
        if not config and has_legacy and r[11] is not None:
            config = {"concurrency_level": r[11]}
        out.append(
            {
                "id": str(r[0]),
                "created_at": r[1].isoformat() if r[1] else None,
                "engine": r[2] or "psycopg",
                "label": r[3],
                "project": r[4],
                "config": config,
                "queries": r[6] or [],
                "baseline_report": r[7],
                "optimized_report": r[8],
                "index_ddls": r[9] or [],
                "created_by": r[10],
            }
        )
    return out
