"""Optimize service: derive index/tuning recommendations for Lakebase.

Two complementary strategies (per the redesign plan):
1. Query parse — extract WHERE/JOIN/ORDER BY columns from the tested SQL and emit
   candidate ``CREATE INDEX`` DDL (B-Tree; composite for multiple equality cols).
2. Live introspection — when a connection is available, run the OLTP Technical
   Guide's detection SQL (seq-vs-index scans, unused indexes, cache-hit ratio,
   pg_stat_statements) and map results to fix actions. Falls back to query-parse
   only when pg_stat_statements / permissions are unavailable.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Optional

import psycopg

from .lakebase_service import PgCredentials

# --- Query parsing -----------------------------------------------------------

_IDENT = r'[a-zA-Z_][a-zA-Z0-9_$]*'
_QUALIFIED = rf'(?:{_IDENT}\.)*{_IDENT}'


@dataclass
class IndexSuggestion:
    table: str
    columns: list[str]
    rationale: str
    ddl: str


def _strip_comments(sql: str) -> str:
    return "\n".join(
        ln for ln in sql.split("\n") if not ln.strip().startswith("--")
    ).strip()


def _first_table(sql: str) -> Optional[str]:
    m = re.search(rf'\bfrom\s+({_QUALIFIED})', sql, re.IGNORECASE)
    if m:
        return m.group(1)
    m = re.search(rf'\b(?:update|into)\s+({_QUALIFIED})', sql, re.IGNORECASE)
    return m.group(1) if m else None


def _bare(col: str) -> str:
    """Return the unqualified column name (drop table alias prefix)."""
    return col.split(".")[-1]


def _index_name(table: str, columns: list[str]) -> str:
    t = _bare(table)
    return f"idx_{t}_" + "_".join(columns)


def parse_candidate_indexes(queries: list[tuple[str, str]]) -> list[IndexSuggestion]:
    """Heuristically derive candidate indexes from (identifier, sql) pairs.

    Equality/IN predicates on the same table are grouped into one (composite) index;
    range and ORDER BY columns each get a single-column suggestion. De-duplicated by
    (table, columns).
    """
    seen: set[tuple[str, tuple[str, ...]]] = set()
    suggestions: list[IndexSuggestion] = []

    def add(table: str, cols: list[str], rationale: str) -> None:
        cols = [c for c in dict.fromkeys(cols)]  # de-dupe, keep order
        if not cols:
            return
        key = (_bare(table), tuple(cols))
        if key in seen:
            return
        seen.add(key)
        name = _index_name(table, cols)
        col_list = ", ".join(cols)
        suggestions.append(
            IndexSuggestion(
                table=table,
                columns=cols,
                rationale=rationale,
                ddl=f"CREATE INDEX IF NOT EXISTS {name} ON {table} ({col_list});",
            )
        )

    for _ident, raw in queries:
        sql = _strip_comments(raw)
        table = _first_table(sql)
        if not table:
            continue

        # SELECT-list aliases (e.g. "sum(ss_net_paid) AS revenue") are not real
        # columns — indexing them fails ("column does not exist"), so exclude them
        # from ORDER BY candidates.
        select_aliases = {a.lower() for a in re.findall(rf'\bas\s+({_IDENT})', sql, re.IGNORECASE)}

        # Equality / IN predicates → composite candidate
        eq_cols = [
            _bare(c)
            for c in re.findall(rf'({_QUALIFIED})\s*(?:=|\bin\b)', sql, re.IGNORECASE)
            if not re.fullmatch(r'\d+', _bare(c))
        ]
        # drop SQL keywords / function-ish tokens accidentally captured
        eq_cols = [c for c in eq_cols if c.lower() not in _SQL_NOISE]
        if eq_cols:
            add(table, eq_cols, "Equality/IN predicate(s) — point-lookup index.")

        # Range predicates → single-column each
        for c in re.findall(rf'({_QUALIFIED})\s*(?:<|>|<=|>=|between)', sql, re.IGNORECASE):
            cb = _bare(c)
            if cb.lower() not in _SQL_NOISE and not re.fullmatch(r'\d+', cb):
                add(table, [cb], "Range predicate — supports range scans.")

        # ORDER BY → single-column (leading) each, but only for real base columns:
        # skip SELECT aliases and expressions/aggregates (e.g. "sum(x)"), which can't
        # be indexed as a plain column.
        ob = re.search(r'\border\s+by\s+(.+?)(?:\blimit\b|;|$)', sql, re.IGNORECASE | re.DOTALL)
        if ob:
            for part in ob.group(1).split(","):
                term = part.strip()
                if not term or "(" in term:  # expression / aggregate — not a plain column
                    continue
                cb = _bare(term.split()[0])
                if (
                    cb
                    and cb.lower() not in _SQL_NOISE
                    and cb.lower() not in select_aliases
                    and not re.fullmatch(r'\d+', cb)
                ):
                    add(table, [cb], "ORDER BY column — avoids a sort.")

    return suggestions


_SQL_NOISE = {
    "select", "from", "where", "and", "or", "not", "null", "true", "false",
    "case", "when", "then", "else", "end", "as", "on", "join", "left", "right",
    "inner", "outer", "group", "order", "by", "limit", "offset", "having",
}

# The app's own bookkeeping table — never a benchmark target, so always excluded
# from live findings.
_HISTORY_TABLE = "_accelerator_run_history"


def tables_in_queries(queries: list[tuple[str, str]]) -> set[str]:
    """Bare, lowercased table names referenced by the benchmark queries (FROM/JOIN/UPDATE/INTO)."""
    tables: set[str] = set()
    for _ident, raw in queries:
        sql = _strip_comments(raw)
        for m in re.finditer(rf'\b(?:from|join|update|into)\s+({_QUALIFIED})', sql, re.IGNORECASE):
            tables.add(_bare(m.group(1)).lower())
    return tables


def _keep_table(name: str, focus: Optional[set[str]]) -> bool:
    """Keep a table in live findings only if it's a benchmark target (and never the
    app's own history table). With no focus set (no queries), keep all but history."""
    n = (name or "").lower()
    if n == _HISTORY_TABLE:
        return False
    return n in focus if focus else True


def filter_existing_indexes(
    suggestions: list[IndexSuggestion], existing: Optional[list[dict[str, Any]]]
) -> tuple[list[IndexSuggestion], list[IndexSuggestion]]:
    """Split suggestions into (still-needed, already-covered) using live index metadata.

    ``existing`` is a list of ``{"table", "columns"}`` (ordered columns) from
    :func:`live_introspection`. A suggestion is considered already covered when an
    existing index on the same table has the suggestion's columns as a leading prefix
    (order-sensitive) or as the same column set (for composite equality indexes).
    """
    if not existing:
        return suggestions, []

    by_table: dict[str, list[list[str]]] = {}
    for e in existing:
        by_table.setdefault(_bare(e["table"]).lower(), []).append(
            [str(c).lower() for c in e.get("columns", [])]
        )

    kept: list[IndexSuggestion] = []
    dropped: list[IndexSuggestion] = []
    for s in suggestions:
        cols = [c.lower() for c in s.columns]
        covered = False
        for idx_cols in by_table.get(_bare(s.table).lower(), []):
            if idx_cols[: len(cols)] == cols:  # prefix match (covers range / ORDER BY too)
                covered = True
                break
            if len(idx_cols) == len(cols) and set(idx_cols) == set(cols):  # composite, any order
                covered = True
                break
        (dropped if covered else kept).append(s)
    return kept, dropped


# --- Live introspection ------------------------------------------------------

# Detection queries from the Lakebase OLTP Technical Guide.
_CACHE_HIT_SQL = (
    "SELECT round(sum(blks_hit)*100.0/nullif(sum(blks_hit+blks_read),0), 2) AS cache_hit_pct "
    "FROM pg_stat_database WHERE datname = current_database();"
)
_SEQ_SCAN_SQL = (
    "SELECT relname, seq_scan, idx_scan FROM pg_stat_user_tables "
    "WHERE seq_scan > COALESCE(idx_scan, 0) ORDER BY seq_scan DESC LIMIT 10;"
)
_UNUSED_IDX_SQL = (
    "SELECT relname, indexrelname FROM pg_stat_user_indexes "
    "WHERE idx_scan = 0 ORDER BY relname LIMIT 20;"
)
_PG_STAT_STATEMENTS_SQL = (
    "SELECT query, calls, total_exec_time/nullif(calls,0) AS avg_ms "
    "FROM pg_stat_statements ORDER BY total_exec_time DESC LIMIT 10;"
)
# Existing indexes with their ordered column lists (user schemas only), so we can
# avoid re-suggesting indexes that are already present.
_EXISTING_INDEXES_SQL = (
    "SELECT t.relname AS table_name, array_agg(a.attname ORDER BY x.ord) AS columns "
    "FROM pg_index ix "
    "JOIN pg_class t ON t.oid = ix.indrelid "
    "JOIN pg_class i ON i.oid = ix.indexrelid "
    "JOIN pg_namespace n ON n.oid = t.relnamespace "
    "JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS x(attnum, ord) ON true "
    "JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = x.attnum "
    "WHERE n.nspname NOT IN ('pg_catalog', 'information_schema') AND x.attnum > 0 "
    "GROUP BY t.relname, i.relname;"
)


@dataclass
class Finding:
    severity: str
    category: str
    title: str
    detail: str
    actions: list[str] = field(default_factory=list)


def live_introspection(
    creds: PgCredentials, focus_tables: Optional[set[str]] = None
) -> tuple[list[Finding], dict[str, Any]]:
    """Connect and run detection SQL. Returns (findings, raw_stats). Best-effort:
    individual probes that fail (e.g. missing pg_stat_statements) are skipped.

    Table-scoped findings (sequential scans, unused indexes) are restricted to
    ``focus_tables`` — the tables the benchmark queries actually touch — and never
    include the app's own history table.
    """
    findings: list[Finding] = []
    stats: dict[str, Any] = {}

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
            # Cache hit ratio
            try:
                cur.execute(_CACHE_HIT_SQL)
                row = cur.fetchone()
                if row and row[0] is not None:
                    pct = float(row[0])
                    stats["cache_hit_pct"] = pct
                    if pct < 99:
                        findings.append(Finding(
                            severity="medium", category="cache",
                            title="Cache hit ratio below 99%",
                            detail=f"Cache hit ratio is {pct:.2f}% (OLTP target > 99%).",
                            actions=["Add indexes on frequently queried columns to reduce disk reads."],
                        ))
            except Exception:  # noqa: BLE001
                conn.rollback()

            # Sequential scans (scoped to benchmark tables; never the history table)
            try:
                cur.execute(_SEQ_SCAN_SQL)
                seq = [
                    {"table": r[0], "seq_scan": r[1], "idx_scan": r[2]}
                    for r in cur.fetchall()
                    if _keep_table(r[0], focus_tables)
                ]
                stats["seq_scan_tables"] = seq
                if seq:
                    names = ", ".join(s["table"] for s in seq[:5])
                    findings.append(Finding(
                        severity="high", category="seq_scan",
                        title="Tables doing more sequential than index scans",
                        detail=f"Seq-scan-heavy tables: {names}.",
                        actions=[
                            "Create indexes on the predicates these tables are filtered by.",
                            "Run ANALYZE to refresh planner statistics.",
                        ],
                    ))
            except Exception:  # noqa: BLE001
                conn.rollback()

            # Unused indexes (scoped to benchmark tables; never the history table)
            try:
                cur.execute(_UNUSED_IDX_SQL)
                unused = [
                    {"table": r[0], "index": r[1]}
                    for r in cur.fetchall()
                    if _keep_table(r[0], focus_tables)
                ]
                stats["unused_indexes"] = unused
                if unused:
                    findings.append(Finding(
                        severity="low", category="unused_index",
                        title="Unused indexes present",
                        detail=f"{len(unused)} index(es) have never been scanned; they slow writes.",
                        actions=["Drop indexes confirmed unused after a representative workload."],
                    ))
            except Exception:  # noqa: BLE001
                conn.rollback()

            # Existing indexes — used to drop already-satisfied suggestions
            try:
                cur.execute(_EXISTING_INDEXES_SQL)
                stats["existing_indexes"] = [
                    {"table": r[0], "columns": list(r[1] or [])} for r in cur.fetchall()
                ]
            except Exception:  # noqa: BLE001
                conn.rollback()

            # pg_stat_statements (optional extension)
            try:
                cur.execute(_PG_STAT_STATEMENTS_SQL)
                stats["top_statements"] = [
                    {"query": r[0][:300], "calls": r[1], "avg_ms": round(float(r[2] or 0), 2)}
                    for r in cur.fetchall()
                ]
            except Exception:  # noqa: BLE001
                conn.rollback()
                stats["pg_stat_statements_available"] = False

    return findings, stats


_ALLOWED_DDL_PREFIXES = ("create index", "drop index", "analyze")


def apply_indexes(creds: PgCredentials, ddls: list[str]) -> list[dict[str, Any]]:
    """Execute index DDL against Lakebase. Only CREATE INDEX / DROP INDEX / ANALYZE
    statements are permitted (no arbitrary SQL). Each statement is applied
    independently; failures are reported per-statement rather than aborting the rest.
    """
    results: list[dict[str, Any]] = []
    with psycopg.connect(
        host=creds.host,
        port=creds.port,
        dbname=creds.database,
        user=creds.user,
        password=creds.password,
        sslmode=creds.ssl_mode,
        connect_timeout=10,
        autocommit=True,
    ) as conn:
        with conn.cursor() as cur:
            for ddl in ddls:
                stmt = _strip_comments(ddl).rstrip(";").strip()
                if not stmt.lower().startswith(_ALLOWED_DDL_PREFIXES):
                    results.append({
                        "ddl": ddl, "ok": False,
                        "detail": "Only CREATE INDEX / DROP INDEX / ANALYZE are allowed.",
                    })
                    continue
                try:
                    cur.execute(stmt)  # ty: ignore[no-matching-overload]
                    results.append({"ddl": ddl, "ok": True, "detail": "Applied."})
                except Exception as e:  # noqa: BLE001
                    results.append({"ddl": ddl, "ok": False, "detail": str(e)})
    return results


def explain_query(creds: PgCredentials, sql: str) -> str:
    """Run EXPLAIN (ANALYZE, BUFFERS) for a single statement and return the plan text."""
    sql = _strip_comments(sql).rstrip(";")
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
            # Dynamic EXPLAIN of caller-supplied SQL (controlled internal op).
            cur.execute(f"EXPLAIN (ANALYZE, BUFFERS) {sql}")  # ty: ignore[no-matching-overload]
            return "\n".join(row[0] for row in cur.fetchall())
