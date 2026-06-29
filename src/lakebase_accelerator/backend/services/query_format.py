"""The single, tool-agnostic test-query format shared by psycopg and pgbench.

A query is one SQL statement using pgbench-style ``:name`` placeholders, preceded by
optional comment directives. The *same* string drives both runners — only the run
config (concurrency vs clients/duration) differs between them.

Format::

    -- WEIGHT: 40                       # pgbench relative transaction weight (default 1)
    -- EXEC_COUNT: 20                   # psycopg executions of this query (default 5)
    -- PARAM ticket = random(1, 240000) # named integer generator (0+ per query)
    SELECT ss_item_sk, ss_net_paid
    FROM store_sales
    WHERE ss_ticket_number = :ticket;

Translation:
* **pgbench** — each ``-- PARAM`` becomes a ``\\set name random(min, max)`` line
  prepended to the SQL (``:name`` kept verbatim); ``WEIGHT`` is applied as ``-f file@N``.
* **psycopg** — ``:name`` is rewritten to ``%(name)s`` and a fresh random value is
  drawn per execution into a parameter dict; ``EXEC_COUNT`` controls repetitions.
"""

from __future__ import annotations

import random
import re
from dataclasses import dataclass, field
from typing import Any

DEFAULT_EXEC_COUNT = 5
DEFAULT_WEIGHT = 1

# ``-- PARAM name = random(min, max)``
_PARAM_RE = re.compile(
    r"^--\s*PARAM\s+([a-zA-Z_]\w*)\s*=\s*random\(\s*(-?\d+)\s*,\s*(-?\d+)\s*\)\s*$",
    re.IGNORECASE,
)
# ``:name`` placeholder, but not ``::cast`` and not inside ``:=``.
_PLACEHOLDER_RE = re.compile(r"(?<!:):([a-zA-Z_]\w*)")


@dataclass
class ParamSpec:
    """An integer parameter drawn uniformly from ``[min_value, max_value]``."""

    name: str
    min_value: int
    max_value: int

    def draw(self) -> int:
        lo, hi = (self.min_value, self.max_value) if self.min_value <= self.max_value else (
            self.max_value,
            self.min_value,
        )
        return random.randint(lo, hi)


@dataclass
class ParsedQuery:
    identifier: str
    sql: str  # comments stripped; still uses :name placeholders
    params: list[ParamSpec] = field(default_factory=list)
    weight: int = DEFAULT_WEIGHT
    exec_count: int = DEFAULT_EXEC_COUNT


def parse_query(identifier: str, content: str) -> ParsedQuery:
    """Parse one unified query body into SQL + directives."""
    params: list[ParamSpec] = []
    weight = DEFAULT_WEIGHT
    exec_count = DEFAULT_EXEC_COUNT
    sql_lines: list[str] = []

    for raw in content.split("\n"):
        line = raw.strip()
        if not line:
            continue
        upper = line.upper()
        if upper.startswith("-- WEIGHT:"):
            try:
                weight = max(1, int(line.split(":", 1)[1].strip()))
            except ValueError:
                weight = DEFAULT_WEIGHT
        elif upper.startswith("-- EXEC_COUNT:"):
            try:
                exec_count = max(1, int(line.split(":", 1)[1].strip()))
            except ValueError:
                exec_count = DEFAULT_EXEC_COUNT
        elif upper.startswith("-- PARAM"):
            m = _PARAM_RE.match(line)
            if m:
                params.append(ParamSpec(m.group(1), int(m.group(2)), int(m.group(3))))
        elif not line.startswith("--"):
            sql_lines.append(raw)

    sql = "\n".join(sql_lines).strip()
    return ParsedQuery(
        identifier=identifier,
        sql=sql,
        params=params,
        weight=weight,
        exec_count=exec_count,
    )


# --------------------------------------------------------------------------- #
# psycopg rendering
# --------------------------------------------------------------------------- #
def render_psycopg_sql(parsed: ParsedQuery) -> str:
    """Rewrite ``:name`` placeholders to psycopg's ``%(name)s`` named form."""
    return _PLACEHOLDER_RE.sub(lambda m: f"%({m.group(1)})s", parsed.sql)


def to_execution_queries(parsed: list[ParsedQuery]) -> list[dict[str, Any]]:
    """Shape parsed queries for :meth:`ConnectionPool.run_concurrent`.

    Carries the psycopg-rendered SQL plus the param specs, so the runner can draw
    fresh values per execution.
    """
    return [
        {
            "query_identifier": p.identifier,
            "query_content": render_psycopg_sql(p),
            "param_specs": [
                {"name": s.name, "min_value": s.min_value, "max_value": s.max_value}
                for s in p.params
            ],
            "execution_count": p.exec_count,
        }
        for p in parsed
    ]


def draw_from_specs(param_specs: list[dict[str, Any]]) -> dict[str, int]:
    """Draw one bind dict from the serialized param-spec dicts used by the runner."""
    return {
        s["name"]: ParamSpec(s["name"], int(s["min_value"]), int(s["max_value"])).draw()
        for s in param_specs
    }


# --------------------------------------------------------------------------- #
# pgbench rendering
# --------------------------------------------------------------------------- #
def render_pgbench_script(parsed: ParsedQuery) -> str:
    """Build a native pgbench script: ``\\set`` lines + SQL with ``:name`` kept."""
    set_lines = [
        f"\\set {p.name} random({p.min_value}, {p.max_value})" for p in parsed.params
    ]
    body = parsed.sql if parsed.sql.endswith(";") else parsed.sql + ";"
    return "\n".join([*set_lines, body]).strip()


def to_pgbench_queries(parsed: list[ParsedQuery]) -> list[dict[str, Any]]:
    """Shape parsed queries for the pgbench job notebook ({name, content, weight})."""
    return [
        {
            "name": p.identifier,
            "content": render_pgbench_script(p),
            "weight": p.weight,
        }
        for p in parsed
    ]
