"""The single, tool-agnostic test-query format shared by psycopg and pgbench.

A query is one SQL statement using pgbench-style ``:name`` placeholders, preceded by
optional comment directives. The *same* string drives both runners — only the run
config (concurrency vs clients/duration) differs between them.

Format::

    -- WEIGHT: 40                       # relative mix weight (default 1)
    -- PARAM ticket = random(1, 240000) # named integer generator (0+ per query)
    SELECT ss_item_sk, ss_net_paid
    FROM store_sales
    WHERE ss_ticket_number = :ticket;

``WEIGHT`` is the single, tool-agnostic mix control:
* **pgbench** — applied as ``-f file@N``; over the run's ``-T`` duration pgbench picks
  scripts in proportion to their weight.
* **psycopg** — the tab's *total executions* are distributed across queries in
  proportion to weight (see :func:`distribute_executions`); ``:name`` is rewritten to
  ``%(name)s`` with a fresh random value drawn per execution.

Each ``-- PARAM`` declares a per-execution generator, either:

* ``random(min, max)`` — a uniform integer. pgbench: ``\\set name random(min, max)``
  with ``:name`` kept verbatim. psycopg: a fresh ``randint`` per execution.
* ``values(v1, v2, ...)`` — one value picked at random from a discrete set of integers
  and/or quoted strings, e.g. ``values('CA', 'NY', 'TX')``. psycopg binds the chosen value
  directly; pgbench (whose variables hold only numbers) draws a random index and indexes a
  literal ``ARRAY[...]`` in the SQL.
"""

from __future__ import annotations

import random
import re
from dataclasses import dataclass, field
from typing import Any

DEFAULT_WEIGHT = 1

# ``-- PARAM name = <generator>`` where <generator> is one of:
#   random(min, max)      integer range, drawn uniformly per execution
#   values(v1, v2, ...)   discrete set of ints and/or quoted strings, one picked per execution
_PARAM_LINE_RE = re.compile(r"^--\s*PARAM\s+([a-zA-Z_]\w*)\s*=\s*(.+?)\s*$", re.IGNORECASE)
_RANDOM_RE = re.compile(r"^random\(\s*(-?\d+)\s*,\s*(-?\d+)\s*\)$", re.IGNORECASE)
_VALUES_RE = re.compile(r"^values\(\s*(.+?)\s*\)$", re.IGNORECASE)
# One item of a values(...) list: a single/double-quoted string (doubled quote escapes)
# or an integer.
_VALUE_ITEM_RE = re.compile(r"""'((?:[^']|'')*)'|"((?:[^"]|"")*)"|(-?\d+)""")
# ``:name`` placeholder, but not ``::cast`` and not inside ``:=``.
_PLACEHOLDER_RE = re.compile(r"(?<!:):([a-zA-Z_]\w*)")


@dataclass
class ParamSpec:
    """A test parameter drawn fresh per execution. Either a uniform integer range
    (``min_value``/``max_value``) or a discrete ``values`` list of ints/strings."""

    name: str
    min_value: int | None = None
    max_value: int | None = None
    values: list[int | str] | None = None

    def draw(self) -> int | str:
        if self.values:
            return random.choice(self.values)
        if self.min_value is None or self.max_value is None:
            raise ValueError(f"ParamSpec {self.name!r} has neither a values list nor a min/max range.")
        lo, hi = (
            (self.min_value, self.max_value)
            if self.min_value <= self.max_value
            else (self.max_value, self.min_value)
        )
        return random.randint(lo, hi)


def _parse_values(inner: str) -> list[int | str]:
    """Parse the inside of ``values(...)`` into ints and/or strings."""
    items: list[int | str] = []
    for m in _VALUE_ITEM_RE.finditer(inner):
        if m.group(1) is not None:
            items.append(m.group(1).replace("''", "'"))
        elif m.group(2) is not None:
            items.append(m.group(2).replace('""', '"'))
        elif m.group(3) is not None:
            items.append(int(m.group(3)))
    return items


def _parse_param(line: str) -> ParamSpec | None:
    """Parse one ``-- PARAM`` directive, or None if it isn't a PARAM line.

    Raises ``ValueError`` for a recognized PARAM with an unsupported generator, so a
    typo'd directive surfaces clearly rather than silently leaving the placeholder undeclared.
    """
    m = _PARAM_LINE_RE.match(line)
    if not m:
        return None
    name, gen = m.group(1), m.group(2).strip()
    rm = _RANDOM_RE.match(gen)
    if rm:
        return ParamSpec(name, min_value=int(rm.group(1)), max_value=int(rm.group(2)))
    vm = _VALUES_RE.match(gen)
    if vm:
        values = _parse_values(vm.group(1))
        if values:
            return ParamSpec(name, values=values)
    raise ValueError(
        f"PARAM '{name}': unsupported generator '{gen}'. Use random(min, max) or "
        f"values(v1, v2, ...) — integers and/or quoted strings, e.g. values('CA', 'NY')."
    )


@dataclass
class ParsedQuery:
    identifier: str
    sql: str  # comments stripped; still uses :name placeholders
    params: list[ParamSpec] = field(default_factory=list)
    weight: int = DEFAULT_WEIGHT


def parse_query(identifier: str, content: str) -> ParsedQuery:
    """Parse one unified query body into SQL + directives.

    A legacy ``-- EXEC_COUNT:`` line is harmlessly ignored (psycopg now derives counts
    from weight + a global total).

    Raises ``ValueError`` if the SQL references a ``:name`` placeholder that has no
    matching ``-- PARAM`` declaration — caught early here so the user gets a clear,
    actionable message instead of a cryptic bind-parameter error mid-run.
    """
    params: list[ParamSpec] = []
    weight = DEFAULT_WEIGHT
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
        elif upper.startswith("-- PARAM"):
            spec = _parse_param(line)
            if spec:
                params.append(spec)
        elif not line.startswith("--"):
            sql_lines.append(raw)

    sql = "\n".join(sql_lines).strip()
    _validate_placeholders(identifier, sql, params)
    return ParsedQuery(identifier=identifier, sql=sql, params=params, weight=weight)


def _validate_placeholders(identifier: str, sql: str, params: list[ParamSpec]) -> None:
    """Ensure every ``:name`` used in the SQL has a ``-- PARAM`` declaration."""
    declared = {p.name for p in params}
    used = set(_PLACEHOLDER_RE.findall(sql))
    undeclared = sorted(used - declared)
    if undeclared:
        names = ", ".join(f":{n}" for n in undeclared)
        example = undeclared[0]
        raise ValueError(
            f"Query '{identifier}': placeholder(s) {names} have no '-- PARAM' declaration. "
            f"Add a line like '-- PARAM {example} = random(min, max)' for each, "
            f"or replace it with a fixed literal value."
        )


# --------------------------------------------------------------------------- #
# psycopg rendering
# --------------------------------------------------------------------------- #
def render_psycopg_sql(parsed: ParsedQuery) -> str:
    """Rewrite ``:name`` placeholders to psycopg's ``%(name)s`` named form."""
    return _PLACEHOLDER_RE.sub(lambda m: f"%({m.group(1)})s", parsed.sql)


def to_execution_queries(parsed: list[ParsedQuery]) -> list[dict[str, Any]]:
    """Shape parsed queries for :meth:`ConnectionPool.run_concurrent`.

    Carries the psycopg-rendered SQL, param specs, and the mix ``weight`` (the runner
    distributes the tab's total executions across queries by weight).
    """
    return [
        {
            "query_identifier": p.identifier,
            "query_content": render_psycopg_sql(p),
            "param_specs": [_serialize_spec(s) for s in p.params],
            "weight": p.weight,
        }
        for p in parsed
    ]


def _serialize_spec(s: ParamSpec) -> dict[str, Any]:
    """JSON-friendly param spec for the runner (range or values list)."""
    if s.values is not None:
        return {"name": s.name, "values": s.values}
    return {"name": s.name, "min_value": s.min_value, "max_value": s.max_value}


def distribute_executions(weights: list[int], total: int) -> list[int]:
    """Split ``total`` executions across queries in proportion to their weights.

    Every query runs at least once; the remainder is allocated largest-fraction-first
    so the counts sum to exactly ``total`` (when ``total >= len(weights)``).
    """
    n = len(weights)
    if n == 0:
        return []
    total = max(total, n)  # at least one execution per query
    w = [max(1, x) for x in weights]
    wsum = sum(w)
    exact = [total * x / wsum for x in w]
    counts = [max(1, int(e)) for e in exact]
    # Distribute the leftover to the largest fractional parts for a stable total.
    leftover = total - sum(counts)
    order = sorted(range(n), key=lambda i: exact[i] - int(exact[i]), reverse=True)
    i = 0
    while leftover > 0 and order:
        counts[order[i % n]] += 1
        leftover -= 1
        i += 1
    return counts


def draw_from_specs(param_specs: list[dict[str, Any]]) -> dict[str, int | str]:
    """Draw one bind dict from the serialized param-spec dicts used by the runner."""
    out: dict[str, int | str] = {}
    for s in param_specs:
        if s.get("values") is not None:
            out[s["name"]] = ParamSpec(s["name"], values=list(s["values"])).draw()
        else:
            out[s["name"]] = ParamSpec(
                s["name"], min_value=int(s["min_value"]), max_value=int(s["max_value"])
            ).draw()
    return out


# --------------------------------------------------------------------------- #
# pgbench rendering
# --------------------------------------------------------------------------- #
def _pg_array_literal(values: list[int | str]) -> str:
    """A Postgres ARRAY[...] literal; strings are single-quoted (quotes doubled)."""
    items = [
        "'" + v.replace("'", "''") + "'" if isinstance(v, str) else str(v)
        for v in values
    ]
    return "ARRAY[" + ", ".join(items) + "]"


def render_pgbench_script(parsed: ParsedQuery) -> str:
    """Build a native pgbench script: ``\\set`` lines + SQL.

    Range params map straight to ``\\set name random(min, max)`` with ``:name`` kept in
    the SQL. pgbench variables can only hold numbers, so a ``values(...)`` param instead
    gets a random *index* variable (``\\set name__idx random(1, N)``) and each ``:name``
    in the SQL is rewritten to ``(ARRAY[...])[:name__idx]`` — Postgres indexes the literal
    array (1-based), which works uniformly for ints and strings.
    """
    set_lines: list[str] = []
    array_exprs: dict[str, str] = {}  # values-param name -> SQL expression replacing :name
    for p in parsed.params:
        if p.values is not None:
            idx = f"{p.name}__idx"
            set_lines.append(f"\\set {idx} random(1, {len(p.values)})")
            array_exprs[p.name] = f"({_pg_array_literal(p.values)})[:{idx}]"
        else:
            set_lines.append(f"\\set {p.name} random({p.min_value}, {p.max_value})")

    sql = _PLACEHOLDER_RE.sub(lambda m: array_exprs.get(m.group(1), m.group(0)), parsed.sql)
    body = sql if sql.endswith(";") else sql + ";"
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
