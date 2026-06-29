"""Parse the shared ``.sql`` test-query format used by both pgbench and psycopg.

Format (one query per logical entry):
    -- any comment lines are ignored
    -- PARAMETERS: [[1], [437], ["Electronics"]]   # JSON list-of-lists, one set per scenario
    -- EXEC_COUNT: 20                               # times to run each scenario (default 5)
    SELECT ... WHERE col = %s;
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

DEFAULT_EXEC_COUNT = 5


@dataclass
class TestScenario:
    name: str
    parameters: list[Any]
    execution_count: int


@dataclass
class ParsedQuery:
    query_identifier: str
    query_content: str
    test_scenarios: list[TestScenario] = field(default_factory=list)


def parse_query(identifier: str, content: str) -> ParsedQuery:
    """Parse one ``.sql`` body into a query + its test scenarios."""
    parameter_sets: list[Any] = []
    exec_count = DEFAULT_EXEC_COUNT
    sql_lines: list[str] = []

    for raw in content.split("\n"):
        line = raw.strip()
        if line.startswith("-- PARAMETERS:"):
            try:
                parameter_sets = json.loads(line.replace("-- PARAMETERS:", "").strip())
            except json.JSONDecodeError:
                parameter_sets = []
        elif line.startswith("-- EXEC_COUNT:"):
            try:
                exec_count = int(line.replace("-- EXEC_COUNT:", "").strip())
            except ValueError:
                exec_count = DEFAULT_EXEC_COUNT
        elif not line.startswith("--"):
            sql_lines.append(raw)

    sql = "\n".join(sql_lines).strip()

    if parameter_sets:
        scenarios = [
            TestScenario(name=f"scenario_{i}", parameters=params, execution_count=exec_count)
            for i, params in enumerate(parameter_sets, start=1)
        ]
    else:
        scenarios = [TestScenario(name="scenario_1", parameters=[], execution_count=exec_count)]

    return ParsedQuery(query_identifier=identifier, query_content=sql, test_scenarios=scenarios)


def to_execution_queries(parsed: list[ParsedQuery]) -> list[dict[str, Any]]:
    """Convert parsed queries into the dict shape ``ConnectionPool.run_concurrent`` expects."""
    return [
        {
            "query_identifier": p.query_identifier,
            "query_content": p.query_content,
            "test_scenarios": [
                {"name": s.name, "parameters": s.parameters, "execution_count": s.execution_count}
                for s in p.test_scenarios
            ],
        }
        for p in parsed
    ]
