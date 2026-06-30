"""Shared statistics helpers for the testing runners.

A single percentile implementation keeps psycopg and pgbench (local) results
consistent with each other and with the Databricks-job notebook, which uses
``numpy.percentile``'s default linear interpolation. This is the same method.
"""

from __future__ import annotations

import math


def percentile(sorted_values: list[float], pct: float) -> float:
    """Linear-interpolated percentile (``pct`` in 0–100), matching numpy's default.

    ``sorted_values`` must be sorted ascending and non-empty.
    """
    n = len(sorted_values)
    if n == 1:
        return sorted_values[0]
    rank = (pct / 100) * (n - 1)
    lo, hi = math.floor(rank), math.ceil(rank)
    if lo == hi:
        return sorted_values[lo]
    return sorted_values[lo] + (sorted_values[hi] - sorted_values[lo]) * (rank - lo)
