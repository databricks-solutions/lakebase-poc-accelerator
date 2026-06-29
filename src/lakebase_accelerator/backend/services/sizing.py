"""Autoscaling CU sizing recommendation.

Derives a recommended min/max compute-unit (CU) range for a Lakebase Autoscaling
endpoint from a workload description. The workload→CU ratios are ported from the
legacy provisioned cost estimator; the output is adapted to autoscaling economics
(0.5–112 CU, 2 GB/CU, scale-to-zero) and the autoscaling constraint that the
dynamic range (max − min) must not exceed 8 CU.
"""

from __future__ import annotations

from dataclasses import dataclass

# Workload throughput per CU (ported from lakebase_cost_estimator.py)
BULK_CU_RATIO = 14000      # bulk write rows/sec per CU
CONTINUOUS_CU_RATIO = 1500  # continuous write rows/sec per CU
READ_CU_RATIO = 10000      # reads/sec (QPS) per CU

# Valid autoscaling CU steps. Lakebase Autoscaling supports 0.5–32 CU for a
# dynamic range; 36–112 CU is fixed-size only (no autoscaling).
_VALID_CU = [0.5, 1, 2, 4, 6, 8, 10, 12, 16, 20, 24, 28, 32]
# Hard constraints from the Lakebase Autoscaling compute docs:
MAX_AUTOSCALE_CU = 32.0     # autoscale ceiling; above this is fixed-size only
MIN_AUTOSCALE_CU = 0.5      # smallest CU
MAX_DYNAMIC_SPREAD = 8.0    # max_cu - min_cu must not exceed 8 CU


@dataclass
class SizingResult:
    bulk_cu: float
    continuous_cu: float
    read_cu: float
    total_cu: float
    recommended_min_cu: float
    recommended_max_cu: float
    rationale: str


def _ceil_to_valid(cu: float) -> float:
    for step in _VALID_CU:
        if cu <= step:
            return step
    return _VALID_CU[-1]


def recommend_cu(
    *,
    bulk_writes_per_second: float = 0,
    continuous_writes_per_second: float = 0,
    reads_per_second: float = 0,
) -> SizingResult:
    """Recommend a min/max CU autoscaling range for the given workload."""
    bulk_cu = bulk_writes_per_second / BULK_CU_RATIO
    continuous_cu = continuous_writes_per_second / CONTINUOUS_CU_RATIO
    read_cu = reads_per_second / READ_CU_RATIO
    total_cu = bulk_cu + continuous_cu + read_cu

    # Max CU = the peak demand, rounded up to a valid step and capped at the
    # autoscale ceiling (32 CU). Workloads above that need a fixed-size compute.
    exceeds_autoscale = total_cu > MAX_AUTOSCALE_CU
    max_cu = min(_ceil_to_valid(max(total_cu, MIN_AUTOSCALE_CU)), MAX_AUTOSCALE_CU)

    # Min CU: ~half of peak (rounded to a valid step). The docs recommend setting
    # min high enough to keep the working set cached in RAM, while staying within
    # the max−min ≤ 8 CU dynamic-range constraint.
    min_cu = _ceil_to_valid(max(total_cu * 0.5, MIN_AUTOSCALE_CU))
    if min_cu > max_cu:
        min_cu = max_cu
    if max_cu - min_cu > MAX_DYNAMIC_SPREAD:
        min_cu = _ceil_to_valid(max_cu - MAX_DYNAMIC_SPREAD)

    rationale = (
        f"Peak ≈ {total_cu:.2f} CU (bulk {bulk_cu:.2f} + continuous {continuous_cu:.2f} "
        f"+ read {read_cu:.2f}). Recommending autoscale {min_cu}–{max_cu} CU "
        f"(within the max−min ≤ {MAX_DYNAMIC_SPREAD:.0f} CU constraint). Enable "
        "scale-to-zero to suspend on idle and cut cost; raise min CU if cold-start "
        "latency or working-set caching matters."
    )
    if exceeds_autoscale:
        rationale += (
            f" Peak exceeds the {MAX_AUTOSCALE_CU:.0f} CU autoscale ceiling — capped at "
            f"{MAX_AUTOSCALE_CU:.0f}; consider a fixed-size compute (36–112 CU) instead."
        )

    return SizingResult(
        bulk_cu=round(bulk_cu, 3),
        continuous_cu=round(continuous_cu, 3),
        read_cu=round(read_cu, 3),
        total_cu=round(total_cu, 3),
        recommended_min_cu=min_cu,
        recommended_max_cu=max_cu,
        rationale=rationale,
    )
