"""
Pipeline integration: compare current dataset to a baseline (e.g. same dataset one week earlier),
evaluate degradation thresholds, and detect volume drop, schema changes, and stale/identical data.

Callers get a single result dict (passed, warnings, details); the pipeline decides whether
to alert or stop ingestion. Load baseline and current from your environment (e.g. tables);
this module is stateless.
"""

from typing import Any, Callable, Dict, List, Optional, Union

import pandas as pd

from data_quality.comparison import compare_two_reports, run_same_rules_on_two_datasets
from data_quality.utils import normalize_columns


def compare_schema(
    df_baseline: pd.DataFrame,
    df_current: pd.DataFrame,
    check_dtypes: bool = False,
) -> Dict[str, Any]:
    """
    Diff column names (and optionally dtypes) between baseline and current.

    Returns:
        {"added": [...], "removed": [...], "type_changes": [...]}
        type_changes is only populated when check_dtypes=True; each entry is
        {"column": str, "baseline_dtype": str, "current_dtype": str}.
    """
    cols_baseline = set(df_baseline.columns)
    cols_current = set(df_current.columns)
    added = sorted(cols_current - cols_baseline)
    removed = sorted(cols_baseline - cols_current)
    type_changes: List[Dict[str, Any]] = []
    if check_dtypes:
        common = cols_baseline & cols_current
        for col in sorted(common):
            b_dtype = str(df_baseline[col].dtype)
            c_dtype = str(df_current[col].dtype)
            if b_dtype != c_dtype:
                type_changes.append({
                    "column": col,
                    "baseline_dtype": b_dtype,
                    "current_dtype": c_dtype,
                })
    return {
        "added": added,
        "removed": removed,
        "type_changes": type_changes,
    }


def compare_volume(
    df_baseline: pd.DataFrame,
    df_current: pd.DataFrame,
    date_column: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Compare row counts and optionally whether new data appeared by time.

    Returns:
        {
            "row_count_baseline": int,
            "row_count_current": int,
            "pct_change": float,
            "no_new_data": bool | None,
        }
    no_new_data is True only when date_column is provided and
    max(current[date_column]) <= max(baseline[date_column]) (no new data by time).
    Otherwise no_new_data is None.
    """
    n_baseline = len(df_baseline)
    n_current = len(df_current)
    pct_change = (
        ((n_current - n_baseline) / n_baseline * 100.0) if n_baseline else 0.0
    )
    no_new_data: Optional[bool] = None
    if date_column and date_column in df_baseline.columns and date_column in df_current.columns:
        base_dates = pd.to_datetime(df_baseline[date_column], errors="coerce")
        curr_dates = pd.to_datetime(df_current[date_column], errors="coerce")
        max_baseline = base_dates.max()
        max_current = curr_dates.max()
        if pd.isna(max_baseline) and pd.isna(max_current):
            no_new_data = True
        elif pd.isna(max_current):
            no_new_data = True
        elif pd.isna(max_baseline):
            no_new_data = False
        else:
            no_new_data = bool(max_current <= max_baseline)
    return {
        "row_count_baseline": n_baseline,
        "row_count_current": n_current,
        "pct_change": round(pct_change, 2),
        "no_new_data": no_new_data,
    }


def _key_set(df: pd.DataFrame, key_column: Union[str, List[str]]) -> set:
    """Set of key values (single column) or set of tuples (multi-column)."""
    keys = df[key_column]
    if isinstance(key_column, str):
        return set(keys.dropna().unique())
    return set(keys.dropna().apply(tuple, axis=1).unique())


def detect_identical_or_stale(
    df_baseline: pd.DataFrame,
    df_current: pd.DataFrame,
    key_column: Optional[Union[str, List[str]]] = None,
    tolerance_pct: float = 0.0,
) -> Dict[str, Any]:
    """
    Detect when the two datasets are identical or nearly the same (possible stale data).

    Returns:
        {"identical": bool, "stale_warning": bool, "reason": str | None}
    - If row counts match and key_column is given: compare sets of key values;
      if overlap >= (100 - tolerance_pct)%, set identical=True, stale_warning=True.
    - If row counts match and no key_column: stale_warning=True with reason
      "Same row count; possible stale data (no key column to compare)."
    """
    n_b = len(df_baseline)
    n_c = len(df_current)
    identical = False
    stale_warning = False
    reason: Optional[str] = None

    if n_b != n_c:
        return {"identical": False, "stale_warning": False, "reason": None}

    key_list = normalize_columns(key_column) if key_column else None
    if key_list is not None:
        for k in key_list:
            if k not in df_baseline.columns or k not in df_current.columns:
                return {"identical": False, "stale_warning": False, "reason": None}
        set_b = _key_set(df_baseline, key_list)
        set_c = _key_set(df_current, key_list)
        if not set_b and not set_c:
            reason = "Same row count; key column(s) are all null in both datasets."
            stale_warning = True
        else:
            overlap = len(set_b & set_c)
            total = len(set_b | set_c) or 1
            overlap_pct = (overlap / total) * 100.0
            threshold = 100.0 - tolerance_pct
            if overlap_pct >= threshold:
                identical = True
                stale_warning = True
                reason = (
                    "Datasets are identical or nearly identical (same row count and key set); "
                    "possible stale data."
                )
    else:
        stale_warning = True
        reason = (
            "Same row count; possible stale data (no key column to compare)."
        )

    return {"identical": identical, "stale_warning": stale_warning, "reason": reason}


def compare_snapshots(
    df_baseline: pd.DataFrame,
    df_current: pd.DataFrame,
    rules_runner: Callable[[pd.DataFrame, List], None],
    *,
    dataset_name_baseline: str = "baseline",
    dataset_name_current: str = "current",
    critical_columns: Optional[List[str]] = None,
    min_overall_health: Optional[float] = None,
    min_per_dimension: Optional[Dict[str, float]] = None,
    fail_on_schema_change: bool = False,
    fail_on_volume_drop_pct: Optional[float] = None,
    date_column: Optional[str] = None,
    schema_check_dtypes: bool = False,
    warn_on_stale: bool = True,
    stale_key_column: Optional[Union[str, List[str]]] = None,
) -> Dict[str, Any]:
    """
    Compare current dataset to baseline: schema, volume, stale detection, and quality (same rules on both).

    Load baseline and current (e.g. 1 week apart); call with your rules_runner. Use
    min_overall_health=80, fail_on_volume_drop_pct=-25, warn_on_stale=True. If not
    result["passed"]: alert or stop ingestion; check result["warnings"] for stale data.

    Args:
        df_baseline: Baseline snapshot (e.g. last week).
        df_current: Current snapshot.
        rules_runner: Callable(df, results) that appends expectations to results.
        dataset_name_baseline: Name for baseline in reports.
        dataset_name_current: Name for current in reports.
        critical_columns: Optional list for report critical-elements.
        min_overall_health: If set, current overall_health_score must be >= this or passed=False.
        min_per_dimension: If set, each dimension score (current) must be >= value or passed=False.
        fail_on_schema_change: If True, added/removed columns set passed=False.
        fail_on_volume_drop_pct: If set (e.g. -20), pct_change below this sets passed=False.
        date_column: Optional column name for no_new_data check.
        schema_check_dtypes: If True, compare_schema includes type_changes.
        warn_on_stale: If True, run detect_identical_or_stale and append to warnings when stale.
        stale_key_column: Optional key column(s) for stale detection.

    Returns:
        {
            "passed": bool,
            "warnings": list[str],
            "schema_changes": compare_schema output,
            "volume": compare_volume output,
            "stale": detect_identical_or_stale output (if warn_on_stale),
            "comparison": compare_two_reports output,
            "below_threshold": {"overall": bool, "dimensions": list},
        }
    """
    warnings: List[str] = []
    passed = True

    schema_changes = compare_schema(df_baseline, df_current, check_dtypes=schema_check_dtypes)
    if schema_changes["added"] or schema_changes["removed"]:
        msg = (
            f"Schema change: added columns {schema_changes['added']!r}, "
            f"removed columns {schema_changes['removed']!r}."
        )
        warnings.append(msg)
        if fail_on_schema_change:
            passed = False
    if schema_changes.get("type_changes"):
        warnings.append(f"Schema type changes: {schema_changes['type_changes']!r}.")

    volume = compare_volume(df_baseline, df_current, date_column=date_column)
    if volume["no_new_data"] is True:
        warnings.append("No new data by date: max(current date) <= max(baseline date).")
        passed = False
    if fail_on_volume_drop_pct is not None and volume["pct_change"] < fail_on_volume_drop_pct:
        warnings.append(
            f"Volume drop {volume['pct_change']}% exceeds threshold {fail_on_volume_drop_pct}%."
        )
        passed = False

    stale: Dict[str, Any] = {}
    if warn_on_stale:
        stale = detect_identical_or_stale(
            df_baseline, df_current,
            key_column=stale_key_column,
        )
        if stale.get("stale_warning") and stale.get("reason"):
            warnings.append(stale["reason"])

    report_baseline, report_current = run_same_rules_on_two_datasets(
        df_baseline,
        df_current,
        rules_runner,
        dataset_name_a=dataset_name_baseline,
        dataset_name_b=dataset_name_current,
        critical_columns=critical_columns,
    )
    comparison = compare_two_reports(report_baseline, report_current)

    below_overall = False
    below_dimensions: List[str] = []

    if "error_a" in comparison or "error_b" in comparison:
        warnings.append(
            f"Report errors: baseline={comparison.get('error_a')}, current={comparison.get('error_b')}."
        )
        passed = False
    else:
        km_current = report_current.get("key_metrics", {})
        score_current = km_current.get("overall_health_score", 0)
        dim_scores_current = km_current.get("per_dimension_scores", {})

        if min_overall_health is not None and score_current < min_overall_health:
            below_overall = True
            passed = False
            warnings.append(
                f"Overall health score {score_current} is below threshold {min_overall_health}."
            )
        if min_per_dimension:
            for dim, min_score in min_per_dimension.items():
                actual = dim_scores_current.get(dim)
                if actual is not None and actual < min_score:
                    below_dimensions.append(dim)
                    passed = False
                    warnings.append(
                        f"Dimension {dim!r} score {actual} is below threshold {min_score}."
                    )

    below_threshold: Dict[str, Any] = {
        "overall": below_overall,
        "dimensions": below_dimensions,
    }

    return {
        "passed": passed,
        "warnings": warnings,
        "schema_changes": schema_changes,
        "volume": volume,
        "stale": stale,
        "comparison": comparison,
        "below_threshold": below_threshold,
    }
