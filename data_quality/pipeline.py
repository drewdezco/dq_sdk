"""
Pipeline integration: compare current dataset to a baseline (e.g. same dataset one week earlier),
evaluate degradation thresholds, and detect volume drop, schema changes, and stale/identical data.

Callers get a single result dict (passed, warnings, details); the pipeline decides whether
to alert or stop ingestion. Load baseline and current from your environment (e.g. tables or CSV paths);
this module is stateless.
"""

from pathlib import Path
from typing import Any, Callable, Dict, List, Literal, Optional, Union

import pandas as pd

from data_quality.comparison import compare_two_reports, run_same_rules_on_two_datasets
from data_quality.utils import normalize_columns

# Default threshold values applied when use_default_thresholds=True and the user did not pass a value.
DEFAULT_THRESHOLDS: Dict[str, Any] = {
    "min_overall_health": 80,
    "fail_on_volume_drop_pct": -25,
}


def load_dataframe(
    source: Union[pd.DataFrame, str, Path],
    **read_csv_kwargs: Any,
) -> pd.DataFrame:
    """
    Normalize a snapshot source to a pandas DataFrame.

    If source is already a DataFrame, return it unchanged. If source is a str or Path,
    load with pd.read_csv(source, **read_csv_kwargs). Use this for CSV files or pass
    through to compare_snapshots / compare_snapshots_multi when using paths.

    Args:
        source: A DataFrame, or a path to a CSV (or other file pandas can read).
        **read_csv_kwargs: Passed to pd.read_csv when source is a path (e.g. encoding, sep).

    Returns:
        The DataFrame (either the input or loaded from the path).
    """
    if isinstance(source, pd.DataFrame):
        return source
    path = Path(source) if isinstance(source, str) else source
    return pd.read_csv(path, **read_csv_kwargs)


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
    df_baseline: Union[pd.DataFrame, str, Path],
    df_current: Union[pd.DataFrame, str, Path],
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
    use_default_thresholds: bool = False,
    read_csv_kwargs: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Compare current dataset to baseline: schema, volume, stale detection, and quality (same rules on both).

    Load baseline and current (e.g. 1 week apart) as DataFrames or CSV/path; call with your rules_runner.
    Use min_overall_health=80, fail_on_volume_drop_pct=-25, warn_on_stale=True. Or set use_default_thresholds=True
    to apply those defaults only where you do not pass a value. If not result["passed"]: alert or stop ingestion;
    check result["warnings"] for stale data.

    Args:
        df_baseline: Baseline snapshot (e.g. last week); DataFrame or path to CSV.
        df_current: Current snapshot; DataFrame or path to CSV.
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
        use_default_thresholds: If True, apply DEFAULT_THRESHOLDS (min_overall_health=80, fail_on_volume_drop_pct=-25) only where the corresponding arg is None.
        read_csv_kwargs: Optional dict passed to pd.read_csv when df_baseline or df_current is a path (e.g. {"encoding": "utf-8", "sep": ";"}).

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
    read_kwargs = read_csv_kwargs or {}
    df_baseline = load_dataframe(df_baseline, **read_kwargs)
    df_current = load_dataframe(df_current, **read_kwargs)

    if use_default_thresholds:
        if min_overall_health is None and "min_overall_health" in DEFAULT_THRESHOLDS:
            min_overall_health = DEFAULT_THRESHOLDS["min_overall_health"]
        if fail_on_volume_drop_pct is None and "fail_on_volume_drop_pct" in DEFAULT_THRESHOLDS:
            fail_on_volume_drop_pct = DEFAULT_THRESHOLDS["fail_on_volume_drop_pct"]

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


def compare_snapshots_multi(
    snapshots: List[Union[pd.DataFrame, str, Path]],
    rules_runner: Callable[[pd.DataFrame, List], None],
    *,
    mode: Literal["consecutive", "baseline"] = "consecutive",
    read_csv_kwargs: Optional[Dict[str, Any]] = None,
    **compare_snapshots_kwargs: Any,
) -> Dict[str, Any]:
    """
    Compare multiple snapshots in order: either consecutive pairs (s0 vs s1, s1 vs s2, ...)
    or each snapshot against the first (s0 vs s1, s0 vs s2, ...). Snapshot order is the list order:
    index 0 = oldest (or baseline in baseline mode).

    Args:
        snapshots: Ordered list of DataFrames or paths to CSV files (oldest first).
        rules_runner: Callable(df, results) passed to compare_snapshots for each pair.
        mode: "consecutive" = compare (s0,s1), (s1,s2), ...; "baseline" = compare (s0,s1), (s0,s2), ....
        read_csv_kwargs: Optional dict for pd.read_csv when an element of snapshots is a path.
        **compare_snapshots_kwargs: Forwarded to compare_snapshots (e.g. min_overall_health, fail_on_volume_drop_pct, use_default_thresholds).

    Returns:
        {
            "passed": bool (True iff all pairs passed),
            "warnings": list[str] (all per-pair warnings, prefixed with pair id),
            "results": list[dict] (one compare_snapshots result per pair, with "baseline_index", "current_index", "baseline_label", "current_label"),
        }
    """
    if len(snapshots) < 2:
        return {
            "passed": True,
            "warnings": [],
            "results": [],
        }
    read_kwargs = read_csv_kwargs or {}
    loaded: List[pd.DataFrame] = [load_dataframe(s, **read_kwargs) for s in snapshots]

    pairs: List[tuple] = []
    if mode == "consecutive":
        for i in range(len(loaded) - 1):
            pairs.append((i, i + 1))
    else:
        for i in range(1, len(loaded)):
            pairs.append((0, i))

    results: List[Dict[str, Any]] = []
    all_warnings: List[str] = []
    all_passed = True

    for idx_b, idx_c in pairs:
        label_b = f"snapshot_{idx_b}"
        label_c = f"snapshot_{idx_c}"
        result = compare_snapshots(
            loaded[idx_b],
            loaded[idx_c],
            rules_runner,
            dataset_name_baseline=label_b,
            dataset_name_current=label_c,
            **compare_snapshots_kwargs,
        )
        result["baseline_index"] = idx_b
        result["current_index"] = idx_c
        result["baseline_label"] = label_b
        result["current_label"] = label_c
        results.append(result)
        if not result["passed"]:
            all_passed = False
        for w in result.get("warnings", []):
            all_warnings.append(f"[{label_b} vs {label_c}] {w}")

    return {
        "passed": all_passed,
        "warnings": all_warnings,
        "results": results,
    }
