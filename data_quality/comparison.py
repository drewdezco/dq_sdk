"""
Cross-dataset comparison: join quality, reconciliation (match rates and similarity of related fields),
and same-rules report comparison. All functions append to a results list (same shape as expectations/similarity).
"""

import pandas as pd
import numpy as np
from data_quality.utils import normalize_columns, levenshtein_ratio


def _key_set(df, key_column):
    """Return set of key tuples for multi-column key, or set of values for single column."""
    keys = df[key_column]
    if isinstance(key_column, str):
        return set(keys.dropna().unique())
    return set(keys.dropna().apply(tuple, axis=1).unique())


def suggest_key_columns(
    df_left: pd.DataFrame,
    df_right: pd.DataFrame,
    *,
    min_uniqueness: float = 0.9,
    max_null_rate: float = 0.1,
    max_candidates: int = 3,
) -> list[str]:
    """
    Suggest candidate key columns shared by two DataFrames.

    A column is considered a key candidate when ALL of the following hold on BOTH sides:
        - The column exists in both DataFrames
        - Null-rate <= max_null_rate
        - Uniqueness (nunique / non-null) >= min_uniqueness

    Args:
        df_left: Left DataFrame.
        df_right: Right DataFrame.
        min_uniqueness: Minimum required uniqueness ratio (0-1) per side.
        max_null_rate: Maximum allowed null ratio (0-1) per side.
        max_candidates: Maximum number of column names to return, ordered by strongest uniqueness.

    Returns:
        List of column names that are plausible primary-key candidates, sorted by
        descending geometric mean of left/right uniqueness.
    """
    shared_cols = [c for c in df_left.columns if c in df_right.columns]
    candidates: list[tuple[str, float]] = []

    for col in shared_cols:
        left_series = df_left[col]
        right_series = df_right[col]

        left_non_null = left_series.notna().sum()
        right_non_null = right_series.notna().sum()
        if left_non_null == 0 or right_non_null == 0:
            continue

        left_null_rate = 1.0 - (left_non_null / len(df_left)) if len(df_left) else 1.0
        right_null_rate = 1.0 - (right_non_null / len(df_right)) if len(df_right) else 1.0
        if left_null_rate > max_null_rate or right_null_rate > max_null_rate:
            continue

        left_uniqueness = left_series.nunique(dropna=True) / left_non_null
        right_uniqueness = right_series.nunique(dropna=True) / right_non_null
        if left_uniqueness < min_uniqueness or right_uniqueness < min_uniqueness:
            continue

        # Use geometric-like mean to reward high uniqueness on both sides.
        score = float(np.sqrt(left_uniqueness * right_uniqueness))
        candidates.append((col, score))

    # Sort by score descending and return column names only.
    candidates.sort(key=lambda x: x[1], reverse=True)
    return [c for c, _ in candidates[:max_candidates]]


def reconcile_with_auto_key(
    df_left: pd.DataFrame,
    df_right: pd.DataFrame,
    results,
    *,
    columns_to_compare=None,
    include_similarity: bool = False,
    similarity_threshold: float = 0.8,
    right_name: str = "B",
    min_uniqueness: float = 0.9,
    max_null_rate: float = 0.1,
) -> dict:
    """
    Automatically pick a key column, then delegate to reconcile_on_key.

    This is a convenience wrapper around suggest_key_columns + reconcile_on_key for
    quick exploration. It is intended for ad-hoc analysis and notebooks rather than
    strict production contracts where keys are known.

    Args:
        df_left: Left DataFrame.
        df_right: Right DataFrame.
        results: List that reconciliation results will be appended to.
        columns_to_compare: Optional subset of columns to reconcile; by default all
            common non-key columns are compared.
        include_similarity: If True, compute text similarity metrics as well.
        similarity_threshold: Threshold for considering pairs \"similar\".
        right_name: Label for the right-hand dataset in rule labels.
        min_uniqueness: Minimum required uniqueness ratio (0-1) for an auto-picked key.
        max_null_rate: Maximum allowed null ratio (0-1) for an auto-picked key.

    Returns:
        The same summary dict as reconcile_on_key, with an extra field:
            - auto_key_column: the column name that was selected.

    Raises:
        ValueError: If no suitable key column can be inferred.
    """
    candidates = suggest_key_columns(
        df_left,
        df_right,
        min_uniqueness=min_uniqueness,
        max_null_rate=max_null_rate,
        max_candidates=1,
    )
    if not candidates:
        raise ValueError(
            "Could not infer a key column automatically. "
            "Try passing key_column explicitly to reconcile_on_key, "
            "or relax min_uniqueness / max_null_rate."
        )

    key_column = candidates[0]
    summary = reconcile_on_key(
        df_left,
        df_right,
        key_column=key_column,
        results=results,
        columns_to_compare=columns_to_compare,
        include_similarity=include_similarity,
        similarity_threshold=similarity_threshold,
        right_name=right_name,
    )
    summary["auto_key_column"] = key_column
    return summary


def reconcile_on_key(
    df_left: pd.DataFrame,
    df_right: pd.DataFrame,
    key_column,
    results,
    columns_to_compare=None,
    include_similarity: bool = False,
    similarity_threshold: float = 0.8,
    right_name: str = "B",
) -> dict:
    """
    Join two DataFrames on key_column, assess join quality, then compute exact match rate
    (and optionally similarity) per compared column. Append all metrics to results with dimension Consistency.

    key_column: str or list of str. columns_to_compare: None (use all common non-key columns) or list.
    include_similarity: if True, for string-like columns append a second result with similarity % above threshold.
    Returns summary dict: join_quality, columns_compared, overall_match_rate, optional similarity summary.
    """
    key_list = normalize_columns(key_column)
    for k in key_list:
        if k not in df_left.columns or k not in df_right.columns:
            raise ValueError(f"Key column {k!r} missing in one or both DataFrames")

    left_keys = _key_set(df_left, key_list)
    right_keys = _key_set(df_right, key_list)
    matched_keys = left_keys & right_keys
    only_in_left = left_keys - right_keys
    only_in_right = right_keys - left_keys

    n_left = len(left_keys)
    n_right = len(right_keys)
    n_matched = len(matched_keys)
    match_rate_left = (n_matched / n_left * 100) if n_left else 0
    match_rate_right = (n_matched / n_right * 100) if n_right else 0

    join_quality = {
        "matched_pairs": n_matched,
        "only_in_left": len(only_in_left),
        "only_in_right": len(only_in_right),
        "left_key_count": n_left,
        "right_key_count": n_right,
        "match_rate_left_pct": round(match_rate_left, 1),
        "match_rate_right_pct": round(match_rate_right, 1),
    }
    success_rate_join = (match_rate_left + match_rate_right) / 2 if (n_left or n_right) else 0
    results.append({
        "column": ", ".join(key_list),
        "rule": "join quality",
        "success_rate": round(success_rate_join, 1),
        "details": join_quality,
        "dimension": "Consistency",
    })

    if n_matched == 0:
        return {
            "join_quality": join_quality,
            "columns_compared": [],
            "overall_match_rate": 0,
        }

    merged = pd.merge(
        df_left,
        df_right,
        on=key_list,
        how="inner",
        suffixes=("_left", "_right"),
    )

    common_non_key = [c for c in df_left.columns if c in df_right.columns and c not in key_list]
    if columns_to_compare is not None:
        to_compare = normalize_columns(columns_to_compare)
        to_compare = [c for c in to_compare if c in common_non_key]
    else:
        to_compare = common_non_key

    total_rows = len(merged)
    match_rates = []
    for col in to_compare:
        left_col = f"{col}_left" if f"{col}_left" in merged.columns else col
        right_col = f"{col}_right" if f"{col}_right" in merged.columns else col
        if left_col not in merged.columns or right_col not in merged.columns:
            continue
        left_vals = merged[left_col]
        right_vals = merged[right_col]
        both_null = left_vals.isna() & right_vals.isna()
        exact = ((left_vals == right_vals) | both_null).sum()
        failed = total_rows - exact
        success_rate = (exact / total_rows * 100) if total_rows else 0
        match_rates.append(success_rate)
        results.append({
            "column": col,
            "rule": f"reconcile vs {right_name}",
            "success_rate": round(success_rate, 1),
            "details": {"total": total_rows, "passed": exact, "failed": failed},
            "dimension": "Consistency",
        })

        if include_similarity:
            left_str = merged[left_col].fillna("").astype(str)
            right_str = merged[right_col].fillna("").astype(str)
            similar_count = 0
            ratios = []
            for a, b in zip(left_str, right_str):
                r = levenshtein_ratio(a, b)
                ratios.append(r)
                if r >= similarity_threshold:
                    similar_count += 1
            sim_pct = (similar_count / total_rows * 100) if total_rows else 0
            results.append({
                "column": col,
                "rule": f"reconcile similarity vs {right_name} (threshold {similarity_threshold})",
                "success_rate": round(sim_pct, 1),
                "details": {
                    "total": total_rows,
                    "above_threshold": similar_count,
                    "similarity_threshold": similarity_threshold,
                    "average_similarity": round(np.mean(ratios), 3) if ratios else 0,
                },
                "dimension": "Consistency",
            })

    overall_match = round(np.mean(match_rates), 1) if match_rates else 0
    return {
        "join_quality": join_quality,
        "columns_compared": to_compare,
        "overall_match_rate": overall_match,
    }


def get_reconciliation_diffs(
    df_left: pd.DataFrame,
    df_right: pd.DataFrame,
    key_column,
    column: str,
) -> pd.DataFrame:
    """
    Return rows where the two sides differ for the given column.

    The result has columns: key (or key tuple as string), value_left, value_right.
    """
    key_list = normalize_columns(key_column)
    merged = pd.merge(
        df_left,
        df_right,
        on=key_list,
        how="inner",
        suffixes=("_left", "_right"),
    )
    left_col = f"{column}_left" if f"{column}_left" in merged.columns else column
    right_col = f"{column}_right" if f"{column}_right" in merged.columns else column
    if left_col not in merged.columns or right_col not in merged.columns:
        return pd.DataFrame()
    key_name = key_list[0] if len(key_list) == 1 else "key"
    if len(key_list) == 1:
        key_vals = merged[key_list[0]]
    else:
        key_vals = merged[key_list].astype(str).agg("|".join, axis=1)
    left_vals = merged[left_col]
    right_vals = merged[right_col]
    diff_mask = left_vals.fillna("__NA__").astype(str) != right_vals.fillna("__NA__").astype(str)
    out = pd.DataFrame({
        key_name: key_vals[diff_mask].values,
        "value_left": left_vals[diff_mask].values,
        "value_right": right_vals[diff_mask].values,
    })
    return out


def run_same_rules_on_two_datasets(
    df_a: pd.DataFrame,
    df_b: pd.DataFrame,
    rules_runner,
    dataset_name_a: str = "A",
    dataset_name_b: str = "B",
    critical_columns=None,
) -> tuple:
    """
    Run the same rules (expectations/similarity) on both DataFrames via rules_runner(df, results).
    Build two comprehensive reports and return (report_a, report_b).
    rules_runner must be a callable(df, results) -> None that appends to results.
    """
    from data_quality import reporting as rep
    results_a = []
    results_b = []
    rules_runner(df_a, results_a)
    rules_runner(df_b, results_b)
    critical = critical_columns or []
    report_a = rep.get_comprehensive_results(
        df_a, results_a, dataset_name_a, critical, title=f"Report {dataset_name_a}"
    )
    report_b = rep.get_comprehensive_results(
        df_b, results_b, dataset_name_b, critical, title=f"Report {dataset_name_b}"
    )
    return report_a, report_b


def compare_two_reports(report_a: dict, report_b: dict) -> dict:
    """
    Compare two comprehensive report dicts. Returns a dict with overall scores, deltas,
    per_dimension scores for each, and per_rule_diffs (list of rule/column with success_rate_a, success_rate_b, delta).
    """
    if "error" in report_a or "error" in report_b:
        return {
            "error_a": report_a.get("error"),
            "error_b": report_b.get("error"),
        }
    km_a = report_a.get("key_metrics", {})
    km_b = report_b.get("key_metrics", {})
    score_a = km_a.get("overall_health_score", 0)
    score_b = km_b.get("overall_health_score", 0)
    dim_a = km_a.get("per_dimension_scores", {})
    dim_b = km_b.get("per_dimension_scores", {})

    all_dims = set(dim_a) | set(dim_b)
    per_dimension_diffs = {
        d: {
            "score_a": dim_a.get(d),
            "score_b": dim_b.get(d),
            "delta": (dim_b.get(d) or 0) - (dim_a.get(d) or 0),
        }
        for d in all_dims
    }

    details_a = report_a.get("detailed_results", [])
    details_b = report_b.get("detailed_results", [])
    by_key_a = {(r.get("column"), r.get("rule")): r.get("success_rate") for r in details_a}
    by_key_b = {(r.get("column"), r.get("rule")): r.get("success_rate") for r in details_b}
    all_keys = set(by_key_a) | set(by_key_b)
    per_rule_diffs = []
    for (col, rule) in all_keys:
        sa = by_key_a.get((col, rule))
        sb = by_key_b.get((col, rule))
        if sa is None:
            sa = ""
        if sb is None:
            sb = ""
        delta = (sb if sb != "" else 0) - (sa if sa != "" else 0) if isinstance(sa, (int, float)) and isinstance(sb, (int, float)) else None
        per_rule_diffs.append({
            "column": col,
            "rule": rule,
            "success_rate_a": sa,
            "success_rate_b": sb,
            "delta": delta,
        })

    return {
        "overall_health_score_a": score_a,
        "overall_health_score_b": score_b,
        "delta": score_b - score_a,
        "per_dimension_a": dim_a,
        "per_dimension_b": dim_b,
        "per_dimension_diffs": per_dimension_diffs,
        "per_rule_diffs": per_rule_diffs,
    }


class DatasetComparator:
    """
    Convenience wrapper around the comparison helpers.

    Holds two DataFrames plus key information and exposes:
    - reconcile(): run join-quality and optional similarity checks
    - run_same_rules(): run the same rules via a rules_runner on both datasets
    - get_comparison_report(): run rules then compare the two reports
    """

    def __init__(self, df_a: pd.DataFrame, df_b: pd.DataFrame, key_column, name_a: str = "A", name_b: str = "B") -> None:
        self.df_a = df_a
        self.df_b = df_b
        self.key_column = key_column
        self.name_a = name_a
        self.name_b = name_b
        self.results = []

    def reconcile(
        self,
        columns_to_compare=None,
        include_similarity: bool = False,
        similarity_threshold: float = 0.8,
    ) -> dict:
        """Run reconcile_on_key and append to internal results; return summary dict."""
        summary = reconcile_on_key(
            self.df_a,
            self.df_b,
            self.key_column,
            self.results,
            columns_to_compare=columns_to_compare,
            include_similarity=include_similarity,
            similarity_threshold=similarity_threshold,
            right_name=self.name_b,
        )
        return summary

    def run_same_rules(self, rules_runner, critical_columns=None) -> tuple:
        """Run same rules on both datasets; return (report_a, report_b)."""
        return run_same_rules_on_two_datasets(
            self.df_a,
            self.df_b,
            rules_runner,
            dataset_name_a=self.name_a,
            dataset_name_b=self.name_b,
            critical_columns=critical_columns,
        )

    def get_comparison_report(self, rules_runner, critical_columns=None) -> dict:
        """Run same rules on both datasets, then compare the resulting reports."""
        report_a, report_b = self.run_same_rules(rules_runner, critical_columns=critical_columns)
        return compare_two_reports(report_a, report_b)
