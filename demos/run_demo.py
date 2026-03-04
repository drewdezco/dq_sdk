"""
Demo runner: use case 1 (all validations + similarity), use case 2 (reconcile / compare reports).
Supports --list-validations to print dimensions and validations with definitions.
"""

import argparse
import os
import sys

# Ensure project root is on path so data_quality and demos are importable
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_SCRIPT_DIR)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

import pandas as pd
from data_quality import (
    DataQualityChecker,
    reconcile_on_key,
    run_same_rules_on_two_datasets,
    compare_two_reports,
)

from demos.demo_data import build_demo_df_all_validations, build_comparison_dfs
from demos.reference import print_validations_reference


def _format_details_short(details):
    """Turn a result details dict into a short human-readable string for terminal display."""
    if not details or not isinstance(details, dict):
        return ""
    passed = details.get("passed")
    failed = details.get("failed")
    total = details.get("total")
    if total is not None and passed is not None and failed is not None:
        return f"{int(passed)}/{int(total)} passed ({int(failed)} failed)"
    similar = details.get("similar_pairs") or details.get("within_threshold")
    total_comp = details.get("total_comparisons") or details.get("total")
    if total_comp is not None and similar is not None:
        return f"{int(similar)}/{int(total_comp)} above threshold"
    if "matched_pairs" in details:
        jq = details
        return f"{jq.get('matched_pairs', 0)} matched, {jq.get('only_in_left', 0)} only left, {jq.get('only_in_right', 0)} only right"
    return ""


def _print_rule_results_table(results_df):
    """Print a clean table of rule results without raw dict dumps."""
    # Build display rows: column, rule, success %, dimension, short details
    rows = []
    for _, r in results_df.iterrows():
        details_short = _format_details_short(r.get("details"))
        rows.append({
            "Column": str(r.get("column", ""))[:28],
            "Rule": str(r.get("rule", ""))[:42],
            "Success %": f"{float(r.get('success_rate', 0)):.1f}",
            "Dimension": str(r.get("dimension", "")),
            "Details": details_short[:40],
        })
    out = pd.DataFrame(rows)
    # Use to_string with index=False for a clean table; pandas will align columns
    print(out.to_string(index=False))
    print()


def _print_summary(report):
    """Print key_metrics with clear labels and native types for readable output."""
    km = report.get("key_metrics", {})
    if not km:
        return
    # Scalar metrics first
    scalars = [
        ("Total records", "total_records"),
        ("Total columns", "total_columns"),
        ("Total cells", "total_cells"),
        ("Null cells", "null_cells"),
        ("Data completeness rate %", "data_completeness_rate"),
        ("Total rules executed", "total_rules_executed"),
        ("Overall health score", "overall_health_score"),
        ("Overall health status", "overall_health_status"),
    ]
    for label, key in scalars:
        if key in km:
            v = km[key]
            if hasattr(v, "item"):  # numpy scalar
                v = v.item() if hasattr(v, "item") else float(v)
            elif isinstance(v, float):
                v = round(v, 1) if v != int(v) else int(v)
            print(f"  {label}: {v}")
    # Per-dimension scores on their own lines
    dim_scores = km.get("per_dimension_scores")
    if isinstance(dim_scores, dict) and dim_scores:
        print("  Per-dimension scores:")
        for dim, score in dim_scores.items():
            s = score
            if hasattr(s, "item"):
                s = round(float(s.item()), 1)
            else:
                s = round(float(s), 1)
            print(f"    {dim}: {s}")
    print()


def run_use_case_1(save_csv=True):
    """Run all validations and similarity on the single-dataset demo DataFrame."""
    df = build_demo_df_all_validations()
    checker = DataQualityChecker(
        df,
        dataset_name="Demo - All validations",
        critical_columns=["id", "customer_id"],
    )

    # Reference set for Accuracy (match_reference)
    category_reference = {"Electronics", "Clothing", "Home"}
    # Reference date for Timeliness (recent within 365 days)
    reference_date = pd.Timestamp("2024-06-01")
    # Date range for Validity (in_date_range): 2023-01-01 to 2025-12-31
    min_date, max_date = "2023-01-01", "2025-12-31"

    # -------- Completeness --------
    checker.expect_column_values_to_not_be_null("id")
    checker.expect_column_values_to_not_be_null("customer_id")
    checker.expect_columns_values_to_not_be_null(["id", "customer_id"])

    # -------- Uniqueness --------
    checker.expect_column_values_to_be_unique("id")
    checker.expect_columns_values_to_be_unique(["customer_id", "order_date"])

    # -------- Validity --------
    checker.expect_column_values_to_be_in_set("region", allowed_values=["North", "South", "East", "West"])
    checker.expect_column_values_to_be_in_set("status", allowed_values=["active", "inactive", "pending"])
    checker.expect_column_values_to_be_in_range("amount", 0, 100000)
    checker.expect_column_values_to_be_in_date_range("order_date", min_date, max_date)
    checker.expect_column_values_to_match_regex("sku", pattern=r"^[A-Z]\d+$")
    checker.expect_columns_values_to_be_in_sets(
        ["region", "status"],
        {"region": ["North", "South", "East", "West"], "status": ["active", "inactive", "pending"]},
    )
    checker.expect_columns_values_to_match_patterns(
        ["sku"],
        {"sku": r"^[A-Z]\d+$"},
    )
    checker.expect_columns_values_to_be_in_ranges(
        ["amount", "id"],
        {"amount": (0, 100000), "id": (1, 1000)},
    )

    # -------- Timeliness --------
    checker.expect_column_values_to_be_recent("order_date", max_age_days=365, reference_date=reference_date)

    # -------- Accuracy --------
    checker.expect_column_values_to_match_reference("category", category_reference)

    # -------- Consistency --------
    checker.analyze_column_similarity_levenshtein("name_primary", "name_alt", similarity_threshold=0.8)

    # Results
    results_df = checker.get_results()
    report = checker.get_comprehensive_results(title="Demo - All validations")

    print("Use case 1: Single dataset - all validations")
    print("=" * 60)
    print("Rule results:")
    _print_rule_results_table(results_df)
    if "error" in report:
        print("Report error:", report["error"])
        return
    print("Summary (key_metrics):")
    _print_summary(report)

    if save_csv:
        report_path = os.path.join(_PROJECT_ROOT, "data", "demo_quality_report.csv")
        os.makedirs(os.path.dirname(report_path), exist_ok=True)
        checker.save_comprehensive_results_to_csv(
            title="Demo - All validations",
            csv_filename=report_path,
            include_field_summary=True,
        )
        print(f"Report saved to {report_path} (and field details)")
        print()


def run_use_case_2():
    """Run reconciliation and optional same-rules comparison on two small DataFrames."""
    df_left, df_right = build_comparison_dfs()
    results = []

    print("Use case 2: Comparing two datasets")
    print("=" * 60)

    # Reconciliation on key
    summary = reconcile_on_key(
        df_left,
        df_right,
        key_column="id",
        results=results,
        right_name="Source",
        include_similarity=True,
        similarity_threshold=0.8,
    )
    print("Reconciliation summary:")
    jq = summary.get("join_quality") or {}
    print(f"  Matched pairs:     {jq.get('matched_pairs', 0)}")
    print(f"  Only in left:      {jq.get('only_in_left', 0)}")
    print(f"  Only in right:     {jq.get('only_in_right', 0)}")
    print(f"  Match rate (left): {jq.get('match_rate_left_pct', 0)}%")
    print(f"  Match rate (right): {jq.get('match_rate_right_pct', 0)}%")
    print(f"  Columns compared:  {', '.join(summary.get('columns_compared') or [])}")
    print(f"  Overall match rate: {summary.get('overall_match_rate', 0)}%")
    print()

    # Attach results to a checker and get comprehensive view
    checker = DataQualityChecker(df_left, dataset_name="Warehouse")
    checker.results = results
    report = checker.get_comprehensive_results(title="Reconciliation")
    if "error" not in report:
        score = report["key_metrics"].get("overall_health_score")
        if hasattr(score, "item"):
            score = round(float(score.item()), 1)
        else:
            score = round(float(score), 1) if score is not None else ""
        print("Reconciliation report (key_metrics):")
        print(f"  Overall health score: {score}")
    print()

    # Same rules on both, then compare reports
    def rules_runner(df, results_list):
        c = DataQualityChecker(df, dataset_name="", critical_columns=["id"])
        c.df = df
        c.results = results_list
        c.expect_column_values_to_not_be_null("id")
        c.expect_column_values_to_be_unique("id")
        c.expect_column_values_to_be_in_range("amount", 0, 500)

    report_a, report_b = run_same_rules_on_two_datasets(
        df_left,
        df_right,
        rules_runner,
        dataset_name_a="Warehouse",
        dataset_name_b="Source",
        critical_columns=["id"],
    )
    diff = compare_two_reports(report_a, report_b)
    if "error_a" in diff or "error_b" in diff:
        print("Compare reports: error", diff)
        return
    sa = diff.get("overall_health_score_a")
    sb = diff.get("overall_health_score_b")
    if hasattr(sa, "item"):
        sa = round(float(sa.item()), 1)
    else:
        sa = round(float(sa), 1) if sa is not None else ""
    if hasattr(sb, "item"):
        sb = round(float(sb.item()), 1)
    else:
        sb = round(float(sb), 1) if sb is not None else ""
    delta = diff.get("delta")
    if hasattr(delta, "item"):
        delta = round(float(delta.item()), 1)
    elif delta is not None:
        delta = round(float(delta), 1)
    print("Compare two reports (same rules on Warehouse vs Source):")
    print(f"  Overall health score (Warehouse): {sa}")
    print(f"  Overall health score (Source):   {sb}")
    print(f"  Delta (B - A):                  {delta}")
    print("  Per-rule diffs (sample):")
    for r in (diff.get("per_rule_diffs") or [])[:5]:
        col = r.get("column", "")
        rule = r.get("rule", "")
        ra = r.get("success_rate_a", "")
        rb = r.get("success_rate_b", "")
        d = r.get("delta", "")
        if isinstance(ra, (int, float)):
            ra = f"{float(ra):.1f}"
        if isinstance(rb, (int, float)):
            rb = f"{float(rb):.1f}"
        if isinstance(d, (int, float)):
            d = f"{float(d):.1f}"
        print(f"    {col} / {rule}: A={ra}  B={rb}  delta={d}")
    print()


def main():
    parser = argparse.ArgumentParser(description="Run data quality demo use cases.")
    parser.add_argument(
        "--list-validations",
        "--list",
        action="store_true",
        dest="list_validations",
        help="Print dimensions and validations with definitions, then exit.",
    )
    parser.add_argument(
        "--no-csv",
        action="store_true",
        help="Skip saving demo report CSV in use case 1.",
    )
    args = parser.parse_args()

    if args.list_validations:
        print_validations_reference()
        return

    run_use_case_1(save_csv=not args.no_csv)
    run_use_case_2()


if __name__ == "__main__":
    main()
