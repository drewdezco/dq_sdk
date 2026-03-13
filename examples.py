"""
Examples: data quality validations, suggestions, and pipeline integration.

Copy each section below into a separate notebook cell (or run this whole
file from the project root). All data is defined inline or written to
temporary CSVs; no pre-existing files are required.
"""

import os
import tempfile

import pandas as pd
from data_quality import (
    DataQualityChecker,
    compare_schema,
    compare_snapshots,
    compare_snapshots_multi,
    compare_volume,
    detect_identical_or_stale,
    get_getting_started_guide,
    get_pipeline_markdown,
    get_usage_markdown,
    load_dataframe,
    print_docs_overview,
)


# =============================================================================
# SECTION 0: Docs helpers — where to find information
# =============================================================================
# Run this section to see a one-line, ready-to-copy call that lists all docs.

print("SECTION 0 — Docs index")
print_docs_overview()
print("\n" + "=" * 80 + "\n")

print("SECTION 0 — Getting started (markdown)")
print(get_getting_started_guide())
print("\n" + "=" * 80 + "\n")

print("SECTION 0 — Usage overview (markdown excerpt)")
print(get_usage_markdown().split("\n\n", maxsplit=3)[0])
print("\n" + "=" * 80 + "\n")

print("SECTION 0 — Pipeline docs (markdown excerpt)")
print(get_pipeline_markdown().split("\n\n", maxsplit=3)[0])
print("\n" + "=" * 80 + "\n")


# =============================================================================
# SECTION 1: Quick & easy — minimal validations
# =============================================================================
# Shows how little code is needed to run validations and see results.

# Step 1: Define a small DataFrame (no file read).
df = pd.DataFrame({
    "id": [1, 2, 3, 4, 5],
    "name": ["Alice", "Bob", "Charlie", "Dave", "Eve"],
    "status": ["active", "inactive", "active", "active", "pending"],
    "score": [85, 92, 78, 95, 88],
})

# Step 2: Create a checker and run a few expectations (not null, unique, in set, in range).
checker = DataQualityChecker(df, dataset_name="Quick Example")
checker.expect_column_values_to_not_be_null("id")
checker.expect_column_values_to_be_unique("id")
checker.expect_column_values_to_be_in_set("status", allowed_values=["active", "inactive", "pending"])
checker.expect_column_values_to_be_in_range("score", 0, 100)

# Step 3: Get results as a DataFrame and print data + validation summary.
results = checker.get_results()
print("SECTION 1 — Quick & easy")
print("Data:")
print(df)
print("\nValidation results:")
print(results[["column", "rule", "success_rate", "dimension"]].to_string(index=False))
print("\n" + "=" * 80 + "\n")


# =============================================================================
# SECTION 2: Single-column validations (deeper)
# =============================================================================
# Not null, unique, in set, numeric range, regex. Data and results visible.

# Step 1: Define data with intentional issues (one null, one negative amount, one bad email).
df = pd.DataFrame({
    "order_id": ["ORD-001", "ORD-002", "ORD-003", "ORD-004", "ORD-005", "ORD-006"],
    "customer_id": ["C101", "C102", "C101", "C103", "C104", None],
    "region": ["North", "South", "East", "North", "West", "South"],
    "amount": [99.99, 150.00, 200.50, -10.0, 75.25, 88.00],
    "email": [
        "a@example.com", "b@test.org", "c@example.com",
        "invalid", "e@example.com", "f@test.org",
    ],
})

# Step 2: Run single-column expectations including regex for email format.
checker = DataQualityChecker(df, dataset_name="Orders")
checker.expect_column_values_to_not_be_null("order_id")
checker.expect_column_values_to_be_unique("order_id")
checker.expect_column_values_to_not_be_null("customer_id")
checker.expect_column_values_to_be_in_set("region", allowed_values=["North", "South", "East", "West"])
checker.expect_column_values_to_be_in_range("amount", 0, 1_000_000)
checker.expect_column_values_to_match_regex("email", pattern=r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")

# Step 3: Get results; details show passed/failed counts per rule.
results = checker.get_results()
print("SECTION 2 — Single-column validations")
print("Data:")
print(df)
print("\nValidation results:")
print(results[["column", "rule", "success_rate", "dimension", "details"]].to_string(index=False))
print("\n" + "=" * 80 + "\n")


# =============================================================================
# SECTION 3: Multi-column validations
# =============================================================================
# Composite uniqueness and per-column allowed sets.

# Step 1: Define data with duplicate (first_name, last_name) pair: (Alice, Smith) appears twice.
df = pd.DataFrame({
    "first_name": ["Alice", "Bob", "Alice", "Charlie"],
    "last_name": ["Smith", "Jones", "Smith", "Brown"],
    "dept": ["Eng", "Eng", "Sales", "Eng"],
    "level": ["L1", "L2", "L1", "L2"],
})

# Step 2: Run multi-column expectations — combination of columns unique, and per-column allowed sets.
checker = DataQualityChecker(df, dataset_name="Staff")
checker.expect_columns_values_to_be_unique(["first_name", "last_name"])
checker.expect_columns_values_to_be_in_sets(
    columns=["dept", "level"],
    allowed_values={"dept": ["Eng", "Sales", "HR"], "level": ["L1", "L2", "L3"]},
)

# Step 3: Get and print results.
results = checker.get_results()
print("SECTION 3 — Multi-column validations")
print("Data:")
print(df)
print("\nValidation results:")
print(results[["column", "rule", "success_rate", "dimension"]].to_string(index=False))
print("\n" + "=" * 80 + "\n")


# =============================================================================
# SECTION 4: Dates — date range and recency
# =============================================================================
# Expect dates within a range and optionally "recent" (within last N days).

# Step 1: Define data with dates; one before range (2023), one after (2025).
df = pd.DataFrame({
    "id": [1, 2, 3, 4, 5],
    "event_date": pd.to_datetime(["2024-03-01", "2024-06-15", "2024-09-20", "2023-01-01", "2025-12-31"]),
})
reference_date = pd.Timestamp("2024-06-01")

# Step 2: Run date expectations — in range (2024-01-01 to 2024-12-31) and recent (within 365 days of reference).
checker = DataQualityChecker(df, dataset_name="Events")
checker.expect_column_values_to_be_in_date_range("event_date", min_date="2024-01-01", max_date="2024-12-31")
checker.expect_column_values_to_be_recent("event_date", max_age_days=365, reference_date=reference_date)

# Step 3: Get and print results (some rows will fail date range or recency).
results = checker.get_results()
print("SECTION 4 — Dates and recency")
print("Data:")
print(df)
print("\nValidation results:")
print(results[["column", "rule", "success_rate", "dimension"]].to_string(index=False))
print("\n" + "=" * 80 + "\n")


# =============================================================================
# SECTION 5: Comprehensive report (health score, dimensions)
# =============================================================================
# Full summary with per-dimension scores and key metrics.

# Step 1: Define data and mark "id" as a critical column for the report.
df = pd.DataFrame({
    "id": [1, 2, 3, 4, 5],
    "status": ["active", "inactive", "active", "pending", "active"],
    "amount": [100, 200, 150, 300, 250],
})
checker = DataQualityChecker(df, dataset_name="Report Demo", critical_columns=["id"])
checker.expect_column_values_to_not_be_null("id")
checker.expect_column_values_to_be_unique("id")
checker.expect_column_values_to_be_in_set("status", allowed_values=["active", "inactive", "pending"])
checker.expect_column_values_to_be_in_range("amount", 0, 1000)

# Step 2: Build a full report (key_metrics, per-dimension scores, overall health score).
report = checker.get_comprehensive_results(title="Demo Quality Report")
print("SECTION 5 — Comprehensive report")
if "error" in report:
    print(report["error"])
else:
    print("Key metrics:", report["key_metrics"])
    print("\nPer-dimension scores:", report.get("per_dimension_scores"))
    print("\nOverall health score:", report.get("overall_health_score"))
print("\n" + "=" * 80 + "\n")


# =============================================================================
# SECTION 6: Auto-suggestions
# =============================================================================
# Let the library suggest validations from the data; review and optionally apply.

# Step 1: Define data; the library will analyze columns and suggest rules (e.g. not null, unique, in range).
df = pd.DataFrame({
    "id": [1, 2, 3, 4, 5],
    "code": ["A", "B", "C", "D", "E"],
    "value": [10.0, 20.0, 30.0, 40.0, 50.0],
    "updated": pd.to_datetime(["2024-06-01", "2024-06-02", "2024-06-03", "2024-06-04", "2024-06-05"]),
})

# Step 2: Generate suggestions (each has column, method, confidence, reason).
checker = DataQualityChecker(df, dataset_name="Suggestions Demo")
suggestions = checker.generate_suggestions()

print("SECTION 6 — Auto-suggestions")
print("Suggested validations (from data shape and sample):")
for s in suggestions:
    print(f"  {s['column']}: {s['method']} — confidence {s['confidence']:.2f} — {s['reason']}")

# Step 3: Optionally apply only high-confidence suggestions, then get results.
high = [s for s in suggestions if s["confidence"] >= 0.8]
n = checker.apply_suggestions(high)
print(f"\nApplied {n} suggestion(s). Results:")
print(checker.get_results()[["column", "rule", "success_rate"]].to_string(index=False))
print("\n" + "=" * 80 + "\n")


# =============================================================================
# SECTION 7: Pipeline helpers — schema, volume, stale detection
# =============================================================================

print("SECTION 7 — Pipeline helpers: schema, volume, stale detection")

# Schema comparison
df_baseline = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"], "amount": [10, 20, 30]})
df_current = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"], "score": [10.0, 20.0, 30.0]})
out = compare_schema(df_baseline, df_current, check_dtypes=False)
print("Schema diff (no dtypes):")
print("  added:", out["added"])
print("  removed:", out["removed"])
print("  type_changes:", out["type_changes"])

out_with_dtypes = compare_schema(df_baseline, df_current, check_dtypes=True)
print("\nSchema diff (with dtypes):")
print("  added:", out_with_dtypes["added"])
print("  removed:", out_with_dtypes["removed"])
print("  type_changes:", out_with_dtypes["type_changes"])

# Volume comparison with and without date column
df_baseline = pd.DataFrame({
    "id": [1, 2, 3],
    "updated": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
})
df_current = pd.DataFrame({
    "id": [1, 2, 3, 4, 5],
    "updated": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"]),
})
out = compare_volume(df_baseline, df_current, date_column=None)
print("\nVolume (no date column):")
print("  row_count_baseline:", out["row_count_baseline"])
print("  row_count_current:", out["row_count_current"])
print("  pct_change:", out["pct_change"])
print("  no_new_data:", out["no_new_data"])

out_with_date = compare_volume(df_baseline, df_current, date_column="updated")
print("\nVolume (with date_column='updated'):")
print("  no_new_data:", out_with_date["no_new_data"])

df_stale = pd.DataFrame({
    "id": [1, 2, 3],
    "updated": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
})
out_stale = compare_volume(df_baseline, df_stale, date_column="updated")
print("\nVolume when current has same max date as baseline:")
print("  no_new_data:", out_stale["no_new_data"])

# Stale detection
df_b = pd.DataFrame({"id": [1, 2, 3], "x": [10, 20, 30]})
df_c = pd.DataFrame({"id": [1, 2, 3], "x": [10, 20, 30]})
out = detect_identical_or_stale(df_b, df_c, key_column="id")
print("\nIdentical key set (same id's):")
print("  identical:", out["identical"])
print("  stale_warning:", out["stale_warning"])
print("  reason:", out["reason"])

df_c_diff = pd.DataFrame({"id": [1, 2, 4], "x": [10, 20, 40]})
out_diff = detect_identical_or_stale(df_b, df_c_diff, key_column="id")
print("\nDifferent key set (current has id=4 instead of 3):")
print("  stale_warning:", out_diff["stale_warning"])

df_same_rows_no_key = pd.DataFrame({"id": [1, 2, 3], "x": [10, 20, 30]})
out_no_key = detect_identical_or_stale(df_b, df_same_rows_no_key, key_column=None)
print("\nSame row count, no key column:")
print("  stale_warning:", out_no_key["stale_warning"])
print("  reason:", out_no_key["reason"])
print("\n" + "=" * 80 + "\n")


# =============================================================================
# SECTION 8: Full pipeline check with compare_snapshots
# =============================================================================

print("SECTION 8 — compare_snapshots (full pipeline check)")


def _rules_runner(df, results_list):
    c = DataQualityChecker(df, dataset_name="")
    c.df = df
    c.results = results_list
    c.expect_column_values_to_not_be_null("id")
    c.expect_column_values_to_be_unique("id")
    c.expect_column_values_to_be_in_set("status", allowed_values=["active", "inactive"])


df_baseline = pd.DataFrame({
    "id": [1, 2, 3],
    "status": ["active", "inactive", "active"],
})
df_current = pd.DataFrame({
    "id": [1, 2, 3, 4],
    "status": ["active", "inactive", "active", "active"],
})
result = compare_snapshots(
    df_baseline,
    df_current,
    _rules_runner,
    min_overall_health=80,
    fail_on_volume_drop_pct=-50,
    warn_on_stale=True,
    stale_key_column="id",
)
print("compare_snapshots (passing):")
print("  passed:", result["passed"])
print("  warnings:", result["warnings"])
print("  volume:", result["volume"])
print("  below_threshold:", result["below_threshold"])

df_baseline = pd.DataFrame({"id": [1, 2], "old_col": [10, 20]})
df_current = pd.DataFrame({"id": [1, 2], "new_col": [10, 20]})


def _rules_runner_id_only(df, results_list):
    c = DataQualityChecker(df, dataset_name="")
    c.df = df
    c.results = results_list
    c.expect_column_values_to_not_be_null("id")


result = compare_snapshots(
    df_baseline,
    df_current,
    _rules_runner_id_only,
    fail_on_schema_change=True,
    warn_on_stale=False,
)
print("\ncompare_snapshots (fail_on_schema_change=True):")
print("  passed:", result["passed"])
print("  warnings:", result["warnings"])
print("  schema_changes:", result["schema_changes"])

df_baseline = pd.DataFrame({"id": [1, 2, 3, 4, 5], "status": ["active"] * 5})
df_current = pd.DataFrame({"id": [1, 2], "status": ["active", "active"]})
result = compare_snapshots(
    df_baseline,
    df_current,
    _rules_runner_id_only,
    fail_on_volume_drop_pct=-30,
    warn_on_stale=False,
)
print("\ncompare_snapshots (fail_on_volume_drop_pct=-30):")
print("  passed:", result["passed"])
print("  volume.pct_change:", result["volume"]["pct_change"])
print("  warnings:", result["warnings"])

df_baseline = pd.DataFrame({"id": [1, 2, 3], "status": ["active", "active", "active"]})
df_current = pd.DataFrame({"id": [1, 2, None], "status": ["active", "inactive", "active"]})
result = compare_snapshots(
    df_baseline,
    df_current,
    _rules_runner_id_only,
    min_overall_health=100.0,
    warn_on_stale=False,
)
print("\ncompare_snapshots (min_overall_health=100, current has null id):")
print("  passed:", result["passed"])
print("  below_threshold:", result["below_threshold"])
print("  warnings:", result["warnings"])

df_baseline = pd.DataFrame({"id": [1, 2, 3], "status": ["active", "inactive", "active"]})
df_current = pd.DataFrame({"id": [1, 2, 3], "status": ["active", "inactive", "active"]})
result = compare_snapshots(
    df_baseline,
    df_current,
    _rules_runner,
    warn_on_stale=True,
    stale_key_column="id",
)
print("\ncompare_snapshots (identical data -> stale warning):")
print("  passed:", result["passed"])
print("  stale:", result["stale"])
print("  warnings:", result["warnings"])
print("\n" + "=" * 80 + "\n")


# =============================================================================
# SECTION 9: CSV paths and multi-snapshot comparisons
# =============================================================================

print("SECTION 9 — CSV paths, load_dataframe, and compare_snapshots_multi")


def _rules_runner_for_csv(df, results_list):
    c = DataQualityChecker(df, dataset_name="")
    c.df = df
    c.results = results_list
    c.expect_column_values_to_not_be_null("id")


with tempfile.TemporaryDirectory() as tmpdir:
    baseline_path = os.path.join(tmpdir, "baseline.csv")
    current_path = os.path.join(tmpdir, "current.csv")
    pd.DataFrame({"id": [1, 2, 3], "x": [10, 20, 30]}).to_csv(baseline_path, index=False)
    pd.DataFrame({"id": [1, 2, 3, 4], "x": [10, 20, 30, 40]}).to_csv(current_path, index=False)
    result = compare_snapshots(
        baseline_path,
        current_path,
        _rules_runner_for_csv,
        warn_on_stale=False,
    )
    print("compare_snapshots with CSV paths:")
    print("  passed:", result["passed"])
    print("  volume:", result["volume"])

df_from_memory = pd.DataFrame({"a": [1, 2]})
assert load_dataframe(df_from_memory) is df_from_memory

df_week1 = pd.DataFrame({"id": [1, 2, 3], "x": [10, 20, 30]})
df_week2 = pd.DataFrame({"id": [1, 2, 3, 4], "x": [10, 20, 30, 40]})
df_week3 = pd.DataFrame({"id": [1, 2, 3, 4, 5], "x": [10, 20, 30, 40, 50]})
multi_consecutive = compare_snapshots_multi(
    [df_week1, df_week2, df_week3],
    _rules_runner_for_csv,
    mode="consecutive",
    warn_on_stale=False,
)
print("\ncompare_snapshots_multi (mode='consecutive'):")
print("  passed:", multi_consecutive["passed"])
print("  number of pair results:", len(multi_consecutive["results"]))
print(
    "  first pair baseline_label:",
    multi_consecutive["results"][0]["baseline_label"],
    "current_label:",
    multi_consecutive["results"][0]["current_label"],
)
print(
    "  second pair baseline_label:",
    multi_consecutive["results"][1]["baseline_label"],
    "current_label:",
    multi_consecutive["results"][1]["current_label"],
)

multi_baseline = compare_snapshots_multi(
    [df_week1, df_week2, df_week3],
    _rules_runner_for_csv,
    mode="baseline",
    warn_on_stale=False,
)
print("\ncompare_snapshots_multi (mode='baseline'):")
print("  passed:", multi_baseline["passed"])
print("  number of pair results:", len(multi_baseline["results"]))
print(
    "  both pairs have baseline_index=0:",
    all(r["baseline_index"] == 0 for r in multi_baseline["results"]),
)

df_b = pd.DataFrame({"id": [1, 2, 3, 4, 5], "x": range(5)})
df_c = pd.DataFrame({"id": [1, 2], "x": [0, 1]})
result_defaults = compare_snapshots(
    df_b,
    df_c,
    _rules_runner_for_csv,
    use_default_thresholds=True,
    warn_on_stale=False,
)
print("\ncompare_snapshots (use_default_thresholds=True, no explicit thresholds):")
print("  passed:", result_defaults["passed"])
print("  volume pct_change:", result_defaults["volume"]["pct_change"])
print("  (default fail_on_volume_drop_pct=-25; -60% triggers fail)")

