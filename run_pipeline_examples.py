"""
Pipeline integration use cases — Sectioned for Databricks (or run as one script).

Copy each section below into a separate Databricks cell, or run the whole file.
Demonstrates: compare_schema, compare_volume, detect_identical_or_stale, compare_snapshots.
All data is defined inline; no external files required.
"""

import pandas as pd
from data_quality import (
    compare_schema,
    compare_volume,
    detect_identical_or_stale,
    compare_snapshots,
    DataQualityChecker,
)


# =============================================================================
# SECTION 1: compare_schema — detect added, removed, or type-changed columns
# =============================================================================
# Use this to catch schema drift (e.g. new or dropped columns) before running rules.

# Step 1: Define baseline and current; current has "score" instead of "amount".
df_baseline = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"], "amount": [10, 20, 30]})
df_current = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"], "score": [10.0, 20.0, 30.0]})

# Step 2: Compare schema without dtypes — returns added/removed column names.
out = compare_schema(df_baseline, df_current, check_dtypes=False)
print("Schema diff (no dtypes):")
print("  added:", out["added"])
print("  removed:", out["removed"])
print("  type_changes:", out["type_changes"])

# Step 3: With check_dtypes=True, also report columns whose dtype changed (e.g. int to float).
out_with_dtypes = compare_schema(df_baseline, df_current, check_dtypes=True)
print("\nSchema diff (with dtypes):")
print("  added:", out_with_dtypes["added"])
print("  removed:", out_with_dtypes["removed"])
print("  type_changes:", out_with_dtypes["type_changes"])

# =============================================================================
# SECTION 2: compare_volume — row counts and "no new data" by date
# =============================================================================
# Use this to detect "data stopped coming in" (row drop or max date not moving).

# Step 1: Baseline has 3 rows; current has 5 rows with newer dates.
df_baseline = pd.DataFrame({
    "id": [1, 2, 3],
    "updated": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
})
df_current = pd.DataFrame({
    "id": [1, 2, 3, 4, 5],
    "updated": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"]),
})

# Step 2: Without date_column, you get row counts and pct_change only; no_new_data is None.
out = compare_volume(df_baseline, df_current, date_column=None)
print("Volume (no date column):")
print("  row_count_baseline:", out["row_count_baseline"])
print("  row_count_current:", out["row_count_current"])
print("  pct_change:", out["pct_change"])
print("  no_new_data:", out["no_new_data"])

# Step 3: With date_column, no_new_data is True only when max(current date) <= max(baseline date).
out_with_date = compare_volume(df_baseline, df_current, date_column="updated")
print("\nVolume (with date_column='updated'):")
print("  no_new_data:", out_with_date["no_new_data"])

# Step 4: When current has the same max date as baseline, no_new_data=True (possible stale feed).
df_stale = pd.DataFrame({
    "id": [1, 2, 3],
    "updated": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
})
out_stale = compare_volume(df_baseline, df_stale, date_column="updated")
print("\nVolume when current has same max date as baseline:")
print("  no_new_data:", out_stale["no_new_data"])

# =============================================================================
# SECTION 3: detect_identical_or_stale — same data = possible stale pipeline
# =============================================================================
# Use this to warn when baseline and current are identical (pipeline may not be refreshing).

# Step 1: Same row count and same key values -> identical=True, stale_warning=True.
df_b = pd.DataFrame({"id": [1, 2, 3], "x": [10, 20, 30]})
df_c = pd.DataFrame({"id": [1, 2, 3], "x": [10, 20, 30]})

out = detect_identical_or_stale(df_b, df_c, key_column="id")
print("Identical key set (same id's):")
print("  identical:", out["identical"])
print("  stale_warning:", out["stale_warning"])
print("  reason:", out["reason"])

# Step 2: Same row count but different key set (e.g. id=4 vs 3) -> no stale warning.
df_c_diff = pd.DataFrame({"id": [1, 2, 4], "x": [10, 20, 40]})
out_diff = detect_identical_or_stale(df_b, df_c_diff, key_column="id")
print("\nDifferent key set (current has id=4 instead of 3):")
print("  stale_warning:", out_diff["stale_warning"])

# Step 3: Same row count with no key column -> stale_warning=True (soft signal; can't compare keys).
df_same_rows_no_key = pd.DataFrame({"id": [1, 2, 3], "x": [10, 20, 30]})
out_no_key = detect_identical_or_stale(df_b, df_same_rows_no_key, key_column=None)
print("\nSame row count, no key column:")
print("  stale_warning:", out_no_key["stale_warning"])
print("  reason:", out_no_key["reason"])

# =============================================================================
# SECTION 4: compare_snapshots — full pipeline check (passing)
# =============================================================================
# One call runs schema, volume, stale, and quality checks; returns passed + warnings + details.

# Step 1: Define a rules_runner that runs the same expectations on both DataFrames.
def rules_runner(df, results):
    c = DataQualityChecker(df, dataset_name="")
    c.df = df
    c.results = results
    c.expect_column_values_to_not_be_null("id")
    c.expect_column_values_to_be_unique("id")
    c.expect_column_values_to_be_in_set("status", allowed_values=["active", "inactive"])

# Step 2: Baseline 3 rows, current 4 rows; same schema; quality will pass.
df_baseline = pd.DataFrame({
    "id": [1, 2, 3],
    "status": ["active", "inactive", "active"],
})
df_current = pd.DataFrame({
    "id": [1, 2, 3, 4],
    "status": ["active", "inactive", "active", "active"],
})

# Step 3: Call compare_snapshots with thresholds; result.passed=True and warnings empty.
result = compare_snapshots(
    df_baseline, df_current, rules_runner,
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

# =============================================================================
# SECTION 5: compare_snapshots — fail on schema change
# =============================================================================
# When columns are added or removed, fail_on_schema_change=True sets passed=False.

# Step 1: Minimal rules_runner (only "id") so we can compare DataFrames with different other columns.
def rules_runner_id_only(df, results):
    c = DataQualityChecker(df, dataset_name="")
    c.df = df
    c.results = results
    c.expect_column_values_to_not_be_null("id")

# Step 2: Baseline has old_col, current has new_col -> schema change.
df_baseline = pd.DataFrame({"id": [1, 2], "old_col": [10, 20]})
df_current = pd.DataFrame({"id": [1, 2], "new_col": [10, 20]})

# Step 3: compare_snapshots with fail_on_schema_change=True -> passed=False, warning added.
result = compare_snapshots(
    df_baseline, df_current, rules_runner_id_only,
    fail_on_schema_change=True,
    warn_on_stale=False,
)
print("compare_snapshots (fail_on_schema_change=True):")
print("  passed:", result["passed"])
print("  warnings:", result["warnings"])
print("  schema_changes:", result["schema_changes"])

# =============================================================================
# SECTION 6: compare_snapshots — fail on volume drop
# =============================================================================
# When current row count drops below a percentage threshold, passed=False.

# Step 1: Baseline 5 rows, current 2 rows -> 60% drop.
df_baseline = pd.DataFrame({"id": [1, 2, 3, 4, 5], "status": ["active"] * 5})
df_current = pd.DataFrame({"id": [1, 2], "status": ["active", "active"]})

# Step 2: fail_on_volume_drop_pct=-30 means "fail if pct_change < -30"; -60% triggers fail.
result = compare_snapshots(
    df_baseline, df_current, rules_runner_id_only,
    fail_on_volume_drop_pct=-30,
    warn_on_stale=False,
)
print("compare_snapshots (fail_on_volume_drop_pct=-30):")
print("  passed:", result["passed"])
print("  volume.pct_change:", result["volume"]["pct_change"])
print("  warnings:", result["warnings"])

# =============================================================================
# SECTION 7: compare_snapshots — fail on min_overall_health
# =============================================================================
# When current report's overall health score is below the minimum, passed=False.

# Step 1: Current has one null id -> not_null rule fails -> overall score < 100.
df_baseline = pd.DataFrame({"id": [1, 2, 3], "status": ["active", "active", "active"]})
df_current = pd.DataFrame({"id": [1, 2, None], "status": ["active", "inactive", "active"]})

# Step 2: min_overall_health=100 and current score ~66.7 -> passed=False, below_threshold.overall=True.
result = compare_snapshots(
    df_baseline, df_current, rules_runner_id_only,
    min_overall_health=100.0,
    warn_on_stale=False,
)
print("compare_snapshots (min_overall_health=100, current has null id):")
print("  passed:", result["passed"])
print("  below_threshold:", result["below_threshold"])
print("  warnings:", result["warnings"])

# =============================================================================
# SECTION 8: compare_snapshots — warn on stale (identical datasets)
# =============================================================================
# When baseline and current are identical, warn_on_stale adds a warning (passed stays True unless you use fail_on_stale).

# Step 1: Same data in both DataFrames (identical row count and key set).
df_baseline = pd.DataFrame({"id": [1, 2, 3], "status": ["active", "inactive", "active"]})
df_current = pd.DataFrame({"id": [1, 2, 3], "status": ["active", "inactive", "active"]})

# Step 2: warn_on_stale=True and stale_key_column="id" -> stale warning in result.warnings; passed still True.
result = compare_snapshots(
    df_baseline, df_current, rules_runner,
    warn_on_stale=True,
    stale_key_column="id",
)
print("compare_snapshots (identical data -> stale warning):")
print("  passed:", result["passed"])
print("  stale:", result["stale"])
print("  warnings:", result["warnings"])
