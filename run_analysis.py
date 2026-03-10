"""
Data Quality Use Cases — Sectioned for Databricks (or run as one script).

Copy each section below into a separate Databricks cell, or run the whole file.
All data is defined inline; no external CSV or file paths required.
"""

import pandas as pd
from data_quality import DataQualityChecker

# =============================================================================
# SECTION 1: Quick & easy — minimal validations
# =============================================================================
# Shows how little code is needed to run validations and see results.

df = pd.DataFrame({
    "id": [1, 2, 3, 4, 5],
    "name": ["Alice", "Bob", "Charlie", "Dave", "Eve"],
    "status": ["active", "inactive", "active", "active", "pending"],
    "score": [85, 92, 78, 95, 88],
})

checker = DataQualityChecker(df, dataset_name="Quick Example")
checker.expect_column_values_to_not_be_null("id")
checker.expect_column_values_to_be_unique("id")
checker.expect_column_values_to_be_in_set("status", allowed_values=["active", "inactive", "pending"])
checker.expect_column_values_to_be_in_range("score", 0, 100)

results = checker.get_results()
print("Data:")
print(df)
print("\nValidation results:")
print(results[["column", "rule", "success_rate", "dimension"]].to_string(index=False))

# =============================================================================
# SECTION 2: Single-column validations (deeper)
# =============================================================================
# Not null, unique, in set, numeric range, regex. Data and results visible.

df = pd.DataFrame({
    "order_id": ["ORD-001", "ORD-002", "ORD-003", "ORD-004", "ORD-005", "ORD-006"],
    "customer_id": ["C101", "C102", "C101", "C103", "C104", None],   # one null
    "region": ["North", "South", "East", "North", "West", "South"],   # all valid if we allow these
    "amount": [99.99, 150.00, 200.50, -10.0, 75.25, 88.00],          # one negative (invalid)
    "email": [
        "a@example.com", "b@test.org", "c@example.com",
        "invalid", "e@example.com", "f@test.org"                      # one invalid pattern
    ],
})

checker = DataQualityChecker(df, dataset_name="Orders")
checker.expect_column_values_to_not_be_null("order_id")
checker.expect_column_values_to_be_unique("order_id")
checker.expect_column_values_to_not_be_null("customer_id")
checker.expect_column_values_to_be_in_set("region", allowed_values=["North", "South", "East", "West"])
checker.expect_column_values_to_be_in_range("amount", 0, 1_000_000)
checker.expect_column_values_to_match_regex("email", pattern=r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")

results = checker.get_results()
print("Data:")
print(df)
print("\nValidation results:")
print(results[["column", "rule", "success_rate", "dimension", "details"]].to_string(index=False))

# =============================================================================
# SECTION 3: Multi-column validations
# =============================================================================
# Composite uniqueness and per-column allowed sets.

df = pd.DataFrame({
    "first_name": ["Alice", "Bob", "Alice", "Charlie"],
    "last_name": ["Smith", "Jones", "Smith", "Brown"],   # (Alice, Smith) duplicated
    "dept": ["Eng", "Eng", "Sales", "Eng"],
    "level": ["L1", "L2", "L1", "L2"],
})

checker = DataQualityChecker(df, dataset_name="Staff")
checker.expect_columns_values_to_be_unique(["first_name", "last_name"])
checker.expect_columns_values_to_be_in_sets(
    columns=["dept", "level"],
    allowed_values={"dept": ["Eng", "Sales", "HR"], "level": ["L1", "L2", "L3"]}
)

results = checker.get_results()
print("Data:")
print(df)
print("\nValidation results:")
print(results[["column", "rule", "success_rate", "dimension"]].to_string(index=False))

# =============================================================================
# SECTION 4: Dates — date range and recency
# =============================================================================
# Expect dates within a range and optionally "recent" (within last N days).

df = pd.DataFrame({
    "id": [1, 2, 3, 4, 5],
    "event_date": pd.to_datetime(["2024-03-01", "2024-06-15", "2024-09-20", "2023-01-01", "2025-12-31"]),
})
reference_date = pd.Timestamp("2024-06-01")  # "today" for recency

checker = DataQualityChecker(df, dataset_name="Events")
checker.expect_column_values_to_be_in_date_range("event_date", min_date="2024-01-01", max_date="2024-12-31")
checker.expect_column_values_to_be_recent("event_date", max_age_days=365, reference_date=reference_date)

results = checker.get_results()
print("Data:")
print(df)
print("\nValidation results:")
print(results[["column", "rule", "success_rate", "dimension"]].to_string(index=False))

# =============================================================================
# SECTION 5: Comprehensive report (health score, dimensions)
# =============================================================================
# Full summary with per-dimension scores and key metrics.

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

report = checker.get_comprehensive_results(title="Demo Quality Report")
if "error" in report:
    print(report["error"])
else:
    print("Key metrics:", report["key_metrics"])
    print("\nPer-dimension scores:", report.get("per_dimension_scores"))
    print("\nOverall health score:", report.get("overall_health_score"))

# =============================================================================
# SECTION 6: Auto-suggestions
# =============================================================================
# Let the library suggest validations from the data; review and optionally apply.

df = pd.DataFrame({
    "id": [1, 2, 3, 4, 5],
    "code": ["A", "B", "C", "D", "E"],
    "value": [10.0, 20.0, 30.0, 40.0, 50.0],
    "updated": pd.to_datetime(["2024-06-01", "2024-06-02", "2024-06-03", "2024-06-04", "2024-06-05"]),
})

checker = DataQualityChecker(df, dataset_name="Suggestions Demo")
suggestions = checker.generate_suggestions()

print("Suggested validations (from data shape and sample):")
for s in suggestions:
    print(f"  {s['column']}: {s['method']} — confidence {s['confidence']:.2f} — {s['reason']}")

# Optional: apply high-confidence suggestions and run them
high = [s for s in suggestions if s["confidence"] >= 0.8]
n = checker.apply_suggestions(high)
print(f"\nApplied {n} suggestion(s). Results:")
print(checker.get_results()[["column", "rule", "success_rate"]].to_string(index=False))
