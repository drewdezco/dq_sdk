"""
Reference definitions for data quality dimensions and validations.
Used by the demo (--list-validations) and kept in sync with VALIDATIONS_AND_DIMENSIONS.md.
"""

DIMENSION_DEFINITIONS = {
    "Accuracy": "Data correctly represents the real-world object or event.",
    "Completeness": "All required data is present.",
    "Consistency": "Data is uniform across systems and datasets (e.g. column similarity).",
    "Timeliness": "Data is available when needed and up to date.",
    "Validity": "Data conforms to defined formats, rules, or constraints.",
    "Uniqueness": "No unintended duplicates exist.",
}

VALIDATION_DEFINITIONS = [
    # Completeness
    {"dimension": "Completeness", "method": "expect_column_values_to_not_be_null", "definition": "No nulls in column."},
    {"dimension": "Completeness", "method": "expect_columns_values_to_not_be_null", "definition": "No nulls in any of the columns (per row)."},
    # Uniqueness
    {"dimension": "Uniqueness", "method": "expect_column_values_to_be_unique", "definition": "All values in column are unique."},
    {"dimension": "Uniqueness", "method": "expect_columns_values_to_be_unique", "definition": "Combination of columns is unique."},
    # Validity
    {"dimension": "Validity", "method": "expect_column_values_to_be_in_set", "definition": "Values are in an allowed set."},
    {"dimension": "Validity", "method": "expect_column_values_to_match_regex", "definition": "Values match a regex pattern."},
    {"dimension": "Validity", "method": "expect_column_values_to_be_in_range", "definition": "Numeric values are within min/max range."},
    {"dimension": "Validity", "method": "expect_column_values_to_be_in_date_range", "definition": "Date values are within min/max date range."},
    {"dimension": "Validity", "method": "expect_columns_values_to_be_in_sets", "definition": "Per-column values are in their allowed sets."},
    {"dimension": "Validity", "method": "expect_columns_values_to_match_patterns", "definition": "Per-column values match their regex patterns."},
    {"dimension": "Validity", "method": "expect_columns_values_to_be_in_ranges", "definition": "Per-column numeric values are in their ranges."},
    # Timeliness
    {"dimension": "Timeliness", "method": "expect_column_values_to_be_recent", "definition": "Date values are within the last N days."},
    # Accuracy
    {"dimension": "Accuracy", "method": "expect_column_values_to_match_reference", "definition": "Values match a reference set or Series."},
    # Consistency
    {"dimension": "Consistency", "method": "analyze_column_similarity_levenshtein", "definition": "Levenshtein similarity between two columns."},
    {"dimension": "Consistency", "method": "reconcile_on_key", "definition": "Join quality and match rates (and optional similarity) when comparing two datasets on a key."},
]


def print_validations_reference():
    """Print dimensions and validations with definitions to stdout."""
    print("=" * 60)
    print("Data quality dimensions (definitions)")
    print("=" * 60)
    for dim, definition in DIMENSION_DEFINITIONS.items():
        print(f"  {dim}: {definition}")
    print()
    print("Validations by dimension")
    print("-" * 60)
    current_dim = None
    for v in VALIDATION_DEFINITIONS:
        if v["dimension"] != current_dim:
            current_dim = v["dimension"]
            print(f"\n  {current_dim}")
        print(f"    {v['method']}")
        print(f"      -> {v['definition']}")
    print()
