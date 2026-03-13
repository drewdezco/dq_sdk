# Validations and dimensions reference

This document lists every data quality **dimension** and **validation** (expectation) used by the checker, with short definitions.

## Dimensions

Each rule is tagged with one dimension. The **overall health score** is the average of **per-dimension scores** for dimensions that have at least one rule in the current run.

| Dimension | Definition |
|-----------|------------|
| **Accuracy** | Data correctly represents the real-world object or event. |
| **Completeness** | All required data is present. |
| **Consistency** | Data is uniform across systems and datasets (e.g. column similarity). |
| **Timeliness** | Data is available when needed and up to date. |
| **Validity** | Data conforms to defined formats, rules, or constraints. |
| **Uniqueness** | No unintended duplicates exist. |

## Validations (expectations)

Available checks, grouped by dimension. Single-column methods take a column name; multi-column methods take a list of columns.

### Completeness

| Method | Definition |
|--------|------------|
| `expect_column_values_to_not_be_null(column)` | No nulls in column. |
| `expect_columns_values_to_not_be_null(columns)` | No nulls in any of the columns (per row). |

### Uniqueness

| Method | Definition |
|--------|------------|
| `expect_column_values_to_be_unique(column)` | All values in column are unique. |
| `expect_columns_values_to_be_unique(columns)` | Combination of columns is unique. |

### Validity

| Method | Definition |
|--------|------------|
| `expect_column_values_to_be_in_set(column, allowed_values)` | Values are in an allowed set. |
| `expect_column_values_to_match_regex(column, pattern)` | Values match a regex pattern. |
| `expect_column_values_to_be_in_range(column, min_val, max_val)` | Numeric values are within min/max range. |
| `expect_column_values_to_be_in_date_range(column, min_date, max_date)` | Date values are within min/max date range. |
| `expect_columns_values_to_be_in_sets(columns, allowed_values)` | Per-column values are in their allowed sets. |
| `expect_columns_values_to_match_patterns(columns, patterns_dict)` | Per-column values match their regex patterns. |
| `expect_columns_values_to_be_in_ranges(columns, ranges_dict)` | Per-column numeric values are in their ranges. |

### Timeliness

| Method | Definition |
|--------|------------|
| `expect_column_values_to_be_recent(column, max_age_days, reference_date=None)` | Date values are within the last N days. |

### Accuracy

| Method | Definition |
|--------|------------|
| `expect_column_values_to_match_reference(column, reference_series_or_set)` | Values match a reference set or Series. |

### Consistency

| Method | Definition |
|--------|------------|
| `analyze_column_similarity_levenshtein(column1, column2, similarity_threshold=0.8)` | Levenshtein similarity between two columns (appends a Consistency result). |
| `reconcile_on_key(df_left, df_right, key_column, ...)` | Join quality and match rates (and optional similarity) when comparing two datasets on a key. |

## Running Expectations from JSON Config

You can run expectations from a JSON/dictionary config using `run_rules_from_json(rules)`. The JSON structure maps expectation method names to lists of parameter dictionaries:

```python
rules = {
    "expect_column_values_to_not_be_null": [
        {"column": "id"}
    ],
    "expect_column_values_to_be_unique": [
        {"column": "id"}
    ],
    "expect_column_values_to_be_in_set": [
        {"column": "status", "allowed_values": ["active", "inactive", "pending"]}
    ],
    "expect_column_values_to_be_in_range": [
        {"column": "score", "min_val": 0, "max_val": 100}
    ]
}

checker = DataQualityChecker(df)
checker.run_rules_from_json(rules)
```

**JSON Structure:**
- Top-level keys are expectation method names (e.g., `"expect_column_values_to_not_be_null"`)
- Each key maps to a list of parameter dictionaries
- Each parameter dict must include `"column"` and any other required parameters for that expectation type
- Multiple columns can use the same expectation type by adding multiple dicts to the list

**Auto-generating JSON from suggestions:**

```python
from data_quality.suggestion import suggestions_to_json

suggestions = checker.generate_suggestions()
json_rules = suggestions_to_json(suggestions)
checker.run_rules_from_json(json_rules)
```