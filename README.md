# Modularized Data Quality Checker

Run data quality expectations and similarity analysis on pandas DataFrames, then get or export results.

## Install

From the project root:

```bash
pip install -e .
```

If you don't have a `pyproject.toml` or `setup.py`, add the project root to `PYTHONPATH` or run scripts from the project root so `import data_quality` resolves.

## Quick Start

New to the library? Get a printable getting started guide:

```python
from data_quality import get_getting_started_guide
print(get_getting_started_guide())
```

This will print a comprehensive markdown guide with installation steps, basic examples, common use cases, and links to more documentation.

## Usage

```python
import pandas as pd
from data_quality import DataQualityChecker

df = pd.read_csv("your_data.csv")
checker = DataQualityChecker(df, dataset_name="My Dataset")

# Run expectations
checker.expect_column_values_to_not_be_null("id")
checker.expect_column_values_to_be_unique("id")
checker.expect_column_values_to_be_in_set("status", allowed_values=["active", "inactive"])

# Get results
results_df = checker.get_results()
report = checker.get_comprehensive_results(title="Weekly Quality Report")

# Save to CSV (pass a path that works in your environment)
checker.save_comprehensive_results_to_csv(
    title="Weekly Quality Report",
    csv_filename="data_quality_history.csv",
    include_field_summary=True,
)
```

## Auto-Generating Validations

Instead of manually defining validations, you can automatically generate suggestions based on your dataframe's characteristics:

```python
checker = DataQualityChecker(df, dataset_name="My Dataset")

# Generate suggestions for all columns
suggestions = checker.generate_suggestions()
for suggestion in suggestions:
    print(f"{suggestion['column']}: {suggestion['method']}")
    print(f"  Reason: {suggestion['reason']}")
    print(f"  Confidence: {suggestion['confidence']:.2f}")

# Apply suggestions automatically
checker.suggest_and_apply(auto_apply=True)

# Or apply selectively based on confidence
high_confidence = [s for s in suggestions if s['confidence'] > 0.8]
checker.apply_suggestions(high_confidence)
```

**Customizing Suggestions**

You can customize thresholds and behavior:

```python
options = {
    "null_rate_threshold": 0.05,  # Suggest not_null if <5% nulls
    "uniqueness_threshold": 0.95,  # Suggest unique if >95% unique
    "categorical_max_distinct": 20,  # Max distinct values for "in_set" suggestion
    "pattern_detection_enabled": True,  # Enable regex pattern detection
}

suggestions = checker.generate_suggestions(options=options)
```

**What Gets Suggested**

The auto-suggestion feature analyzes each column and suggests validations based on:

- **Completeness**: Low null rates → `expect_column_values_to_not_be_null`
- **Uniqueness**: High uniqueness → `expect_column_values_to_be_unique`
- **Categorical data**: Low cardinality → `expect_column_values_to_be_in_set` with observed values
- **Numeric ranges**: Min/max values → `expect_column_values_to_be_in_range`
- **Date ranges**: Min/max dates → `expect_column_values_to_be_in_date_range`
- **Patterns**: Detects common formats (email, UUID, IDs) → `expect_column_values_to_match_regex`
- **Timeliness**: Recent dates → `expect_column_values_to_be_recent`

## Demo

A structured demo runs through **all validations** and shows data quality dimensions and results. From the project root:

```bash
python run_demo.py
```

This runs three use cases:

1. **Use case 1: Single dataset with all validations** - Demonstrates every expectation type and similarity check on a single dataset. Includes intentional failures for demo effect. Report output is printed and saved to `data/demo_quality_report.csv` (and field details).

2. **Use case 2: Comparing two datasets** - Shows reconciliation on a key column and same-rules report comparison between two datasets. Demonstrates how to compare data quality across different data sources.

3. **Use case 3: Auto-generation of validation suggestions** - Demonstrates the auto-suggestion feature that analyzes a DataFrame and automatically generates validation suggestions based on data characteristics. Shows generating suggestions, filtering by confidence, and applying them.

### Demo Options

To list every **dimension** and **validation** with short definitions (no data run):

```bash
python run_demo.py --list-validations
```

To skip saving the CSV report in use case 1:

```bash
python run_demo.py --no-csv
```

To skip the auto-suggestion demo (use case 3):

```bash
python run_demo.py --skip-suggestions
```

Full reference: [demos/VALIDATIONS_AND_DIMENSIONS.md](demos/VALIDATIONS_AND_DIMENSIONS.md).

## Data quality dimensions

The checker uses six standard dimensions. Each rule you run is tagged with one dimension; the **overall health score** is the average of **per-dimension scores** for dimensions that have at least one rule in the current run.

| Dimension      | Meaning |
|----------------|--------|
| **Accuracy**   | Data correctly represents the real-world object or event. |
| **Completeness** | All required data is present. |
| **Consistency** | Data is uniform across systems and datasets (e.g. column similarity). |
| **Timeliness** | Data is available when needed and up to date. |
| **Validity**   | Data conforms to defined formats, rules, or constraints. |
| **Uniqueness** | No unintended duplicates exist. |

You can restrict which dimensions contribute to the score by passing **dimensions_filter** to `get_comprehensive_results` or `save_comprehensive_results_to_csv`. Valid names: `from data_quality import DIMENSIONS`.

```python
from data_quality import DataQualityChecker, DIMENSIONS

checker = DataQualityChecker(df, dataset_name="My Dataset")
checker.expect_column_values_to_not_be_null("id")
checker.expect_column_values_to_be_unique("id")

# Score from all dimensions present in results
report = checker.get_comprehensive_results()

# Score only from selected dimensions
report = checker.get_comprehensive_results(
    dimensions_filter=["Completeness", "Validity"],
)
```

## Validations (expectations)

Available checks, grouped by dimension. Single-column methods take a column name; multi-column methods take a list of columns. For a full list with definitions, see [demos/VALIDATIONS_AND_DIMENSIONS.md](demos/VALIDATIONS_AND_DIMENSIONS.md).

> **Tip**: You can auto-generate validations based on your dataframe's characteristics. See the [Auto-Generating Validations](#auto-generating-validations) section above.

- **Completeness**  
  - `expect_column_values_to_not_be_null(column)` — no nulls in column  
  - `expect_columns_values_to_not_be_null(columns)` — no nulls in any of the columns (per row)

- **Uniqueness**  
  - `expect_column_values_to_be_unique(column)` — all values unique  
  - `expect_columns_values_to_be_unique(columns)` — combination of columns is unique

- **Validity**  
  - `expect_column_values_to_be_in_set(column, allowed_values)` — values in allowed set  
  - `expect_column_values_to_match_regex(column, pattern)` — values match regex  
  - `expect_column_values_to_be_in_range(column, min_val, max_val)` — numeric range  
  - `expect_column_values_to_be_in_date_range(column, min_date, max_date)` — date range  
  - `expect_columns_values_to_be_in_sets(columns, allowed_values)` — per-column allowed sets  
  - `expect_columns_values_to_match_patterns(columns, patterns_dict)` — per-column regex  
  - `expect_columns_values_to_be_in_ranges(columns, ranges_dict)` — per-column numeric ranges

- **Timeliness**  
  - `expect_column_values_to_be_recent(column, max_age_days, reference_date=None)` — dates within last N days

- **Accuracy**  
  - `expect_column_values_to_match_reference(column, reference_series_or_set)` — values match a reference set or Series

- **Consistency**  
  - `analyze_column_similarity_levenshtein(column1, column2, similarity_threshold=0.8)` — similarity between two columns (appends a Consistency result)

### Running Expectations from JSON Config

You can run expectations from a JSON/dictionary config using `run_rules_from_json(rules)`. The JSON structure maps expectation method names to lists of parameter dictionaries:

```python
# Manual JSON config
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
    ],
    "expect_column_values_to_match_regex": [
        {"column": "email", "pattern": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"}
    ],
    "expect_column_values_to_be_in_date_range": [
        {"column": "created_at", "min_date": "2024-01-01", "max_date": "2024-12-31"}
    ],
    "expect_column_values_to_be_recent": [
        {"column": "updated_at", "max_age_days": 30, "reference_date": "2024-06-01"}
    ]
}

checker = DataQualityChecker(df)
checker.run_rules_from_json(rules)
```

**Auto-generating JSON from suggestions:**

You can also convert auto-generated suggestions directly to JSON format:

```python
from data_quality import DataQualityChecker
from data_quality.suggestion import suggestions_to_json

checker = DataQualityChecker(df)

# Generate suggestions
suggestions = checker.generate_suggestions()

# Convert to JSON format
json_rules = suggestions_to_json(suggestions)

# Run the JSON rules
checker.run_rules_from_json(json_rules)
```

**JSON Structure:**
- Top-level keys are expectation method names (e.g., `"expect_column_values_to_not_be_null"`)
- Each key maps to a list of parameter dictionaries
- Each parameter dict must include `"column"` and any other required parameters for that expectation type
- Multiple columns can use the same expectation type by adding multiple dicts to the list

## Comparing two datasets

Join two DataFrames on a key field (or list of key fields) and assess **join quality** (key match rates, only-in-left/right counts) and **reconciliation** (exact match rate and optional similarity for related columns). You can also run the same expectations on both datasets and compare the two reports.

**Reconciliation on a key**

```python
import pandas as pd
from data_quality import reconcile_on_key, DatasetComparator, DataQualityChecker

df_left = pd.read_csv("warehouse_orders.csv")
df_right = pd.read_csv("source_orders.csv")
results = []
summary = reconcile_on_key(
    df_left, df_right,
    key_column="order_id",
    results=results,
    right_name="source",
    include_similarity=True,
    similarity_threshold=0.9,
)
# results now contains join quality + per-column match and similarity; use with get_comprehensive_results
checker = DataQualityChecker(df_left, dataset_name="Warehouse")
checker.results = results
report = checker.get_comprehensive_results()
```

Or use the **DatasetComparator** facade:

```python
from data_quality import DatasetComparator

comp = DatasetComparator(df_warehouse, df_source, key_column="id", name_a="WH", name_b="Source")
summary = comp.reconcile(include_similarity=True)
# comp.results holds all reconciliation results
```

**Run same rules on both and compare reports**

```python
from data_quality import run_same_rules_on_two_datasets, compare_two_reports

def my_rules(df, results):
    c = DataQualityChecker(df, dataset_name="")
    c.df, c.results = df, results
    c.expect_column_values_to_not_be_null("id")
    c.expect_column_values_to_be_unique("id")

report_a, report_b = run_same_rules_on_two_datasets(
    df_a, df_b, my_rules, dataset_name_a="A", dataset_name_b="B"
)
diff = compare_two_reports(report_a, report_b)
# diff has overall_health_score_a/b, delta, per_dimension_diffs, per_rule_diffs
```

## Test suite

Tests live in `tests/` and use **pytest**. Coverage runs from unit tests (individual functions) through integration (several modules together) to end-to-end (full checker flow including CSV output).

**Install and run**

```bash
pip install -e ".[dev]"
```

Then run the full suite (verbose):

```bash
pytest tests/ -v
```

If `pytest` is not on your PATH (e.g. on some Windows setups), use:

```bash
python -m pytest tests/ -v
```

**What to run**

- **Full suite:** `pytest tests/ -v` (or `python -m pytest tests/ -v`)
- **Exclude end-to-end** (faster, unit + integration only): `pytest tests/ -v -m "not e2e"`
- **Single file:** `pytest tests/test_expectations.py -v`

**What’s covered**

- **test_utils.py** — Unit tests for every utils function (normalize_columns, Levenshtein, classify_data_type, quality_scores, is_critical_data_element).
- **test_expectations.py** — Unit tests for each expectation (pass/fail, single and multi-column).
- **test_similarity.py** — Unit tests for similarity analysis and summary/detailed helpers.
- **test_reporting.py** — Unit tests for `get_comprehensive_results` and flatten; integration tests for `save_*` to CSV (using a temp path).
- **test_checker.py** — Unit tests for checker state and single methods; integration tests for multi-step flows without disk; includes tests for suggestion generation and application methods.
- **test_suggestion.py** — Unit tests for suggestion generation logic, pattern detection, and analysis functions.
- **test_e2e.py** — End-to-end: create checker, run expectations and similarity, get report, save CSVs; asserts on files and report (marked `@pytest.mark.e2e`); includes end-to-end test for suggestion workflow.
- **test_comparison.py** — Unit and integration tests for cross-dataset comparison: `reconcile_on_key` (join quality, match rates, similarity), `compare_two_reports`, `run_same_rules_on_two_datasets`, `DatasetComparator`.

## Layout and docs

- **Package:** `data_quality/` — `checker.py`, `utils.py`, `expectations.py`, `similarity.py`, `reporting.py`, `comparison.py`, `suggestion.py`
- **Architecture:** [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — module roles and data flow
- **Leadership / shareable overview:** [docs/overview.html](docs/overview.html) — same content as this README in HTML (open in a browser; use File → Print → Save as PDF if needed)

## Modularization (changelog)

The original single-file `data_quality_checker.py` was split into a package while keeping the functionality:

- **utils.py** — `normalize_columns`, Levenshtein helpers, `classify_data_type`, `calculate_quality_scores`, `is_critical_data_element`
- **expectations.py** — All `expect_column_*` and `expect_columns_*` logic as functions `(df, results, ...)`
- **similarity.py** — Levenshtein analysis and `get_similarity_summary_table` / `get_detailed_similarity_comparisons`
- **reporting.py** — `get_comprehensive_results`, `save_comprehensive_results_to_csv`, `save_field_summary_to_csv`, `flatten_comprehensive_results`
- **checker.py** — `DataQualityChecker` holds state and delegates to the above; includes `run_rules_from_json` and suggestion methods (`generate_suggestions`, `apply_suggestions`, `suggest_and_apply`)
- **suggestion.py** — Auto-generation of validation suggestions based on dataframe analysis (`analyze_column_for_suggestions`, `generate_suggestions`, `suggestions_to_json`)
- **comparison.py** — Cross-dataset: `reconcile_on_key`, `get_reconciliation_diffs`, `run_same_rules_on_two_datasets`, `compare_two_reports`, `DatasetComparator`
