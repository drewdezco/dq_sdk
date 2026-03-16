# Functionality and use

This document describes functionality and how to use the library in detail. For a quick overview and install, see [README.md](README.md).

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

Available checks, grouped by dimension. Single-column methods take a column name; multi-column methods take a list of columns. For a full list with definitions, see `docs/VALIDATIONS_AND_DIMENSIONS.md`.

> **Tip**: You can auto-generate validations based on your dataframe's characteristics. See the [Auto-Generating Validations](#auto-generating-validations) section below.

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

### Running expectations from JSON config

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

**JSON structure:**
- Top-level keys are expectation method names (e.g., `"expect_column_values_to_not_be_null"`)
- Each key maps to a list of parameter dictionaries
- Each parameter dict must include `"column"` and any other required parameters for that expectation type
- Multiple columns can use the same expectation type by adding multiple dicts to the list

## Auto-generating validations

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

**Customizing suggestions**

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

**What gets suggested**

The auto-suggestion feature analyzes each column and suggests validations based on:

- **Completeness**: Low null rates → `expect_column_values_to_not_be_null`
- **Uniqueness**: High uniqueness → `expect_column_values_to_be_unique`
- **Categorical data**: Low cardinality → `expect_column_values_to_be_in_set` with observed values
- **Numeric ranges**: Min/max values → `expect_column_values_to_be_in_range`
- **Date ranges**: Min/max dates → `expect_column_values_to_be_in_date_range`
- **Patterns**: Detects common formats (email, UUID, IDs) → `expect_column_values_to_match_regex`
- **Timeliness**: Recent dates → `expect_column_values_to_be_recent`

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

**Auto-detect the key (quick exploration)**

If you don't know the key column up front, you can ask the library to infer a reasonable key candidate from the shared columns, then run reconciliation:

```python
from data_quality import reconcile_with_auto_key

results = []
summary = reconcile_with_auto_key(df_left, df_right, results, right_name="source")
print("Auto-picked key:", summary["auto_key_column"])
print(summary["join_quality"])
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

**Compare health on shared columns only**

If the two datasets have different schemas, you can still compare “like-for-like” by running
rules only on the intersection of columns:

```python
shared_cols = sorted(set(df_a.columns) & set(df_b.columns))

def shared_rules(df, results):
    c = DataQualityChecker(df, dataset_name="")
    c.df, c.results = df, results

    if "id" in shared_cols:
        c.expect_column_values_to_not_be_null("id")
        c.expect_column_values_to_be_unique("id")

    if "status" in shared_cols:
        c.expect_column_values_to_not_be_null("status")

report_a, report_b = run_same_rules_on_two_datasets(df_a, df_b, shared_rules)
diff = compare_two_reports(report_a, report_b)
```

This produces a report comparison where each `(column, rule)` exists on both sides, making the
diff easier to interpret even when the overall schemas differ.

## Pipeline integration

Compare a current dataset to a baseline (for example, the same table one week earlier) and evaluate **schema**, **volume**, **staleness**, and **health thresholds** in one call. The pipeline helpers are thin, testable functions; your ingestion or orchestration layer decides whether to **alert**, **stop**, or **proceed** based on the returned result dict.

This section is the source for `get_pipeline_markdown()` / `get_pipeline_html()` in `data_quality.docs_utils`.

### Core helpers

- **`compare_schema(df_baseline, df_current, check_dtypes=False)`**  
  Return added / removed columns and, optionally, dtype changes between two snapshots.

- **`compare_volume(df_baseline, df_current, date_column=None)`**  
  Compare row counts and compute `pct_change`. When `date_column` is provided, also compute a `no_new_data` flag based on the max timestamp in each snapshot.

- **`detect_identical_or_stale(df_baseline, df_current, key_column=None, tolerance_pct=0.0)`**  
  Detect identical or nearly-identical datasets (by row count and key set) and return `{"identical": bool, "stale_warning": bool, "reason": str | None}`.

- **`load_dataframe(source, **read_csv_kwargs)`**  
  Normalize a snapshot source to a `pandas.DataFrame`. If `source` is already a DataFrame, it is returned unchanged; if it is a path/str, `pd.read_csv` is called.

- **`compare_snapshots(...)`**  
  High-level baseline vs current comparison that:
  - runs `compare_schema`, `compare_volume`, and optional `detect_identical_or_stale`
  - runs a user-supplied `rules_runner(df, results)` on both snapshots
  - enforces thresholds (`min_overall_health`, per-dimension minimums, `fail_on_schema_change`, `fail_on_volume_drop_pct`)
  - returns a single dict containing all of the above.

- **`compare_snapshots_multi(snapshots, rules_runner, mode="consecutive"|"baseline", ...)`**  
  Run `compare_snapshots` across multiple snapshots in order:
  - `"consecutive"`: compare `(s0, s1)`, `(s1, s2)`, ...
  - `"baseline"`: compare `(s0, s1)`, `(s0, s2)`, ...

- **`DEFAULT_THRESHOLDS` and `use_default_thresholds`**  
  A small dict that holds default values (such as `min_overall_health` and `fail_on_volume_drop_pct`). When calling `compare_snapshots(..., use_default_thresholds=True, ...)`, any of these values that you **do not** pass explicitly are filled from `DEFAULT_THRESHOLDS`.

### Typical pipeline call

```python
from data_quality import compare_snapshots, DataQualityChecker

def my_rules(df, results):
    c = DataQualityChecker(df, dataset_name="")
    c.df, c.results = df, results
    c.expect_column_values_to_not_be_null("id")
    c.expect_column_values_to_be_unique("id")

# Load baseline and current (e.g. from tables, CSV files, or DataFrames)
result = compare_snapshots(
    df_baseline,
    df_current,
    my_rules,
    min_overall_health=80,
    fail_on_volume_drop_pct=-25,
    date_column="updated_at",
    warn_on_stale=True,
    stale_key_column="id",
)

if not result["passed"]:
    # Alert or stop ingestion
    print(result["warnings"])
```

The same pattern is used throughout `examples.py` (Sections 7–9) and in `tests/test_pipeline.py`.

### Scenarios and examples (see `examples.py`)

The pipeline helpers are demonstrated in detail in `examples.py`:

- **Schema change detection (Section 7 — Pipeline helpers)**  
  - **What:** Use `compare_schema` to list added/removed columns and optional dtype changes.  
  - **Why:** Catch breaking schema changes (for example, dropped key columns or renamed metrics) before they hit downstream consumers.  
  - **Benefit/impact:** Allows you to fail fast or raise a targeted alert when `fail_on_schema_change=True`.  
  - **Expected result:** `schema_changes["added"]` / `["removed"]` are non-empty; when `check_dtypes=True`, `schema_changes["type_changes"]` holds a list of changed columns.

- **Volume change and no-new-data detection (Section 7)**  
  - **What:** Use `compare_volume` to compute row-count deltas and the `no_new_data` flag when a date column is provided.  
  - **Why:** Detect cases where volume drops sharply or no new rows arrive even though a job ran.  
  - **Benefit/impact:** Lets you enforce `fail_on_volume_drop_pct` and/or alert on `no_new_data=True`.  
  - **Expected result:**  
    - `volume["pct_change"]` shows the percent change in row count.  
    - `volume["no_new_data"]` is `True` when `max(current[date]) <= max(baseline[date])`.

- **Stale data detection on key sets (Section 7)**  
  - **What:** Use `detect_identical_or_stale` (directly or via `compare_snapshots` with `warn_on_stale=True`) to check whether the current dataset is identical or nearly identical to the baseline.  
  - **Why:** Spot situations where data is accidentally reloaded or a job re-emits the same slice over and over.  
  - **Expected result:**  
    - `stale_warning=True` and a human-readable `reason` when row counts are the same and key sets overlap above the configured tolerance.

- **Single snapshot comparison: baseline vs current (Section 8 — `compare_snapshots`)**  
  - **What:** Run the same rules on baseline and current, then aggregate the results and apply thresholds.  
  - **Why:** Detect regression in overall health (or per-dimension scores) and combine this with schema/volume/stale checks.  
  - **Benefit/impact:** A single `result` dict drives go/no-go decisions.  
  - **Expected result:**  
    - `result["passed"]` is `False` when any of the following hold:  
      - schema change and `fail_on_schema_change=True`  
      - volume drop below `fail_on_volume_drop_pct`  
      - overall health below `min_overall_health`  
      - any dimension below its per-dimension minimum in `min_per_dimension`.  
    - `result["warnings"]` contains human-readable messages for schema changes, volume drops, and stale datasets.

- **Multi-snapshot comparison (Section 9 — `compare_snapshots_multi`)**  
  - **What:** Compare multiple snapshots either consecutively (`mode="consecutive"`) or all against the first (`mode="baseline"`).  
  - **Why:** Track how health and volume evolve over time, or validate a long migration where each step must not regress.  
  - **Expected result:**  
    - Returned dict has `"results"` (a list of `compare_snapshots` outputs, each annotated with `baseline_index`, `current_index`, `baseline_label`, `current_label`) and a top-level `"passed"` that is `True` only if all pairs passed.

- **CSV path support and `load_dataframe` (Section 9)**  
  - **What:** Pass file paths directly into `compare_snapshots` or `compare_snapshots_multi`; internally, `load_dataframe` converts them to DataFrames.  
  - **Why:** Make it easy to plug in CSV exports or files staged by your orchestration tool.  
  - **Expected result:**  
    - A call such as `compare_snapshots("baseline.csv", "current.csv", rules_runner, ...)` behaves the same as if you had passed DataFrames, with `load_dataframe` handling `pd.read_csv`.

- **Default thresholds (Sections 9–10)**  
  - **What:** Use `use_default_thresholds=True` so that `compare_snapshots` fills in `min_overall_health` and `fail_on_volume_drop_pct` from `DEFAULT_THRESHOLDS` when you do not pass explicit values.  
  - **Why:** Provide safe defaults without hardcoding magic numbers throughout the codebase.  
  - **Expected result:**  
    - In `examples.py`, a large volume drop triggers a failure even when `fail_on_volume_drop_pct` is omitted, because the default is applied.

At the end of this section in `examples.py`, you can see all of these scenarios in action; the examples are designed to be copy-paste friendly for Databricks and other notebook environments.

### Interpreting the pipeline result dict

`compare_snapshots` returns a nested dict. The most important keys are:

- **`passed: bool`** — overall go/no-go flag.
- **`warnings: list[str]`** — human-readable messages explaining why a check failed or what to investigate.
- **`schema_changes`** — output of `compare_schema` (added/removed columns and optional `type_changes` list).
- **`volume`** — output of `compare_volume` (`row_count_baseline`, `row_count_current`, `pct_change`, `no_new_data`).
- **`stale`** — output of `detect_identical_or_stale` when `warn_on_stale=True` (may be empty otherwise).
- **`comparison`** — result of `compare_two_reports`, summarizing baseline vs current health and per-dimension/per-rule diffs.
- **`below_threshold`** — dict with:  
  - `overall: bool` — whether overall health is below `min_overall_health`.  
  - `dimensions: list[str]` — names of dimensions whose scores fell below their respective minimums.

Most pipelines only need to branch on `passed` and log or surface `warnings`, but all of the nested structures are available if you want to build dashboards or more detailed alerts.

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

**What's covered**

- **test_utils.py** — Unit tests for every utils function (normalize_columns, Levenshtein, classify_data_type, quality_scores, is_critical_data_element).
- **test_expectations.py** — Unit tests for each expectation (pass/fail, single and multi-column).
- **test_similarity.py** — Unit tests for similarity analysis and summary/detailed helpers.
- **test_reporting.py** — Unit tests for `get_comprehensive_results` and flatten; integration tests for `save_*` to CSV (using a temp path).
- **test_checker.py** — Unit tests for checker state and single methods; integration tests for multi-step flows without disk; includes tests for suggestion generation and application methods.
- **test_suggestion.py** — Unit tests for suggestion generation logic, pattern detection, and analysis functions.
- **test_e2e.py** — End-to-end: create checker, run expectations and similarity, get report, save CSVs; asserts on files and report (marked `@pytest.mark.e2e`); includes end-to-end test for suggestion workflow.
- **test_comparison.py** — Unit and integration tests for cross-dataset comparison: `reconcile_on_key` (join quality, match rates, similarity), `compare_two_reports`, `run_same_rules_on_two_datasets`, `DatasetComparator`.
- **test_pipeline.py** — Unit and integration tests for pipeline: `compare_schema`, `compare_volume`, `detect_identical_or_stale`, `compare_snapshots`, `load_dataframe`, `compare_snapshots_multi`, default thresholds.

## Modularization (changelog)

The original single-file `data_quality_checker.py` was split into a package while keeping the same public API:

- **utils.py** — `normalize_columns`, Levenshtein helpers, `classify_data_type`, `calculate_quality_scores`, `is_critical_data_element`
- **expectations.py** — All `expect_column_*` and `expect_columns_*` logic as functions `(df, results, ...)`
- **similarity.py** — Levenshtein analysis and `get_similarity_summary_table` / `get_detailed_similarity_comparisons`
- **reporting.py** — `get_comprehensive_results`, `save_comprehensive_results_to_csv`, `save_field_summary_to_csv`, `flatten_comprehensive_results`
- **checker.py** — `DataQualityChecker` holds state and delegates to the above; includes `run_rules_from_json` and suggestion methods (`generate_suggestions`, `apply_suggestions`, `suggest_and_apply`)
- **suggestion.py** — Auto-generation of validation suggestions based on dataframe analysis (`analyze_column_for_suggestions`, `generate_suggestions`, `suggestions_to_json`)
- **comparison.py** — Cross-dataset: `reconcile_on_key`, `get_reconciliation_diffs`, `run_same_rules_on_two_datasets`, `compare_two_reports`, `DatasetComparator`
- **pipeline.py** — Baseline vs current comparison: `compare_schema`, `compare_volume`, `detect_identical_or_stale`, `compare_snapshots`, `load_dataframe`, `compare_snapshots_multi`, `DEFAULT_THRESHOLDS`
