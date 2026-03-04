# Modularized Data Quality Checker

Run data quality expectations and similarity analysis on pandas DataFrames, then get or export results. Usable on your PC and in Databricks with the same code.

## Install

From the project root:

```bash
pip install -e .
```

If you don't have a `pyproject.toml` or `setup.py`, add the project root to `PYTHONPATH` or run scripts from the project root so `import data_quality` resolves.

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

## Demo

A structured demo runs through **all validations** and shows data quality dimensions and results. From the project root:

```bash
python run_demo.py
```

This runs two use cases: (1) a single dataset with every expectation and similarity check, and (2) comparison of two datasets (reconciliation on a key and same-rules report comparison). Report output is printed and, for use case 1, saved to `data/demo_quality_report.csv` (and field details).

To list every **dimension** and **validation** with short definitions (no data run):

```bash
python run_demo.py --list-validations
```

Full reference: [demos/VALIDATIONS_AND_DIMENSIONS.md](demos/VALIDATIONS_AND_DIMENSIONS.md).

## Using on Databricks

Use the same import. Install the package on the cluster (e.g. from a repo or wheel) or attach the project. For CSV output, pass a path writable from the cluster, for example:

- `/dbfs/FileStore/your_folder/data_quality_history.csv`
- A path on a mounted volume

Example:

```python
from data_quality import DataQualityChecker
# ... build checker and run expectations ...
checker.save_comprehensive_results_to_csv(
    csv_filename="/dbfs/FileStore/data_quality_history.csv",
)
```

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

You can also run expectations from a JSON config with `run_rules_from_json(rules)`; see the checker API for the expected structure.

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

## Backward compatibility

If you previously used:

```python
from data_quality_checker import DataQualityChecker
```

that still works: the root `data_quality_checker.py` re-exports `DataQualityChecker` from the `data_quality` package.

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
- **test_checker.py** — Unit tests for checker state and single methods; integration tests for multi-step flows without disk.
- **test_e2e.py** — End-to-end: create checker, run expectations and similarity, get report, save CSVs; asserts on files and report (marked `@pytest.mark.e2e`).
- **test_comparison.py** — Unit and integration tests for cross-dataset comparison: `reconcile_on_key` (join quality, match rates, similarity), `compare_two_reports`, `run_same_rules_on_two_datasets`, `DatasetComparator`.

## Layout and docs

- **Package:** `data_quality/` — `checker.py`, `utils.py`, `expectations.py`, `similarity.py`, `reporting.py`, `comparison.py`
- **Architecture:** [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — module roles and data flow
- **Leadership / shareable overview:** [docs/overview.html](docs/overview.html) — same content as this README in HTML (open in a browser; use File → Print → Save as PDF if needed)

## Modularization (changelog)

The original single-file `data_quality_checker.py` was split into a package while keeping the same public API:

- **utils.py** — `normalize_columns`, Levenshtein helpers, `classify_data_type`, `calculate_quality_scores`, `is_critical_data_element`
- **expectations.py** — All `expect_column_*` and `expect_columns_*` logic as functions `(df, results, ...)`
- **similarity.py** — Levenshtein analysis and `get_similarity_summary_table` / `get_detailed_similarity_comparisons`
- **reporting.py** — `get_comprehensive_results`, `save_comprehensive_results_to_csv`, `save_field_summary_to_csv`, `flatten_comprehensive_results`
- **checker.py** — `DataQualityChecker` holds state and delegates to the above; includes `run_rules_from_json`
- **comparison.py** — Cross-dataset: `reconcile_on_key`, `get_reconciliation_diffs`, `run_same_rules_on_two_datasets`, `compare_two_reports`, `DatasetComparator`

The root `data_quality_checker.py` is a thin re-export for backward compatibility.
