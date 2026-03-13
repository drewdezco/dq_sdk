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

## What you can do

- **Run expectations** (not_null, unique, in_set, regex, ranges, etc.) and get results or comprehensive reports.
- **Auto-generate** validation suggestions from your data and apply them.
- **Compare two datasets** (reconcile on key, run same rules on both, compare reports).
- **Pipeline integration**: compare baseline vs current (schema, volume, stale, quality thresholds); optional CSV paths and multi-snapshot.
- Use **six dimensions** (Accuracy, Completeness, Consistency, Timeliness, Validity, Uniqueness) for scoring.

Full validations list, JSON config, comparison and pipeline examples: [USAGE.md](USAGE.md).

## Demo

Open and run `examples.py` from the project root (or copy sections into your notebook) to see validations, suggestions, dataset comparison, and pipeline checks in action.

## Tests

Run `pytest tests/ -v` (or `python -m pytest tests/ -v`); use `-m "not e2e"` to exclude end-to-end tests. Details in [USAGE.md](USAGE.md).

## Layout and docs

- **Package:** `data_quality/` — checker, expectations, similarity, reporting, comparison, suggestion, pipeline, utils.
- **Architecture:** [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — module roles and data flow.
- **Functionality and use:** [USAGE.md](USAGE.md) — dimensions, validations, JSON, auto-suggestions, comparing datasets, pipeline, test suite, modularization.
- **Shareable overview:** Use `get_getting_started_html()` or `get_usage_html()` from `data_quality` to render HTML in your environment.
