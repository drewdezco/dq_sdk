# Getting Started with modularized_data_quality

## Installation

From the project root:

```bash
pip install -e .
```

If you don't have a `pyproject.toml` or `setup.py`, add the project root to `PYTHONPATH` or run scripts from the project root so `import data_quality` resolves.

## First example: basic validations

```python
import pandas as pd
from data_quality import DataQualityChecker

df = pd.DataFrame({
    "id": [1, 2, 3, 4, 5],
    "status": ["active", "inactive", "active", "active", "pending"],
    "score": [85, 92, 78, 95, 88],
})

checker = DataQualityChecker(df, dataset_name="My Dataset")

# Run a few expectations
checker.expect_column_values_to_not_be_null("id")
checker.expect_column_values_to_be_unique("id")
checker.expect_column_values_to_be_in_set("status", allowed_values=["active", "inactive", "pending"])

# Get results
results_df = checker.get_results()
print(results_df[["column", "rule", "success_rate", "dimension"]])

# Get a comprehensive report
report = checker.get_comprehensive_results(title="My Quality Report")
print(report["key_metrics"])
```

## Auto-generate validations

Let the library analyze your data and suggest validations:

```python
checker = DataQualityChecker(df, dataset_name="My Dataset")

# Generate suggestions automatically
suggestions = checker.generate_suggestions()
for s in suggestions:
    print(f"{s['column']}: {s['method']} (confidence: {s['confidence']:.2f})")

# Apply high-confidence suggestions
high = [s for s in suggestions if s["confidence"] >= 0.8]
checker.apply_suggestions(high)

results_df = checker.get_results()
```

## JSON configuration

Define validations in JSON/dict form and run them in one call:

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
}

checker = DataQualityChecker(df)
checker.run_rules_from_json(rules)
```

## Next steps

- Run `examples.py` from the project root to see sectioned examples for:
  - validations and suggestions
  - comparing two datasets
  - pipeline comparisons (baseline vs current, CSV paths, multi-snapshots)
- Read the full usage reference in `docs/USAGE.md`.
- For architecture details, see `docs/ARCHITECTURE.md`.

