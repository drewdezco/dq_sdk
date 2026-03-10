"""
Getting started guide for new users.
Returns markdown-formatted quick start guide.
"""


def get_getting_started_guide() -> str:
    """
    Returns a markdown-formatted getting started guide for new users.
    
    Returns:
        str: Markdown-formatted guide with installation, basic usage, and examples
    """
    return """# Getting Started with Data Quality Checker

## Installation

From the project root:

```bash
pip install -e .
```

If you don't have a `pyproject.toml` or `setup.py`, add the project root to `PYTHONPATH` or run scripts from the project root so `import data_quality` resolves.

## Quick Start

### Basic Usage

```python
import pandas as pd
from data_quality import DataQualityChecker

# Load your data
df = pd.read_csv("your_data.csv")

# Create a checker instance
checker = DataQualityChecker(df, dataset_name="My Dataset")

# Run some basic validations
checker.expect_column_values_to_not_be_null("id")
checker.expect_column_values_to_be_unique("id")
checker.expect_column_values_to_be_in_set("status", allowed_values=["active", "inactive"])

# Get results
results_df = checker.get_results()
print(results_df)

# Get comprehensive report
report = checker.get_comprehensive_results(title="My Quality Report")
print(report["key_metrics"])
```

### Auto-Generate Validations

Instead of manually defining validations, let the SDK analyze your data and suggest validations:

```python
checker = DataQualityChecker(df, dataset_name="My Dataset")

# Generate suggestions automatically
suggestions = checker.generate_suggestions()
for s in suggestions:
    print(f"{s['column']}: {s['method']} (confidence: {s['confidence']:.2f})")

# Apply suggestions automatically
checker.suggest_and_apply(auto_apply=True)

# View results
results_df = checker.get_results()
```

### Using JSON Config

You can also define validations in JSON format:

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
    ]
}

checker = DataQualityChecker(df)
checker.run_rules_from_json(rules)
```

## Next Steps

1. **Run the demo** to see all features in action:
   ```bash
   python run_demo.py
   ```

2. **Explore validations** - See all available validations:
   ```bash
   python run_demo.py --list-validations
   ```

3. **Read the full documentation**:
   - [README.md](../README.md) - Complete usage guide
   - [demos/VALIDATIONS_AND_DIMENSIONS.md](../demos/VALIDATIONS_AND_DIMENSIONS.md) - All validations reference

4. **Check out examples**:
   - `demos/run_demo.py` - Three comprehensive use cases
   - `run_analysis.py` - Simple starter script

## Common Use Cases

### Check Data Completeness
```python
checker.expect_column_values_to_not_be_null("customer_id")
checker.expect_columns_values_to_not_be_null(["id", "email", "name"])
```

### Validate Data Ranges
```python
checker.expect_column_values_to_be_in_range("age", 0, 120)
checker.expect_column_values_to_be_in_date_range("order_date", "2024-01-01", "2024-12-31")
```

### Check Data Patterns
```python
checker.expect_column_values_to_match_regex("email", pattern=r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$")
```

### Compare Two Datasets
```python
from data_quality import reconcile_on_key

summary = reconcile_on_key(
    df_left, df_right,
    key_column="order_id",
    results=[],
    include_similarity=True
)
```

## Need Help?

- Check the [README.md](../README.md) for detailed documentation
- Run `python run_demo.py` to see examples
- Review [demos/VALIDATIONS_AND_DIMENSIONS.md](../demos/VALIDATIONS_AND_DIMENSIONS.md) for all available validations
"""
