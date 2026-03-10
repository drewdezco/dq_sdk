"""
Quick start example: Print the getting started guide and run a simple example.

Run from project root:
    python quick_start_example.py
"""

import pandas as pd
from data_quality import DataQualityChecker, get_getting_started_guide

def main():
    print("=" * 70)
    print("GETTING STARTED GUIDE")
    print("=" * 70)
    print()
    print(get_getting_started_guide())
    print()
    
    print("=" * 70)
    print("QUICK EXAMPLE")
    print("=" * 70)
    print()
    
    # Create a simple example DataFrame
    df = pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "Dave", "Eve"],
        "status": ["active", "inactive", "active", "active", "pending"],
        "score": [85, 92, 78, 95, 88],
    })
    
    print(f"Sample DataFrame ({len(df)} rows, {len(df.columns)} columns):")
    print(df)
    print()
    
    # Create checker and run some basic validations
    checker = DataQualityChecker(df, dataset_name="Quick Start Example")
    
    print("Running basic validations...")
    checker.expect_column_values_to_not_be_null("id")
    checker.expect_column_values_to_be_unique("id")
    checker.expect_column_values_to_be_in_set("status", allowed_values=["active", "inactive", "pending"])
    checker.expect_column_values_to_be_in_range("score", 0, 100)
    
    # Get results
    results_df = checker.get_results()
    print("\nValidation Results:")
    print(results_df[["column", "rule", "success_rate", "dimension"]].to_string(index=False))
    print()
    
    # Show summary
    report = checker.get_comprehensive_results(title="Quick Start Example Report")
    print("Summary:")
    print(f"  Overall health score: {report['key_metrics']['overall_health_score']:.1f}%")
    print(f"  Total rules executed: {report['key_metrics']['total_rules_executed']}")
    print()
    
    print("=" * 70)
    print("Try auto-generating suggestions:")
    print("  suggestions = checker.generate_suggestions()")
    print("  checker.suggest_and_apply(auto_apply=True)")
    print("=" * 70)

if __name__ == "__main__":
    main()
