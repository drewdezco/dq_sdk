"""
Starter script: load sample data, run data quality checks, print results and save report.
Run from project root after generating data: python scripts/generate_sample_data.py
Then: python run_analysis.py
"""

import os
import pandas as pd
from data_quality import DataQualityChecker

# Path to the sample CSV (change if your file is elsewhere)
DATA_PATH = os.path.join(os.path.dirname(__file__), "data", "sample_data.csv")
REPORT_PATH = os.path.join(os.path.dirname(__file__), "data", "quality_report.csv")

def main():
    if not os.path.exists(DATA_PATH):
        print(f"Data not found at {DATA_PATH}. Run first: python scripts/generate_sample_data.py")
        return

    df = pd.read_csv(DATA_PATH)
    checker = DataQualityChecker(df, dataset_name="Sample Business Data", critical_columns=["id", "customer_id"])

    # Run expectations
    checker.expect_column_values_to_not_be_null("id")
    checker.expect_column_values_to_be_unique("id")
    checker.expect_column_values_to_not_be_null("customer_id")
    checker.expect_column_values_to_be_in_set("region", allowed_values=["North", "South", "East", "West"])
    checker.expect_column_values_to_be_in_set("status", allowed_values=["active", "inactive", "pending"])
    checker.expect_column_values_to_be_in_range("amount", 0, 100000)

    # Results table
    results_df = checker.get_results()
    print("Rule results:")
    print(results_df.to_string())
    print()

    # Summary report
    report = checker.get_comprehensive_results(title="Sample Data Quality Report")
    if "error" in report:
        print(report["error"])
        return
    print("Summary:", report["key_metrics"])
    print()

    # Save to CSV
    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    checker.save_comprehensive_results_to_csv(
        title="Sample Data Quality Report",
        csv_filename=REPORT_PATH,
        include_field_summary=True,
    )
    print(f"Report saved to {REPORT_PATH} (and field details)")

if __name__ == "__main__":
    main()
