"""End-to-end tests: full checker flow and CSV on disk."""

import pytest
import pandas as pd
from data_quality import DataQualityChecker


@pytest.mark.e2e
def test_full_flow_with_csv(sample_df, tmp_path):
    c = DataQualityChecker(
        sample_df, dataset_name="E2E", critical_columns=["id"]
    )
    c.expect_column_values_to_not_be_null("id")
    c.expect_column_values_to_be_unique("id")
    c.analyze_column_similarity_levenshtein("name", "name", similarity_threshold=0.8)
    c.run_rules_from_json({"expect_column_values_to_be_in_range": [{"column": "score", "min_val": 0, "max_val": 100}]})

    results_df = c.get_results()
    assert len(results_df) >= 3

    report = c.get_comprehensive_results(title="E2E Report")
    assert report["metadata"]["dataset_name"] == "E2E"
    assert "key_metrics" in report
    assert report["key_metrics"]["total_records"] == len(sample_df)
    assert "critical_data_elements" in report

    history_path = tmp_path / "history.csv"
    c.save_comprehensive_results_to_csv(
        title="E2E",
        csv_filename=str(history_path),
        include_field_summary=True,
    )
    assert history_path.exists()
    lines = history_path.read_text(encoding="utf-8").strip().split("\n")
    assert len(lines) >= 2
    field_path = tmp_path / "history_field_details.csv"
    assert field_path.exists()
    field_lines = field_path.read_text(encoding="utf-8").strip().split("\n")
    assert len(field_lines) == len(sample_df.columns) + 1

    fields_path = tmp_path / "fields.csv"
    c.save_field_summary_to_csv(csv_filename=str(fields_path))
    assert fields_path.exists()
    assert len(fields_path.read_text(encoding="utf-8").strip().split("\n")) == len(sample_df.columns) + 1
