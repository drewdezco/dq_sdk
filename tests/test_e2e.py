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


@pytest.mark.e2e
def test_suggestion_workflow_e2e(tmp_path):
    """End-to-end test for suggestion generation and application."""
    # Create a dataframe with various data types
    df = pd.DataFrame({
        "id": [1, 2, 3, 4, 5, 6],  # Unique, not null
        "status": ["active", "inactive", "pending"] * 2,  # Categorical (6 items)
        "score": [10, 20, 30, 40, 50, 60],  # Numeric (6 items)
        "email": ["test@example.com", "user@domain.org", "admin@site.net", "info@test.com", "contact@example.org", "support@example.com"],
    })
    
    c = DataQualityChecker(df, dataset_name="Suggestion E2E")
    
    # Generate suggestions
    suggestions = c.generate_suggestions()
    assert isinstance(suggestions, list)
    assert len(suggestions) > 0
    
    # Verify suggestion structure
    for suggestion in suggestions:
        assert "column" in suggestion
        assert "method" in suggestion
        assert "params" in suggestion
        assert "confidence" in suggestion
        assert "reason" in suggestion
        assert "dimension" in suggestion
    
    # Apply suggestions
    result = c.suggest_and_apply(auto_apply=True)
    assert result["applied_count"] > 0
    assert len(c.results) == result["applied_count"]
    
    # Verify results were generated correctly
    results_df = c.get_results()
    assert len(results_df) > 0
    assert "column" in results_df.columns
    assert "rule" in results_df.columns
    assert "dimension" in results_df.columns
    
    # Verify comprehensive results include suggestion-generated validations
    report = c.get_comprehensive_results(title="Suggestion E2E Report")
    assert report["metadata"]["dataset_name"] == "Suggestion E2E"
    assert "key_metrics" in report
    assert report["key_metrics"]["total_rules_executed"] == len(c.results)
    assert len(report["detailed_results"]) == len(c.results)
    
    # Save to CSV
    history_path = tmp_path / "suggestion_history.csv"
    c.save_comprehensive_results_to_csv(
        title="Suggestion E2E",
        csv_filename=str(history_path),
        include_field_summary=True,
    )
    assert history_path.exists()
