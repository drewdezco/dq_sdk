"""Unit and integration tests for DataQualityChecker."""

import pytest
import pandas as pd
from data_quality import DataQualityChecker


# -------- Unit: state and delegation --------


def test_constructor_stores_state(sample_df):
    c = DataQualityChecker(sample_df, dataset_name="MyDS", critical_columns=["id"])
    assert c.df is sample_df
    assert c.dataset_name == "MyDS"
    assert c.user_specified_critical_columns == ["id"]
    assert c.results == []


def test_set_get_dataset_name(sample_df):
    c = DataQualityChecker(sample_df)
    c.set_dataset_name("NewName")
    assert c.get_dataset_name() == "NewName"


def test_set_get_critical_columns(sample_df):
    c = DataQualityChecker(sample_df)
    c.set_critical_columns(["a", "b"])
    assert c.get_critical_columns() == ["a", "b"]


def test_add_remove_critical_column(sample_df):
    c = DataQualityChecker(sample_df, critical_columns=["id"])
    c.add_critical_column("name")
    assert "name" in c.get_critical_columns()
    c.remove_critical_column("name")
    assert "name" not in c.get_critical_columns()


def test_expect_column_values_to_not_be_null_appends_result(sample_df):
    c = DataQualityChecker(sample_df)
    c.expect_column_values_to_not_be_null("id")
    assert len(c.results) == 1
    assert c.results[0]["rule"] == "not null"
    assert c.results[0]["column"] == "id"


def test_get_results_returns_dataframe(sample_df):
    c = DataQualityChecker(sample_df)
    c.expect_column_values_to_not_be_null("id")
    df = c.get_results()
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert "column" in df.columns and "rule" in df.columns


def test_get_comprehensive_results_after_one_expectation(sample_df):
    c = DataQualityChecker(sample_df)
    c.expect_column_values_to_not_be_null("id")
    report = c.get_comprehensive_results(title="T")
    assert "key_metrics" in report
    assert "detailed_results" in report
    assert len(report["detailed_results"]) == 1


def test_run_rules_from_json(sample_df):
    c = DataQualityChecker(sample_df)
    c.run_rules_from_json({"expect_column_values_to_not_be_null": [{"column": "id"}]})
    assert len(c.results) == 1
    assert c.results[0]["rule"] == "not null"


def test_run_rules_from_json_unknown_expectation_no_crash(sample_df):
    c = DataQualityChecker(sample_df)
    c.run_rules_from_json({"nonexistent_method": [{"column": "id"}]})
    assert len(c.results) == 0


# -------- Auto-suggestion --------


def test_generate_suggestions_returns_list(sample_df):
    """Test that generate_suggestions returns a list of suggestions."""
    c = DataQualityChecker(sample_df)
    suggestions = c.generate_suggestions()
    assert isinstance(suggestions, list)
    if len(suggestions) > 0:
        assert "column" in suggestions[0]
        assert "method" in suggestions[0]
        assert "params" in suggestions[0]
        assert "confidence" in suggestions[0]
        assert "reason" in suggestions[0]
        assert "dimension" in suggestions[0]


def test_generate_suggestions_with_options(sample_df):
    """Test generating suggestions with custom options."""
    c = DataQualityChecker(sample_df)
    options = {"null_rate_threshold": 0.5}  # Higher threshold
    suggestions = c.generate_suggestions(options=options)
    assert isinstance(suggestions, list)


def test_generate_suggestions_specific_columns(sample_df):
    """Test generating suggestions for specific columns."""
    c = DataQualityChecker(sample_df)
    suggestions = c.generate_suggestions(columns=["id"])
    assert isinstance(suggestions, list)
    # All suggestions should be for the specified column
    for s in suggestions:
        assert s["column"] == "id"


def test_apply_suggestions_adds_results(sample_df):
    """Test that applying suggestions adds results to checker."""
    c = DataQualityChecker(sample_df)
    initial_count = len(c.results)
    
    # Create a simple suggestion
    suggestions = [{
        "column": "id",
        "method": "expect_column_values_to_not_be_null",
        "params": {},
        "confidence": 0.95,
        "reason": "Test",
        "dimension": "Completeness",
    }]
    
    applied_count = c.apply_suggestions(suggestions, auto_apply=True)
    assert applied_count > 0
    assert len(c.results) > initial_count


def test_apply_suggestions_selective():
    """Test applying subset of suggestions based on confidence."""
    df = pd.DataFrame({"id": [1, 2, 3, 4, 5]})
    c = DataQualityChecker(df)
    
    suggestions = [
        {
            "column": "id",
            "method": "expect_column_values_to_not_be_null",
            "params": {},
            "confidence": 0.95,  # High confidence - should apply
            "reason": "High confidence",
            "dimension": "Completeness",
        },
        {
            "column": "id",
            "method": "expect_column_values_to_be_unique",
            "params": {},
            "confidence": 0.5,  # Low confidence - should not apply
            "reason": "Low confidence",
            "dimension": "Uniqueness",
        },
    ]
    
    # Without auto_apply, only high confidence should apply
    applied_count = c.apply_suggestions(suggestions, auto_apply=False)
    assert applied_count == 1
    assert len(c.results) == 1
    
    # With auto_apply, both should apply
    c2 = DataQualityChecker(df)
    applied_count2 = c2.apply_suggestions(suggestions, auto_apply=True)
    assert applied_count2 == 2
    assert len(c2.results) == 2


def test_suggest_and_apply_integration():
    """Test full suggest_and_apply workflow."""
    df = pd.DataFrame({
        "id": [1, 2, 3, 4, 5, 6],
        "status": ["active", "inactive", "pending"] * 2,
    })
    c = DataQualityChecker(df)
    
    result = c.suggest_and_apply(auto_apply=False)
    assert "suggestions" in result
    assert "applied_count" in result
    assert "applied_suggestions" in result
    assert isinstance(result["suggestions"], list)
    assert isinstance(result["applied_suggestions"], list)
    assert result["applied_count"] >= 0
    assert len(c.results) == result["applied_count"]


def test_suggestions_integration_with_existing_expectations(sample_df):
    """Ensure suggestions work with manually added expectations."""
    c = DataQualityChecker(sample_df)
    
    # Add a manual expectation
    c.expect_column_values_to_not_be_null("id")
    initial_count = len(c.results)
    
    # Generate and apply suggestions
    result = c.suggest_and_apply(auto_apply=True)
    
    # Should have both manual and suggested expectations
    assert len(c.results) >= initial_count
    assert result["applied_count"] >= 0


# -------- Integration: multi-step, no disk --------


def test_multiple_expectations_and_get_comprehensive_results(sample_df):
    c = DataQualityChecker(sample_df)
    c.expect_column_values_to_not_be_null("id")
    c.expect_column_values_to_be_unique("id")
    c.expect_column_values_to_be_in_range("score", 0, 100)
    report = c.get_comprehensive_results()
    assert len(c.results) == 3
    assert report["key_metrics"]["total_rules_executed"] == 3
    assert len(report["detailed_results"]) == 3


def test_similarity_then_summary_table(sample_df):
    c = DataQualityChecker(sample_df)
    c.analyze_column_similarity_levenshtein("id", "score", similarity_threshold=0.8)
    table = c.get_similarity_summary_table()
    assert not table.empty
    assert len(table) == 1
    assert table.iloc[0]["Column_Pair"] == "id vs score"
