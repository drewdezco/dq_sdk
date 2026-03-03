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
