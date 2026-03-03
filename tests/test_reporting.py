"""Unit and integration tests for data_quality.reporting."""

import pytest
import pandas as pd
from data_quality import reporting as rep


# -------- Unit: get_comprehensive_results --------


def test_get_comprehensive_results_structure(sample_df, sample_results):
    out = rep.get_comprehensive_results(
        sample_df,
        sample_results,
        "TestDataset",
        [],
        title="Test Report",
    )
    assert "metadata" in out
    assert "key_metrics" in out
    assert "overall_data_quality" in out
    assert "critical_data_elements" in out
    assert "other_fields" in out
    assert "rule_execution_summary" in out
    assert "detailed_results" in out
    assert "per_dimension_scores" in out["key_metrics"]
    assert out["key_metrics"]["per_dimension_scores"]["Completeness"] == 100.0
    assert out["key_metrics"]["per_dimension_scores"]["Uniqueness"] == 100.0
    assert out["key_metrics"]["overall_health_score"] == 100.0
    assert out["key_metrics"]["total_records"] == len(sample_df)
    assert out["metadata"]["dataset_name"] == "TestDataset"
    assert out["metadata"]["title"] == "Test Report"


def test_get_comprehensive_results_df_none_returns_error():
    out = rep.get_comprehensive_results(
        None, [], "X", [], title="T"
    )
    assert "error" in out
    assert "dataframe" in out["error"].lower()


def test_overall_health_score_is_mean_of_dimension_scores(sample_df):
    """Overall score should be the average of per-dimension scores (only dimensions with rules)."""
    results_two_dims = [
        {"column": "id", "rule": "not null", "success_rate": 80.0, "details": {}, "dimension": "Completeness"},
        {"column": "name", "rule": "unique", "success_rate": 100.0, "details": {}, "dimension": "Uniqueness"},
    ]
    out = rep.get_comprehensive_results(
        sample_df, results_two_dims, "DS", [], title="T"
    )
    assert out["key_metrics"]["per_dimension_scores"]["Completeness"] == 80.0
    assert out["key_metrics"]["per_dimension_scores"]["Uniqueness"] == 100.0
    assert out["key_metrics"]["overall_health_score"] == 90.0


def test_dimensions_filter_restricts_scoring(sample_df, sample_results):
    """When dimensions_filter is set, only those dimensions contribute to per_dimension_scores and overall."""
    out = rep.get_comprehensive_results(
        sample_df, sample_results, "DS", [], title="T", dimensions_filter=["Completeness"]
    )
    assert list(out["key_metrics"]["per_dimension_scores"].keys()) == ["Completeness"]
    assert out["key_metrics"]["per_dimension_scores"]["Completeness"] == 100.0
    assert out["key_metrics"]["overall_health_score"] == 100.0


# -------- Unit: flatten_comprehensive_results --------


def test_flatten_comprehensive_results(sample_df, sample_results):
    comprehensive = rep.get_comprehensive_results(
        sample_df, sample_results, "DS", [], title="T"
    )
    flat = rep.flatten_comprehensive_results(comprehensive)
    assert "timestamp" in flat
    assert "total_records" in flat
    assert "overall_health_score" in flat
    assert flat["total_records"] == len(sample_df)
    assert "critical_avg_completeness" in flat
    assert "other_avg_completeness" in flat
    assert "dimension_score_Completeness" in flat
    assert "dimension_score_Uniqueness" in flat


# -------- Integration: save_* to tmp_path --------


def test_save_comprehensive_results_to_csv(sample_df, sample_results, tmp_path):
    csv_path = tmp_path / "history.csv"
    rep.save_comprehensive_results_to_csv(
        sample_df,
        sample_results,
        "TestDS",
        [],
        title="T",
        csv_filename=str(csv_path),
        include_field_summary=False,
    )
    assert csv_path.exists()
    content = csv_path.read_text(encoding="utf-8")
    lines = content.strip().split("\n")
    assert len(lines) >= 2  # header + at least one data row
    assert "total_records" in lines[0] or "timestamp" in lines[0]


def test_save_comprehensive_results_to_csv_with_field_summary(sample_df, sample_results, tmp_path):
    csv_path = tmp_path / "main.csv"
    rep.save_comprehensive_results_to_csv(
        sample_df,
        sample_results,
        "DS",
        [],
        csv_filename=str(csv_path),
        include_field_summary=True,
    )
    assert csv_path.exists()
    field_path = tmp_path / "main_field_details.csv"
    assert field_path.exists()
    field_content = field_path.read_text(encoding="utf-8")
    lines = field_content.strip().split("\n")
    assert len(lines) >= 2
    assert "column_name" in lines[0] or "data_type" in lines[0]


def test_save_field_summary_to_csv(sample_df, tmp_path):
    rep.save_field_summary_to_csv(
        sample_df,
        "DS",
        [],
        title="T",
        csv_filename=str(tmp_path / "fields.csv"),
    )
    path = tmp_path / "fields.csv"
    assert path.exists()
    content = path.read_text(encoding="utf-8")
    lines = content.strip().split("\n")
    assert len(lines) == len(sample_df.columns) + 1  # header + one row per column
    assert "column_name" in lines[0]
    assert "data_type" in lines[0]
    assert "completeness" in lines[0]
    assert "is_critical" in lines[0]
