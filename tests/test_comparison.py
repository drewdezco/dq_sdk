"""Unit and integration tests for data_quality.comparison."""

import pytest
import pandas as pd
from data_quality import (
    compare_two_reports,
    reconcile_on_key,
    reconcile_with_auto_key,
    run_same_rules_on_two_datasets,
    DatasetComparator,
    get_reconciliation_diffs,
    DataQualityChecker,
)


# -------- reconcile_on_key --------


def test_reconcile_on_key_identical_dfs_100_match():
    df = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"], "val": [10, 20, 30]})
    results = []
    summary = reconcile_on_key(df, df.copy(), "id", results, right_name="B")
    assert summary["join_quality"]["match_rate_left_pct"] == 100.0
    assert summary["join_quality"]["match_rate_right_pct"] == 100.0
    assert summary["join_quality"]["only_in_left"] == 0
    assert summary["join_quality"]["only_in_right"] == 0
    assert len(results) >= 1
    join_result = next(r for r in results if r["rule"] == "join quality")
    assert join_result["dimension"] == "Consistency"
    assert "name" in summary["columns_compared"] or "val" in summary["columns_compared"]
    for r in results:
        if r["rule"] == "reconcile vs B":
            assert r["success_rate"] == 100.0


def test_reconcile_on_key_one_diff_correct_rate():
    df_left = pd.DataFrame({"id": [1, 2, 3], "x": [10, 20, 30]})
    df_right = pd.DataFrame({"id": [1, 2, 3], "x": [10, 99, 30]})
    results = []
    summary = reconcile_on_key(df_left, df_right, "id", results, right_name="R")
    assert summary["join_quality"]["matched_pairs"] == 3
    col_results = [r for r in results if r["rule"] == "reconcile vs R" and r["column"] == "x"]
    assert len(col_results) == 1
    assert col_results[0]["success_rate"] == round(2 / 3 * 100, 1)
    assert col_results[0]["details"]["failed"] == 1


def test_reconcile_on_key_only_in_one_side():
    df_left = pd.DataFrame({"id": [1, 2, 3], "x": [10, 20, 30]})
    df_right = pd.DataFrame({"id": [1, 3], "x": [10, 30]})
    results = []
    summary = reconcile_on_key(df_left, df_right, "id", results)
    assert summary["join_quality"]["left_key_count"] == 3
    assert summary["join_quality"]["right_key_count"] == 2
    assert summary["join_quality"]["matched_pairs"] == 2
    assert summary["join_quality"]["only_in_left"] == 1
    assert summary["join_quality"]["only_in_right"] == 0
    assert summary["join_quality"]["match_rate_left_pct"] == round(2 / 3 * 100, 1)


def test_reconcile_on_key_multi_column_key():
    df_left = pd.DataFrame({"a": [1, 1, 2], "b": [10, 20, 10], "x": [100, 200, 300]})
    df_right = pd.DataFrame({"a": [1, 1, 2], "b": [10, 20, 10], "x": [100, 200, 999]})
    results = []
    summary = reconcile_on_key(df_left, df_right, ["a", "b"], results, right_name="R")
    assert summary["join_quality"]["matched_pairs"] == 3
    col_results = [r for r in results if r["rule"] == "reconcile vs R" and r["column"] == "x"]
    assert len(col_results) == 1
    assert col_results[0]["success_rate"] == round(2 / 3 * 100, 1)


def test_reconcile_on_key_include_similarity():
    df_left = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
    df_right = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
    results = []
    reconcile_on_key(df_left, df_right, "id", results, include_similarity=True, similarity_threshold=0.8)
    similarity_results = [r for r in results if "similarity" in r["rule"]]
    assert len(similarity_results) >= 1
    assert similarity_results[0]["dimension"] == "Consistency"


def test_reconcile_on_key_missing_key_raises():
    df_left = pd.DataFrame({"id": [1], "x": [10]})
    df_right = pd.DataFrame({"other": [1], "x": [10]})
    results = []
    with pytest.raises(ValueError, match="Key column"):
        reconcile_on_key(df_left, df_right, "id", results)


def test_get_reconciliation_diffs():
    df_left = pd.DataFrame({"id": [1, 2], "x": [10, 20]})
    df_right = pd.DataFrame({"id": [1, 2], "x": [10, 99]})
    diffs = get_reconciliation_diffs(df_left, df_right, "id", "x")
    assert len(diffs) == 1
    assert diffs.iloc[0]["value_left"] == 20
    assert diffs.iloc[0]["value_right"] == 99


def test_reconcile_with_auto_key_picks_shared_unique_key():
    df_left = pd.DataFrame({"order_id": ["O1", "O2", "O3"], "x": [10, 20, 30]})
    df_right = pd.DataFrame({"order_id": ["O2", "O3", "O4"], "x": [11, 20, 30]})
    results = []
    summary = reconcile_with_auto_key(df_left, df_right, results, right_name="R")
    assert summary["auto_key_column"] == "order_id"
    assert summary["join_quality"]["matched_pairs"] == 2
    assert any(r["rule"] == "join quality" for r in results)


def test_reconcile_with_auto_key_raises_when_no_candidate_key():
    # No column is sufficiently unique on both sides to be a key candidate by default.
    # Both shared columns are low-uniqueness on at least one side.
    df_left = pd.DataFrame({"k": [1, 1, 1], "x": [1, 1, 1]})
    df_right = pd.DataFrame({"k": [1, 1, 1], "x": [1, 1, 1]})
    results = []
    with pytest.raises(ValueError, match="infer a key column"):
        reconcile_with_auto_key(df_left, df_right, results)


# -------- compare_two_reports --------


def test_compare_two_reports_deltas():
    report_a = {
        "key_metrics": {"overall_health_score": 80.0, "per_dimension_scores": {"Completeness": 80.0, "Uniqueness": 80.0}},
        "detailed_results": [
            {"column": "id", "rule": "not null", "success_rate": 80.0},
            {"column": "id", "rule": "unique", "success_rate": 80.0},
        ],
    }
    report_b = {
        "key_metrics": {"overall_health_score": 90.0, "per_dimension_scores": {"Completeness": 90.0, "Uniqueness": 90.0}},
        "detailed_results": [
            {"column": "id", "rule": "not null", "success_rate": 90.0},
            {"column": "id", "rule": "unique", "success_rate": 90.0},
        ],
    }
    diff = compare_two_reports(report_a, report_b)
    assert diff["overall_health_score_a"] == 80.0
    assert diff["overall_health_score_b"] == 90.0
    assert diff["delta"] == 10.0
    assert "per_dimension_diffs" in diff
    assert "per_rule_diffs" in diff
    assert len(diff["per_rule_diffs"]) >= 2


# -------- run_same_rules_on_two_datasets --------


def test_run_same_rules_on_two_datasets_returns_two_reports(sample_df):
    df_b = sample_df.copy()
    df_b["id"] = [1, 2, 3, 4, 5]

    def rules_runner(df, results):
        checker = DataQualityChecker(df, dataset_name="")
        checker.df = df
        checker.results = results
        checker.expect_column_values_to_not_be_null("id")

    report_a, report_b = run_same_rules_on_two_datasets(
        sample_df, df_b, rules_runner, dataset_name_a="A", dataset_name_b="B"
    )
    assert "error" not in report_a
    assert "error" not in report_b
    assert report_a["metadata"]["dataset_name"] == "A"
    assert report_b["metadata"]["dataset_name"] == "B"
    assert "key_metrics" in report_a and "key_metrics" in report_b
    diff = compare_two_reports(report_a, report_b)
    assert "overall_health_score_a" in diff
    assert "overall_health_score_b" in diff


# -------- DatasetComparator --------


def test_dataset_comparator_reconcile():
    df_a = pd.DataFrame({"id": [1, 2], "x": [10, 20]})
    df_b = pd.DataFrame({"id": [1, 2], "x": [10, 20]})
    comp = DatasetComparator(df_a, df_b, "id", name_a="A", name_b="B")
    summary = comp.reconcile()
    assert summary["join_quality"]["matched_pairs"] == 2
    assert len(comp.results) >= 1
    assert any(r["rule"] == "join quality" for r in comp.results)


def test_dataset_comparator_run_same_rules(sample_df):
    df_b = sample_df.copy()

    def rules_runner(df, results):
        c = DataQualityChecker(df, dataset_name="")
        c.df, c.results = df, results
        c.expect_column_values_to_not_be_null("id")

    comp = DatasetComparator(sample_df, df_b, "id", name_a="A", name_b="B")
    report_a, report_b = comp.run_same_rules(rules_runner)
    assert report_a["metadata"]["dataset_name"] == "A"
    assert report_b["metadata"]["dataset_name"] == "B"
    cmp_report = comp.get_comparison_report(rules_runner)
    assert "overall_health_score_a" in cmp_report
