"""Unit tests for data_quality.expectations."""

import pytest
import pandas as pd
from data_quality import expectations as exp


def _df(column, values):
    return pd.DataFrame({column: values})


# -------- Single-column: not null --------


def test_expect_column_values_to_not_be_null_pass():
    df = _df("x", [1, 2, 3])
    results = []
    exp.expect_column_values_to_not_be_null(df, results, "x")
    assert len(results) == 1
    assert results[0]["column"] == "x"
    assert results[0]["rule"] == "not null"
    assert results[0]["success_rate"] == 100.0
    assert results[0]["dimension"] == "Completeness"
    assert results[0]["details"]["total"] == 3 and results[0]["details"]["failed"] == 0


def test_expect_column_values_to_not_be_null_fail():
    df = _df("x", [1, None, 3])
    results = []
    exp.expect_column_values_to_not_be_null(df, results, "x")
    assert len(results) == 1
    assert results[0]["success_rate"] < 100
    assert results[0]["details"]["failed"] == 1


# -------- Single-column: unique --------


def test_expect_column_values_to_be_unique_pass():
    df = _df("x", [1, 2, 3])
    results = []
    exp.expect_column_values_to_be_unique(df, results, "x")
    assert len(results) == 1
    assert results[0]["rule"] == "unique"
    assert results[0]["dimension"] == "Uniqueness"
    assert results[0]["success_rate"] == 100.0


def test_expect_column_values_to_be_unique_fail():
    df = _df("x", [1, 1, 2])
    results = []
    exp.expect_column_values_to_be_unique(df, results, "x")
    assert len(results) == 1
    assert results[0]["success_rate"] < 100
    assert results[0]["details"]["failed"] == 2


# -------- Single-column: in set --------


def test_expect_column_values_to_be_in_set_pass():
    df = _df("status", ["a", "b", "a"])
    results = []
    exp.expect_column_values_to_be_in_set(df, results, "status", ["a", "b"])
    assert len(results) == 1
    assert results[0]["rule"] == "in allowed set"
    assert results[0]["success_rate"] == 100.0


def test_expect_column_values_to_be_in_set_fail():
    df = _df("status", ["a", "x", "b"])
    results = []
    exp.expect_column_values_to_be_in_set(df, results, "status", ["a", "b"])
    assert len(results) == 1
    assert results[0]["success_rate"] < 100
    assert results[0]["details"]["failed"] == 1


# -------- Single-column: regex --------


def test_expect_column_values_to_match_regex_pass():
    df = _df("code", ["A1", "A2", "A3"])
    results = []
    exp.expect_column_values_to_match_regex(df, results, "code", r"^A\d$")
    assert len(results) == 1
    assert results[0]["rule"] == "matches regex"
    assert results[0]["success_rate"] == 100.0


def test_expect_column_values_to_match_regex_fail():
    df = _df("code", ["A1", "B2", "A3"])
    results = []
    exp.expect_column_values_to_match_regex(df, results, "code", r"^A\d$")
    assert len(results) == 1
    assert results[0]["success_rate"] < 100


# -------- Single-column: range --------


def test_expect_column_values_to_be_in_range_pass():
    df = _df("score", [10, 20, 30])
    results = []
    exp.expect_column_values_to_be_in_range(df, results, "score", 0, 100)
    assert len(results) == 1
    assert "in range" in results[0]["rule"]
    assert results[0]["success_rate"] == 100.0


def test_expect_column_values_to_be_in_range_fail():
    df = _df("score", [10, 200, 30])
    results = []
    exp.expect_column_values_to_be_in_range(df, results, "score", 0, 100)
    assert len(results) == 1
    assert results[0]["success_rate"] < 100


# -------- Single-column: date range --------


def test_expect_column_values_to_be_in_date_range_pass():
    df = pd.DataFrame({"d": pd.to_datetime(["2024-01-01", "2024-01-15", "2024-01-31"])})
    results = []
    exp.expect_column_values_to_be_in_date_range(
        df, results, "d", "2024-01-01", "2024-02-01"
    )
    assert len(results) == 1
    assert results[0]["rule"] == "in date range"
    assert results[0]["success_rate"] == 100.0


def test_expect_column_values_to_be_in_date_range_fail():
    df = pd.DataFrame({"d": pd.to_datetime(["2024-01-01", "2024-03-01"])})
    results = []
    exp.expect_column_values_to_be_in_date_range(
        df, results, "d", "2024-01-01", "2024-02-01"
    )
    assert len(results) == 1
    assert results[0]["success_rate"] < 100


# -------- Multi-column --------


def test_expect_columns_values_to_not_be_null():
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, None, 6]})
    results = []
    exp.expect_columns_values_to_not_be_null(df, results, ["a", "b"])
    assert len(results) == 1
    assert results[0]["rule"] == "all not null"
    assert "a" in results[0]["column"] and "b" in results[0]["column"]
    assert results[0]["details"]["columns"] == ["a", "b"]
    assert results[0]["details"]["failed"] == 1


def test_expect_columns_values_to_be_unique():
    df = pd.DataFrame({"a": [1, 1, 2], "b": [10, 10, 20]})
    results = []
    exp.expect_columns_values_to_be_unique(df, results, ["a", "b"])
    assert len(results) == 1
    assert results[0]["rule"] == "unique combination"
    assert results[0]["details"]["failed"] == 2


def test_expect_columns_values_to_be_in_sets():
    df = pd.DataFrame({"x": ["a", "b"], "y": [1, 2]})
    results = []
    exp.expect_columns_values_to_be_in_sets(
        df, results, ["x", "y"], {"x": ["a", "b"], "y": [1, 2]}
    )
    assert len(results) == 1
    assert results[0]["rule"] == "all in allowed sets"
    assert results[0]["success_rate"] == 100.0


def test_expect_columns_values_to_match_patterns():
    df = pd.DataFrame({"code": ["A1", "B2"], "id": ["1", "2"]})
    results = []
    exp.expect_columns_values_to_match_patterns(
        df, results, ["code", "id"], {"code": r"^[AB]\d$", "id": r"^\d$"}
    )
    assert len(results) == 1
    assert results[0]["rule"] == "all match patterns"


def test_expect_columns_values_to_be_in_ranges():
    df = pd.DataFrame({"score": [10, 20], "age": [25, 35]})
    results = []
    exp.expect_columns_values_to_be_in_ranges(
        df, results, ["score", "age"], {"score": (0, 100), "age": (0, 120)}
    )
    assert len(results) == 1
    assert results[0]["rule"] == "all in ranges"
    assert results[0]["success_rate"] == 100.0


def test_two_expectations_append_two_results():
    df = _df("x", [1, 2, 3])
    results = []
    exp.expect_column_values_to_not_be_null(df, results, "x")
    exp.expect_column_values_to_be_unique(df, results, "x")
    assert len(results) == 2


# -------- Optional: Timeliness and Accuracy --------


def test_expect_column_values_to_be_recent_pass():
    df = pd.DataFrame({"d": pd.to_datetime(["2024-06-01", "2024-06-02"])})
    results = []
    exp.expect_column_values_to_be_recent(
        df, results, "d", max_age_days=30, reference_date="2024-06-15"
    )
    assert len(results) == 1
    assert results[0]["dimension"] == "Timeliness"
    assert "recent within" in results[0]["rule"]
    assert results[0]["success_rate"] == 100.0


def test_expect_column_values_to_be_recent_fail():
    df = pd.DataFrame({"d": pd.to_datetime(["2020-01-01", "2024-06-01"])})
    results = []
    exp.expect_column_values_to_be_recent(
        df, results, "d", max_age_days=30, reference_date="2024-06-15"
    )
    assert len(results) == 1
    assert results[0]["success_rate"] < 100


def test_expect_column_values_to_match_reference_pass():
    df = _df("id", [1, 2, 3])
    results = []
    exp.expect_column_values_to_match_reference(df, results, "id", [1, 2, 3, 4])
    assert len(results) == 1
    assert results[0]["dimension"] == "Accuracy"
    assert results[0]["rule"] == "match reference"
    assert results[0]["success_rate"] == 100.0


def test_expect_column_values_to_match_reference_fail():
    df = _df("id", [1, 2, 99])
    results = []
    exp.expect_column_values_to_match_reference(df, results, "id", [1, 2, 3])
    assert len(results) == 1
    assert results[0]["success_rate"] < 100
