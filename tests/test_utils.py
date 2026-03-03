"""Unit tests for data_quality.utils."""

import pytest
import pandas as pd
from data_quality.utils import (
    normalize_columns,
    levenshtein_distance,
    levenshtein_ratio,
    classify_data_type,
    calculate_quality_scores,
    is_critical_data_element,
)


class TestNormalizeColumns:
    def test_string_returns_list(self):
        assert normalize_columns("col") == ["col"]

    def test_list_returns_list(self):
        assert normalize_columns(["a", "b"]) == ["a", "b"]

    def test_tuple_returns_list(self):
        assert normalize_columns(("a", "b")) == ["a", "b"]

    def test_invalid_type_raises(self):
        with pytest.raises(ValueError, match="string, list, or tuple"):
            normalize_columns(123)
        with pytest.raises(ValueError):
            normalize_columns(None)


class TestLevenshteinDistance:
    def test_known_pair(self):
        assert levenshtein_distance("kitten", "sitting") == 3

    def test_empty_string(self):
        assert levenshtein_distance("", "abc") == 3
        assert levenshtein_distance("abc", "") == 3

    def test_equal_strings(self):
        assert levenshtein_distance("same", "same") == 0

    def test_numeric_coerced_to_str(self):
        assert levenshtein_distance(123, 123) == 0


class TestLevenshteinRatio:
    def test_equal_strings(self):
        assert levenshtein_ratio("same", "same") == 1.0

    def test_derived_from_distance(self):
        # kitten -> sitting distance 3, max_len 7 -> (7-3)/7
        assert abs(levenshtein_ratio("kitten", "sitting") - (4 / 7)) < 1e-9

    def test_zero_length(self):
        assert levenshtein_ratio("", "") == 1.0


class TestClassifyDataType:
    def test_datetime(self):
        s = pd.Series(pd.to_datetime(["2024-01-01", "2024-01-02"]))
        assert classify_data_type(s) == "Date/Time"

    def test_integer(self):
        s = pd.Series([1, 2, 3])
        assert classify_data_type(s) == "Integer"

    def test_float_decimal(self):
        s = pd.Series([1.0, 2.5, 3.0])
        assert classify_data_type(s) == "Decimal"

    def test_bool(self):
        s = pd.Series([True, False, True])
        assert classify_data_type(s) == "Boolean"

    def test_object_string(self):
        s = pd.Series(["a", "b", "c"])
        # May be "Text/String" (object dtype) or "Str" (StringDtype in newer pandas)
        out = classify_data_type(s)
        assert out in ("Text/String", "Str")

    def test_category(self):
        s = pd.Series(["a", "b", "a"], dtype="category")
        assert classify_data_type(s) == "Category"


class TestCalculateQualityScores:
    def test_returns_keys(self):
        s = pd.Series([1, 2, 3])
        out = calculate_quality_scores(s)
        assert "completeness" in out
        assert "uniqueness" in out
        assert "consistency" in out

    def test_with_nulls(self):
        s = pd.Series([1, 2, None, 4, 5])
        out = calculate_quality_scores(s)
        assert 0 <= out["completeness"] <= 100
        assert out["completeness"] == 80.0  # 4/5 non-null

    def test_full_unique(self):
        s = pd.Series([1, 2, 3, 4, 5])
        out = calculate_quality_scores(s)
        assert out["completeness"] == 100.0
        assert out["uniqueness"] == 100.0
        assert 0 <= out["consistency"] <= 100


class TestIsCriticalDataElement:
    def test_user_specified_in_list(self):
        s = pd.Series([1, 2, 3])
        assert is_critical_data_element("id", s, ["id", "name"]) is True

    def test_user_specified_not_in_list(self):
        s = pd.Series([1, 2, 3])
        assert is_critical_data_element("other", s, ["id", "name"]) is False

    def test_auto_high_completeness(self):
        s = pd.Series([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])  # no nulls
        assert is_critical_data_element("x", s, []) == True

    def test_auto_low_completeness(self):
        s = pd.Series([1, None, None, None, None])  # 20% complete
        assert is_critical_data_element("x", s, []) == False
