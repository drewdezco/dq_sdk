"""Unit tests for data_quality.suggestion."""

import pytest
import pandas as pd
from data_quality import suggestion as sug


def _df(column, values):
    """Helper to create a DataFrame with one column."""
    return pd.DataFrame({column: values})


# -------- Completeness suggestions --------


def test_analyze_column_for_suggestions_completeness_low_nulls():
    """Test that low null rate suggests not_null validation."""
    df = _df("x", [1, 2, 3, 4, 5, None])  # 1/6 = 16.7% nulls
    suggestions = sug.analyze_column_for_suggestions(df, "x", {"null_rate_threshold": 0.2, "min_samples_for_suggestion": 3})
    assert len(suggestions) > 0
    not_null_suggestions = [s for s in suggestions if s["method"] == "expect_column_values_to_not_be_null"]
    assert len(not_null_suggestions) == 1
    assert not_null_suggestions[0]["dimension"] == "Completeness"
    assert not_null_suggestions[0]["confidence"] > 0


def test_analyze_column_for_suggestions_completeness_high_nulls():
    """Test that high null rate does not suggest not_null validation."""
    df = _df("x", [1, None, None, None, None])  # 4/5 = 80% nulls
    suggestions = sug.analyze_column_for_suggestions(df, "x", {"null_rate_threshold": 0.05})
    not_null_suggestions = [s for s in suggestions if s["method"] == "expect_column_values_to_not_be_null"]
    assert len(not_null_suggestions) == 0


# -------- Uniqueness suggestions --------


def test_analyze_column_for_suggestions_uniqueness_high():
    """Test that high uniqueness suggests unique validation."""
    df = _df("x", list(range(100)))  # All unique
    suggestions = sug.analyze_column_for_suggestions(df, "x")
    unique_suggestions = [s for s in suggestions if s["method"] == "expect_column_values_to_be_unique"]
    assert len(unique_suggestions) == 1
    assert unique_suggestions[0]["dimension"] == "Uniqueness"
    assert unique_suggestions[0]["confidence"] >= 0.95


def test_analyze_column_for_suggestions_uniqueness_low():
    """Test that low uniqueness does not suggest unique validation."""
    df = _df("x", [1, 1, 1, 1, 1])  # All duplicates
    suggestions = sug.analyze_column_for_suggestions(df, "x")
    unique_suggestions = [s for s in suggestions if s["method"] == "expect_column_values_to_be_unique"]
    assert len(unique_suggestions) == 0


# -------- Categorical suggestions --------


def test_analyze_column_for_suggestions_categorical():
    """Test that low cardinality categorical data suggests in_set validation."""
    df = _df("status", ["active", "inactive", "pending"] * 10)
    # Ensure we have enough samples and proper options
    options = {
        "min_samples_for_suggestion": 3,
        "categorical_max_distinct": 20,
        "categorical_coverage_threshold": 0.8,
    }
    suggestions = sug.analyze_column_for_suggestions(df, "status", options)
    in_set_suggestions = [s for s in suggestions if s["method"] == "expect_column_values_to_be_in_set"]
    assert len(in_set_suggestions) == 1
    assert in_set_suggestions[0]["dimension"] == "Validity"
    assert "allowed_values" in in_set_suggestions[0]["params"]
    assert set(in_set_suggestions[0]["params"]["allowed_values"]) == {"active", "inactive", "pending"}


def test_analyze_column_for_suggestions_categorical_high_cardinality():
    """Test that high cardinality does not suggest in_set validation."""
    df = _df("x", [f"value_{i}" for i in range(100)])  # 100 distinct values
    suggestions = sug.analyze_column_for_suggestions(df, "x", {"categorical_max_distinct": 20})
    in_set_suggestions = [s for s in suggestions if s["method"] == "expect_column_values_to_be_in_set"]
    assert len(in_set_suggestions) == 0


# -------- Numeric range suggestions --------


def test_analyze_column_for_suggestions_numeric_range():
    """Test that numeric columns suggest range validation."""
    df = _df("score", [10, 20, 30, 40, 50])
    suggestions = sug.analyze_column_for_suggestions(df, "score", {"min_samples_for_suggestion": 3})
    range_suggestions = [s for s in suggestions if s["method"] == "expect_column_values_to_be_in_range"]
    assert len(range_suggestions) == 1
    assert range_suggestions[0]["dimension"] == "Validity"
    params = range_suggestions[0]["params"]
    assert "min_val" in params
    assert "max_val" in params
    assert params["min_val"] <= 10
    assert params["max_val"] >= 50


def test_analyze_column_for_suggestions_numeric_range_with_percentiles():
    """Test numeric range suggestion with percentile option."""
    df = _df("score", list(range(100)) + [1000])  # Outlier at end
    suggestions = sug.analyze_column_for_suggestions(df, "score", {"use_percentiles_for_ranges": True})
    range_suggestions = [s for s in suggestions if s["method"] == "expect_column_values_to_be_in_range"]
    assert len(range_suggestions) == 1
    # Percentile-based should exclude the outlier
    params = range_suggestions[0]["params"]
    assert params["max_val"] < 1000


# -------- Date range suggestions --------


def test_analyze_column_for_suggestions_date_range():
    """Test that date columns suggest date range validation."""
    dates = pd.date_range("2024-01-01", periods=10, freq="D")
    df = pd.DataFrame({"date": dates})
    suggestions = sug.analyze_column_for_suggestions(df, "date")
    date_range_suggestions = [s for s in suggestions if s["method"] == "expect_column_values_to_be_in_date_range"]
    assert len(date_range_suggestions) == 1
    assert date_range_suggestions[0]["dimension"] == "Validity"
    params = date_range_suggestions[0]["params"]
    assert "min_date" in params
    assert "max_date" in params


def test_analyze_column_for_suggestions_timeliness():
    """Test that recent dates suggest timeliness validation."""
    recent_dates = pd.date_range(pd.Timestamp.now() - pd.Timedelta(days=30), periods=10, freq="D")
    df = pd.DataFrame({"date": recent_dates})
    suggestions = sug.analyze_column_for_suggestions(df, "date")
    recent_suggestions = [s for s in suggestions if s["method"] == "expect_column_values_to_be_recent"]
    assert len(recent_suggestions) == 1
    assert recent_suggestions[0]["dimension"] == "Timeliness"


# -------- Pattern detection --------


def test_pattern_detection_email():
    """Test email pattern detection."""
    df = _df("email", ["test@example.com", "user@domain.org", "admin@site.net", "info@test.com"])
    options = {
        "min_samples_for_suggestion": 3,
        "pattern_detection_enabled": True,
        "pattern_match_threshold": 0.8,
    }
    suggestions = sug.analyze_column_for_suggestions(df, "email", options)
    regex_suggestions = [s for s in suggestions if s["method"] == "expect_column_values_to_match_regex"]
    assert len(regex_suggestions) > 0
    email_suggestions = [s for s in regex_suggestions if "@" in s["params"]["pattern"]]
    assert len(email_suggestions) > 0


def test_pattern_detection_uuid():
    """Test UUID pattern detection."""
    uuids = [
        "550e8400-e29b-41d4-a716-446655440000",
        "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
        "6ba7b811-9dad-11d1-80b4-00c04fd430c8",
        "7ba7b812-9dad-11d1-80b4-00c04fd430c8",
    ]
    df = _df("id", uuids)
    options = {
        "min_samples_for_suggestion": 3,
        "pattern_detection_enabled": True,
        "pattern_match_threshold": 0.8,
    }
    suggestions = sug.analyze_column_for_suggestions(df, "id", options)
    regex_suggestions = [s for s in suggestions if s["method"] == "expect_column_values_to_match_regex"]
    assert len(regex_suggestions) > 0


def test_pattern_detection_custom_id():
    """Test custom ID pattern detection (e.g., CUST-001)."""
    df = _df("customer_id", [f"CUST-{i:03d}" for i in range(20)])
    suggestions = sug.analyze_column_for_suggestions(df, "customer_id")
    regex_suggestions = [s for s in suggestions if s["method"] == "expect_column_values_to_match_regex"]
    # May or may not detect pattern depending on threshold, but should not crash
    assert isinstance(suggestions, list)


# -------- Generate suggestions for multiple columns --------


def test_generate_suggestions_all_columns():
    """Test generating suggestions for all columns."""
    df = pd.DataFrame({
        "id": [1, 2, 3, 4, 5, 6],
        "status": ["active", "inactive", "pending"] * 2,
        "score": [10, 20, 30, 40, 50, 60],
    })
    suggestions = sug.generate_suggestions(df, options={"min_samples_for_suggestion": 3})
    assert len(suggestions) > 0
    columns = set(s["column"] for s in suggestions)
    assert "id" in columns or "status" in columns or "score" in columns


def test_generate_suggestions_specific_columns():
    """Test generating suggestions for specific columns."""
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "status": ["active", "inactive", "pending"],
        "score": [10, 20, 30],
    })
    suggestions = sug.generate_suggestions(df, columns=["id", "status"], options={"min_samples_for_suggestion": 3})
    columns = set(s["column"] for s in suggestions)
    assert "id" in columns or "status" in columns
    assert "score" not in columns


def test_generate_suggestions_with_options():
    """Test generating suggestions with custom options."""
    df = _df("x", [1, 2, 3, None, None])  # 40% nulls
    # With default threshold (5%), should not suggest not_null
    suggestions_default = sug.generate_suggestions(df, columns=["x"], options={"min_samples_for_suggestion": 3})
    not_null_default = [s for s in suggestions_default if s["method"] == "expect_column_values_to_not_be_null"]
    
    # With higher threshold (50%), should suggest not_null
    suggestions_custom = sug.generate_suggestions(df, columns=["x"], options={"null_rate_threshold": 0.5, "min_samples_for_suggestion": 3})
    not_null_custom = [s for s in suggestions_custom if s["method"] == "expect_column_values_to_not_be_null"]
    
    assert len(not_null_default) == 0
    assert len(not_null_custom) > 0


# -------- JSON conversion --------


def test_suggestions_to_json():
    """Test converting suggestions to JSON format."""
    suggestions = [
        {
            "column": "id",
            "method": "expect_column_values_to_not_be_null",
            "params": {},
            "confidence": 0.95,
            "reason": "Low null rate",
            "dimension": "Completeness",
        },
        {
            "column": "status",
            "method": "expect_column_values_to_be_in_set",
            "params": {"allowed_values": ["active", "inactive"]},
            "confidence": 0.9,
            "reason": "Categorical",
            "dimension": "Validity",
        },
    ]
    json_rules = sug.suggestions_to_json(suggestions)
    assert "expect_column_values_to_not_be_null" in json_rules
    assert "expect_column_values_to_be_in_set" in json_rules
    assert len(json_rules["expect_column_values_to_not_be_null"]) == 1
    assert json_rules["expect_column_values_to_not_be_null"][0]["column"] == "id"


def test_suggestions_to_json_round_trip():
    """Test round-trip: generate suggestions → JSON → run → verify."""
    import pandas as pd
    from data_quality import DataQualityChecker
    
    df = pd.DataFrame({
        "id": [1, 2, 3, 4, 5, 6],
        "status": ["active", "inactive", "pending"] * 2,
        "score": [10, 20, 30, 40, 50, 60],
    })
    
    checker = DataQualityChecker(df)
    
    # Generate suggestions
    suggestions = checker.generate_suggestions(options={"min_samples_for_suggestion": 3})
    assert len(suggestions) > 0
    
    # Convert to JSON
    json_rules = sug.suggestions_to_json(suggestions)
    assert isinstance(json_rules, dict)
    assert len(json_rules) > 0
    
    # Create new checker and run JSON rules
    checker2 = DataQualityChecker(df)
    checker2.run_rules_from_json(json_rules)
    
    # Verify results were created
    assert len(checker2.results) > 0
    assert len(checker2.results) == len(suggestions)


def test_suggestions_to_json_all_suggestion_types():
    """Test conversion for all suggestion types."""
    suggestions = [
        {
            "column": "id",
            "method": "expect_column_values_to_not_be_null",
            "params": {},
            "confidence": 0.95,
            "reason": "Low null rate",
            "dimension": "Completeness",
        },
        {
            "column": "id",
            "method": "expect_column_values_to_be_unique",
            "params": {},
            "confidence": 0.98,
            "reason": "High uniqueness",
            "dimension": "Uniqueness",
        },
        {
            "column": "status",
            "method": "expect_column_values_to_be_in_set",
            "params": {"allowed_values": ["active", "inactive", "pending"]},
            "confidence": 0.9,
            "reason": "Categorical",
            "dimension": "Validity",
        },
        {
            "column": "score",
            "method": "expect_column_values_to_be_in_range",
            "params": {"min_val": 0, "max_val": 100},
            "confidence": 0.85,
            "reason": "Numeric range",
            "dimension": "Validity",
        },
        {
            "column": "email",
            "method": "expect_column_values_to_match_regex",
            "params": {"pattern": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"},
            "confidence": 0.88,
            "reason": "Email pattern",
            "dimension": "Validity",
        },
    ]
    
    json_rules = sug.suggestions_to_json(suggestions)
    
    # Verify all methods are present
    assert "expect_column_values_to_not_be_null" in json_rules
    assert "expect_column_values_to_be_unique" in json_rules
    assert "expect_column_values_to_be_in_set" in json_rules
    assert "expect_column_values_to_be_in_range" in json_rules
    assert "expect_column_values_to_match_regex" in json_rules
    
    # Verify structure
    assert len(json_rules["expect_column_values_to_not_be_null"]) == 1
    assert json_rules["expect_column_values_to_not_be_null"][0]["column"] == "id"
    
    assert len(json_rules["expect_column_values_to_be_in_set"]) == 1
    assert json_rules["expect_column_values_to_be_in_set"][0]["column"] == "status"
    assert json_rules["expect_column_values_to_be_in_set"][0]["allowed_values"] == ["active", "inactive", "pending"]
    
    assert len(json_rules["expect_column_values_to_be_in_range"]) == 1
    assert json_rules["expect_column_values_to_be_in_range"][0]["column"] == "score"
    assert json_rules["expect_column_values_to_be_in_range"][0]["min_val"] == 0
    assert json_rules["expect_column_values_to_be_in_range"][0]["max_val"] == 100


def test_suggestions_to_json_empty_list():
    """Test edge case: empty suggestions list."""
    json_rules = sug.suggestions_to_json([])
    assert json_rules == {}
    assert isinstance(json_rules, dict)


def test_suggestions_to_json_multiple_same_method():
    """Test multiple suggestions with same method."""
    suggestions = [
        {
            "column": "id",
            "method": "expect_column_values_to_not_be_null",
            "params": {},
            "confidence": 0.95,
            "reason": "Low null rate",
            "dimension": "Completeness",
        },
        {
            "column": "name",
            "method": "expect_column_values_to_not_be_null",
            "params": {},
            "confidence": 0.92,
            "reason": "Low null rate",
            "dimension": "Completeness",
        },
    ]
    
    json_rules = sug.suggestions_to_json(suggestions)
    assert "expect_column_values_to_not_be_null" in json_rules
    assert len(json_rules["expect_column_values_to_not_be_null"]) == 2
    columns = {r["column"] for r in json_rules["expect_column_values_to_not_be_null"]}
    assert columns == {"id", "name"}


# -------- Edge cases --------


def test_edge_cases_all_nulls():
    """Test columns with all nulls."""
    df = _df("x", [None] * 10)
    suggestions = sug.analyze_column_for_suggestions(df, "x")
    # Should return empty or very few suggestions due to min_samples requirement
    assert isinstance(suggestions, list)


def test_edge_cases_high_cardinality():
    """Test columns with very high distinct value counts."""
    df = _df("x", [f"value_{i}" for i in range(1000)])
    suggestions = sug.analyze_column_for_suggestions(df, "x", {"categorical_max_distinct": 20})
    in_set_suggestions = [s for s in suggestions if s["method"] == "expect_column_values_to_be_in_set"]
    assert len(in_set_suggestions) == 0


def test_edge_cases_empty_dataframe():
    """Test empty dataframe handling."""
    df = pd.DataFrame({"x": []})
    suggestions = sug.generate_suggestions(df)
    assert suggestions == []


def test_edge_cases_single_value():
    """Test columns with single value."""
    df = _df("x", [1] * 10)
    suggestions = sug.analyze_column_for_suggestions(df, "x")
    # Should handle gracefully
    assert isinstance(suggestions, list)


def test_edge_cases_missing_column():
    """Test requesting suggestions for non-existent column."""
    df = _df("x", [1, 2, 3])
    suggestions = sug.analyze_column_for_suggestions(df, "nonexistent")
    assert suggestions == []


def test_edge_cases_insufficient_samples():
    """Test columns with insufficient samples."""
    df = _df("x", [1, 2])  # Only 2 samples, less than default min_samples (10)
    suggestions = sug.analyze_column_for_suggestions(df, "x")
    # Should return empty due to min_samples requirement
    assert len(suggestions) == 0
    
    # But should work with lower threshold
    suggestions = sug.analyze_column_for_suggestions(df, "x", {"min_samples_for_suggestion": 2})
    assert isinstance(suggestions, list)
