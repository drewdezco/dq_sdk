"""Shared pytest fixtures for data_quality tests."""

import pytest
import pandas as pd


@pytest.fixture
def sample_df():
    """Small DataFrame (3-5 rows) with id, name, score, date for reuse."""
    return pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "Dave", "Eve"],
        "score": [10, 20, 30, 40, 50],
        "date": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"]),
    })


@pytest.fixture
def empty_results():
    """Mutable empty list for results (tests do fn(df, results, ...); assert len(results))."""
    return []


@pytest.fixture
def sample_results():
    """List of 1-2 result dicts (column, rule, success_rate, details, dimension) for reporting tests."""
    return [
        {
            "column": "id",
            "rule": "not null",
            "success_rate": 100.0,
            "details": {"total": 5, "passed": 5, "failed": 0},
            "dimension": "Completeness",
        },
        {
            "column": "name",
            "rule": "unique",
            "success_rate": 100.0,
            "details": {"total": 5, "passed": 5, "failed": 0},
            "dimension": "Uniqueness",
        },
    ]
