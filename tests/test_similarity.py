"""Unit tests for data_quality.similarity."""

import pytest
import pandas as pd
from data_quality import similarity as sim


def test_analyze_column_similarity_levenshtein_identical():
    df = pd.DataFrame({"a": ["x", "y", "z"], "b": ["x", "y", "z"]})
    results = []
    out = sim.analyze_column_similarity_levenshtein(df, results, "a", "b", similarity_threshold=0.8)
    assert len(results) == 1
    assert results[0]["column"] == "a vs b"
    assert "levenshtein similarity" in results[0]["rule"]
    assert out["total_comparisons"] == 3
    assert out["similarity_percentage"] == 100.0
    assert "statistics" in out
    assert "detailed_comparisons" in out
    assert len(out["detailed_comparisons"]) == 3


def test_analyze_column_similarity_levenshtein_different():
    df = pd.DataFrame({"a": ["foo", "bar"], "b": ["xyz", "abc"]})
    results = []
    out = sim.analyze_column_similarity_levenshtein(df, results, "a", "b")
    assert len(results) == 1
    assert out["total_comparisons"] == 2
    assert out["similarity_percentage"] < 100


def test_get_similarity_summary_table_empty():
    out = sim.get_similarity_summary_table([])
    assert out.empty


def test_get_similarity_summary_table_one_result():
    df = pd.DataFrame({"x": ["a"], "y": ["a"]})
    results = []
    sim.analyze_column_similarity_levenshtein(df, results, "x", "y")
    table = sim.get_similarity_summary_table(results)
    assert not table.empty
    assert "Column_Pair" in table.columns
    assert "Total_Comparisons" in table.columns
    assert len(table) == 1
    assert table.iloc[0]["Column_Pair"] == "x vs y"


def test_get_detailed_similarity_comparisons_found():
    df = pd.DataFrame({"p": ["aa", "ab"], "q": ["aa", "ab"]})
    results = []
    sim.analyze_column_similarity_levenshtein(df, results, "p", "q")
    detail = sim.get_detailed_similarity_comparisons(results, "p", "q", min_similarity=0.5, max_similarity=1.0)
    assert not detail.empty
    assert "similarity_ratio" in detail.columns
    assert len(detail) == 2


def test_get_detailed_similarity_comparisons_not_found():
    out = sim.get_detailed_similarity_comparisons([], "a", "b")
    assert out.empty


def test_get_detailed_similarity_comparisons_filter_range():
    df = pd.DataFrame({"a": ["x", "y", "z"], "b": ["x", "yy", "zzz"]})
    results = []
    sim.analyze_column_similarity_levenshtein(df, results, "a", "b")
    detail = sim.get_detailed_similarity_comparisons(
        results, "a", "b", min_similarity=0.0, max_similarity=0.5
    )
    assert "similarity_ratio" in detail.columns
    assert (detail["similarity_ratio"] <= 0.5).all()
