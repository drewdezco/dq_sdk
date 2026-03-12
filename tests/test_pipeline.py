"""Unit and integration tests for data_quality.pipeline."""

import pytest
import pandas as pd
from data_quality import (
    compare_schema,
    compare_volume,
    detect_identical_or_stale,
    compare_snapshots,
    DataQualityChecker,
)


# -------- compare_schema --------


def test_compare_schema_no_changes():
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    out = compare_schema(df, df.copy(), check_dtypes=False)
    assert out["added"] == []
    assert out["removed"] == []
    assert out["type_changes"] == []


def test_compare_schema_added_removed():
    df_baseline = pd.DataFrame({"a": [1], "b": [2]})
    df_current = pd.DataFrame({"a": [1], "c": [3]})
    out = compare_schema(df_baseline, df_current, check_dtypes=False)
    assert out["added"] == ["c"]
    assert out["removed"] == ["b"]
    assert out["type_changes"] == []


def test_compare_schema_type_changes():
    df_baseline = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    df_current = pd.DataFrame({"a": [1.0, 2.0], "b": ["x", "y"]})
    out = compare_schema(df_baseline, df_current, check_dtypes=True)
    assert out["added"] == [] and out["removed"] == []
    assert len(out["type_changes"]) == 1
    assert out["type_changes"][0]["column"] == "a"
    assert "int" in out["type_changes"][0]["baseline_dtype"] or "int64" in out["type_changes"][0]["baseline_dtype"]
    assert "float" in out["type_changes"][0]["current_dtype"] or "float64" in out["type_changes"][0]["current_dtype"]


# -------- compare_volume --------


def test_compare_volume_basic():
    df_b = pd.DataFrame({"x": [1, 2, 3]})
    df_c = pd.DataFrame({"x": [1, 2, 3, 4, 5]})
    out = compare_volume(df_b, df_c, date_column=None)
    assert out["row_count_baseline"] == 3
    assert out["row_count_current"] == 5
    assert out["pct_change"] == pytest.approx(200.0 / 3, rel=1e-2)
    assert out["no_new_data"] is None


def test_compare_volume_no_new_data_by_date():
    df_b = pd.DataFrame({
        "id": [1, 2],
        "dt": pd.to_datetime(["2024-01-01", "2024-01-02"]),
    })
    df_c = pd.DataFrame({
        "id": [1, 2],
        "dt": pd.to_datetime(["2024-01-01", "2024-01-02"]),
    })
    out = compare_volume(df_b, df_c, date_column="dt")
    assert out["no_new_data"] is True


def test_compare_volume_new_data_by_date():
    df_b = pd.DataFrame({
        "id": [1],
        "dt": pd.to_datetime(["2024-01-01"]),
    })
    df_c = pd.DataFrame({
        "id": [1, 2],
        "dt": pd.to_datetime(["2024-01-01", "2024-01-03"]),
    })
    out = compare_volume(df_b, df_c, date_column="dt")
    assert out["no_new_data"] is False


def test_compare_volume_drop_pct():
    df_b = pd.DataFrame({"x": range(100)})
    df_c = pd.DataFrame({"x": range(80)})
    out = compare_volume(df_b, df_c)
    assert out["row_count_baseline"] == 100
    assert out["row_count_current"] == 80
    assert out["pct_change"] == -20.0


# -------- detect_identical_or_stale --------


def test_detect_identical_or_stale_different_row_count():
    df_b = pd.DataFrame({"id": [1, 2, 3]})
    df_c = pd.DataFrame({"id": [1, 2]})
    out = detect_identical_or_stale(df_b, df_c, key_column="id")
    assert out["stale_warning"] is False
    assert out["identical"] is False
    assert out["reason"] is None


def test_detect_identical_or_stale_same_row_count_no_key():
    df_b = pd.DataFrame({"x": [1, 2, 3]})
    df_c = pd.DataFrame({"x": [1, 2, 3]})
    out = detect_identical_or_stale(df_b, df_c, key_column=None)
    assert out["stale_warning"] is True
    assert "Same row count" in (out["reason"] or "")
    assert "no key column" in (out["reason"] or "").lower()


def test_detect_identical_or_stale_same_key_set():
    df_b = pd.DataFrame({"id": [1, 2, 3], "v": [10, 20, 30]})
    df_c = pd.DataFrame({"id": [1, 2, 3], "v": [10, 20, 99]})
    out = detect_identical_or_stale(df_b, df_c, key_column="id")
    assert out["identical"] is True
    assert out["stale_warning"] is True
    assert "identical or nearly identical" in (out["reason"] or "").lower()


def test_detect_identical_or_stale_different_key_set():
    df_b = pd.DataFrame({"id": [1, 2, 3], "v": [10, 20, 30]})
    df_c = pd.DataFrame({"id": [1, 2, 4], "v": [10, 20, 40]})
    out = detect_identical_or_stale(df_b, df_c, key_column="id")
    assert out["stale_warning"] is False
    assert out["identical"] is False


def test_detect_identical_or_stale_multi_column_key_identical():
    df_b = pd.DataFrame({"a": [1, 1], "b": [10, 20], "v": [100, 200]})
    df_c = pd.DataFrame({"a": [1, 1], "b": [10, 20], "v": [100, 999]})
    out = detect_identical_or_stale(df_b, df_c, key_column=["a", "b"])
    assert out["identical"] is True
    assert out["stale_warning"] is True


# -------- compare_snapshots --------


def _simple_rules_runner(df, results):
    c = DataQualityChecker(df, dataset_name="")
    c.df = df
    c.results = results
    c.expect_column_values_to_not_be_null("id")
    c.expect_column_values_to_be_unique("id")


def test_compare_snapshots_passed_no_thresholds():
    df_b = pd.DataFrame({"id": [1, 2, 3], "x": [10, 20, 30]})
    df_c = pd.DataFrame({"id": [1, 2, 3, 4], "x": [10, 20, 30, 40]})
    out = compare_snapshots(
        df_b, df_c, _simple_rules_runner,
        warn_on_stale=False,
    )
    assert out["passed"] is True
    assert "comparison" in out
    assert "schema_changes" in out
    assert "volume" in out
    assert out["volume"]["row_count_baseline"] == 3
    assert out["volume"]["row_count_current"] == 4
    assert out["schema_changes"]["added"] == [] and out["schema_changes"]["removed"] == []


def test_compare_snapshots_fail_on_schema_change():
    df_b = pd.DataFrame({"id": [1, 2], "a": [10, 20]})
    df_c = pd.DataFrame({"id": [1, 2], "b": [10, 20]})
    out = compare_snapshots(
        df_b, df_c, _simple_rules_runner,
        fail_on_schema_change=True,
        warn_on_stale=False,
    )
    assert out["passed"] is False
    assert any("Schema change" in w for w in out["warnings"])
    assert "a" in out["schema_changes"]["removed"]
    assert "b" in out["schema_changes"]["added"]


def test_compare_snapshots_fail_on_volume_drop():
    df_b = pd.DataFrame({"id": [1, 2, 3, 4, 5], "x": range(5)})
    df_c = pd.DataFrame({"id": [1, 2], "x": [0, 1]})
    out = compare_snapshots(
        df_b, df_c, _simple_rules_runner,
        fail_on_volume_drop_pct=-30,
        warn_on_stale=False,
    )
    assert out["passed"] is False
    assert any("Volume drop" in w for w in out["warnings"])
    assert out["volume"]["pct_change"] == -60.0


def test_compare_snapshots_fail_on_min_overall_health():
    df_b = pd.DataFrame({"id": [1, 2, 3], "x": [10, 20, 30]})
    df_c = pd.DataFrame({"id": [1, 2, None], "x": [10, 20, 30]})
    out = compare_snapshots(
        df_b, df_c, _simple_rules_runner,
        min_overall_health=100.0,
        warn_on_stale=False,
    )
    assert out["passed"] is False
    assert out["below_threshold"]["overall"] is True
    assert any("below threshold" in w.lower() for w in out["warnings"])


def test_compare_snapshots_warn_on_stale():
    df_b = pd.DataFrame({"id": [1, 2, 3], "x": [10, 20, 30]})
    df_c = pd.DataFrame({"id": [1, 2, 3], "x": [10, 20, 30]})
    out = compare_snapshots(
        df_b, df_c, _simple_rules_runner,
        warn_on_stale=True,
        stale_key_column="id",
    )
    assert out["stale"]["stale_warning"] is True
    assert out["stale"]["identical"] is True
    assert any("identical" in w.lower() or "stale" in w.lower() for w in out["warnings"])
    assert out["passed"] is True


def test_compare_snapshots_min_per_dimension():
    df_b = pd.DataFrame({"id": [1, 2, 3], "status": ["a", "a", "a"]})
    df_c = pd.DataFrame({"id": [1, 2, None], "status": ["a", "b", "c"]})
    out = compare_snapshots(
        df_b, df_c, _simple_rules_runner,
        min_per_dimension={"Completeness": 100.0},
        warn_on_stale=False,
    )
    assert out["passed"] is False
    assert "Completeness" in out["below_threshold"]["dimensions"]
