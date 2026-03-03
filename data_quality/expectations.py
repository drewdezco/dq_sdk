"""
Expectation functions for data quality checks.
Each function takes (df, results, ...) and appends one result dict to results.
Single-column and multi-column expectations; use utils.normalize_columns for column lists.
"""

import re
import pandas as pd
from data_quality.utils import normalize_columns


def _record(results, column, rule, success_rate, details):
    """Append a result dict to the results list."""
    results.append({
        "column": column,
        "rule": rule,
        "success_rate": success_rate,
        "details": details,
    })


def expect_column_values_to_not_be_null(df, results, column):
    total = len(df[column])
    missing = df[column].isnull().sum()
    passed = total - missing
    success_rate = (passed / total) * 100 if total > 0 else 0
    _record(
        results, column, "not null", success_rate,
        {"total": total, "passed": passed, "failed": missing},
    )


def expect_column_values_to_be_unique(df, results, column):
    total = len(df[column])
    value_counts = df[column].value_counts()
    duplicate_values = value_counts[value_counts > 1]
    failed = duplicate_values.sum()
    passed = total - failed
    success_rate = (passed / total) * 100 if total > 0 else 0
    _record(
        results, column, "unique", success_rate,
        {"total": total, "passed": passed, "failed": failed},
    )


def expect_column_values_to_be_in_set(df, results, column, allowed_values):
    total = len(df[column])
    invalid_mask = ~df[column].isin(allowed_values)
    failed = invalid_mask.sum()
    passed = total - failed
    success_rate = (passed / total) * 100 if total > 0 else 0
    _record(
        results, column, "in allowed set", success_rate,
        {"total": total, "passed": passed, "failed": failed, "allowed_values": allowed_values},
    )


def expect_column_values_to_match_regex(df, results, column, pattern):
    regex = re.compile(pattern)
    total = len(df[column])
    null_count = df[column].isnull().sum()
    non_matching = df[column].dropna().apply(
        lambda x: not bool(regex.match(str(x)))
    ).sum()
    failed = null_count + non_matching
    passed = total - failed
    success_rate = (passed / total) * 100 if total > 0 else 0
    _record(
        results, column, "matches regex", success_rate,
        {"total": total, "passed": passed, "failed": failed, "pattern": pattern},
    )


def expect_column_values_to_be_in_range(df, results, column, min_val, max_val):
    total = len(df[column])
    null_count = df[column].isnull().sum()
    out_of_range = df[column].dropna().apply(
        lambda x: not (min_val <= x <= max_val)
    ).sum()
    failed = null_count + out_of_range
    passed = total - failed
    success_rate = (passed / total) * 100 if total > 0 else 0
    _record(
        results, column, f"in range {min_val}-{max_val}", success_rate,
        {"total": total, "passed": passed, "failed": failed, "range": (min_val, max_val)},
    )


def expect_column_values_to_be_in_date_range(df, results, column, min_date, max_date):
    dates = pd.to_datetime(df[column], errors="coerce")
    total = dates.notnull().sum()
    min_dt = pd.to_datetime(min_date)
    max_dt = pd.to_datetime(max_date)
    if total > 0:
        sample_date = dates.dropna().iloc[0] if not dates.dropna().empty else None
        if sample_date is not None:
            data_has_tz = hasattr(sample_date, "tz") and sample_date.tz is not None
            bounds_have_tz = hasattr(min_dt, "tz") and min_dt.tz is not None
            if data_has_tz and not bounds_have_tz:
                min_dt = min_dt.tz_localize("UTC")
                max_dt = max_dt.tz_localize("UTC")
            elif not data_has_tz and bounds_have_tz:
                min_dt = min_dt.tz_localize(None) if hasattr(min_dt, "tz_localize") else min_dt.replace(tzinfo=None)
                max_dt = max_dt.tz_localize(None) if hasattr(max_dt, "tz_localize") else max_dt.replace(tzinfo=None)
            elif data_has_tz and bounds_have_tz:
                data_tz = sample_date.tz
                min_dt = min_dt.tz_convert(data_tz)
                max_dt = max_dt.tz_convert(data_tz)
    out_of_range = ((dates < min_dt) | (dates > max_dt)).sum()
    passed = total - out_of_range
    success_rate = (passed / total) * 100 if total > 0 else 0
    _record(
        results, column, "in date range", success_rate,
        {"total": total, "passed": passed, "failed": out_of_range, "range": (min_date, max_date)},
    )


# -------- Multi-Column Expectations --------


def expect_columns_values_to_not_be_null(df, results, columns):
    """Check that values in multiple columns are not null (all columns non-null per row)."""
    columns = normalize_columns(columns)
    total = len(df)
    null_mask = df[columns].isnull().any(axis=1)
    failed = null_mask.sum()
    passed = total - failed
    success_rate = (passed / total) * 100 if total > 0 else 0
    column_str = ", ".join(columns)
    _record(
        results, column_str, "all not null", success_rate,
        {"total": total, "passed": passed, "failed": failed, "columns": columns},
    )


def expect_columns_values_to_be_unique(df, results, columns):
    """Check that the combination of values across multiple columns is unique."""
    columns = normalize_columns(columns)
    total = len(df)
    duplicate_mask = df.duplicated(subset=columns, keep=False)
    failed = duplicate_mask.sum()
    passed = total - failed
    success_rate = (passed / total) * 100 if total > 0 else 0
    column_str = ", ".join(columns)
    _record(
        results, column_str, "unique combination", success_rate,
        {"total": total, "passed": passed, "failed": failed, "columns": columns},
    )


def expect_columns_values_to_be_in_sets(df, results, columns, allowed_values):
    """
    Check that values in multiple columns are in their respective allowed sets.
    allowed_values: dict mapping column names to allowed values, or list of lists (one per column).
    """
    columns = normalize_columns(columns)
    total = len(df)
    if isinstance(allowed_values, list):
        if len(allowed_values) != len(columns):
            raise ValueError("Number of allowed value lists must match number of columns")
        allowed_values_dict = {col: values for col, values in zip(columns, allowed_values)}
    else:
        allowed_values_dict = allowed_values
    invalid_mask = pd.Series([False] * total)
    for col in columns:
        if col in allowed_values_dict:
            col_invalid = ~df[col].isin(allowed_values_dict[col])
            invalid_mask = invalid_mask | col_invalid
    failed = invalid_mask.sum()
    passed = total - failed
    success_rate = (passed / total) * 100 if total > 0 else 0
    column_str = ", ".join(columns)
    _record(
        results, column_str, "all in allowed sets", success_rate,
        {"total": total, "passed": passed, "failed": failed, "columns": columns, "allowed_values": allowed_values_dict},
    )


def expect_columns_values_to_match_patterns(df, results, columns, patterns_dict):
    """Check that values in multiple columns match their respective regex patterns."""
    columns = normalize_columns(columns)
    total = len(df)
    invalid_mask = pd.Series([False] * total)
    for col in columns:
        if col in patterns_dict:
            regex = re.compile(patterns_dict[col])
            null_mask = df[col].isnull()
            non_matching = df[col].dropna().apply(
                lambda x: not bool(regex.match(str(x)))
            )
            col_invalid = null_mask.copy()
            col_invalid.loc[~null_mask] = non_matching
            invalid_mask = invalid_mask | col_invalid
    failed = invalid_mask.sum()
    passed = total - failed
    success_rate = (passed / total) * 100 if total > 0 else 0
    column_str = ", ".join(columns)
    _record(
        results, column_str, "all match patterns", success_rate,
        {"total": total, "passed": passed, "failed": failed, "columns": columns, "patterns": patterns_dict},
    )


def expect_columns_values_to_be_in_ranges(df, results, columns, ranges_dict):
    """Check that values in multiple columns are in their respective (min, max) ranges."""
    columns = normalize_columns(columns)
    total = len(df)
    invalid_mask = pd.Series([False] * total)
    for col in columns:
        if col in ranges_dict:
            min_val, max_val = ranges_dict[col]
            null_mask = df[col].isnull()
            out_of_range = df[col].dropna().apply(lambda x: not (min_val <= x <= max_val))
            col_invalid = null_mask.copy()
            col_invalid.loc[~null_mask] = out_of_range
            invalid_mask = invalid_mask | col_invalid
    failed = invalid_mask.sum()
    passed = total - failed
    success_rate = (passed / total) * 100 if total > 0 else 0
    column_str = ", ".join(columns)
    _record(
        results, column_str, "all in ranges", success_rate,
        {"total": total, "passed": passed, "failed": failed, "columns": columns, "ranges": ranges_dict},
    )
