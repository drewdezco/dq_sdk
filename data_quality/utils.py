"""
Shared utilities for the data_quality package.
Pure functions for column normalization, Levenshtein distance/similarity,
data type classification, quality scores, and critical-element detection.
"""

import re
import pandas as pd


def normalize_columns(columns):
    """Normalize columns input to a list (string, list, or tuple -> list)."""
    if isinstance(columns, str):
        return [columns]
    if isinstance(columns, (list, tuple)):
        return list(columns)
    raise ValueError("Columns must be a string, list, or tuple")


def levenshtein_distance(s1, s2):
    """Calculate Levenshtein distance between two strings."""
    s1, s2 = str(s1), str(s2)
    if len(s1) < len(s2):
        return levenshtein_distance(s2, s1)
    if len(s2) == 0:
        return len(s1)
    previous_row = list(range(len(s2) + 1))
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            insertions = previous_row[j + 1] + 1
            deletions = current_row[j] + 1
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row
    return previous_row[-1]


def levenshtein_ratio(s1, s2):
    """Calculate Levenshtein similarity ratio between two strings (0-1)."""
    s1, s2 = str(s1), str(s2)
    distance = levenshtein_distance(s1, s2)
    max_len = max(len(s1), len(s2))
    return (max_len - distance) / max_len if max_len > 0 else 1.0


def classify_data_type(col_data):
    """Classify column data type with user-friendly names (e.g. Date/Time, Text/String)."""
    if pd.api.types.is_datetime64_any_dtype(col_data):
        return "Date/Time"
    if pd.api.types.is_bool_dtype(col_data):
        return "Boolean"
    if pd.api.types.is_numeric_dtype(col_data):
        if "int" in str(col_data.dtype).lower():
            return "Integer"
        if "float" in str(col_data.dtype).lower():
            return "Decimal"
        return "Numeric"
    if pd.api.types.is_object_dtype(col_data):
        sample = col_data.dropna().head(100)
        if len(sample) > 0:
            bool_count = sum(1 for x in sample if isinstance(x, bool))
            if bool_count == len(sample):
                return "Boolean"
            bool_string_count = sum(
                1 for x in sample if str(x).lower() in ["true", "false", "1", "0"]
            )
            if bool_string_count == len(sample):
                return "Boolean"
            if all(isinstance(x, str) for x in sample):
                return "Text/String"
        return "Text/String"
    dtype_str = str(col_data.dtype).lower()
    if "category" in dtype_str:
        return "Category"
    return dtype_str.title()


def calculate_quality_scores(col_data):
    """
    Calculate completeness, uniqueness, and consistency scores for a column.
    Returns dict with keys: completeness, uniqueness, consistency (0-100).
    """
    total_count = len(col_data)
    null_count = col_data.isnull().sum()
    distinct_count = col_data.nunique()
    completeness = ((total_count - null_count) / total_count) * 100 if total_count > 0 else 0
    uniqueness = (distinct_count / total_count) * 100 if total_count > 0 else 0
    consistency = 100.0
    non_null_data = col_data.dropna()

    if len(non_null_data) == 0:
        consistency = 0.0
    elif pd.api.types.is_object_dtype(col_data):
        sample = non_null_data.head(200)
        issues_count = 0
        total_checks = len(sample)
        if len(sample) > 0:
            type_counts = {}
            for item in sample:
                item_type = type(item).__name__
                type_counts[item_type] = type_counts.get(item_type, 0) + 1
            if len(type_counts) > 1:
                minority_types = sum(
                    count
                    for type_name, count in type_counts.items()
                    if type_name != max(type_counts, key=type_counts.get)
                )
                if minority_types / len(sample) > 0.1:
                    issues_count += minority_types
            if any(
                "email" in str(col_data.name).lower() or "@" in str(val)
                for val in sample[:5]
            ):
                email_pattern = re.compile(
                    r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
                )
                for val in sample:
                    if "@" in str(val) and not email_pattern.match(str(val)):
                        issues_count += 1
            if len(sample) > 5:
                str_lengths = [len(str(val)) for val in sample]
                mean_length = sum(str_lengths) / len(str_lengths)
                for length in str_lengths:
                    if (
                        abs(length - mean_length) > mean_length * 0.5
                        and mean_length > 5
                    ):
                        issues_count += 0.5
            if sample.dtype == "object":
                case_patterns = {"upper": 0, "lower": 0, "title": 0, "mixed": 0}
                for val in sample:
                    str_val = str(val)
                    if str_val.isalpha():
                        if str_val.isupper():
                            case_patterns["upper"] += 1
                        elif str_val.islower():
                            case_patterns["lower"] += 1
                        elif str_val.istitle():
                            case_patterns["title"] += 1
                        else:
                            case_patterns["mixed"] += 1
                total_alpha = sum(case_patterns.values())
                if total_alpha > 5:
                    patterns_used = sum(
                        1 for count in case_patterns.values() if count > 0
                    )
                    if patterns_used > 2:
                        issues_count += total_alpha * 0.1
        consistency = max(0, 100 - (issues_count / total_checks * 100))
    elif pd.api.types.is_numeric_dtype(col_data):
        if len(non_null_data) > 2:
            try:
                Q1 = non_null_data.quantile(0.25)
                Q3 = non_null_data.quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                outliers = non_null_data[
                    (non_null_data < lower_bound) | (non_null_data > upper_bound)
                ]
                outlier_percentage = len(outliers) / len(non_null_data) * 100
                consistency = max(70, 100 - outlier_percentage * 2)
            except Exception:
                consistency = 95.0
    else:
        consistency = 95.0

    return {
        "completeness": round(completeness, 1),
        "uniqueness": round(uniqueness, 1),
        "consistency": round(consistency, 1),
    }


def is_critical_data_element(column_name, col_data, user_specified_critical_columns):
    """
    Determine if a column is a critical data element.
    If user_specified_critical_columns is non-empty, use that list.
    Otherwise use automatic determination (completeness >= 80, density >= 0.7, etc.).
    """
    if user_specified_critical_columns:
        return column_name in user_specified_critical_columns
    total_count = len(col_data)
    null_count = col_data.isnull().sum()
    completeness = (
        ((total_count - null_count) / total_count) * 100 if total_count > 0 else 0
    )
    data_density = (total_count - null_count) / total_count if total_count > 0 else 0
    return (
        completeness >= 80
        and data_density >= 0.7
        and null_count < total_count * 0.5
    )
