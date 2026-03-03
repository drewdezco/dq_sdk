"""
Reporting: comprehensive results dict, CSV exports, and flattening.
All functions take (df, results, dataset_name, user_specified_critical_columns, ...) as needed.
Uses utils for classify_data_type, calculate_quality_scores, is_critical_data_element.
"""

import csv
import os
from datetime import datetime
import pandas as pd
from data_quality.utils import (
    classify_data_type,
    calculate_quality_scores,
    is_critical_data_element,
)


def get_comprehensive_results(
    df,
    results,
    dataset_name,
    user_specified_critical_columns,
    title="Data Quality Report",
):
    """
    Build a comprehensive data quality snapshot (metadata, key_metrics, overall_data_quality,
    critical_data_elements, other_fields, column_type_distribution, rule_execution_summary,
    detailed_results). Returns a dict or {"error": "..."} if no df.
    """
    if df is None:
        return {"error": "No dataframe available for analysis"}
    df_results = pd.DataFrame(results)
    total_checks = len(df_results)
    avg_pass_rate = (
        df_results["success_rate"].mean()
        if total_checks > 0 and "success_rate" in df_results.columns
        else 0
    )
    total_rows = len(df)
    total_columns = len(df.columns)
    total_cells = total_rows * total_columns
    null_cells = df.isnull().sum().sum()
    completeness_rate = (
        ((total_cells - null_cells) / total_cells) * 100 if total_cells > 0 else 0
    )

    critical_columns_list = []
    other_columns_list = []
    all_column_details = {}
    for column in df.columns:
        col_data = df[column]
        data_type = classify_data_type(col_data)
        quality_scores = calculate_quality_scores(col_data)
        is_critical = is_critical_data_element(
            column, col_data, user_specified_critical_columns
        )
        column_info = {
            "name": column,
            "data_type": data_type,
            "total_count": len(col_data),
            "null_count": col_data.isnull().sum(),
            "distinct_count": col_data.nunique(),
            "completeness": quality_scores["completeness"],
            "uniqueness": quality_scores["uniqueness"],
            "consistency": quality_scores["consistency"],
            "is_critical": is_critical,
        }
        if pd.api.types.is_numeric_dtype(col_data):
            column_info.update({
                "mean": round(col_data.mean(), 2) if not col_data.empty else None,
                "median": round(col_data.median(), 2) if not col_data.empty else None,
                "std": round(col_data.std(), 2) if not col_data.empty else None,
                "min": col_data.min() if not col_data.empty else None,
                "max": col_data.max() if not col_data.empty else None,
            })
        all_column_details[column] = column_info
        if is_critical:
            critical_columns_list.append(column_info)
        else:
            other_columns_list.append(column_info)

    completeness_scores = [info["completeness"] for info in all_column_details.values()]
    uniqueness_scores = [info["uniqueness"] for info in all_column_details.values()]
    consistency_scores = [info["consistency"] for info in all_column_details.values()]
    overall_completeness = (
        sum(completeness_scores) / len(completeness_scores) if completeness_scores else 0
    )
    overall_uniqueness = (
        sum(uniqueness_scores) / len(uniqueness_scores) if uniqueness_scores else 0
    )
    overall_consistency = (
        sum(consistency_scores) / len(consistency_scores) if consistency_scores else 0
    )
    type_distribution = {}
    for column_info in all_column_details.values():
        data_type = column_info["data_type"]
        type_distribution[data_type] = type_distribution.get(data_type, 0) + 1

    def get_health_status(rate):
        if rate >= 90:
            return "Excellent"
        if rate >= 75:
            return "Good"
        if rate >= 50:
            return "Fair"
        return "Poor"

    rule_health_distribution = {"excellent": 0, "good": 0, "fair": 0, "poor": 0}
    if total_checks > 0 and "success_rate" in df_results.columns:
        for rate in df_results["success_rate"]:
            if rate >= 90:
                rule_health_distribution["excellent"] += 1
            elif rate >= 75:
                rule_health_distribution["good"] += 1
            elif rate >= 50:
                rule_health_distribution["fair"] += 1
            else:
                rule_health_distribution["poor"] += 1

    comprehensive_snapshot = {
        "metadata": {
            "timestamp": datetime.now().isoformat(),
            "title": title,
            "dataset_name": dataset_name,
        },
        "key_metrics": {
            "total_records": total_rows,
            "total_columns": total_columns,
            "total_cells": total_cells,
            "null_cells": null_cells,
            "data_completeness_rate": round(completeness_rate, 1),
            "total_rules_executed": total_checks,
            "overall_health_score": round(avg_pass_rate, 1),
            "overall_health_status": get_health_status(avg_pass_rate),
        },
        "overall_data_quality": {
            "completeness": round(overall_completeness, 1),
            "uniqueness": round(overall_uniqueness, 1),
            "consistency": round(overall_consistency, 1),
            "combined_score": round(
                (overall_completeness + overall_uniqueness + overall_consistency) / 3, 1
            ),
        },
        "critical_data_elements": {
            "count": len(critical_columns_list),
            "columns": critical_columns_list,
        },
        "other_fields": {
            "count": len(other_columns_list),
            "columns": other_columns_list,
        },
        "column_type_distribution": type_distribution,
        "rule_execution_summary": {
            "total_rules": total_checks,
            "excellent_rules": rule_health_distribution["excellent"],
            "good_rules": rule_health_distribution["good"],
            "fair_rules": rule_health_distribution["fair"],
            "poor_rules": rule_health_distribution["poor"],
            "health_percentages": {
                "excellent": round(
                    (rule_health_distribution["excellent"] / total_checks) * 100, 1
                )
                if total_checks > 0
                else 0,
                "good": round(
                    (rule_health_distribution["good"] / total_checks) * 100, 1
                )
                if total_checks > 0
                else 0,
                "fair": round(
                    (rule_health_distribution["fair"] / total_checks) * 100, 1
                )
                if total_checks > 0
                else 0,
                "poor": round(
                    (rule_health_distribution["poor"] / total_checks) * 100, 1
                )
                if total_checks > 0
                else 0,
            },
        },
        "detailed_results": (
            df_results.to_dict("records") if not df_results.empty else []
        ),
    }
    return comprehensive_snapshot


def flatten_comprehensive_results(results_dict):
    """Flatten the nested comprehensive results dict into a single row for CSV export."""
    flattened = {}
    flattened.update({
        "timestamp": results_dict["metadata"]["timestamp"],
        "title": results_dict["metadata"]["title"],
        "dataset_name": results_dict["metadata"]["dataset_name"],
    })
    key_metrics = results_dict["key_metrics"]
    flattened.update({
        "total_records": key_metrics["total_records"],
        "total_columns": key_metrics["total_columns"],
        "total_cells": key_metrics["total_cells"],
        "null_cells": key_metrics["null_cells"],
        "data_completeness_rate": key_metrics["data_completeness_rate"],
        "total_rules_executed": key_metrics["total_rules_executed"],
        "overall_health_score": key_metrics["overall_health_score"],
        "overall_health_status": key_metrics["overall_health_status"],
    })
    quality = results_dict["overall_data_quality"]
    flattened.update({
        "overall_completeness": quality["completeness"],
        "overall_uniqueness": quality["uniqueness"],
        "overall_consistency": quality["consistency"],
        "combined_quality_score": quality["combined_score"],
    })
    flattened.update({
        "critical_elements_count": results_dict["critical_data_elements"]["count"],
        "other_fields_count": results_dict["other_fields"]["count"],
    })
    rule_summary = results_dict["rule_execution_summary"]
    flattened.update({
        "excellent_rules_count": rule_summary["excellent_rules"],
        "good_rules_count": rule_summary["good_rules"],
        "fair_rules_count": rule_summary["fair_rules"],
        "poor_rules_count": rule_summary["poor_rules"],
        "excellent_rules_percentage": rule_summary["health_percentages"]["excellent"],
        "good_rules_percentage": rule_summary["health_percentages"]["good"],
        "fair_rules_percentage": rule_summary["health_percentages"]["fair"],
        "poor_rules_percentage": rule_summary["health_percentages"]["poor"],
    })
    type_dist = results_dict["column_type_distribution"]
    for data_type, count in type_dist.items():
        key = f"columns_{data_type.lower().replace('/', '_').replace(' ', '_')}_count"
        flattened[key] = count
    crit = results_dict["critical_data_elements"]["columns"]
    if crit:
        flattened.update({
            "critical_avg_completeness": round(
                sum(c["completeness"] for c in crit) / len(crit), 1
            ),
            "critical_avg_uniqueness": round(
                sum(c["uniqueness"] for c in crit) / len(crit), 1
            ),
            "critical_avg_consistency": round(
                sum(c["consistency"] for c in crit) / len(crit), 1
            ),
        })
    else:
        flattened.update({
            "critical_avg_completeness": 0,
            "critical_avg_uniqueness": 0,
            "critical_avg_consistency": 0,
        })
    other = results_dict["other_fields"]["columns"]
    if other:
        flattened.update({
            "other_avg_completeness": round(
                sum(c["completeness"] for c in other) / len(other), 1
            ),
            "other_avg_uniqueness": round(
                sum(c["uniqueness"] for c in other) / len(other), 1
            ),
            "other_avg_consistency": round(
                sum(c["consistency"] for c in other) / len(other), 1
            ),
        })
    else:
        flattened.update({
            "other_avg_completeness": 0,
            "other_avg_uniqueness": 0,
            "other_avg_consistency": 0,
        })
    return flattened


def save_comprehensive_results_to_csv(
    df,
    results,
    dataset_name,
    user_specified_critical_columns,
    title="Data Quality Report",
    csv_filename="data_quality_history.csv",
    include_field_summary=True,
):
    """
    Append one row of comprehensive metrics to csv_filename. If include_field_summary,
    also call save_field_summary_to_csv with a derived filename. Returns (csv_filename, field_csv_filename or csv_filename).
    """
    comprehensive = get_comprehensive_results(
        df, results, dataset_name, user_specified_critical_columns, title=title
    )
    if "error" in comprehensive:
        print(f"Error: {comprehensive['error']}")
        return None
    flattened_row = flatten_comprehensive_results(comprehensive)
    file_exists = os.path.exists(csv_filename)
    with open(csv_filename, "a", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=flattened_row.keys())
        if not file_exists:
            writer.writeheader()
        writer.writerow(flattened_row)
    print(f"Data quality results saved to: {csv_filename}")
    field_csv_filename = None
    if include_field_summary:
        base_name = csv_filename.rsplit(".", 1)[0]
        field_csv_filename = f"{base_name}_field_details.csv"
        save_field_summary_to_csv(
            df, dataset_name, user_specified_critical_columns,
            title=title, csv_filename=field_csv_filename,
        )
    return (csv_filename, field_csv_filename if include_field_summary else csv_filename)


def save_field_summary_to_csv(
    df,
    dataset_name,
    user_specified_critical_columns,
    title="Data Quality Report",
    csv_filename="field_summary_history.csv",
):
    """
    Append field-level rows (one per column) to csv_filename. Returns the filename or None on error.
    """
    if df is None:
        print("Error: No data available for field summary export.")
        return None
    timestamp = datetime.now().isoformat()
    field_rows = []
    for column in df.columns:
        col_data = df[column]
        data_type = classify_data_type(col_data)
        quality_scores = calculate_quality_scores(col_data)
        is_critical = is_critical_data_element(
            column, col_data, user_specified_critical_columns
        )
        row = {
            "timestamp": timestamp,
            "title": title,
            "dataset_name": dataset_name,
            "column_name": column,
            "data_type": data_type,
            "total_count": len(col_data),
            "null_count": col_data.isnull().sum(),
            "distinct_count": col_data.nunique(),
            "completeness": quality_scores["completeness"],
            "uniqueness": quality_scores["uniqueness"],
            "consistency": quality_scores["consistency"],
            "is_critical": is_critical,
            "mean": "",
            "median": "",
            "std": "",
            "min": "",
            "max": "",
        }
        if pd.api.types.is_numeric_dtype(col_data):
            try:
                numeric_data = col_data.dropna()
                if len(numeric_data) > 0:
                    row["mean"] = round(numeric_data.mean(), 2)
                    row["median"] = round(numeric_data.median(), 2)
                    row["std"] = round(numeric_data.std(), 2)
                    row["min"] = numeric_data.min()
                    row["max"] = numeric_data.max()
            except Exception:
                pass
        field_rows.append(row)
    file_exists = os.path.exists(csv_filename)
    with open(csv_filename, "a", newline="", encoding="utf-8") as csvfile:
        if field_rows:
            writer = csv.DictWriter(csvfile, fieldnames=field_rows[0].keys())
            if not file_exists:
                writer.writeheader()
            writer.writerows(field_rows)
    print(f"Field summary data saved to: {csv_filename}")
    print(f"   Exported details for {len(field_rows)} columns")
    critical_count = sum(1 for r in field_rows if r["is_critical"])
    print(f"   Critical elements: {critical_count}, Other fields: {len(field_rows) - critical_count}")
    return csv_filename
