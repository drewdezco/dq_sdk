"""
Similarity analysis functions (Levenshtein).
Functions take (df, results, ...) and append to results; summary helpers take results.
"""

import pandas as pd
import numpy as np
from data_quality.utils import levenshtein_distance, levenshtein_ratio


def analyze_column_similarity_levenshtein(
    df, results, column1, column2, similarity_threshold=0.8
):
    """
    Analyze Levenshtein similarity between two columns; append result to results.
    Returns the detailed similarity_analysis dict.
    """
    total = len(df)
    similarities = []
    distances = []
    similar_count = 0
    col1_values = df[column1].fillna("").astype(str)
    col2_values = df[column2].fillna("").astype(str)
    detailed_comparisons = []

    for i, (val1, val2) in enumerate(zip(col1_values, col2_values)):
        distance = levenshtein_distance(val1, val2)
        ratio = levenshtein_ratio(val1, val2)
        similarities.append(ratio)
        distances.append(distance)
        if ratio >= similarity_threshold:
            similar_count += 1
        detailed_comparisons.append({
            "row_index": i,
            "value1": val1,
            "value2": val2,
            "distance": distance,
            "similarity_ratio": ratio,
            "is_similar": ratio >= similarity_threshold,
        })

    avg_similarity = np.mean(similarities) if similarities else 0
    min_similarity = np.min(similarities) if similarities else 0
    max_similarity = np.max(similarities) if similarities else 0
    std_similarity = np.std(similarities) if similarities else 0
    avg_distance = np.mean(distances) if distances else 0
    min_distance = np.min(distances) if distances else 0
    max_distance = np.max(distances) if distances else 0
    similarity_percentage = (similar_count / total) * 100 if total > 0 else 0

    similarity_analysis = {
        "total_comparisons": total,
        "similar_pairs": similar_count,
        "similarity_percentage": similarity_percentage,
        "similarity_threshold": similarity_threshold,
        "exact_matches": sum(1 for s in similarities if s == 1.0),
        "within_threshold": sum(
            1 for s in similarities if s >= similarity_threshold and s < 1.0
        ),
        "outside_threshold": sum(1 for s in similarities if s < similarity_threshold),
        "statistics": {
            "average_similarity": avg_similarity,
            "min_similarity": min_similarity,
            "max_similarity": max_similarity,
            "std_similarity": std_similarity,
            "average_distance": avg_distance,
            "min_distance": min_distance,
            "max_distance": max_distance,
        },
        "detailed_comparisons": detailed_comparisons,
        "similarity_distribution": {
            "very_high": sum(1 for s in similarities if s >= 0.9),
            "high": sum(1 for s in similarities if 0.8 <= s < 0.9),
            "medium": sum(1 for s in similarities if 0.6 <= s < 0.8),
            "low": sum(1 for s in similarities if 0.4 <= s < 0.6),
            "very_low": sum(1 for s in similarities if s < 0.4),
        },
    }

    column_comparison = f"{column1} vs {column2}"
    results.append({
        "column": column_comparison,
        "rule": f"levenshtein similarity (threshold: {similarity_threshold})",
        "success_rate": similarity_percentage,
        "details": similarity_analysis,
        "dimension": "Consistency",
    })
    return similarity_analysis


def get_similarity_summary_table(results, similarity_threshold=0.8):
    """Build a summary DataFrame of all similarity results in results list."""
    similarity_results = [
        r for r in results if "levenshtein similarity" in r["rule"]
    ]
    if not similarity_results:
        return pd.DataFrame()
    summary_data = []
    for result in similarity_results:
        details = result["details"]
        summary_data.append({
            "Column_Pair": result["column"],
            "Total_Comparisons": details["total_comparisons"],
            "Similar_Pairs": details["similar_pairs"],
            "Similarity_Percentage": f"{details['similarity_percentage']:.1f}%",
            "Avg_Similarity": f"{details['statistics']['average_similarity']:.3f}",
            "Min_Similarity": f"{details['statistics']['min_similarity']:.3f}",
            "Max_Similarity": f"{details['statistics']['max_similarity']:.3f}",
            "Avg_Distance": f"{details['statistics']['average_distance']:.1f}",
            "Threshold_Used": details["similarity_threshold"],
        })
    return pd.DataFrame(summary_data)


def get_detailed_similarity_comparisons(
    results, column1, column2, min_similarity=0.0, max_similarity=1.0
):
    """Return a DataFrame of row-by-row comparisons for column1 vs column2 from results."""
    column_comparison = f"{column1} vs {column2}"
    similarity_result = None
    for r in results:
        if r["column"] == column_comparison and "levenshtein similarity" in r["rule"]:
            similarity_result = r
            break
    if not similarity_result:
        return pd.DataFrame()
    detailed_comparisons = similarity_result["details"]["detailed_comparisons"]
    filtered = [
        c
        for c in detailed_comparisons
        if min_similarity <= c["similarity_ratio"] <= max_similarity
    ]
    if filtered:
        df_comparisons = pd.DataFrame(filtered)
        df_comparisons = df_comparisons.round({"similarity_ratio": 3})
        return df_comparisons
    return pd.DataFrame()
