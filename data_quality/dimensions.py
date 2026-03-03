"""
Data quality dimensions used for tagging rules and computing dimension-level scores.
Health score is the average of per-dimension scores (only for dimensions that have at least one rule).
"""

DIMENSIONS = (
    "Accuracy",
    "Completeness",
    "Consistency",
    "Timeliness",
    "Validity",
    "Uniqueness",
)
