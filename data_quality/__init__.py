"""Data quality checking package. Run expectations, similarity analysis, reporting, and cross-dataset comparison on pandas DataFrames."""

from data_quality.checker import DataQualityChecker
from data_quality.comparison import (
    DatasetComparator,
    compare_two_reports,
    get_reconciliation_diffs,
    reconcile_on_key,
    run_same_rules_on_two_datasets,
)
from data_quality.dimensions import DIMENSIONS
from data_quality.getting_started import get_getting_started_guide

__all__ = [
    "DataQualityChecker",
    "DIMENSIONS",
    "DatasetComparator",
    "compare_two_reports",
    "get_getting_started_guide",
    "get_reconciliation_diffs",
    "reconcile_on_key",
    "run_same_rules_on_two_datasets",
]
