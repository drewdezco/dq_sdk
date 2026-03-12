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
from data_quality.pipeline import (
    compare_schema,
    compare_snapshots,
    compare_volume,
    detect_identical_or_stale,
)

__all__ = [
    "DataQualityChecker",
    "DIMENSIONS",
    "DatasetComparator",
    "compare_schema",
    "compare_snapshots",
    "compare_two_reports",
    "compare_volume",
    "detect_identical_or_stale",
    "get_getting_started_guide",
    "get_reconciliation_diffs",
    "reconcile_on_key",
    "run_same_rules_on_two_datasets",
]
