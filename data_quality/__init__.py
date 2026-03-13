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
from data_quality.docs_utils import (
    get_architecture_html,
    get_architecture_markdown,
    get_docs_index,
    get_getting_started_html,
    get_getting_started_markdown,
    get_pipeline_html,
    get_pipeline_markdown,
    get_readme_html,
    get_readme_markdown,
    get_usage_html,
    get_usage_markdown,
    print_docs_overview,
)
from data_quality.pipeline import (
    DEFAULT_THRESHOLDS,
    compare_schema,
    compare_snapshots,
    compare_snapshots_multi,
    compare_volume,
    detect_identical_or_stale,
    load_dataframe,
)

__all__ = [
    "DataQualityChecker",
    "DEFAULT_THRESHOLDS",
    "DIMENSIONS",
    "DatasetComparator",
    "compare_schema",
    "compare_snapshots",
    "compare_snapshots_multi",
    "compare_two_reports",
    "compare_volume",
    "detect_identical_or_stale",
    "get_getting_started_guide",
    "get_architecture_html",
    "get_architecture_markdown",
    "get_docs_index",
    "get_reconciliation_diffs",
    "get_getting_started_html",
    "get_getting_started_markdown",
    "get_pipeline_html",
    "get_pipeline_markdown",
    "get_readme_html",
    "get_readme_markdown",
    "get_usage_html",
    "get_usage_markdown",
    "print_docs_overview",
    "load_dataframe",
    "reconcile_on_key",
    "run_same_rules_on_two_datasets",
]
