"""
DataQualityChecker: main facade that holds state (df, results, dataset_name, critical_columns)
and delegates to expectations, similarity, and reporting modules.
"""

import pandas as pd
from data_quality import expectations as exp
from data_quality import similarity as sim
from data_quality import reporting as rep


class DataQualityChecker:
    """
    Run data quality expectations and similarity analysis on a pandas DataFrame,
    then get or export results. Usable on PC and Databricks; pass file paths as arguments.
    """

    def __init__(
        self,
        df: pd.DataFrame,
        dataset_name: str = "Unknown Dataset",
        critical_columns: list = None,
    ):
        self.df = df
        self.results = []
        self.dataset_name = dataset_name
        self.user_specified_critical_columns = critical_columns or []

    # -------- Dataset Management --------
    def set_dataset_name(self, dataset_name: str):
        """Set or update the dataset name for this checker instance."""
        self.dataset_name = dataset_name

    def get_dataset_name(self):
        """Get the current dataset name."""
        return self.dataset_name

    def set_critical_columns(self, critical_columns: list):
        """Set or update the list of user-specified critical columns."""
        self.user_specified_critical_columns = critical_columns or []

    def get_critical_columns(self):
        """Get the current list of user-specified critical columns."""
        return self.user_specified_critical_columns.copy()

    def add_critical_column(self, column_name: str):
        """Add a single column to the critical columns list."""
        if column_name not in self.user_specified_critical_columns:
            self.user_specified_critical_columns.append(column_name)

    def remove_critical_column(self, column_name: str):
        """Remove a column from the critical columns list."""
        if column_name in self.user_specified_critical_columns:
            self.user_specified_critical_columns.remove(column_name)

    # -------- Single-column expectations --------
    def expect_column_values_to_not_be_null(self, column):
        exp.expect_column_values_to_not_be_null(self.df, self.results, column)

    def expect_column_values_to_be_unique(self, column):
        exp.expect_column_values_to_be_unique(self.df, self.results, column)

    def expect_column_values_to_be_in_set(self, column, allowed_values):
        exp.expect_column_values_to_be_in_set(
            self.df, self.results, column, allowed_values
        )

    def expect_column_values_to_match_regex(self, column, pattern):
        exp.expect_column_values_to_match_regex(
            self.df, self.results, column, pattern
        )

    def expect_column_values_to_be_in_range(self, column, min_val, max_val):
        exp.expect_column_values_to_be_in_range(
            self.df, self.results, column, min_val, max_val
        )

    def expect_column_values_to_be_in_date_range(
        self, column, min_date, max_date
    ):
        exp.expect_column_values_to_be_in_date_range(
            self.df, self.results, column, min_date, max_date
        )

    # -------- Multi-column expectations --------
    def expect_columns_values_to_not_be_null(self, columns):
        exp.expect_columns_values_to_not_be_null(self.df, self.results, columns)

    def expect_columns_values_to_be_unique(self, columns):
        exp.expect_columns_values_to_be_unique(self.df, self.results, columns)

    def expect_columns_values_to_be_in_sets(self, columns, allowed_values):
        exp.expect_columns_values_to_be_in_sets(
            self.df, self.results, columns, allowed_values
        )

    def expect_columns_values_to_match_patterns(self, columns, patterns_dict):
        exp.expect_columns_values_to_match_patterns(
            self.df, self.results, columns, patterns_dict
        )

    def expect_columns_values_to_be_in_ranges(self, columns, ranges_dict):
        exp.expect_columns_values_to_be_in_ranges(
            self.df, self.results, columns, ranges_dict
        )

    # -------- Similarity --------
    def analyze_column_similarity_levenshtein(
        self, column1, column2, similarity_threshold=0.8
    ):
        """Analyze Levenshtein similarity between two columns. Returns detailed analysis dict."""
        return sim.analyze_column_similarity_levenshtein(
            self.df,
            self.results,
            column1,
            column2,
            similarity_threshold=similarity_threshold,
        )

    def get_similarity_summary_table(self, similarity_threshold=0.8):
        """Generate a summary DataFrame of all similarity analyses in results."""
        return sim.get_similarity_summary_table(
            self.results, similarity_threshold=similarity_threshold
        )

    def get_detailed_similarity_comparisons(
        self, column1, column2, min_similarity=0.0, max_similarity=1.0
    ):
        """Get detailed row-by-row similarity comparisons for column1 vs column2."""
        return sim.get_detailed_similarity_comparisons(
            self.results,
            column1,
            column2,
            min_similarity=min_similarity,
            max_similarity=max_similarity,
        )

    # -------- JSON rules --------
    def run_rules_from_json(self, rules):
        """Run expectations from a dict: { "expect_...": [ {"column": "x", ...}, ... ], ... }."""
        for expectation, configs in rules.items():
            method = getattr(self, expectation, None)
            if not method:
                print(f"Warning: {expectation} not implemented.")
                continue
            for config in configs:
                method(**config)

    # -------- Results and reporting --------
    def get_results(self):
        """Return results as a pandas DataFrame."""
        return pd.DataFrame(self.results)

    def get_comprehensive_results(self, title: str = "Data Quality Report"):
        """Return full snapshot dict (metadata, key_metrics, critical_data_elements, etc.)."""
        return rep.get_comprehensive_results(
            self.df,
            self.results,
            self.dataset_name,
            self.user_specified_critical_columns,
            title=title,
        )

    def save_comprehensive_results_to_csv(
        self,
        title: str = "Data Quality Report",
        csv_filename: str = "data_quality_history.csv",
        include_field_summary: bool = True,
    ):
        """Append one row to csv_filename; optionally save field summary to a second CSV."""
        return rep.save_comprehensive_results_to_csv(
            self.df,
            self.results,
            self.dataset_name,
            self.user_specified_critical_columns,
            title=title,
            csv_filename=csv_filename,
            include_field_summary=include_field_summary,
        )

    def save_field_summary_to_csv(
        self,
        title: str = "Data Quality Report",
        csv_filename: str = "field_summary_history.csv",
    ):
        """Append field-level rows (one per column) to csv_filename."""
        return rep.save_field_summary_to_csv(
            self.df,
            self.dataset_name,
            self.user_specified_critical_columns,
            title=title,
            csv_filename=csv_filename,
        )
