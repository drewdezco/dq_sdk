"""
DataQualityChecker: main facade that holds state (df, results, dataset_name, critical_columns)
and delegates to expectations, similarity, and reporting modules.
"""

from typing import Optional, List, Dict, Any

import pandas as pd
from data_quality import expectations as exp
from data_quality import similarity as sim
from data_quality import reporting as rep
from data_quality import suggestion as sug


class DataQualityChecker:
    """
    Orchestrate data quality checks for a pandas DataFrame.

    This class stores the dataset, accumulates rule results, and provides helpers to:
    - run expectations (single- and multi-column)
    - run similarity analysis
    - generate and apply suggestions
    - build or export comprehensive reports
    """

    def __init__(
        self,
        df: pd.DataFrame,
        dataset_name: str = "Unknown Dataset",
        critical_columns: Optional[list] = None,
    ) -> None:
        self.df = df
        self.results = []
        self.dataset_name = dataset_name
        self.user_specified_critical_columns = critical_columns or []

    # -------- Dataset Management --------
    def set_dataset_name(self, dataset_name: str) -> None:
        """Set or update the dataset name for this checker instance."""
        self.dataset_name = dataset_name

    def get_dataset_name(self) -> str:
        """Get the current dataset name."""
        return self.dataset_name

    def set_critical_columns(self, critical_columns: list) -> None:
        """Set or update the list of user-specified critical columns."""
        self.user_specified_critical_columns = critical_columns or []

    def get_critical_columns(self) -> list:
        """Get the current list of user-specified critical columns."""
        return self.user_specified_critical_columns.copy()

    def add_critical_column(self, column_name: str) -> None:
        """Add a single column to the critical columns list."""
        if column_name not in self.user_specified_critical_columns:
            self.user_specified_critical_columns.append(column_name)

    def remove_critical_column(self, column_name: str) -> None:
        """Remove a column from the critical columns list."""
        if column_name in self.user_specified_critical_columns:
            self.user_specified_critical_columns.remove(column_name)

    # -------- Single-column expectations --------
    def expect_column_values_to_not_be_null(self, column: str) -> None:
        """Expect column values to be non-null (Completeness)."""
        exp.expect_column_values_to_not_be_null(self.df, self.results, column)

    def expect_column_values_to_be_unique(self, column: str) -> None:
        """Expect column values to be unique (Uniqueness)."""
        exp.expect_column_values_to_be_unique(self.df, self.results, column)

    def expect_column_values_to_be_in_set(self, column: str, allowed_values) -> None:
        """Expect column values to be in an allowed set (Validity)."""
        exp.expect_column_values_to_be_in_set(
            self.df, self.results, column, allowed_values
        )

    def expect_column_values_to_match_regex(self, column: str, pattern: str) -> None:
        """Expect column values to match a regular expression (Validity)."""
        exp.expect_column_values_to_match_regex(
            self.df, self.results, column, pattern
        )

    def expect_column_values_to_be_in_range(
        self,
        column: str,
        min_val,
        max_val,
    ) -> None:
        """Expect column values to fall within a numeric range (Validity)."""
        exp.expect_column_values_to_be_in_range(
            self.df, self.results, column, min_val, max_val
        )

    def expect_column_values_to_be_in_date_range(
        self,
        column: str,
        min_date,
        max_date,
    ) -> None:
        """Expect date values in column to be between min_date and max_date (Validity)."""
        exp.expect_column_values_to_be_in_date_range(
            self.df, self.results, column, min_date, max_date
        )

    def expect_column_values_to_be_recent(
        self,
        column: str,
        max_age_days: int,
        reference_date=None,
    ) -> None:
        """Expect date values in column to be within the last max_age_days (Timeliness)."""
        exp.expect_column_values_to_be_recent(
            self.df, self.results, column, max_age_days, reference_date=reference_date
        )

    def expect_column_values_to_match_reference(
        self,
        column: str,
        reference_series_or_set,
    ) -> None:
        """Expect column values to match a reference set or Series (Accuracy)."""
        exp.expect_column_values_to_match_reference(
            self.df, self.results, column, reference_series_or_set
        )

    # -------- Multi-column expectations --------
    def expect_columns_values_to_not_be_null(self, columns) -> None:
        exp.expect_columns_values_to_not_be_null(self.df, self.results, columns)

    def expect_columns_values_to_be_unique(self, columns) -> None:
        exp.expect_columns_values_to_be_unique(self.df, self.results, columns)

    def expect_columns_values_to_be_in_sets(self, columns, allowed_values) -> None:
        exp.expect_columns_values_to_be_in_sets(
            self.df, self.results, columns, allowed_values
        )

    def expect_columns_values_to_match_patterns(self, columns, patterns_dict) -> None:
        exp.expect_columns_values_to_match_patterns(
            self.df, self.results, columns, patterns_dict
        )

    def expect_columns_values_to_be_in_ranges(self, columns, ranges_dict) -> None:
        exp.expect_columns_values_to_be_in_ranges(
            self.df, self.results, columns, ranges_dict
        )

    # -------- Similarity --------
    def analyze_column_similarity_levenshtein(
        self,
        column1: str,
        column2: str,
        similarity_threshold: float = 0.8,
    ) -> Dict[str, Any]:
        """Analyze Levenshtein similarity between two columns. Returns detailed analysis dict."""
        return sim.analyze_column_similarity_levenshtein(
            self.df,
            self.results,
            column1,
            column2,
            similarity_threshold=similarity_threshold,
        )

    def get_similarity_summary_table(self, similarity_threshold: float = 0.8) -> pd.DataFrame:
        """Generate a summary DataFrame of all similarity analyses in results."""
        return sim.get_similarity_summary_table(
            self.results, similarity_threshold=similarity_threshold
        )

    def get_detailed_similarity_comparisons(
        self,
        column1: str,
        column2: str,
        min_similarity: float = 0.0,
        max_similarity: float = 1.0,
    ) -> pd.DataFrame:
        """Get detailed row-by-row similarity comparisons for column1 vs column2."""
        return sim.get_detailed_similarity_comparisons(
            self.results,
            column1,
            column2,
            min_similarity=min_similarity,
            max_similarity=max_similarity,
        )

    # -------- JSON rules --------
    def run_rules_from_json(self, rules: Dict[str, List[Dict[str, Any]]]) -> None:
        """Run expectations from a dict: { "expect_...": [ {"column": "x", ...}, ... ], ... }."""
        for expectation, configs in rules.items():
            method = getattr(self, expectation, None)
            if not method:
                print(f"Warning: {expectation} not implemented.")
                continue
            for config in configs:
                method(**config)

    # -------- Auto-suggestion --------
    def generate_suggestions(
        self,
        columns: Optional[List[str]] = None,
        options: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Generate validation suggestions based on dataframe analysis.
        
        Args:
            columns: Optional list of column names to analyze. If None, analyzes all columns.
            options: Optional dict of configuration options (see suggestion.DEFAULT_OPTIONS)
        
        Returns:
            List of suggestion dictionaries, each with:
            - column: column name
            - method: expectation method name
            - params: dict of parameters for the method
            - confidence: float 0-1 indicating confidence in suggestion
            - reason: human-readable explanation
            - dimension: data quality dimension
        """
        return sug.generate_suggestions(self.df, columns=columns, options=options)

    def apply_suggestions(
        self,
        suggestions: List[Dict[str, Any]],
        auto_apply: bool = False,
    ) -> int:
        """
        Apply suggested validations to the checker.
        
        Args:
            suggestions: List of suggestion dictionaries from generate_suggestions()
            auto_apply: If True, applies all suggestions. If False, only applies suggestions
                       with confidence >= 0.8 (default behavior)
        
        Returns:
            Number of suggestions applied
        """
        if not suggestions:
            return 0
        
        applied_count = 0
        for suggestion in suggestions:
            # Filter by confidence if auto_apply is False
            if not auto_apply and suggestion.get("confidence", 0) < 0.8:
                continue
            
            method_name = suggestion["method"]
            params = suggestion["params"].copy()
            # Add column to params (required for all expectation methods)
            params["column"] = suggestion["column"]
            
            # Get the method from this checker instance
            method = getattr(self, method_name, None)
            if method:
                try:
                    method(**params)
                    applied_count += 1
                except Exception as e:
                    print(f"Warning: Failed to apply suggestion {method_name} for column {suggestion['column']}: {e}")
            else:
                print(f"Warning: Method {method_name} not found on DataQualityChecker")
        
        return applied_count

    def suggest_and_apply(
        self,
        columns: Optional[List[str]] = None,
        options: Optional[Dict[str, Any]] = None,
        auto_apply: bool = False,
    ) -> Dict[str, Any]:
        """
        Convenience method to generate suggestions and optionally apply them.
        
        Args:
            columns: Optional list of column names to analyze. If None, analyzes all columns.
            options: Optional dict of configuration options (see suggestion.DEFAULT_OPTIONS)
            auto_apply: If True, automatically applies all suggestions. If False, only applies
                       suggestions with confidence >= 0.8
        
        Returns:
            Dict with:
            - suggestions: List of all generated suggestions
            - applied_count: Number of suggestions that were applied
            - applied_suggestions: List of suggestions that were actually applied
        """
        suggestions = self.generate_suggestions(columns=columns, options=options)
        applied_count = self.apply_suggestions(suggestions, auto_apply=auto_apply)
        
        # Determine which suggestions were actually applied (those that passed confidence check)
        applied_suggestions = []
        for suggestion in suggestions:
            if auto_apply or suggestion.get("confidence", 0) >= 0.8:
                applied_suggestions.append(suggestion)
        
        return {
            "suggestions": suggestions,
            "applied_count": applied_count,
            "applied_suggestions": applied_suggestions,
        }

    # -------- Results and reporting --------
    def get_results(self) -> pd.DataFrame:
        """
        Return all executed validation results as a pandas DataFrame.

        Returns:
            A DataFrame where each row is a single rule execution. Typical columns include:
            - column: column name (or comma-separated list for multi-column rules)
            - rule: short rule label (e.g. "not null", "unique", "in range 0-100")
            - success_rate: float from 0–100 (percentage of records that passed)
            - dimension: data quality dimension (e.g. Completeness, Uniqueness, Validity)
            - details: nested dict with rule-specific metadata (counts, thresholds, etc.)
        """
        return pd.DataFrame(self.results)

    def get_comprehensive_results(
        self,
        title: str = "Data Quality Report",
        dimensions_filter: Optional[list] = None,
    ) -> Dict[str, Any]:
        """
        Build a full report dictionary for the current dataframe and results.

        Args:
            title:
                Human-readable title for the report; stored in the metadata section.
            dimensions_filter:
                Optional list of dimension names to include when computing scores
                (e.g. ["Completeness", "Validity"]). If None, all dimensions are used.

        Returns:
            Dict with keys such as:
            - metadata: high-level context (dataset_name, title, row/column counts)
            - key_metrics: overall metrics (record counts, nulls, rule counts, etc.)
            - overall_health_score: float from 0–100 summarising all included dimensions
            - overall_health_status: label such as "Excellent", "Good", "Poor"
            - per_dimension_scores: mapping of dimension -> score
            - field_summary: optional per-column breakdown of rule outcomes
        """
        return rep.get_comprehensive_results(
            self.df,
            self.results,
            self.dataset_name,
            self.user_specified_critical_columns,
            title=title,
            dimensions_filter=dimensions_filter,
        )

    def save_comprehensive_results_to_csv(
        self,
        title: str = "Data Quality Report",
        csv_filename: str = "data_quality_history.csv",
        include_field_summary: bool = True,
        dimensions_filter: Optional[list] = None,
    ):
        """
        Append one row to csv_filename; optionally save field summary to a second CSV.
        When dimensions_filter is provided, only those dimensions contribute to the score.
        Use data_quality.dimensions.DIMENSIONS for valid dimension names.
        """
        return rep.save_comprehensive_results_to_csv(
            self.df,
            self.results,
            self.dataset_name,
            self.user_specified_critical_columns,
            title=title,
            csv_filename=csv_filename,
            include_field_summary=include_field_summary,
            dimensions_filter=dimensions_filter,
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
