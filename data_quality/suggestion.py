"""
Auto-generation of validation suggestions based on dataframe analysis.
Analyzes columns to suggest appropriate validations based on data characteristics.
"""

import re
import json
from typing import Dict, List, Optional, Any
import pandas as pd
from data_quality.utils import classify_data_type, calculate_quality_scores
from data_quality.dimensions import DIMENSIONS

# Default options for suggestion generation.
# See analyze_column_for_suggestions / generate_suggestions for meaning of each key.
DEFAULT_OPTIONS = {
    "null_rate_threshold": 0.05,  # Suggest not_null if null rate below this
    "uniqueness_threshold": 0.95,  # Suggest unique if uniqueness above this
    "categorical_max_distinct": 20,  # Max distinct values to suggest "in_set"
    "categorical_coverage_threshold": 0.8,  # Min coverage of top values to suggest "in_set"
    "use_percentiles_for_ranges": False,  # Use percentiles vs min/max for numeric ranges
    "range_padding_factor": 0.1,  # Add padding to numeric ranges (10% of range)
    "pattern_detection_enabled": True,  # Enable regex pattern detection
    "pattern_match_threshold": 0.8,  # Min % of values matching pattern to suggest
    "min_samples_for_suggestion": 3,  # Minimum non-null samples needed
    "timeliness_max_age_days": 365,  # Default max_age_days for recent date suggestions
}


def _detect_email_pattern(values: pd.Series, pattern_match_threshold: float = 0.8) -> Optional[str]:
    """Detect if values match email pattern."""
    email_pattern = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
    sample = values.dropna().head(1000)
    if len(sample) == 0:
        return None
    matches = sum(1 for v in sample if email_pattern.match(str(v)))
    if matches / len(sample) >= pattern_match_threshold:
        return r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    return None


def _detect_uuid_pattern(values: pd.Series, pattern_match_threshold: float = 0.8) -> Optional[str]:
    """Detect if values match UUID pattern."""
    uuid_pattern = re.compile(
        r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
        re.IGNORECASE
    )
    sample = values.dropna().head(1000)
    if len(sample) == 0:
        return None
    matches = sum(1 for v in sample if uuid_pattern.match(str(v)))
    if matches / len(sample) >= pattern_match_threshold:
        return r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
    return None


def _detect_phone_pattern(values: pd.Series, pattern_match_threshold: float = 0.8) -> Optional[str]:
    """Detect if values match phone number pattern."""
    # Common phone patterns: (123) 456-7890, 123-456-7890, 123.456.7890, +1-123-456-7890
    phone_patterns = [
        r"^\+?1?[-.\s]?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}$",  # US format
        r"^\+\d{1,3}[-.\s]?\d{1,4}[-.\s]?\d{1,4}[-.\s]?\d{1,9}$",  # International
    ]
    sample = values.dropna().head(1000)
    if len(sample) == 0:
        return None
    for pattern_str in phone_patterns:
        pattern = re.compile(pattern_str)
        matches = sum(1 for v in sample if pattern.match(str(v)))
        if matches / len(sample) >= pattern_match_threshold:
            return pattern_str
    return None


def _detect_custom_id_pattern(values: pd.Series, pattern_match_threshold: float = 0.8) -> Optional[str]:
    """
    Detect custom ID patterns like "CUST-001", "SKU-123", etc.
    Attempts to infer pattern from examples.
    """
    sample = values.dropna().head(100)
    if len(sample) < 3:
        return None
    
    # Convert to strings
    str_values = [str(v) for v in sample]
    
    # Look for patterns with prefix-number or prefix-suffix patterns
    # Try to find common prefix and suffix patterns
    prefix_pattern = None
    suffix_pattern = None
    
    # Check for prefix-number pattern (e.g., "CUST-001", "SKU-123")
    if all("-" in v or "_" in v for v in str_values[:10]):
        # Try to extract prefix pattern
        parts_list = [re.split(r"[-_]", v, maxsplit=1) for v in str_values[:10]]
        if all(len(parts) == 2 for parts in parts_list):
            prefixes = [parts[0] for parts in parts_list]
            suffixes = [parts[1] for parts in parts_list]
            
            # Check if prefixes are consistent (all same or follow pattern)
            if len(set(prefixes)) == 1:
                prefix = prefixes[0]
                # Check if suffixes are numeric
                if all(re.match(r"^\d+$", s) for s in suffixes):
                    # Generate pattern: PREFIX-\d+
                    prefix_pattern = re.escape(prefix) + r"[-_]\d+"
                elif all(len(s) == len(suffixes[0]) for s in suffixes):
                    # Same length suffixes - could be alphanumeric
                    if all(re.match(r"^[A-Z0-9]+$", s.upper()) for s in suffixes):
                        prefix_pattern = re.escape(prefix) + r"[-_][A-Z0-9]+"
    
    if prefix_pattern:
        pattern = re.compile(f"^{prefix_pattern}$", re.IGNORECASE)
        matches = sum(1 for v in str_values if pattern.match(v))
        if matches / len(str_values) >= pattern_match_threshold:
            return f"^{prefix_pattern}$"
    
    return None


def _detect_regex_pattern(values: pd.Series, options: Dict[str, Any]) -> Optional[str]:
    """Detect common regex patterns in column values."""
    if not options.get("pattern_detection_enabled", True):
        return None
    
    pattern_match_threshold = options.get("pattern_match_threshold", DEFAULT_OPTIONS["pattern_match_threshold"])
    
    # Try email first
    pattern = _detect_email_pattern(values, pattern_match_threshold)
    if pattern:
        return pattern
    
    # Try UUID
    pattern = _detect_uuid_pattern(values, pattern_match_threshold)
    if pattern:
        return pattern
    
    # Try phone
    pattern = _detect_phone_pattern(values, pattern_match_threshold)
    if pattern:
        return pattern
    
    # Try custom ID patterns
    pattern = _detect_custom_id_pattern(values, pattern_match_threshold)
    if pattern:
        return pattern
    
    return None


def analyze_column_for_suggestions(
    df: pd.DataFrame,
    column: str,
    options: Optional[Dict[str, Any]] = None
) -> List[Dict[str, Any]]:
    """
    Analyze a single column and return suggested validations.
    
    Args:
        df: DataFrame to analyze
        column: Column name to analyze
        options: Optional dict of configuration options (see DEFAULT_OPTIONS)
    
    Returns:
        List of suggestion dictionaries, each with:
        - column: column name
        - method: expectation method name
        - params: dict of parameters for the method
        - confidence: float 0-1 indicating confidence in suggestion
        - reason: human-readable explanation
        - dimension: data quality dimension
    """
    
    if column not in df.columns:
        return []
    
    # Merge options with defaults
    opts = {**DEFAULT_OPTIONS, **(options or {})}
    
    suggestions = []
    col_data = df[column]
    
    # Skip if not enough samples
    non_null_count = col_data.notna().sum()
    if non_null_count < opts["min_samples_for_suggestion"]:
        return suggestions
    
    # Get data type and quality scores
    data_type = classify_data_type(col_data)
    quality_scores = calculate_quality_scores(col_data)
    null_rate = 1 - (quality_scores["completeness"] / 100)
    uniqueness_rate = quality_scores["uniqueness"] / 100
    
    
    # 1. Completeness suggestion
    if null_rate < opts["null_rate_threshold"]:
        suggestions.append({
            "column": column,
            "method": "expect_column_values_to_not_be_null",
            "params": {},
            "confidence": 1.0 - null_rate,  # Higher confidence if lower null rate
            "reason": f"Null rate is only {null_rate*100:.1f}%",
            "dimension": "Completeness",
        })
    
    # 2. Uniqueness suggestion
    if uniqueness_rate >= opts["uniqueness_threshold"]:
        suggestions.append({
            "column": column,
            "method": "expect_column_values_to_be_unique",
            "params": {},
            "confidence": uniqueness_rate,
            "reason": f"Uniqueness is {uniqueness_rate*100:.1f}%",
            "dimension": "Uniqueness",
        })
    
    # 3. Validity suggestions based on data type
    non_null_data = col_data.dropna()
    
    # Check for Text/String type - ALWAYS check dtype and sample values directly
    # This is the most reliable method, regardless of classify_data_type output
    is_text_string = False
    
    if len(non_null_data) > 0:
        # Check 1: pandas StringDtype directly (newer pandas versions)
        if hasattr(pd, 'StringDtype') and isinstance(col_data.dtype, pd.StringDtype):
            is_text_string = True
        
        # Check 2: object dtype - ALWAYS sample values to verify they're strings
        # This is the PRIMARY check - most reliable for detecting string columns
        if not is_text_string and pd.api.types.is_object_dtype(col_data):
            try:
                # Sample values and check if they're strings
                sample_values = non_null_data.head(100)
                if len(sample_values) > 0:
                    sample_list = list(sample_values)
                    if len(sample_list) > 0:
                        str_count = sum(1 for x in sample_list if isinstance(x, str))
                        # If all sampled values are strings, treat as text
                        if str_count == len(sample_list):
                            is_text_string = True
            except Exception:
                pass
        
        # Check 3: categorical dtype with string values
        if not is_text_string and (data_type == "Category" or (hasattr(pd, 'CategoricalDtype') and isinstance(col_data.dtype, pd.CategoricalDtype))):
            try:
                # Sample values and check if they're strings
                sample_values = non_null_data.head(100)
                if len(sample_values) > 0:
                    sample_list = list(sample_values)
                    if len(sample_list) > 0:
                        str_count = sum(1 for x in sample_list if isinstance(x, str))
                        # If all sampled values are strings, treat as text
                        if str_count == len(sample_list):
                            is_text_string = True
            except Exception:
                pass
        
        # Check 4: classify_data_type says Text/String or Str (fallback)
        if not is_text_string and data_type in ("Text/String", "Str"):
            is_text_string = True
    
    if is_text_string:
        distinct_count = non_null_data.nunique()
        total_count = len(non_null_data)
        
        # Categorical suggestion (in_set)
        if distinct_count <= opts["categorical_max_distinct"] and total_count > 0:
            # Check if top values cover enough of the data
            value_counts = non_null_data.value_counts()
            top_values = value_counts.head(distinct_count)
            coverage = top_values.sum() / total_count
            
            if coverage >= opts["categorical_coverage_threshold"]:
                allowed_values = top_values.index.tolist()
                suggestions.append({
                    "column": column,
                    "method": "expect_column_values_to_be_in_set",
                    "params": {"allowed_values": allowed_values},
                    "confidence": min(coverage, 0.95),  # Cap at 0.95 since we're inferring
                    "reason": f"Low cardinality ({distinct_count} distinct values) with {coverage*100:.1f}% coverage",
                    "dimension": "Validity",
                })
        
        # Regex pattern suggestion
        pattern = _detect_regex_pattern(col_data, opts)
        if pattern:
            suggestions.append({
                "column": column,
                "method": "expect_column_values_to_match_regex",
                "params": {"pattern": pattern},
                "confidence": 0.85,  # Pattern detection is somewhat confident
                "reason": f"Detected pattern matching {opts['pattern_match_threshold']*100:.0f}% of values",
                "dimension": "Validity",
            })
    
    elif data_type in ["Integer", "Decimal", "Numeric"]:
        # Numeric range suggestion
        if len(non_null_data) > 0:
            min_val = float(non_null_data.min())
            max_val = float(non_null_data.max())
            
            if opts["use_percentiles_for_ranges"]:
                # Use percentiles to avoid outliers
                p1 = float(non_null_data.quantile(0.01))
                p99 = float(non_null_data.quantile(0.99))
                range_val = p99 - p1
                padding = range_val * opts["range_padding_factor"]
                suggested_min = p1 - padding
                suggested_max = p99 + padding
            else:
                # Use min/max with padding
                range_val = max_val - min_val
                padding = range_val * opts["range_padding_factor"]
                suggested_min = min_val - padding
                suggested_max = max_val + padding
            
            # Only suggest if range is reasonable (not too wide relative to values)
            # For very small ranges, always suggest; for larger ranges, check ratio
            max_abs_val = max(abs(min_val), abs(max_val), 1)
            range_ratio = (suggested_max - suggested_min) / max_abs_val if max_abs_val > 0 else float('inf')
            if range_val > 0 and (range_ratio < 10 or range_val < 100):
                suggestions.append({
                    "column": column,
                    "method": "expect_column_values_to_be_in_range",
                    "params": {"min_val": suggested_min, "max_val": suggested_max},
                    "confidence": 0.8,  # Moderate confidence for inferred ranges
                    "reason": f"Observed range: {min_val:.2f} to {max_val:.2f}",
                    "dimension": "Validity",
                })
    
    elif data_type == "Date/Time":
        # Date range suggestion
        dates = pd.to_datetime(non_null_data, errors="coerce")
        valid_dates = dates.dropna()
        
        if len(valid_dates) > 0:
            min_date = valid_dates.min()
            max_date = valid_dates.max()
            
            suggestions.append({
                "column": column,
                "method": "expect_column_values_to_be_in_date_range",
                "params": {
                    "min_date": min_date.strftime("%Y-%m-%d"),
                    "max_date": max_date.strftime("%Y-%m-%d"),
                },
                "confidence": 0.85,
                "reason": f"Observed date range: {min_date.strftime('%Y-%m-%d')} to {max_date.strftime('%Y-%m-%d')}",
                "dimension": "Validity",
            })
            
            # Timeliness suggestion (if dates are recent)
            reference_date = pd.Timestamp.now()
            days_old = (reference_date - max_date).days
            
            if days_old < opts["timeliness_max_age_days"]:
                suggestions.append({
                    "column": column,
                    "method": "expect_column_values_to_be_recent",
                    "params": {
                        "max_age_days": opts["timeliness_max_age_days"],
                        "reference_date": None,  # Will use current date
                    },
                    "confidence": 0.75,  # Lower confidence as this is more subjective
                    "reason": f"Most recent date is {days_old} days old",
                    "dimension": "Timeliness",
                })
    
    return suggestions


def generate_suggestions(
    df: pd.DataFrame,
    columns: Optional[List[str]] = None,
    options: Optional[Dict[str, Any]] = None
) -> List[Dict[str, Any]]:
    """
    Generate validation suggestions for specified columns (or all columns).
    
    Args:
        df: DataFrame to analyze
        columns: Optional list of column names to analyze. If None, analyzes all columns.
        options: Optional dict of configuration options (see DEFAULT_OPTIONS)
    
    Returns:
        List of suggestion dictionaries (see analyze_column_for_suggestions)
    """
    if df is None or len(df) == 0:
        return []
    
    if columns is None:
        columns = list(df.columns)
    
    all_suggestions = []
    for column in columns:
        if column in df.columns:
            suggestions = analyze_column_for_suggestions(df, column, options)
            all_suggestions.extend(suggestions)
    
    return all_suggestions


def suggestions_to_json(suggestions: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Convert suggestions to JSON format compatible with run_rules_from_json.
    
    Args:
        suggestions: List of suggestion dictionaries
    
    Returns:
        Dict mapping method names to lists of parameter dicts
    """
    json_rules = {}
    
    for suggestion in suggestions:
        method = suggestion["method"]
        params = suggestion["params"].copy()
        params["column"] = suggestion["column"]
        
        if method not in json_rules:
            json_rules[method] = []
        json_rules[method].append(params)
    
    return json_rules
