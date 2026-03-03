"""Data quality checking package. Run expectations, similarity analysis, and reporting on pandas DataFrames."""

from data_quality.checker import DataQualityChecker
from data_quality.dimensions import DIMENSIONS

__all__ = ["DataQualityChecker", "DIMENSIONS"]
