"""
Data cleaning operations module for MCP Server.
Handles missing values, row operations, value replacements, and string cleaning.
"""

import logging
from typing import List, Optional, Any, Dict
import pandas as pd
import numpy as np

from .core import get_table_data, commit_dataframe, _record_operation

logger = logging.getLogger(__name__)


def drop_rows(
    session_id: str,
    indices: Optional[List[int]] = None,
    condition: Optional[str] = None,
    subset: Optional[List[str]] = None,
    keep: str = "first",
    table_name: str = "current"
) -> Dict[str, Any]:
    """
    Remove rows from a table by index, condition, or duplicates.
    
    Args:
        session_id: Unique session identifier
        indices: List of row indices to drop (optional)
        condition: Boolean condition string (e.g., "Price > 100") (optional)
        subset: Column names for duplicate detection (optional)
        keep: Which duplicates to keep - "first", "last", or False (default: "first")
        table_name: Name of the table (default: "current")
    
    Returns:
        Dictionary with operation result and updated table preview
    """
    try:
        df = get_table_data(session_id, table_name)
        if df is None:
            return {
                "success": False,
                "error": f"Table '{table_name}' not found in session {session_id}"
            }
        
        original_count = len(df)
        
        # Handle different drop methods
        if indices is not None:
            # Drop by index
            df = df.drop(df.index[indices])
            operation_type = "drop_by_index"
            operation_details = {"indices": indices}
        elif condition is not None:
            # Drop by condition
            try:
                mask = df.eval(condition)
                df = df[~mask]
                operation_type = "drop_by_condition"
                operation_details = {"condition": condition}
            except Exception as e:
                return {
                    "success": False,
                    "error": f"Invalid condition '{condition}': {str(e)}"
                }
        elif subset is not None:
            # Drop duplicates
            original_len = len(df)
            df = df.drop_duplicates(subset=subset, keep=keep)
            dropped_count = original_len - len(df)
            operation_type = "drop_duplicates"
            operation_details = {"subset": subset, "keep": keep, "dropped_count": dropped_count}
        else:
            return {
                "success": False,
                "error": "Must specify one of: indices, condition, or subset"
            }
        
        dropped_count = original_count - len(df)
        
        # Commit changes
        if commit_dataframe(session_id, table_name, df):
            # Record operation
            _record_operation(session_id, table_name, {
                "type": operation_type,
                "details": operation_details,
                "dropped_count": dropped_count,
                "original_count": original_count,
                "new_count": len(df)
            })
            
            return {
                "success": True,
                "message": f"Dropped {dropped_count} rows",
                "session_id": session_id,
                "table_name": table_name,
                "original_count": original_count,
                "new_count": len(df),
                "dropped_count": dropped_count,
                "preview": df.head(5).to_dict(orient="records")
            }
        else:
            return {
                "success": False,
                "error": "Failed to save changes to session"
            }
            
    except Exception as e:
        logger.error(f"Failed to drop rows: {e}")
        return {
            "success": False,
            "error": f"Failed to drop rows: {str(e)}"
        }


def fill_missing(
    session_id: str,
    value: Optional[Any] = None,
    method: Optional[str] = None,
    columns: Optional[List[str]] = None,
    table_name: str = "current"
) -> Dict[str, Any]:
    """
    Fill missing (NaN) values in specified columns.
    
    Args:
        session_id: Unique session identifier
        value: Specific value to fill (optional)
        method: Fill method - "ffill", "bfill", "mean", "median", "mode" (optional)
        columns: List of column names to fill (optional, fills all columns if not specified)
        table_name: Name of the table (default: "current")
    
    Returns:
        Dictionary with operation result and number of values filled
    """
    try:
        df = get_table_data(session_id, table_name)
        if df is None:
            return {
                "success": False,
                "error": f"Table '{table_name}' not found in session {session_id}"
            }
        
        # Determine columns to fill
        if columns is None:
            columns = list(df.columns)
        
        # Validate columns exist
        invalid_cols = [col for col in columns if col not in df.columns]
        if invalid_cols:
            return {
                "success": False,
                "error": f"Columns not found: {', '.join(invalid_cols)}"
            }
        
        filled_count = 0
        fill_details = {}
        
        for col in columns:
            if df[col].isnull().any():
                missing_before = df[col].isnull().sum()
                
                if value is not None:
                    # Fill with specific value
                    df[col] = df[col].fillna(value)
                    fill_method = f"value_{value}"
                elif method == "ffill":
                    df[col] = df[col].fillna(method='ffill')
                    fill_method = "forward_fill"
                elif method == "bfill":
                    df[col] = df[col].fillna(method='bfill')
                    fill_method = "backward_fill"
                elif method == "mean":
                    df[col] = df[col].fillna(df[col].mean())
                    fill_method = "mean"
                elif method == "median":
                    df[col] = df[col].fillna(df[col].median())
                    fill_method = "median"
                elif method == "mode":
                    mode_val = df[col].mode()
                    if len(mode_val) > 0:
                        df[col] = df[col].fillna(mode_val[0])
                    fill_method = "mode"
                else:
                    return {
                        "success": False,
                        "error": f"Invalid fill method: {method}"
                    }
                
                missing_after = df[col].isnull().sum()
                filled_in_col = missing_before - missing_after
                filled_count += filled_in_col
                fill_details[col] = {
                    "method": fill_method,
                    "filled": filled_in_col,
                    "remaining_missing": missing_after
                }
        
        # Commit changes
        if commit_dataframe(session_id, table_name, df):
            # Record operation
            _record_operation(session_id, table_name, {
                "type": "fill_missing",
                "method": method,
                "value": value,
                "columns": columns,
                "filled_count": filled_count,
                "fill_details": fill_details
            })
            
            return {
                "success": True,
                "message": f"Filled {filled_count} missing values",
                "session_id": session_id,
                "table_name": table_name,
                "filled_count": filled_count,
                "fill_details": fill_details,
                "preview": df.head(5).to_dict(orient="records")
            }
        else:
            return {
                "success": False,
                "error": "Failed to save changes to session"
            }
            
    except Exception as e:
        logger.error(f"Failed to fill missing values: {e}")
        return {
            "success": False,
            "error": f"Failed to fill missing values: {str(e)}"
        }


def drop_missing(
    session_id: str,
    how: str = "any",
    thresh: Optional[int] = None,
    axis: int = 0,
    subset: Optional[List[str]] = None,
    table_name: str = "current"
) -> Dict[str, Any]:
    """
    Drop rows or columns with missing values.
    
    Args:
        session_id: Unique session identifier
        how: Drop rows/cols with "any" or "all" missing values (default: "any")
        thresh: Minimum number of non-NA values required (optional)
        axis: 0 for rows, 1 for columns (default: 0)
        subset: Column names to consider (optional)
        table_name: Name of the table (default: "current")
    
    Returns:
        Dictionary with operation result and number of rows/columns dropped
    """
    try:
        df = get_table_data(session_id, table_name)
        if df is None:
            return {
                "success": False,
                "error": f"Table '{table_name}' not found in session {session_id}"
            }
        
        original_shape = df.shape
        
        # Drop missing values
        df_cleaned = df.dropna(how=how, thresh=thresh, axis=axis, subset=subset)
        
        if axis == 0:
            # Dropped rows
            dropped_count = original_shape[0] - df_cleaned.shape[0]
            dropped_type = "rows"
        else:
            # Dropped columns
            dropped_count = original_shape[1] - df_cleaned.shape[1]
            dropped_type = "columns"
        
        # Commit changes
        if commit_dataframe(session_id, table_name, df_cleaned):
            # Record operation
            _record_operation(session_id, table_name, {
                "type": "drop_missing",
                "how": how,
                "thresh": thresh,
                "axis": axis,
                "subset": subset,
                "dropped_count": dropped_count,
                "dropped_type": dropped_type,
                "original_shape": original_shape,
                "new_shape": df_cleaned.shape
            })
            
            return {
                "success": True,
                "message": f"Dropped {dropped_count} {dropped_type} with missing values",
                "session_id": session_id,
                "table_name": table_name,
                "dropped_count": dropped_count,
                "dropped_type": dropped_type,
                "original_shape": original_shape,
                "new_shape": df_cleaned.shape,
                "preview": df_cleaned.head(5).to_dict(orient="records")
            }
        else:
            return {
                "success": False,
                "error": "Failed to save changes to session"
            }
            
    except Exception as e:
        logger.error(f"Failed to drop missing values: {e}")
        return {
            "success": False,
            "error": f"Failed to drop missing values: {str(e)}"
        }


def replace_values(
    session_id: str,
    to_replace: Dict[str, Dict[str, Any]],
    value: Optional[Any] = None,
    regex: bool = False,
    table_name: str = "current"
) -> Dict[str, Any]:
    """
    Replace specific values in the table.
    
    Args:
        session_id: Unique session identifier
        to_replace: Dictionary mapping column names to replacement dictionaries
                   (e.g., {"Status": {"old": "new"}, "Type": {0: "No", 1: "Yes"}})
        value: Replacement value for all matches (optional)
        regex: Whether to_replace contains regex patterns (default: False)
        table_name: Name of the table (default: "current")
    
    Returns:
        Dictionary with operation result
    """
    try:
        df = get_table_data(session_id, table_name)
        if df is None:
            return {
                "success": False,
                "error": f"Table '{table_name}' not found in session {session_id}"
            }
        
        replacement_details = {}
        
        for col, replacements in to_replace.items():
            if col not in df.columns:
                return {
                    "success": False,
                    "error": f"Column '{col}' not found in table"
                }
            
            # Count replacements before and after
            before_count = len(df)
            
            # Apply replacements
            if value is not None:
                # Replace all matching values with single value
                df[col] = df[col].replace(replacements, value)
                replacement_details[col] = {
                    "mode": "single_value",
                    "replacements": replacements,
                    "replacement_value": value
                }
            else:
                # Use replacement dictionary
                df[col] = df[col].replace(replacements, regex=regex)
                replacement_details[col] = {
                    "mode": "dictionary",
                    "replacements": replacements,
                    "regex": regex
                }
        
        # Commit changes
        if commit_dataframe(session_id, table_name, df):
            # Record operation
            _record_operation(session_id, table_name, {
                "type": "replace_values",
                "replacements": replacement_details
            })
            
            return {
                "success": True,
                "message": f"Replaced values in {len(to_replace)} columns",
                "session_id": session_id,
                "table_name": table_name,
                "replacements": replacement_details,
                "preview": df.head(5).to_dict(orient="records")
            }
        else:
            return {
                "success": False,
                "error": "Failed to save changes to session"
            }
            
    except Exception as e:
        logger.error(f"Failed to replace values: {e}")
        return {
            "success": False,
            "error": f"Failed to replace values: {str(e)}"
        }


def clean_strings(
    session_id: str,
    columns: List[str],
    operation: str = "strip",
    table_name: str = "current"
) -> Dict[str, Any]:
    """
    Clean string columns (strip whitespace, convert case).
    
    Args:
        session_id: Unique session identifier
        columns: List of column names to clean
        operation: Cleaning operation - "strip", "lower", "upper", "title" (default: "strip")
        table_name: Name of the table (default: "current")
    
    Returns:
        Dictionary with operation result
    """
    try:
        df = get_table_data(session_id, table_name)
        if df is None:
            return {
                "success": False,
                "error": f"Table '{table_name}' not found in session {session_id}"
            }
        
        # Validate columns exist and are string type
        invalid_cols = []
        for col in columns:
            if col not in df.columns:
                invalid_cols.append(f"'{col}' not found")
            elif df[col].dtype != 'object':
                invalid_cols.append(f"'{col}' is not a string column")
        
        if invalid_cols:
            return {
                "success": False,
                "error": f"Invalid columns: {', '.join(invalid_cols)}"
            }
        
        # Apply cleaning operation
        cleaning_details = {}
        
        for col in columns:
            before_count = len(df)
            
            if operation == "strip":
                df[col] = df[col].str.strip()
                cleaned_count = before_count - df[col].isnull().sum()
            elif operation == "lower":
                df[col] = df[col].str.lower()
                cleaned_count = before_count
            elif operation == "upper":
                df[col] = df[col].str.upper()
                cleaned_count = before_count
            elif operation == "title":
                df[col] = df[col].str.title()
                cleaned_count = before_count
            else:
                return {
                    "success": False,
                    "error": f"Invalid operation: {operation}"
                }
            
            cleaning_details[col] = {
                "operation": operation,
                "processed_count": cleaned_count
            }
        
        # Commit changes
        if commit_dataframe(session_id, table_name, df):
            # Record operation
            _record_operation(session_id, table_name, {
                "type": "clean_strings",
                "columns": columns,
                "operation": operation,
                "cleaning_details": cleaning_details
            })
            
            return {
                "success": True,
                "message": f"Cleaned {len(columns)} string columns",
                "session_id": session_id,
                "table_name": table_name,
                "operation": operation,
                "columns_cleaned": columns,
                "cleaning_details": cleaning_details,
                "preview": df.head(5).to_dict(orient="records")
            }
        else:
            return {
                "success": False,
                "error": "Failed to save changes to session"
            }
            
    except Exception as e:
        logger.error(f"Failed to clean strings: {e}")
        return {
            "success": False,
            "error": f"Failed to clean strings: {str(e)}"
        }


def remove_outliers(
    session_id: str,
    columns: List[str],
    method: str = "iqr",
    threshold: float = 1.5,
    table_name: str = "current"
) -> Dict[str, Any]:
    """
    Remove outliers from numeric columns using IQR or z-score method.
    
    Args:
        session_id: Unique session identifier
        columns: List of numeric column names
        method: Outlier detection method - "iqr" or "zscore" (default: "iqr")
        threshold: Threshold multiplier for IQR or z-score (default: 1.5)
        table_name: Name of the table (default: "current")
    
    Returns:
        Dictionary with operation result and number of rows removed
    """
    try:
        df = get_table_data(session_id, table_name)
        if df is None:
            return {
                "success": False,
                "error": f"Table '{table_name}' not found in session {session_id}"
            }
        
        original_count = len(df)
        outlier_details = {}
        
        for col in columns:
            if col not in df.columns:
                return {
                    "success": False,
                    "error": f"Column '{col}' not found in table"
                }
            
            if not pd.api.types.is_numeric_dtype(df[col]):
                return {
                    "success": False,
                    "error": f"Column '{col}' is not numeric"
                }
            
            if method == "iqr":
                # IQR method
                Q1 = df[col].quantile(0.25)
                Q3 = df[col].quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - threshold * IQR
                upper_bound = Q3 + threshold * IQR
                
                outlier_mask = (df[col] < lower_bound) | (df[col] > upper_bound)
                
                outlier_details[col] = {
                    "method": "iqr",
                    "threshold": threshold,
                    "lower_bound": lower_bound,
                    "upper_bound": upper_bound,
                    "outliers_found": outlier_mask.sum()
                }
                
            elif method == "zscore":
                # Z-score method
                z_scores = np.abs((df[col] - df[col].mean()) / df[col].std())
                outlier_mask = z_scores > threshold
                
                outlier_details[col] = {
                    "method": "zscore",
                    "threshold": threshold,
                    "outliers_found": outlier_mask.sum()
                }
            else:
                return {
                    "success": False,
                    "error": f"Invalid method: {method}. Use 'iqr' or 'zscore'"
                }
            
            # Remove outliers
            df = df[~outlier_mask]
        
        dropped_count = original_count - len(df)
        
        # Commit changes
        if commit_dataframe(session_id, table_name, df):
            # Record operation
            _record_operation(session_id, table_name, {
                "type": "remove_outliers",
                "method": method,
                "columns": columns,
                "threshold": threshold,
                "outlier_details": outlier_details,
                "dropped_count": dropped_count
            })
            
            return {
                "success": True,
                "message": f"Removed {dropped_count} outlier rows",
                "session_id": session_id,
                "table_name": table_name,
                "method": method,
                "threshold": threshold,
                "outlier_details": outlier_details,
                "dropped_count": dropped_count,
                "preview": df.head(5).to_dict(orient="records")
            }
        else:
            return {
                "success": False,
                "error": "Failed to save changes to session"
            }
            
    except Exception as e:
        logger.error(f"Failed to remove outliers: {e}")
        return {
            "success": False,
            "error": f"Failed to remove outliers: {str(e)}"
        }