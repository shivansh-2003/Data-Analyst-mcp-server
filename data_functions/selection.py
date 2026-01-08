"""
Data selection operations module for MCP Server.
Handles column selection, row filtering, and sampling operations.
"""

import logging
from typing import List, Optional, Dict, Any
import pandas as pd

from .core import get_table_data, commit_dataframe, _record_operation

logger = logging.getLogger(__name__)


def select_columns(
    session_id: str,
    columns: List[str],
    keep: bool = True,
    table_name: str = "current"
) -> Dict[str, Any]:
    """
    Select or drop specific columns from a table.
    
    Args:
        session_id: Unique session identifier
        columns: List of column names
        keep: If True, keep these columns; if False, drop these columns (default: True)
        table_name: Name of the table (default: "current")
    
    Returns:
        Dictionary with operation result and updated column count
    """
    try:
        df = get_table_data(session_id, table_name)
        if df is None:
            return {
                "success": False,
                "error": f"Table '{table_name}' not found in session {session_id}"
            }
        
        original_columns = list(df.columns)
        
        if keep:
            # Keep only specified columns
            invalid_cols = [col for col in columns if col not in df.columns]
            if invalid_cols:
                return {
                    "success": False,
                    "error": f"Columns not found: {', '.join(invalid_cols)}"
                }
            
            df = df[columns]
            selected_cols = columns
        else:
            # Drop specified columns
            invalid_cols = [col for col in columns if col not in df.columns]
            if invalid_cols:
                return {
                    "success": False,
                    "error": f"Columns not found: {', '.join(invalid_cols)}"
                }
            
            df = df.drop(columns=columns)
            selected_cols = [col for col in original_columns if col not in columns]
        
        # Commit changes
        if commit_dataframe(session_id, table_name, df):
            # Record operation
            _record_operation(session_id, table_name, {
                "type": "select_columns",
                "columns": columns,
                "keep": keep,
                "original_columns": original_columns,
                "new_columns": list(df.columns)
            })
            
            return {
                "success": True,
                "message": f"{'Kept' if keep else 'Dropped'} {len(columns)} columns",
                "session_id": session_id,
                "table_name": table_name,
                "action": "keep" if keep else "drop",
                "columns_affected": columns,
                "original_column_count": len(original_columns),
                "new_column_count": len(df.columns),
                "selected_columns": selected_cols,
                "preview": df.head(5).to_dict(orient="records")
            }
        else:
            return {
                "success": False,
                "error": "Failed to save changes to session"
            }
            
    except Exception as e:
        logger.error(f"Failed to select columns: {e}")
        return {
            "success": False,
            "error": f"Failed to select columns: {str(e)}"
        }


def filter_rows(
    session_id: str,
    condition: str,
    table_name: str = "current"
) -> Dict[str, Any]:
    """
    Filter rows based on a boolean condition.
    
    Args:
        session_id: Unique session identifier
        condition: Boolean expression string (e.g., "Price > 11.0", "Company == 'Apple'")
        table_name: Name of the table (default: "current")
    
    Returns:
        Dictionary with operation result and filtered row count
    """
    try:
        df = get_table_data(session_id, table_name)
        if df is None:
            return {
                "success": False,
                "error": f"Table '{table_name}' not found in session {session_id}"
            }
        
        original_count = len(df)
        
        try:
            # Apply the condition
            mask = df.eval(condition)
            df_filtered = df[mask]
        except Exception as e:
            return {
                "success": False,
                "error": f"Invalid condition '{condition}': {str(e)}"
            }
        
        filtered_count = len(df_filtered)
        dropped_count = original_count - filtered_count
        
        # Commit changes
        if commit_dataframe(session_id, table_name, df_filtered):
            # Record operation
            _record_operation(session_id, table_name, {
                "type": "filter_rows",
                "condition": condition,
                "original_count": original_count,
                "filtered_count": filtered_count,
                "dropped_count": dropped_count
            })
            
            return {
                "success": True,
                "message": f"Filtered to {filtered_count} rows ({dropped_count} rows removed)",
                "session_id": session_id,
                "table_name": table_name,
                "condition": condition,
                "original_count": original_count,
                "filtered_count": filtered_count,
                "dropped_count": dropped_count,
                "preview": df_filtered.head(5).to_dict(orient="records")
            }
        else:
            return {
                "success": False,
                "error": "Failed to save changes to session"
            }
            
    except Exception as e:
        logger.error(f"Failed to filter rows: {e}")
        return {
            "success": False,
            "error": f"Failed to filter rows: {str(e)}"
        }


def sample_rows(
    session_id: str,
    n: Optional[int] = None,
    frac: Optional[float] = None,
    random_state: Optional[int] = None,
    table_name: str = "current"
) -> Dict[str, Any]:
    """
    Sample random rows from a table.
    
    Args:
        session_id: Unique session identifier
        n: Number of rows to sample (optional)
        frac: Fraction of rows to sample (0.0 to 1.0) (optional)
        random_state: Random seed for reproducibility (optional)
        table_name: Name of the table (default: "current")
    
    Returns:
        Dictionary with operation result and sampled row count
    """
    try:
        df = get_table_data(session_id, table_name)
        if df is None:
            return {
                "success": False,
                "error": f"Table '{table_name}' not found in session {session_id}"
            }
        
        original_count = len(df)
        
        # Validate sampling parameters
        if n is not None and frac is not None:
            return {
                "success": False,
                "error": "Cannot specify both 'n' and 'frac' parameters"
            }
        
        if n is not None:
            if n <= 0:
                return {
                    "success": False,
                    "error": "Sample size 'n' must be positive"
                }
            if n > original_count:
                return {
                    "success": False,
                    "error": f"Sample size {n} exceeds table size {original_count}"
                }
        
        if frac is not None:
            if frac <= 0 or frac > 1:
                return {
                    "success": False,
                    "error": "Fraction 'frac' must be between 0 and 1"
                }
        
        # Sample the data
        df_sampled = df.sample(n=n, frac=frac, random_state=random_state)
        sampled_count = len(df_sampled)
        
        # Commit changes (this creates a new table state)
        if commit_dataframe(session_id, table_name, df_sampled):
            # Record operation
            _record_operation(session_id, table_name, {
                "type": "sample_rows",
                "n": n,
                "frac": frac,
                "random_state": random_state,
                "original_count": original_count,
                "sampled_count": sampled_count
            })
            
            return {
                "success": True,
                "message": f"Sampled {sampled_count} rows",
                "session_id": session_id,
                "table_name": table_name,
                "sampling_method": "n_rows" if n is not None else "fraction",
                "sampling_value": n if n is not None else frac,
                "random_state": random_state,
                "original_count": original_count,
                "sampled_count": sampled_count,
                "preview": df_sampled.head(5).to_dict(orient="records")
            }
        else:
            return {
                "success": False,
                "error": "Failed to save changes to session"
            }
            
    except Exception as e:
        logger.error(f"Failed to sample rows: {e}")
        return {
            "success": False,
            "error": f"Failed to sample rows: {str(e)}"
        }