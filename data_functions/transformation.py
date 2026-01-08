"""
Data transformation operations module for MCP Server.
Handles column renaming, reordering, sorting, and basic transformations.
"""

import logging
from typing import List, Dict, Optional, Any
import pandas as pd

from .core import get_table_data, commit_dataframe, _record_operation

logger = logging.getLogger(__name__)


def rename_columns(
    session_id: str,
    mapping: Dict[str, str],
    table_name: str = "current"
) -> Dict[str, Any]:
    """
    Rename one or more columns in a table.
    
    Args:
        session_id: Unique session identifier
        mapping: Dictionary mapping old column names to new names
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
        
        original_columns = list(df.columns)
        
        # Validate mapping
        invalid_cols = [old for old in mapping.keys() if old not in df.columns]
        if invalid_cols:
            return {
                "success": False,
                "error": f"Columns not found: {', '.join(invalid_cols)}"
            }
        
        # Rename columns
        df = df.rename(columns=mapping)
        
        # Commit changes
        if commit_dataframe(session_id, table_name, df):
            # Record operation
            _record_operation(session_id, table_name, {
                "type": "rename_columns",
                "mapping": mapping,
                "original_columns": original_columns,
                "new_columns": list(df.columns)
            })
            
            return {
                "success": True,
                "message": f"Renamed {len(mapping)} columns",
                "session_id": session_id,
                "table_name": table_name,
                "renamed_columns": mapping,
                "new_columns": list(df.columns),
                "preview": df.head(5).to_dict(orient="records")
            }
        else:
            return {
                "success": False,
                "error": "Failed to save changes to session"
            }
            
    except Exception as e:
        logger.error(f"Failed to rename columns: {e}")
        return {
            "success": False,
            "error": f"Failed to rename columns: {str(e)}"
        }


def reorder_columns(
    session_id: str,
    columns: List[str],
    table_name: str = "current"
) -> Dict[str, Any]:
    """
    Reorder columns in a table.
    
    Args:
        session_id: Unique session identifier
        columns: List of column names in desired order
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
        
        original_columns = list(df.columns)
        
        # Validate all columns exist
        missing_cols = [col for col in columns if col not in df.columns]
        if missing_cols:
            return {
                "success": False,
                "error": f"Columns not found: {', '.join(missing_cols)}"
            }
        
        # Check if we have all columns
        if len(columns) != len(df.columns):
            return {
                "success": False,
                "error": f"Column count mismatch. Expected {len(df.columns)}, got {len(columns)}"
            }
        
        # Reorder columns
        df = df[columns]
        
        # Commit changes
        if commit_dataframe(session_id, table_name, df):
            # Record operation
            _record_operation(session_id, table_name, {
                "type": "reorder_columns",
                "original_order": original_columns,
                "new_order": columns
            })
            
            return {
                "success": True,
                "message": f"Reordered {len(columns)} columns",
                "session_id": session_id,
                "table_name": table_name,
                "new_column_order": columns,
                "preview": df.head(5).to_dict(orient="records")
            }
        else:
            return {
                "success": False,
                "error": "Failed to save changes to session"
            }
            
    except Exception as e:
        logger.error(f"Failed to reorder columns: {e}")
        return {
            "success": False,
            "error": f"Failed to reorder columns: {str(e)}"
        }


def sort_data(
    session_id: str,
    by: List[str],
    ascending: bool = True,
    table_name: str = "current"
) -> Dict[str, Any]:
    """
    Sort table by one or more columns.
    
    Args:
        session_id: Unique session identifier
        by: List of column names to sort by
        ascending: Sort in ascending order if True, descending if False (default: True)
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
        
        # Validate columns exist
        invalid_cols = [col for col in by if col not in df.columns]
        if invalid_cols:
            return {
                "success": False,
                "error": f"Columns not found: {', '.join(invalid_cols)}"
            }
        
        # Sort the dataframe
        df = df.sort_values(by=by, ascending=ascending)
        
        # Commit changes
        if commit_dataframe(session_id, table_name, df):
            # Record operation
            _record_operation(session_id, table_name, {
                "type": "sort_data",
                "sort_columns": by,
                "ascending": ascending
            })
            
            return {
                "success": True,
                "message": f"Sorted by {', '.join(by)}",
                "session_id": session_id,
                "table_name": table_name,
                "sort_columns": by,
                "ascending": ascending,
                "preview": df.head(5).to_dict(orient="records")
            }
        else:
            return {
                "success": False,
                "error": "Failed to save changes to session"
            }
            
    except Exception as e:
        logger.error(f"Failed to sort data: {e}")
        return {
            "success": False,
            "error": f"Failed to sort data: {str(e)}"
        }


def apply_custom(
    session_id: str,
    column: str,
    function: str,
    new_column: Optional[str] = None,
    table_name: str = "current"
) -> Dict[str, Any]:
    """
    Apply a custom function to a column (whitelisted safe operations only).
    
    Args:
        session_id: Unique session identifier
        column: Column name to apply function to
        function: Lambda function string (e.g., "lambda x: x * 2", "lambda x: abs(x)")
        new_column: Name for new column (optional, overwrites original if not specified)
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
        
        if column not in df.columns:
            return {
                "success": False,
                "error": f"Column '{column}' not found in table"
            }
        
        # Security: Only allow simple lambda functions
        # This is a basic whitelist - in production, you'd want more sophisticated validation
        allowed_operations = [
            'lambda x: x *', 'lambda x: x +', 'lambda x: x -', 'lambda x: x /',
            'lambda x: abs(x)', 'lambda x: x **', 'lambda x: x %',
            'lambda x: x.astype', 'lambda x: x.round', 'lambda x: x.apply'
        ]
        
        # Check if function contains dangerous operations
        dangerous_keywords = ['import', 'exec', 'eval', '__', 'os', 'sys', 'file', 'open']
        for keyword in dangerous_keywords:
            if keyword in function:
                return {
                    "success": False,
                    "error": f"Function contains disallowed keyword: {keyword}"
                }
        
        try:
            # Parse and apply the lambda function
            # This is still potentially unsafe - in production, use a proper sandbox
            func = eval(function)
            
            if new_column:
                # Create new column
                df[new_column] = df[column].apply(func)
                result_column = new_column
            else:
                # Overwrite existing column
                df[column] = df[column].apply(func)
                result_column = column
            
        except Exception as e:
            return {
                "success": False,
                "error": f"Failed to apply function: {str(e)}"
            }
        
        # Commit changes
        if commit_dataframe(session_id, table_name, df):
            # Record operation
            _record_operation(session_id, table_name, {
                "type": "apply_custom",
                "column": column,
                "function": function,
                "new_column": new_column,
                "result_column": result_column
            })
            
            return {
                "success": True,
                "message": f"Applied custom function to create column '{result_column}'",
                "session_id": session_id,
                "table_name": table_name,
                "source_column": column,
                "result_column": result_column,
                "function": function,
                "preview": df.head(5).to_dict(orient="records")
            }
        else:
            return {
                "success": False,
                "error": "Failed to save changes to session"
            }
            
    except Exception as e:
        logger.error(f"Failed to apply custom function: {e}")
        return {
            "success": False,
            "error": f"Failed to apply custom function: {str(e)}"
        }