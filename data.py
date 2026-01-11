#!/usr/bin/env python3
"""
Data Assistant MCP Server - FastMCP Implementation
Contains MCP tools for data manipulation operations including cleaning, transformation, 
aggregation, feature engineering, and multi-table operations.
"""

import os
from typing import Optional, List, Dict, Any
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
# Import data functions
from data_functions.core import (
    initialize_table,
    get_data_summary,
    list_available_tables,
    undo_last_operation,
    redo_operation
)
from data_functions.cleaning import (
    drop_rows,
    fill_missing,
    drop_missing,
    replace_values,
    clean_strings,
    remove_outliers
)
from data_functions.selection import (
    select_columns,
    filter_rows,
    sample_rows
)
from data_functions.transformation import (
    rename_columns,
    reorder_columns,
    sort_data,
    apply_custom
)

# Create FastMCP server
mcp = FastMCP(
    "Data Assistant MCP Server",
    stateless_http=True,
    transport_security=TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=[
            "localhost:*",
            "127.0.0.1:*",
            "data-analyst-mcp-server.onrender.com:*"  # Your Render domain
        ],
        allowed_origins=[
            "http://localhost:*",
            "https://data-analyst-mcp-server.onrender.com:*"  # HTTPS for Render
        ],
    )
)

# ============================================================================
# Core Operations
# ============================================================================

@mcp.tool()
def initialize_data_table(session_id: str, table_name: str = "current") -> dict:
    """
    Initialize a data table in session. This should be called first to load data into the session.
    
    Args:
        session_id: Unique session identifier
        table_name: Name for the table (default: "current")
    
    Returns:
        Dictionary with success status and initialization details
    
    Note:
        This loads data from the ingestion API. Use the ingestion API to upload files first.
    """
    try:
        result = initialize_table(session_id, table_name)
        return result
    except Exception as e:
        return {
            "success": False,
            "error": f"Failed to initialize table: {str(e)}"
        }


@mcp.tool()
def get_table_summary(session_id: str, table_name: str = "current") -> dict:
    """
    Get summary statistics for a table including row count, column info, data types, and missing values.
    
    Args:
        session_id: Unique session identifier
        table_name: Name of the table (default: "current")
    
    Returns:
        Dictionary containing table summary with rows, columns, dtypes, and missing counts
    
    Example:
        get_table_summary("session_123")
    """
    try:
        result = get_data_summary(session_id, table_name)
        return result
    except Exception as e:
        return {
            "success": False,
            "error": f"Failed to get table summary: {str(e)}"
        }


@mcp.tool()
def list_tables(session_id: str) -> dict:
    """
    List all available tables in a session.
    
    Args:
        session_id: Unique session identifier
    
    Returns:
        Dictionary containing list of table names
    
    Example:
        list_tables("session_123")
    """
    try:
        tables = list_available_tables(session_id)
        return {
            "success": True,
            "tables": tables,
            "count": len(tables)
        }
    except Exception as e:
        return {
            "success": False,
            "error": f"Failed to list tables: {str(e)}"
        }


@mcp.tool()
def undo_operation(session_id: str, table_name: str = "current") -> dict:
    """
    Undo the last operation performed on a table.
    
    Args:
        session_id: Unique session identifier
        table_name: Name of the table (default: "current")
    
    Returns:
        Dictionary with undo result and updated table state
    
    Example:
        undo_operation("session_123")
    """
    try:
        result = undo_last_operation(session_id, table_name)
        return result
    except Exception as e:
        return {
            "success": False,
            "error": f"Failed to undo operation: {str(e)}"
        }


@mcp.tool()
def redo_operation(session_id: str, table_name: str = "current") -> dict:
    """
    Redo the last undone operation on a table.
    
    Args:
        session_id: Unique session identifier
        table_name: Name of the table (default: "current")
    
    Returns:
        Dictionary with redo result and updated table state
    
    Example:
        redo_operation("session_123")
    """
    try:
        result = redo_operation(session_id, table_name)
        return result
    except Exception as e:
        return {
            "success": False,
            "error": f"Failed to redo operation: {str(e)}"
        }


# ============================================================================
# Data Cleaning Operations
# ============================================================================

@mcp.tool()
def drop_rows_from_table(
    session_id: str,
    indices: Optional[List[int]] = None,
    condition: Optional[str] = None,
    subset: Optional[List[str]] = None,
    keep: str = "first",
    table_name: str = "current"
) -> dict:
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
    
    Example:
        drop_rows_from_table("session_123", condition="Price > 1000")
        drop_rows_from_table("session_123", subset=["Company", "Model"], keep="first")
    """
    try:
        result = drop_rows(session_id, indices, condition, subset, keep, table_name)
        return result
    except Exception as e:
        return {
            "success": False,
            "error": f"Failed to drop rows: {str(e)}"
        }


@mcp.tool()
def fill_missing_values(
    session_id: str,
    value: Optional[Any] = None,
    method: Optional[str] = None,
    columns: Optional[List[str]] = None,
    table_name: str = "current"
) -> dict:
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
    
    Example:
        fill_missing_values("session_123", method="mean", columns=["Price"])
        fill_missing_values("session_123", value=0, columns=["Ram", "SSD"])
    """
    try:
        result = fill_missing(session_id, value, method, columns, table_name)
        return result
    except Exception as e:
        return {
            "success": False,
            "error": f"Failed to fill missing values: {str(e)}"
        }


@mcp.tool()
def drop_missing_values(
    session_id: str,
    how: str = "any",
    thresh: Optional[int] = None,
    axis: int = 0,
    subset: Optional[List[str]] = None,
    table_name: str = "current"
) -> dict:
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
    
    Example:
        drop_missing_values("session_123", how="any")
        drop_missing_values("session_123", thresh=5, subset=["Price", "Ram"])
    """
    try:
        result = drop_missing(session_id, how, thresh, axis, subset, table_name)
        return result
    except Exception as e:
        return {
            "success": False,
            "error": f"Failed to drop missing values: {str(e)}"
        }


@mcp.tool()
def replace_table_values(
    session_id: str,
    to_replace: Dict[str, Dict[str, Any]],
    value: Optional[Any] = None,
    regex: bool = False,
    table_name: str = "current"
) -> dict:
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
    
    Example:
        replace_table_values("session_123", {"TouchScreen": {0: "No", 1: "Yes"}})
    """
    try:
        result = replace_values(session_id, to_replace, value, regex, table_name)
        return result
    except Exception as e:
        return {
            "success": False,
            "error": f"Failed to replace values: {str(e)}"
        }


@mcp.tool()
def clean_string_columns(
    session_id: str,
    columns: List[str],
    operation: str = "strip",
    table_name: str = "current"
) -> dict:
    """
    Clean string columns (strip whitespace, convert case).
    
    Args:
        session_id: Unique session identifier
        columns: List of column names to clean
        operation: Cleaning operation - "strip", "lower", "upper", "title" (default: "strip")
        table_name: Name of the table (default: "current")
    
    Returns:
        Dictionary with operation result
    
    Example:
        clean_string_columns("session_123", ["Company", "TypeName"], operation="lower")
    """
    try:
        result = clean_strings(session_id, columns, operation, table_name)
        return result
    except Exception as e:
        return {
            "success": False,
            "error": f"Failed to clean strings: {str(e)}"
        }


@mcp.tool()
def remove_outliers_from_table(
    session_id: str,
    columns: List[str],
    method: str = "iqr",
    threshold: float = 1.5,
    table_name: str = "current"
) -> dict:
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
    
    Example:
        remove_outliers_from_table("session_123", ["Price", "Weight"], method="iqr", threshold=2.0)
    """
    try:
        result = remove_outliers(session_id, columns, method, threshold, table_name)
        return result
    except Exception as e:
        return {
            "success": False,
            "error": f"Failed to remove outliers: {str(e)}"
        }


# ============================================================================
# Selection Operations
# ============================================================================

@mcp.tool()
def select_table_columns(
    session_id: str,
    columns: List[str],
    keep: bool = True,
    table_name: str = "current"
) -> dict:
    """
    Select or drop specific columns from a table.
    
    Args:
        session_id: Unique session identifier
        columns: List of column names
        keep: If True, keep these columns; if False, drop these columns (default: True)
        table_name: Name of the table (default: "current")
    
    Returns:
        Dictionary with operation result and updated column count
    
    Example:
        select_table_columns("session_123", ["Company", "Price", "Ram"], keep=True)
    """
    try:
        result = select_columns(session_id, columns, keep, table_name)
        return result
    except Exception as e:
        return {
            "success": False,
            "error": f"Failed to select columns: {str(e)}"
        }


@mcp.tool()
def filter_table_rows(
    session_id: str,
    condition: str,
    table_name: str = "current"
) -> dict:
    """
    Filter rows based on a boolean condition.
    
    Args:
        session_id: Unique session identifier
        condition: Boolean expression string (e.g., "Price > 11.0", "Company == 'Apple'")
        table_name: Name of the table (default: "current")
    
    Returns:
        Dictionary with operation result and filtered row count
    
    Example:
        filter_table_rows("session_123", "Price > 11.0")
        filter_table_rows("session_123", "Company == 'Apple' and Ram >= 8")
    """
    try:
        result = filter_rows(session_id, condition, table_name)
        return result
    except Exception as e:
        return {
            "success": False,
            "error": f"Failed to filter rows: {str(e)}"
        }


@mcp.tool()
def sample_table_rows(
    session_id: str,
    n: Optional[int] = None,
    frac: Optional[float] = None,
    random_state: Optional[int] = None,
    table_name: str = "current"
) -> dict:
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
    
    Example:
        sample_table_rows("session_123", n=100, random_state=42)
        sample_table_rows("session_123", frac=0.1)
    """
    try:
        result = sample_rows(session_id, n, frac, random_state, table_name)
        return result
    except Exception as e:
        return {
            "success": False,
            "error": f"Failed to sample rows: {str(e)}"
        }


# ============================================================================
# Transformation Operations
# ============================================================================

@mcp.tool()
def rename_table_columns(
    session_id: str,
    mapping: Dict[str, str],
    table_name: str = "current"
) -> dict:
    """
    Rename one or more columns in a table.
    
    Args:
        session_id: Unique session identifier
        mapping: Dictionary mapping old column names to new names
        table_name: Name of the table (default: "current")
    
    Returns:
        Dictionary with operation result
    
    Example:
        rename_table_columns("session_123", {"Company": "Manufacturer", "Price": "Cost"})
    """
    try:
        result = rename_columns(session_id, mapping, table_name)
        return result
    except Exception as e:
        return {
            "success": False,
            "error": f"Failed to rename columns: {str(e)}"
        }


@mcp.tool()
def reorder_table_columns(
    session_id: str,
    columns: List[str],
    table_name: str = "current"
) -> dict:
    """
    Reorder columns in a table.
    
    Args:
        session_id: Unique session identifier
        columns: List of column names in desired order
        table_name: Name of the table (default: "current")
    
    Returns:
        Dictionary with operation result
    
    Example:
        reorder_table_columns("session_123", ["Price", "Company", "TypeName"])
    """
    try:
        result = reorder_columns(session_id, columns, table_name)
        return result
    except Exception as e:
        return {
            "success": False,
            "error": f"Failed to reorder columns: {str(e)}"
        }


@mcp.tool()
def sort_table_data(
    session_id: str,
    by: List[str],
    ascending: bool = True,
    table_name: str = "current"
) -> dict:
    """
    Sort table by one or more columns.
    
    Args:
        session_id: Unique session identifier
        by: List of column names to sort by
        ascending: Sort in ascending order if True, descending if False (default: True)
        table_name: Name of the table (default: "current")
    
    Returns:
        Dictionary with operation result
    
    Example:
        sort_table_data("session_123", ["Price"], ascending=False)
        sort_table_data("session_123", ["Company", "Price"])
    """
    try:
        result = sort_data(session_id, by, ascending, table_name)
        return result
    except Exception as e:
        return {
            "success": False,
            "error": f"Failed to sort data: {str(e)}"
        }


@mcp.tool()
def apply_custom_function(
    session_id: str,
    column: str,
    function: str,
    new_column: Optional[str] = None,
    table_name: str = "current"
) -> dict:
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
    
    Example:
        apply_custom_function("session_123", "Price", "lambda x: x * 1.1", "PriceWithTax")
    """
    try:
        result = apply_custom(session_id, column, function, new_column, table_name)
        return result
    except Exception as e:
        return {
            "success": False,
            "error": f"Failed to apply custom function: {str(e)}"
        }
