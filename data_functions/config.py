"""
Configuration management for MCP Server and Ingestion API integration.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# Ingestion API Configuration
INGESTION_API_URL = os.getenv("INGESTION_API_URL", "https://data-assistant-m4kl.onrender.com")
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))

# Redis Configuration (for direct access if needed)
REDIS_URL = os.getenv("REDIS_URL", "")
REDIS_TOKEN = os.getenv("REDIS_TOKEN", "")

# Session Configuration
DEFAULT_TTL_MINUTES = int(os.getenv("DEFAULT_TTL_MINUTES", "30"))
MAX_SESSION_SIZE_MB = int(os.getenv("MAX_SESSION_SIZE_MB", "100"))

# Logging Configuration
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FORMAT = os.getenv("LOG_FORMAT", "%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# MCP Server Configuration
MCP_SERVER_NAME = os.getenv("MCP_SERVER_NAME", "Data Assistant MCP Server")
MCP_SERVER_VERSION = os.getenv("MCP_SERVER_VERSION", "1.0.0")

# Feature Flags
ENABLE_HTTP_SYNC = os.getenv("ENABLE_HTTP_SYNC", "true").lower() == "true"
ENABLE_REDIS_DIRECT = os.getenv("ENABLE_REDIS_DIRECT", "false").lower() == "true"
ENABLE_CACHE_IN_MEMORY = os.getenv("ENABLE_CACHE_IN_MEMORY", "true").lower() == "true"

def get_config_summary() -> dict:
    """Get a summary of current configuration (excluding sensitive data)."""
    return {
        "ingestion_api_url": INGESTION_API_URL,
        "request_timeout": REQUEST_TIMEOUT,
        "default_ttl_minutes": DEFAULT_TTL_MINUTES,
        "max_session_size_mb": MAX_SESSION_SIZE_MB,
        "log_level": LOG_LEVEL,
        "mcp_server_name": MCP_SERVER_NAME,
        "mcp_server_version": MCP_SERVER_VERSION,
        "enable_http_sync": ENABLE_HTTP_SYNC,
        "enable_redis_direct": ENABLE_REDIS_DIRECT,
        "enable_cache_in_memory": ENABLE_CACHE_IN_MEMORY
    }