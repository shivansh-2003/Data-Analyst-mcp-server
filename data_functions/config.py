"""
Minimal configuration flags for MCP data functions.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# Feature Flags
ENABLE_HTTP_SYNC = os.getenv("ENABLE_HTTP_SYNC", "true").lower() == "true"
ENABLE_CACHE_IN_MEMORY = os.getenv("ENABLE_CACHE_IN_MEMORY", "true").lower() == "true"