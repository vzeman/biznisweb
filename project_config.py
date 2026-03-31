#!/usr/bin/env python3
"""
Backward-compatible shim for callers still importing project_config directly.
"""

from reporting_core.config import *  # noqa: F401,F403
