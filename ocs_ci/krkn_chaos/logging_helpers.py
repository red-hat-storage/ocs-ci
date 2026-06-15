"""
Logging helper functions for Krkn chaos tests.

This module provides standardized logging functions to eliminate repeated code
across all Krkn chaos test files.
"""

import logging


def log_test_start(test_type, target_name, **kwargs):
    """
    Log standardized test start information.

    Args:
        test_type: Type of chaos test (e.g., "MULTI-STRESS container", "network", "resource hog")
        target_name: Target component/node name
        **kwargs: Additional parameters to log (e.g., instance_count, node_selector, stress_level)
    """
    log = logging.getLogger(__name__)

    log.info(f"Starting {test_type} chaos for {target_name}")
    if kwargs:
        details = ", ".join(f"{k}={v}" for k, v in kwargs.items())
        log.info(f"  Parameters: {details}")
