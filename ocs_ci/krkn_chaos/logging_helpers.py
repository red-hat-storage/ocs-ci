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

    log.info(f"🚀 Starting {test_type} chaos for {target_name}")
    log.info("📊 Test Analysis:")

    # Log common parameters
    if "component_name" in kwargs:
        log.info(f"   • Component: {kwargs['component_name']}")
    if "node_type" in kwargs:
        log.info(f"   • Node type: {kwargs['node_type']}")
    if "node_selector" in kwargs:
        log.info(f"   • Node selector: {kwargs['node_selector']}")
    if "instance_count" in kwargs:
        log.info(f"   • Available instances: {kwargs['instance_count']}")
    if "stress_level" in kwargs:
        log.info(f"   • Stress level: {kwargs['stress_level']}")
    if "duration_multiplier" in kwargs:
        log.info(f"   • Duration multiplier: {kwargs['duration_multiplier']}x")
    if "intensity_multiplier" in kwargs:
        log.info(f"   • Intensity multiplier: {kwargs['intensity_multiplier']}x")

    # Log test safety and configuration info
    if "safety_info" in kwargs:
        log.info(f"   • Safety: {kwargs['safety_info']}")
    if "config_info" in kwargs:
        log.info(f"   • Configuration: {kwargs['config_info']}")
    else:
        log.info("   • Configuration: UNIFIED Krkn config with ALL stress levels")


def log_execution_start(test_type, target_name):
    """
    Log standardized execution start information (simplified version).

    Args:
        test_type: Type of chaos test (e.g., "network chaos", "port chaos")
        target_name: Target component/node name
    """
    log = logging.getLogger(__name__)
    log.info(f"🚀 Starting {test_type} test for {target_name}")
