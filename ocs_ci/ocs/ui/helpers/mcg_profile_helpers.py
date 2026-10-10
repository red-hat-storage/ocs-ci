"""
Helper functions for MCG Performance Profile testing

This module provides utility functions for working with MCG performance profiles
in the Configure Performance page and StorageCluster CR.

Related to RHSTOR-8629: Support Performance Profiles for MCG
"""

import logging
from typing import Optional, Dict, Any

from ocs_ci.ocs.ocp import OCP


logger = logging.getLogger(__name__)


# MCG Profiles and their resource values (x86)
MCG_PROFILES = {
    "default": {
        "core_cpu_req": "500m",
        "core_cpu_lim": "1",
        "core_mem_req": "1Gi",
        "core_mem_lim": "4Gi",
        "db_cpu_req": "1",
        "db_cpu_lim": "1",
        "db_mem_req": "2Gi",
        "db_mem_lim": "2Gi",
        "endpoint_cpu_req": "500m",
        "endpoint_cpu_lim": "2",
        "endpoint_mem_req": "1Gi",
        "endpoint_mem_lim": "3Gi",
        "endpoint_min": 1,
        "endpoint_max": 2,
        "db_instances": 2,
        "pvpool_cpu_req": "400m",
        "pvpool_cpu_lim": "400m",
        "pvpool_mem_req": "800Mi",
        "pvpool_mem_lim": "800Mi",
        "pvpool_num_volumes": 3,
    },
    "mixed-workload": {
        "core_cpu_req": "1",
        "core_cpu_lim": "2",
        "core_mem_req": "2Gi",
        "core_mem_lim": "4Gi",
        "db_cpu_req": "4",
        "db_cpu_lim": "4",
        "db_mem_req": "8Gi",
        "db_mem_lim": "8Gi",
        "endpoint_cpu_req": "2",
        "endpoint_cpu_lim": "4",
        "endpoint_mem_req": "2Gi",
        "endpoint_mem_lim": "4Gi",
        "endpoint_min": 2,
        "endpoint_max": 4,
        "db_instances": 2,
        "pvpool_cpu_req": "1",
        "pvpool_cpu_lim": "1",
        "pvpool_mem_req": "2Gi",
        "pvpool_mem_lim": "2Gi",
        "pvpool_num_volumes": 3,
    },
    "small-objects": {
        "core_cpu_req": "1",
        "core_cpu_lim": "2",
        "core_mem_req": "2Gi",
        "core_mem_lim": "6Gi",
        "db_cpu_req": "6",
        "db_cpu_lim": "6",
        "db_mem_req": "16Gi",
        "db_mem_lim": "16Gi",
        "endpoint_cpu_req": "1",
        "endpoint_cpu_lim": "4",
        "endpoint_mem_req": "2Gi",
        "endpoint_mem_lim": "4Gi",
        "endpoint_min": 2,
        "endpoint_max": 4,
        "db_instances": 2,
        "pvpool_cpu_req": "1",
        "pvpool_cpu_lim": "1",
        "pvpool_mem_req": "2Gi",
        "pvpool_mem_lim": "2Gi",
        "pvpool_num_volumes": 3,
    },
}

# IBM Z (s390x) CPU adjustment factor
IBM_Z_CPU_ADJUST_FACTOR = 0.2


def get_mcg_profile_resources(profile_name: str, arch: str = "x86") -> Dict[str, Any]:
    """Get resource requirements for a specific MCG profile."""
    if profile_name not in MCG_PROFILES:
        raise ValueError(
            f"Unknown profile: {profile_name}. "
            f"Must be one of {list(MCG_PROFILES.keys())}"
        )

    resources = MCG_PROFILES[profile_name].copy()

    # Adjust CPU for IBM Z if requested
    if arch.lower() == "s390x":
        logger.info(f"Applying IBM Z CPU adjustment factor {IBM_Z_CPU_ADJUST_FACTOR}")
        cpu_fields = [
            "core_cpu_req",
            "db_cpu_req",
            "endpoint_cpu_req",
            "pvpool_cpu_req",
        ]
        for field in cpu_fields:
            if field in resources:
                original = resources[field]
                adjusted = adjust_cpu_value(original, IBM_Z_CPU_ADJUST_FACTOR)
                resources[field] = adjusted

    return resources


def adjust_cpu_value(cpu_str: str, factor: float) -> str:
    """Adjust a CPU value by a factor."""
    if cpu_str.endswith("m"):
        value_m = int(cpu_str[:-1])
        adjusted_m = int(value_m * factor)
        return f"{adjusted_m}m"
    else:
        value = float(cpu_str)
        adjusted = value * factor
        if adjusted < 1:
            return f"{int(adjusted * 1000)}m"
        return str(int(adjusted))


def get_storagecluster_mcg_profile() -> Optional[str]:
    """Get the current MCG performance profile from StorageCluster CR."""
    try:
        sc = OCP(kind="StorageCluster", namespace="openshift-storage")
        sc_data = sc.get_resource("ocs-storagecluster")
        profile = (
            sc_data.get("spec", {})
            .get("multiCloudGateway", {})
            .get("performanceProfile")
        )
        logger.info(f"Current MCG profile in StorageCluster: {profile}")
        return profile
    except Exception as e:
        logger.error(f"Error reading MCG profile from StorageCluster CR: {e}")
        return None


def get_storagecluster_core_storage_profile() -> Optional[str]:
    """Get the current Core Storage (Ceph) resource profile from StorageCluster CR."""
    try:
        sc = OCP(kind="StorageCluster", namespace="openshift-storage")
        sc_data = sc.get_resource("ocs-storagecluster")
        profile = sc_data.get("spec", {}).get("resourceProfile")
        logger.info(f"Current Core Storage profile in StorageCluster: {profile}")
        return profile
    except Exception as e:
        logger.error(f"Error reading Core Storage profile from StorageCluster CR: {e}")
        return None


def get_noobaa_performance_profile() -> Optional[str]:
    """Get the performance profile from NooBaa CR."""
    try:
        nb = OCP(kind="NooBaa", namespace="openshift-storage")
        nb_data = nb.get_resource("noobaa")
        profile = nb_data.get("spec", {}).get("performanceProfile")
        logger.info(f"Current performance profile in NooBaa CR: {profile}")
        return profile
    except Exception as e:
        logger.error(f"Error reading profile from NooBaa CR: {e}")
        return None


def normalize_cpu_value(cpu_str: str) -> float:
    """Normalize CPU value to a comparable float (in CPU units)."""
    if not cpu_str:
        return 0.0

    if cpu_str.endswith("m"):
        return float(cpu_str[:-1]) / 1000.0
    else:
        return float(cpu_str)


def normalize_memory_value(mem_str: str) -> int:
    """Normalize memory value to bytes."""
    if not mem_str:
        return 0

    mem_str = mem_str.strip()

    if mem_str.endswith("Gi"):
        return int(float(mem_str[:-2]) * 1024 * 1024 * 1024)
    elif mem_str.endswith("Mi"):
        return int(float(mem_str[:-2]) * 1024 * 1024)
    elif mem_str.endswith("Ki"):
        return int(float(mem_str[:-2]) * 1024)
    else:
        return int(float(mem_str))
