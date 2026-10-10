"""Helper functions for MCG Performance Profile testing (RHSTOR-8629)."""

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
    """Get resource requirements for MCG profile."""
    if profile_name not in MCG_PROFILES:
        raise ValueError(f"Unknown profile: {profile_name}")
    resources = MCG_PROFILES[profile_name].copy()
    if arch.lower() == "s390x":
        for field in [
            "core_cpu_req",
            "db_cpu_req",
            "endpoint_cpu_req",
            "pvpool_cpu_req",
        ]:
            if field in resources:
                resources[field] = adjust_cpu_value(
                    resources[field], IBM_Z_CPU_ADJUST_FACTOR
                )
    return resources


def adjust_cpu_value(cpu_str: str, factor: float) -> str:
    """Adjust a CPU value by a factor."""
    if cpu_str.endswith("m"):
        value_m = int(cpu_str[:-1])
        adjusted_m = max(1, round(value_m * factor))
        return f"{adjusted_m}m"
    value = float(cpu_str)
    adjusted = value * factor
    if adjusted < 1:
        return f"{int(adjusted * 1000)}m"
    if adjusted == int(adjusted):
        return str(int(adjusted))
    return f"{int(adjusted * 1000)}m"


def get_storagecluster_mcg_profile() -> Optional[str]:
    """Get MCG profile from StorageCluster CR."""
    from ocs_ci.ocs import constants

    sc = OCP(
        kind="StorageCluster",
        namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        resource_name="ocs-storagecluster",
    )
    data = sc.get()
    return data.get("spec", {}).get("multiCloudGateway", {}).get("performanceProfile")


def get_storagecluster_core_storage_profile() -> Optional[str]:
    """Get Core Storage profile from StorageCluster CR."""
    from ocs_ci.ocs import constants

    sc = OCP(
        kind="StorageCluster",
        namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        resource_name="ocs-storagecluster",
    )
    data = sc.get()
    return data.get("spec", {}).get("resourceProfile")


def get_noobaa_performance_profile() -> Optional[str]:
    """Get performance profile from NooBaa CR."""
    from ocs_ci.ocs import constants

    nb = OCP(
        kind="NooBaa",
        namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        resource_name="noobaa",
    )
    data = nb.get()
    return data.get("spec", {}).get("performanceProfile")


def normalize_cpu_value(cpu_str: str) -> float:
    """Normalize CPU to float (CPU units)."""
    if not cpu_str:
        return 0.0
    return float(cpu_str[:-1]) / 1000.0 if cpu_str.endswith("m") else float(cpu_str)


def normalize_memory_value(mem_str: str) -> int:
    """Normalize memory to bytes."""
    if not mem_str:
        return 0
    mem_str = mem_str.strip()
    if mem_str.endswith("Gi"):
        return int(float(mem_str[:-2]) * 1024 * 1024 * 1024)
    elif mem_str.endswith("Mi"):
        return int(float(mem_str[:-2]) * 1024 * 1024)
    elif mem_str.endswith("Ki"):
        return int(float(mem_str[:-2]) * 1024)
    return int(float(mem_str))
