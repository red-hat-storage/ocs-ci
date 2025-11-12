"""
Workload Registry for Krkn Chaos Testing.

This module provides a centralized registry for workload types, making it easy
to add new workload types without modifying multiple files.
"""

import logging
from dataclasses import dataclass
from typing import List, Optional, Dict

log = logging.getLogger(__name__)


@dataclass
class WorkloadTypeConfig:
    """
    Configuration for a workload type.

    This defines what fixtures and factory methods are needed for each workload type.
    """

    # Workload type name (e.g., "VDBENCH", "CNV_WORKLOAD")
    name: str

    # Required pytest fixture names (e.g., ["resiliency_workload", "vdbench_block_config"])
    required_fixtures: List[str]

    # Factory method name (e.g., "_create_vdbench_workloads_for_project")
    factory_method: str

    # Fixture parameter names in order they should be passed to factory method
    # (e.g., ["resiliency_workload", "vdbench_block_config", "vdbench_filesystem_config"])
    fixture_params: List[str]

    # Whether this workload type is enabled by default
    enabled_by_default: bool = True

    # Optional description
    description: str = ""


class KrknWorkloadRegistry:
    """
    Registry for Krkn workload types.

    This provides a single place to define all workload types and their requirements.
    Adding a new workload type only requires adding an entry here.
    """

    # Registry of workload type configurations
    _registry: Dict[str, WorkloadTypeConfig] = {}

    @classmethod
    def register(cls, config: WorkloadTypeConfig):
        """Register a workload type configuration."""
        cls._registry[config.name] = config
        log.debug(f"Registered workload type: {config.name}")

    @classmethod
    def get(cls, workload_type: str) -> Optional[WorkloadTypeConfig]:
        """Get configuration for a workload type."""
        return cls._registry.get(workload_type)

    @classmethod
    def get_required_fixtures(cls, workload_type: str) -> List[str]:
        """Get list of required fixtures for a workload type."""
        config = cls.get(workload_type)
        return config.required_fixtures if config else []

    @classmethod
    def get_fixture_params(cls, workload_type: str) -> List[str]:
        """Get list of fixture parameters for factory method."""
        config = cls.get(workload_type)
        return config.fixture_params if config else []

    @classmethod
    def get_factory_method(cls, workload_type: str) -> Optional[str]:
        """Get factory method name for a workload type."""
        config = cls.get(workload_type)
        return config.factory_method if config else None

    @classmethod
    def is_registered(cls, workload_type: str) -> bool:
        """Check if a workload type is registered."""
        return workload_type in cls._registry

    @classmethod
    def get_all_types(cls) -> List[str]:
        """Get list of all registered workload types."""
        return list(cls._registry.keys())

    @classmethod
    def get_all_configs(cls) -> List[WorkloadTypeConfig]:
        """Get all registered workload configurations."""
        return list(cls._registry.values())


# ============================================================================
# WORKLOAD TYPE REGISTRATIONS
# ============================================================================
# To add a new workload type, just add a registration here!
# ============================================================================

# VDBENCH Workload
KrknWorkloadRegistry.register(
    WorkloadTypeConfig(
        name="VDBENCH",
        required_fixtures=[
            "resiliency_workload",
            "vdbench_block_config",
            "vdbench_filesystem_config",
        ],
        factory_method="_create_vdbench_workloads_for_project",
        fixture_params=[
            "resiliency_workload",
            "vdbench_block_config",
            "vdbench_filesystem_config",
        ],
        description="Traditional VDBENCH workloads on CephFS and RBD",
    )
)

# CNV Workload
KrknWorkloadRegistry.register(
    WorkloadTypeConfig(
        name="CNV_WORKLOAD",
        required_fixtures=["multi_cnv_workload"],
        factory_method="_create_cnv_workloads_for_project",
        fixture_params=["multi_cnv_workload"],
        description="CNV-based virtual machine workloads",
    )
)

# RGW Workload (S3 workload on RGW buckets)
KrknWorkloadRegistry.register(
    WorkloadTypeConfig(
        name="RGW_WORKLOAD",
        required_fixtures=["awscli_pod"],
        factory_method="_create_rgw_workloads_for_project",
        fixture_params=["awscli_pod"],
        description="RGW/S3 workload for object storage stress testing",
    )
)

# ============================================================================
# TO ADD A NEW WORKLOAD TYPE:
# ============================================================================
# 1. Add the workload implementation in krkn_workload_factory.py
# 2. Add registration here:
#
# KrknWorkloadRegistry.register(WorkloadTypeConfig(
#     name="MY_NEW_WORKLOAD",
#     required_fixtures=["my_fixture_1", "my_fixture_2"],
#     factory_method="_create_my_workloads_for_project",
#     fixture_params=["my_fixture_1", "my_fixture_2"],
#     description="Description of my new workload",
# ))
#
# That's it! No need to modify conftest.py or create_workload_ops().
# ============================================================================
