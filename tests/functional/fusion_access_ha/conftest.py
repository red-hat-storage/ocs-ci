"""
Conftest for RHSTOR-8877 — Fusion Access HA (Stretch Cluster) test suite.

Provides session and function-scoped fixtures that expose cluster topology
metadata to individual test cases.

Deployment is NOT triggered here.  When Jenkins runs with ``--deploy``, the
``Deployment.do_deploy_fusion_access_stretch_cluster()`` method in
``ocs_ci/deployment/deployment.py`` is called automatically as part of the
main ``deploy_cluster()`` chain, triggered by ``DEPLOYMENT.fusion_access: true``
in the deployment config YAML.  By the time any test in this suite executes,
the cluster is already fully deployed and validated.
"""

import logging

import pytest

from ocs_ci.deployment.fusion_access_stretch_cluster import (
    DATA_ZONE_1,
    DATA_ZONE_2,
    get_arbiter_node_name,
    get_data_zone_worker_nodes,
)
from ocs_ci.deployment.zones import are_zone_labels_present
from ocs_ci.framework import config
from ocs_ci.ocs.node import get_master_nodes, get_worker_nodes

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Session-scoped cluster-info fixture
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def fusion_access_stretch_cluster_setup():
    """
    Session-scoped fixture that collects and exposes stretch-cluster topology
    metadata for use by individual test cases.

    Deployment is handled by the main ``deploy_cluster()`` chain (Jenkins
    ``--deploy`` flag + ``DEPLOYMENT.fusion_access: true`` in the YAML config).
    This fixture assumes the cluster is already deployed and simply reads the
    current node topology.

    Skipped automatically on clusters where ``DEPLOYMENT.fusion_access`` is
    not True.

    Yields:
        dict:
            - ``arbiter_node`` (str): arbiter master node name
            - ``data_zone_1_workers`` (list[str]): worker nodes in data-1
            - ``data_zone_2_workers`` (list[str]): worker nodes in data-2
            - ``all_master_nodes`` (list[str]): all master node names
            - ``all_worker_nodes`` (list[str]): all worker node names
    """
    if not config.DEPLOYMENT.get("fusion_access", False):
        pytest.skip(
            "Fusion Access HA cluster is not configured "
            "(set DEPLOYMENT.fusion_access: true in the deployment config)"
        )

    cluster_info = {
        "arbiter_node": get_arbiter_node_name(),
        "data_zone_1_workers": get_data_zone_worker_nodes(DATA_ZONE_1),
        "data_zone_2_workers": get_data_zone_worker_nodes(DATA_ZONE_2),
        "all_master_nodes": get_master_nodes(),
        "all_worker_nodes": get_worker_nodes(),
    }
    logger.info(f"Fusion Access HA cluster topology: {cluster_info}")
    yield cluster_info


# ---------------------------------------------------------------------------
# Function-scoped convenience fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="function")
def arbiter_node_name(fusion_access_stretch_cluster_setup):
    """
    Return the arbiter master node name for the current test.

    Returns:
        str: Arbiter master node name.
    """
    return fusion_access_stretch_cluster_setup["arbiter_node"]


@pytest.fixture(scope="function")
def data_zone_1_workers(fusion_access_stretch_cluster_setup):
    """
    Return worker node names in data-zone-1.

    Returns:
        list[str]: Worker node names in zone ``data-1``.
    """
    return fusion_access_stretch_cluster_setup["data_zone_1_workers"]


@pytest.fixture(scope="function")
def data_zone_2_workers(fusion_access_stretch_cluster_setup):
    """
    Return worker node names in data-zone-2.

    Returns:
        list[str]: Worker node names in zone ``data-2``.
    """
    return fusion_access_stretch_cluster_setup["data_zone_2_workers"]


@pytest.fixture(scope="function")
def all_cluster_nodes(fusion_access_stretch_cluster_setup):
    """
    Return all master and worker node names.

    Returns:
        dict: ``{'masters': list[str], 'workers': list[str]}``
    """
    return {
        "masters": fusion_access_stretch_cluster_setup["all_master_nodes"],
        "workers": fusion_access_stretch_cluster_setup["all_worker_nodes"],
    }


@pytest.fixture(scope="function")
def zone_labels_applied(fusion_access_stretch_cluster_setup):
    """
    Assert all nodes carry zone labels before the test runs.

    Yields control after verifying labels; no teardown required.
    """
    assert are_zone_labels_present(), (
        "Zone labels are missing on one or more nodes before test execution"
    )
    yield
