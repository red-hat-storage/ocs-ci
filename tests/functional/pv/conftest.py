import logging

import pytest

from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.framework import config
from ocs_ci.utility import version

log = logging.getLogger(__name__)


@pytest.fixture
def block_storageclass(request):
    """
    Resolve the block-backed StorageClass to use for a PVC test.

    This enables the same block-device PVC test to run against different
    block-backed StorageClasses (e.g. the default Ceph RBD SC and the
    NVMe-oF SC) via indirect parametrization.

    ``request.param`` is expected to be either:
        * ``None`` - use the test/factory default (Ceph RBD) StorageClass, or
        * a StorageClass name (str), e.g. ``constants.CEPH_NVMEOF_SC``.

    Guard the NVMe-oF parametrization with the ``skipif_no_nvmeof`` marker so
    it is only collected on clusters where NVMe-oF is enabled.

    Returns:
        OCS: StorageClass OCS instance for the requested name, or ``None`` to
            let the test/factory use its default RBD StorageClass.

    """
    sc_name = getattr(request, "param", None)
    if not sc_name:
        return None
    sc_ocp_obj = OCP(
        kind=constants.STORAGECLASS,
        namespace=config.ENV_DATA["cluster_namespace"],
        resource_name=sc_name,
    )
    return OCS(**sc_ocp_obj.get())


def pytest_collection_modifyitems(items):
    """
    Skip tests in a directory based on conditions

    Args:
        items: list of collected tests

    """
    ocs_version = version.get_semantic_ocs_version_from_config()

    if config.ENV_DATA["platform"].lower() in constants.MANAGED_SERVICE_PLATFORMS:
        for item in items.copy():
            if "functional/pv/pvc_snapshot" in str(item.fspath) and (
                ocs_version < version.VERSION_4_11
            ):
                log.debug(
                    f"Test {item} is removed from the collected items. PVC snapshot is not supported on"
                    f" {config.ENV_DATA['platform'].lower()} with ODF < 4.11 due to the bug 2069367"
                )
                items.remove(item)
