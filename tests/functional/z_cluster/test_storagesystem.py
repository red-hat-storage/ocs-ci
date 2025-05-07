import logging

import pytest
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.framework import config
from ocs_ci.framework.testlib import (
    brown_squad,
    ManageTest,
    tier1,
)
from ocs_ci.framework.logger_helper import log_step

logger = logging.getLogger(__name__)


@brown_squad
@pytest.mark.polarion_id("")
class TestStorageSystem(ManageTest):
    """
    Verify the ceph full thresholds storagecluster parameters move to cephcluster

    """

    @tier1
    def test_storagesystem_not_present(self):
        """
        1. Storage System is not present
        2. Storage Cluster owner reference doesn't contain storage system

        """

    log_step("Storage System is not present")
    storage_system = ocp.OCP(
        kind=constants.STORAGESYSTEM, namespace=config.ENV_DATA["cluster_namespace"]
    )
    try:
        storage_system_data = storage_system.get()
    except CommandFailed:
        pass
    else:
        assert False, "Storage System found but it should not be present"
    log_step("Storage Cluster owner reference doesn't contain storage system")
    storage_cluster = ocp.OCP(
        kind=constants.STORAGECLUSTER, namespace=config.ENV_DATA["cluster_namespace"]
    )
    storage_cluster_data = storage_cluster.get()
    owner_references = storage_cluster_data.get("metadata").get("ownerReferences", {})
    assert not any(
        [
            reference
            for reference in owner_references
            if reference["kind"] == "StorageSystem"
        ]
    )
