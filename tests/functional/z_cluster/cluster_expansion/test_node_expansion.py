import logging
import pytest

from ocs_ci.framework.testlib import tier1, ignore_leftovers, ManageTest
from ocs_ci.ocs.cluster import CephCluster
from ocs_ci.framework.pytest_customization.marks import (
    skipif_flexy_deployment,
    skipif_ibm_flash,
    skipif_managed_service,
    skipif_hci_provider_and_client,
    brown_squad,
)
from ocs_ci.ocs.resources.storage_cluster import (
    in_transit_encryption_verification,
    set_in_transit_encryption,
    get_in_transit_encryption_config_state,
)
from ocs_ci.framework import config

logger = logging.getLogger(__name__)


@brown_squad
# https://github.com/red-hat-storage/ocs-ci/issues/4802
@skipif_managed_service
@skipif_hci_provider_and_client
@skipif_flexy_deployment
@skipif_ibm_flash
@ignore_leftovers
@tier1
class TestAddNode(ManageTest):
    """
    Automates adding worker nodes to the cluster while IOs
    """

    @pytest.fixture(autouse=True)
    def set_encryption_at_teardown(self, request):
        def teardown():
            if config.ENV_DATA.get("in_transit_encryption"):
                set_in_transit_encryption()
            else:
                set_in_transit_encryption(enabled=False)

        request.addfinalizer(teardown)

    def test_add_ocs_node(self, add_nodes):
        """
        Test to add ocs nodes and wait till rebalance is completed.

        Following operations will be verify After adding node to the cluster.
        1. Enable intransit encryprion and verify.
        2. Disable intransit encryptiojn and verify.

        """
        add_nodes(ocs_nodes=True)
        ceph_cluster_obj = CephCluster()
        assert ceph_cluster_obj.wait_for_rebalance(
            timeout=3600
        ), "Data re-balance failed to complete"

        # Verify in-transit encryption enable disable operation after adding node.
        if not get_in_transit_encryption_config_state():
            set_in_transit_encryption()

        logger.info("Verifying the in-transit encryption is enable on setup.")
        assert (
            in_transit_encryption_verification()
        ), "Failed to set IN-transit encryption after adding worker node to cluster"

        logger.info("Disabling the in-transit encryption.")
        set_in_transit_encryption(enabled=False)

        # Verify that encryption is actually disabled by checking that a ValueError is raised.
        logger.info("Verifying the in-transit encryption is disabled.")
        with pytest.raises(ValueError):
            assert (
                not in_transit_encryption_verification()
            ), "In-transit Encryption was expected to be disabled, but it's enabled in the setup."
