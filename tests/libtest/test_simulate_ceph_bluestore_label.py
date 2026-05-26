import logging

import pytest

from ocs_ci.framework.testlib import (
    ManageTest,
    ignore_leftovers,
    libtest,
    brown_squad,
    skipif_no_lso,
)

from ocs_ci.framework import config
from ocs_ci.deployment.helpers.ceph_cluster import (
    simulate_full_ceph_bluestore_process_on_wnodes,
    simulate_full_ceph_bluestore_dmcrypt_process_on_wnodes,
)

log = logging.getLogger(__name__)


@brown_squad
@libtest
@ignore_leftovers
@skipif_no_lso
class TestSimulateCephBlueStoreLabel(ManageTest):
    """
    Test that simulate_bluestore_label correctly stamps and verifies a BlueStore label on a test disk.
    """

    def test_simulate_bluestore_label_on_worker_nodes(self):
        """
        Test simulates a Ceph BlueStore label on the worker node disks.

        """
        if not config.ENV_DATA.get("simulate_bluestore_label", False):
            pytest.skip("simulate_bluestore_label not set in config")
        result = simulate_full_ceph_bluestore_process_on_wnodes()
        assert result, "BlueStore label simulation failed on worker nodes"
        log.info("BlueStore label simulation succeeded on all worker nodes disks")

    def test_simulate_bluestore_label_dmcrypt_on_worker_nodes(self):
        """
        Test simulates encrypted Ceph OSD dm-crypt data on the worker node disks.

        Creates a LUKS container on each node's disk, writes BlueStore metadata
        inside the encrypted container via ceph-volume, and stamps the LUKS
        header with Rook-compatible metadata (ceph_fsid subsystem, pvc_name label).

        """
        if not config.ENV_DATA.get("simulate_bluestore_label_dmcrypt", False):
            pytest.skip("simulate_bluestore_label_dmcrypt not set in config")
        result = simulate_full_ceph_bluestore_dmcrypt_process_on_wnodes(
            clear_signatures=False
        )
        assert (
            result
        ), "Encrypted BlueStore (dm-crypt) simulation failed on worker nodes"
        log.info("Encrypted BlueStore simulation succeeded on all worker nodes disks")
