import logging
import pytest

from semantic_version import Version

from ocs_ci.framework import config
from ocs_ci.framework.testlib import (
    post_ocs_upgrade,
    ManageTest,
    skipif_external_mode,
    skipif_ms_consumer,
    skipif_hci_client,
    bugzilla,
)
from ocs_ci.ocs.cluster import CephCluster
from ocs_ci.helpers.helpers import get_mon_pdb
from ocs_ci.ocs.cluster import validate_existence_of_blocking_pdb
from ocs_ci.framework.pytest_customization.marks import (
    brown_squad,
)

log = logging.getLogger(__name__)


@brown_squad
class TestToCheckPDBPostUpgrade(ManageTest):
    """
    Validate post ocs upgrade mon pdb count

    """

    @post_ocs_upgrade
    @skipif_external_mode
    @skipif_ms_consumer
    @skipif_hci_client
    @pytest.mark.polarion_id("OCS-2449")
    def test_check_mon_pdb_post_upgrade(self):
        """
        Testcase to check disruptions_allowed and minimum
        available, maximum unavailable mon count

        """
        ceph_obj = CephCluster()

        # Check for mon count
        mons_after_upgrade = ceph_obj.get_mons_from_cluster()
        log.info(f"Mons after upgrade {mons_after_upgrade}")

        if len(mons_after_upgrade) >= 5:
            # pdb max unavailable is changed to 2 if we have at least 5 mons in a cluster
            # https://bugzilla.redhat.com/show_bug.cgi?id=2264553
            expected_max_unavailable_mon = 2
            expected_disruptions_allowed = 2
        else:
            expected_max_unavailable_mon = 1
            expected_disruptions_allowed = 1

        disruptions_allowed, min_available_mon, max_unavailable_mon = get_mon_pdb()
        log.info(f"Number of Mons Disruptions_allowed {disruptions_allowed}")
        log.info(f"Minimum_available mon count {min_available_mon}")
        log.info(f"Maximum_available mon count {max_unavailable_mon}")

        # The PDB values are considered from OCS 4.5 onwards.
        assert (
            disruptions_allowed == expected_disruptions_allowed
        ), "Mon Disruptions_allowed count not matching"
        ocs_version = config.ENV_DATA["ocs_version"]
        if Version.coerce(ocs_version) < Version.coerce("4.6"):
            assert min_available_mon == 2, "Minimum available mon count is not matching"
        else:
            # This mon pdb change is from 4.6.5, 4.7 on wards, please refer bz1946573, bz1935065
            # (https://bugzilla.redhat.com/show_bug.cgi?id=1946573)
            # (https://bugzilla.redhat.com/show_bug.cgi?id=1935065)
            assert (
                max_unavailable_mon == expected_max_unavailable_mon
            ), "Maximum unavailable mon count is not matching"

    @post_ocs_upgrade
    @bugzilla("1861104")
    @pytest.mark.polarion_id("OCS-2626")
    def test_check_osd_pdb_post_upgrade(self):
        """
        Test to verify OSD PDBs
        1. Post OCP and OCS successful upgrades check for OSD PDBs

        """
        assert (
            not validate_existence_of_blocking_pdb()
        ), "Blocking PDBs present in the cluster"
