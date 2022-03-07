import pytest
import logging

from ocs_ci.framework.pytest_customization.marks import (
    polarion_id,
    pre_upgrade,
    skipif_aws_i3,
    skipif_bm,
    skipif_external_mode,
    skipif_bmpsi,
    skipif_ibm_power,
)
from ocs_ci.framework.testlib import (
    ignore_leftovers,
    ManageTest,
    skipif_ocs_version,
    tier1,
    acceptance,
    cloud_platform_required,
)
from ocs_ci.ocs.resources.storage_cluster import add_capacity_test
from ocs_ci.framework.pytest_customization.marks import skipif_managed_service


logger = logging.getLogger(__name__)


@ignore_leftovers
@tier1
@acceptance
@polarion_id("OCS-1191")
@pytest.mark.second_to_last
@skipif_managed_service
@skipif_aws_i3
@skipif_bm
@skipif_bmpsi
@skipif_external_mode
@skipif_ibm_power
class TestAddCapacity(ManageTest):
    """
    Automates adding variable capacity to the cluster
    """

    def test_add_capacity(self, reduce_and_resume_cluster_load):
        """
        Test to add variable capacity to the OSD cluster while IOs running
        """
        add_capacity_test()


@skipif_ocs_version("<4.4")
@pre_upgrade
@ignore_leftovers
@polarion_id("OCS-1191")
@skipif_aws_i3
@skipif_bm
@skipif_external_mode
@cloud_platform_required
class TestAddCapacityPreUpgrade(ManageTest):
    """
    Automates adding variable capacity to the cluster pre upgrade
    """

    def test_add_capacity_pre_upgrade(self, reduce_and_resume_cluster_load):
        """
        Test to add variable capacity to the OSD cluster while IOs running
        """
        add_capacity_test()
