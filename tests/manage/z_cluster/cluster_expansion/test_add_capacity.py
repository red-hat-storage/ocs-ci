import pytest
import logging

from ocs_ci.framework import config
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
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import get_osd_pods
from ocs_ci.ocs.resources import storage_cluster
from ocs_ci.utility.utils import ceph_health_check
from ocs_ci.ocs.cluster import CephCluster
from ocs_ci.ocs.resources.storage_cluster import osd_encryption_verification
from ocs_ci.framework.pytest_customization.marks import skipif_openshift_dedicated


logger = logging.getLogger(__name__)


def add_capacity_test():
    osd_size = storage_cluster.get_osd_size()
    existing_osd_pods = get_osd_pods()
    existing_osd_pod_names = [pod.name for pod in existing_osd_pods]
    result = storage_cluster.add_capacity(osd_size)
    osd_pods_post_expansion = get_osd_pods()
    osd_pod_names_post_expansion = [pod.name for pod in osd_pods_post_expansion]
    restarted_osds = list()
    logger.info(
        "Checking if existing OSD pods were restarted (deleted) post add capacity (bug 1931601)"
    )

    for pod in existing_osd_pod_names:
        if pod not in osd_pod_names_post_expansion:
            restarted_osds.append(pod)
    assert (
        len(restarted_osds) == 0
    ), f"The following OSD pods were restarted (deleted) post add capacity: {restarted_osds}"

    pod = OCP(kind=constants.POD, namespace=config.ENV_DATA["cluster_namespace"])
    pod.wait_for_resource(
        timeout=300,
        condition=constants.STATUS_RUNNING,
        selector="app=rook-ceph-osd",
        resource_count=result * 3,
    )

    # Verify status of rook-ceph-osd-prepare pods. Verifies bug 1769061
    # pod.wait_for_resource(
    #     timeout=300,
    #     condition=constants.STATUS_COMPLETED,
    #     selector=constants.OSD_PREPARE_APP_LABEL,
    #     resource_count=result * 3
    # )
    # Commented this lines as a workaround due to bug 1842500

    # Verify OSDs are encrypted.
    if config.ENV_DATA.get("encryption_at_rest"):
        osd_encryption_verification()

    ceph_health_check(namespace=config.ENV_DATA["cluster_namespace"], tries=80)
    ceph_cluster_obj = CephCluster()
    assert ceph_cluster_obj.wait_for_rebalance(
        timeout=5400
    ), "Data re-balance failed to complete"


@ignore_leftovers
@tier1
@acceptance
@polarion_id("OCS-1191")
@pytest.mark.last
@skipif_openshift_dedicated
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
