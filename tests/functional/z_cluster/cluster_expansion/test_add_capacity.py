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
    skipif_no_lso,
    skipif_lso,
    skipif_managed_service,
    skipif_stretch_cluster,
    skipif_hci_provider_and_client,
    brown_squad,
    black_squad,
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
from ocs_ci.ocs.resources.pod import (
    get_osd_pods,
    get_ceph_tools_pod,
)
from ocs_ci.ocs.resources import storage_cluster
from ocs_ci.ocs.cluster import (
    check_ceph_health_after_add_capacity,
    is_flexible_scaling_enabled,
)
from ocs_ci.ocs.resources.storage_cluster import (
    get_device_class,
    osd_encryption_verification,
    verify_storage_device_class,
    verify_device_class_in_osd_tree,
)
from ocs_ci.ocs.ui.helpers_ui import ui_add_capacity_conditions, ui_add_capacity
from ocs_ci.utility.utils import is_cluster_y_version_upgraded
from ocs_ci.utility import version


logger = logging.getLogger(__name__)


def add_capacity_test(ui_flag=False):
    """
    Add capacity on non-lso cluster

    Args:
        ui_flag(bool): add capacity via ui [true] or via cli [false]

    """
    osd_size = storage_cluster.get_osd_size()
    existing_osd_pods = get_osd_pods()
    existing_osd_pod_names = [pod.name for pod in existing_osd_pods]
    if ui_add_capacity_conditions() and ui_flag:
        result = ui_add_capacity(osd_size)
    else:
        result = storage_cluster.add_capacity(osd_size)
    osd_pods_post_expansion = get_osd_pods()
    osd_pod_names_post_expansion = [pod.name for pod in osd_pods_post_expansion]
    restarted_osds = list()
    logger.info(
        "Checking if existing OSD pods were restarted (deleted) post adding capacity (bug 1931601)"
    )

    for pod in existing_osd_pod_names:
        if pod not in osd_pod_names_post_expansion:
            restarted_osds.append(pod)
    assert (
        len(restarted_osds) == 0
    ), f"The following OSD pods were restarted (deleted) post add capacity: {restarted_osds}"

    pod = OCP(kind=constants.POD, namespace=config.ENV_DATA["cluster_namespace"])
    if is_flexible_scaling_enabled():
        replica_count = 1
    else:
        replica_count = 3
    pod.wait_for_resource(
        timeout=300,
        condition=constants.STATUS_RUNNING,
        selector="app=rook-ceph-osd",
        resource_count=result * replica_count,
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

    # verify device classes
    ocs_version = version.get_semantic_ocs_version_from_config()
    if ocs_version >= version.VERSION_4_14 and not is_cluster_y_version_upgraded():
        device_class = get_device_class()
        ct_pod = get_ceph_tools_pod()
        verify_storage_device_class(device_class)
        verify_device_class_in_osd_tree(ct_pod, device_class)

    check_ceph_health_after_add_capacity(ceph_rebalance_timeout=5400)


@ignore_leftovers
@polarion_id("OCS-1191")
@pytest.mark.second_to_last
@skipif_managed_service
@skipif_aws_i3
@skipif_bm
@skipif_bmpsi
@skipif_lso
@skipif_external_mode
@skipif_ibm_power
@skipif_managed_service
@skipif_hci_provider_and_client
class TestAddCapacity(ManageTest):
    """
    Automates adding variable capacity to the cluster
    """

    @acceptance
    @brown_squad
    def test_add_capacity_cli(self, reduce_and_resume_cluster_load):
        """
        Add capacity on non-lso cluster via cli on Acceptance suite
        """
        add_capacity_test(ui_flag=False)

    @tier1
    @black_squad
    def test_add_capacity_ui(self, reduce_and_resume_cluster_load):
        """
        Add capacity on non-lso cluster via UI on tier1 suite
        """
        add_capacity_test(ui_flag=True)


@ignore_leftovers
@polarion_id("OCS-4647")
@pytest.mark.second_to_last
@skipif_aws_i3
@skipif_bm
@skipif_bmpsi
@skipif_external_mode
@skipif_ibm_power
@skipif_managed_service
@skipif_hci_provider_and_client
@skipif_no_lso
@skipif_stretch_cluster
class TestAddCapacityLSO(ManageTest):
    """
    Add capacity on lso cluster
    """

    @acceptance
    @brown_squad
    def test_add_capacity_lso_cli(self, reduce_and_resume_cluster_load):
        """
        Add capacity on lso cluster via CLI on Acceptance suite
        """
        storage_cluster.add_capacity_lso(ui_flag=False)

    @tier1
    @black_squad
    def test_add_capacity_lso_ui(self, reduce_and_resume_cluster_load):
        """
        Add capacity on lso cluster via UI on tier1 suite
        """
        storage_cluster.add_capacity_lso(ui_flag=True)


@brown_squad
@skipif_ocs_version("<4.4")
@pre_upgrade
@ignore_leftovers
@polarion_id("OCS-1191")
@skipif_aws_i3
@skipif_bm
@skipif_external_mode
@cloud_platform_required
@skipif_managed_service
@skipif_hci_provider_and_client
class TestAddCapacityPreUpgrade(ManageTest):
    """
    Automates adding variable capacity to the cluster pre upgrade
    """

    def test_add_capacity_pre_upgrade(self, reduce_and_resume_cluster_load):
        """
        Test to add variable capacity to the OSD cluster while IOs running
        """
        add_capacity_test()
