import logging
import time
import pytest

from ocs_ci.framework import config
from ocs_ci.ocs import ocp
from ocs_ci.ocs.node import get_worker_nodes
from ocs_ci.ocs.ui.page_objects.fusion_access_ui import FusionAccessUI
from ocs_ci.framework.testlib import (
    ui,
    ManageTest,
)
from ocs_ci.framework.pytest_customization.marks import (
    fa_functional,
    fusion_access_required,
    yellow_squad,
    ignore_leftovers,
)

logger = logging.getLogger(__name__)

# Device name that will be removed from all worker nodes in Step 1 and
# re-scanned back in Step 4.
LUN_DEVICE = "sdd"


@ui
@yellow_squad
@fusion_access_required
@ignore_leftovers
class TestDeleteLUFromBackend(ManageTest):

    @pytest.fixture(autouse=True)
    def setup_ui(self, setup_ui_class_factory):
        """
        Setup UI session for the test class.

        Args:
            setup_ui_class_factory: Factory fixture to set up the browser session.
        """
        self.setup_ui_class_factory = setup_ui_class_factory

    @pytest.mark.polarion_id("OCS-8380")
    @fa_functional
    def test_delete_lun_from_backend_removes_from_ui_table(self):
        """
        Test that removing a LUN at the backend hides it from the LUN table on
        the UI, and that rescanning the SCSI bus makes it reappear.

        Test Steps:
        1. Record lsblk output and confirm LUN_DEVICE is present on
           all worker nodes.
        2. Remove LUN_DEVICE from all worker nodes.
        3. Verify lsblk no longer lists LUN_DEVICE on any worker node.
        4. Navigate to Storage > External systems > SAN_Storage, open the
           "Create LUN group" popup and confirm LUN_DEVICE is absent from the
           Path column of the LUN table.
        5. Log out and log in the iSCSI session on all worker nodes, wait 30
           seconds, then rescan the iSCSI sessions so that LUN_DEVICE reappears.
        6. Verify lsblk lists LUN_DEVICE again on all worker nodes.
        7. Navigate back to the "Create LUN group" popup and confirm
           LUN_DEVICE is present in the Path column.
        """
        logger.info("=" * 80)
        logger.info("Starting RHSTOR-7243: Delete LU from backend test")
        logger.info("=" * 80)

        ocp_obj = ocp.OCP()
        worker_nodes = get_worker_nodes()
        logger.info(f"Worker nodes under test: {worker_nodes}")

        logger.info(f"Step 1: lsblk — confirm /dev/{LUN_DEVICE} is present")
        for node in worker_nodes:
            output = ocp_obj.exec_oc_cmd(
                f"debug node/{node} -- chroot /host lsblk",
                out_yaml_format=False,
            )
            assert LUN_DEVICE in output, (
                f"Expected /dev/{LUN_DEVICE} to be present on {node} before "
                f"removal, but it was not found in lsblk output:\n{output}"
            )
            logger.info(f"  {node}: /dev/{LUN_DEVICE} is present — OK")

        logger.info(f"Step 2: Removing /dev/{LUN_DEVICE} from all worker nodes")
        for node in worker_nodes:
            logger.info(f"  Removing LUN on {node}")
            ocp_obj.exec_oc_cmd(
                f"debug node/{node} -- chroot /host sh -c "
                f"'echo 1 > /sys/block/{LUN_DEVICE}/device/delete'",
                out_yaml_format=False,
            )
            logger.info(f"  LUN removed on {node}")

        logger.info(
            f"Step 3: Verifying /dev/{LUN_DEVICE} is gone from lsblk on all nodes"
        )
        for node in worker_nodes:
            output = ocp_obj.exec_oc_cmd(
                f"debug node/{node} -- chroot /host lsblk",
                out_yaml_format=False,
            )
            assert (
                LUN_DEVICE not in output
            ), f"/dev/{LUN_DEVICE} still appears on {node} after deletion:\n{output}"
            logger.info(f"  {node}: /dev/{LUN_DEVICE} is absent — OK")

        logger.info(
            f"Step 4: UI check — /dev/{LUN_DEVICE} should not appear in the LUN table"
        )
        self.setup_ui_class_factory()
        fusion_access = FusionAccessUI()

        fusion_access.nav_external_systems_page()
        fusion_access.take_screenshot("external_systems_after_lun_removal")

        fusion_access.navigate_to_san_storage_tab()
        fusion_access.take_screenshot("san_storage_detail_page")

        fusion_access.open_add_lun_group_popup()
        fusion_access.take_screenshot("lun_group_popup_after_removal")

        lun_paths = fusion_access.get_lun_paths_from_popup()
        lun_device_path = f"/dev/{LUN_DEVICE}"
        assert lun_device_path not in lun_paths, (
            f"Expected {lun_device_path} to be absent from the LUN table after "
            f"backend removal, but it still appears. Visible paths: {lun_paths}"
        )
        logger.info(f"  {lun_device_path} is not in the LUN table on UI — OK")

        fusion_access.close_lun_group_popup()
        fusion_access.take_screenshot("lun_group_popup_closed")

        logger.info("Step 5: iSCSI logout and login on all worker nodes")
        san_details = config.AUTH.get("san_details", {})
        iscsi_ip = san_details.get("san_iscsi_ip")
        iscsi_iqn = san_details.get("san_iscsi_iqn")
        assert iscsi_ip and iscsi_iqn, (
            "san_iscsi_ip and san_iscsi_iqn must be set under AUTH san_details "
            "in the config"
        )
        for node in worker_nodes:
            logger.info(f"  iSCSI logout and login on {node}")
            ocp_obj.exec_oc_cmd(
                f"debug node/{node} -- bash -c '"
                f"chroot /host iscsiadm -m session && "
                f"chroot /host iscsiadm -m node -T {iscsi_iqn} "
                f"--portal {iscsi_ip} --logout && "
                f"chroot /host iscsiadm -m node -T {iscsi_iqn} "
                f"--portal {iscsi_ip} --login'",
                out_yaml_format=False,
                timeout=120,
                secrets=[iscsi_ip, iscsi_iqn],
            )
            logger.info(f"  iSCSI logout and login completed on {node}")

        logger.info("Waiting 30 seconds for the iSCSI sessions to settle")
        time.sleep(30)

        logger.info("Step 5a: Rescanning iSCSI sessions on all worker nodes")
        for node in worker_nodes:
            logger.info(f"  Rescanning iSCSI sessions on {node}")
            ocp_obj.exec_oc_cmd(
                f"debug node/{node} -- chroot /host iscsiadm -m node --rescan",
                out_yaml_format=False,
                timeout=120,
            )
            logger.info(f"  iSCSI rescan completed on {node}")

        logger.info(
            f"Step 6: Verifying /dev/{LUN_DEVICE} reappears in lsblk on all nodes"
        )
        for node in worker_nodes:
            output = ocp_obj.exec_oc_cmd(
                f"debug node/{node} -- chroot /host lsblk",
                out_yaml_format=False,
            )
            assert LUN_DEVICE in output, (
                f"/dev/{LUN_DEVICE} did not reappear on {node} after SCSI "
                f"rescan. lsblk output:\n{output}"
            )
            logger.info(f"  {node}: /dev/{LUN_DEVICE} has reappeared — OK")

        logger.info(
            f"Step 7: UI check — /dev/{LUN_DEVICE} should reappear in the LUN table"
        )
        fusion_access.nav_external_systems_page()
        fusion_access.take_screenshot("external_systems_after_rescan")

        fusion_access.navigate_to_san_storage_tab()
        fusion_access.take_screenshot("san_storage_detail_page_after_rescan")

        fusion_access.open_add_lun_group_popup()
        fusion_access.take_screenshot("lun_group_popup_after_rescan")

        lun_paths_after_rescan = fusion_access.get_lun_paths_from_popup()
        assert lun_device_path in lun_paths_after_rescan, (
            f"Expected {lun_device_path} to reappear in the LUN table after "
            f"SCSI rescan, but it is missing. Visible paths: {lun_paths_after_rescan}"
        )
        logger.info(f"  {lun_device_path} is back in the LUN table on UI — OK")

        fusion_access.close_lun_group_popup()
        fusion_access.take_screenshot("test_complete")

        logger.info("PASS — deleted LU removed from UI table; re-scan restores it")
