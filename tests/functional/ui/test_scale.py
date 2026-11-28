import logging
import time
import pytest
from ocs_ci.framework import config
from ocs_ci.helpers import helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.framework.pytest_customization.marks import yellow_squad
from ocs_ci.framework.testlib import (
    cnsa_remote_mount,
    fdf_required,
    polarion_id,
    runs_on_provider,
    skipif_ibm_cloud_managed,
    skipif_ocs_version,
    tier2,
    ui,
)
from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator

logger = logging.getLogger(__name__)

SCALE_CONNECTION_NAME = "scale-cluster-1"
FILESYSTEM_1 = "afm1fs1"
FILESYSTEM_2 = "fs2"


@fdf_required
@runs_on_provider
@cnsa_remote_mount
@yellow_squad
class TestScaleConnection(object):
    """
    Test connecting Scale cluster via ODF External Systems UI page.
    Executed on FDF only.
    """

    @ui
    @skipif_ibm_cloud_managed
    @tier2
    @skipif_ocs_version("<4.21")
    @polarion_id("OCS-7757")
    def test_connect_scale(self, setup_ui_class):
        """
        Test connecting IBM Storage Scale cluster as an External system.
        """
        # Resolve target host/ip endpoint from AUTH configuration
        endpoint = config.AUTH.get("scale_gui_hosts")
        username = config.AUTH.get("scale_gui_user")
        password = config.AUTH.get("scale_gui_password")

        scale_connect_obj = PageNavigator()
        external_systems = scale_connect_obj.nav_external_systems_page()

        external_systems.connect_scale(
            system_name=SCALE_CONNECTION_NAME,
            endpoint=endpoint,
            port="443",
            username=username,
            password=password,
            filesystem_name=FILESYSTEM_2,
        )

        present = external_systems.scale_present_on_page(SCALE_CONNECTION_NAME)
        external_systems.take_screenshot("scale_connection_present")
        external_systems.copy_dom("scale_connection_present")
        logger.assertion(
            f"Scale connection '{SCALE_CONNECTION_NAME}' present on page: "
            f"expected=True, actual={present}"
        )
        assert present, (
            f"Scale connection '{SCALE_CONNECTION_NAME}' was not found on the "
            f"External Systems page."
        )

    @ui
    @skipif_ibm_cloud_managed
    @tier2
    @skipif_ocs_version("<4.21")
    @polarion_id("OCS-7758")
    def test_add_filesystem(self, setup_ui_class):
        """
        Test connecting an additional filesystem when a Scale cluster is connected
        and subsequently deleting it.
        """
        scale_connect_obj = PageNavigator()
        external_systems = scale_connect_obj.nav_external_systems_page()
        external_systems.connect_scale_filesystem(
            scale_name=SCALE_CONNECTION_NAME, filesystem_name=FILESYSTEM_1
        )

    # --- RHSTOR-8585 Scale Dashboard enhancements --------------------------
    # These run only on ODF >= 4.23 (which also covers 5.0). They assume the
    # scale connection created by test_connect_scale still exists, so they are
    # ordered after test_add_filesystem and before test_disconnect_scale.

    def nav_to_dashboard(self):
        """Open the Scale dashboard for the shared test connection."""
        return PageNavigator().nav_to_scale_dashboard(SCALE_CONNECTION_NAME)

    @ui
    @skipif_ibm_cloud_managed
    @tier2
    @skipif_ocs_version("<4.23")
    @polarion_id("OCS-7762")
    def test_filesystem_capacity_columns(self, setup_ui_class):
        """
        TC3: The file systems table exposes Used capacity and Total capacity
        columns and populates them for a known filesystem.
        """
        logger.test_step("Open the Scale dashboard")
        dashboard = self.nav_to_dashboard()

        logger.test_step("Verify the file systems table exposes capacity columns")
        columns_present = dashboard.filesystem_capacity_columns_present()
        dashboard.take_screenshot("filesystem_capacity_columns_visible")
        dashboard.copy_dom("filesystem_capacity_columns_visible")
        logger.assertion(
            f"Capacity columns present: expected=True, actual={columns_present}"
        )
        assert (
            columns_present
        ), "Used/Total capacity columns are missing from the file systems table"

        logger.test_step(f"Read capacity values for filesystem '{FILESYSTEM_1}'")
        capacity = dashboard.get_filesystem_capacity(FILESYSTEM_1)
        logger.assertion(
            f"Used capacity: expected=non-empty, actual='{capacity['used']}'"
        )
        assert capacity["used"], f"Used capacity not populated for '{FILESYSTEM_1}'"
        logger.assertion(
            f"Total capacity: expected=non-empty, actual='{capacity['total']}'"
        )
        assert capacity["total"], f"Total capacity not populated for '{FILESYSTEM_1}'"

    @ui
    @skipif_ibm_cloud_managed
    @tier2
    @skipif_ocs_version("<4.23")
    @polarion_id("OCS-7760")
    def test_kernel_devel_node_validation(self, setup_ui_class):
        """
        RHSTOR-8585: The "Add nodes to local cluster" modal validates the
        kernel-devel packages on the target nodes. Including the control plane
        (master) nodes must surface a kernel-devel validation status: either the
        "verified" state (packages present) or the "missing" hint instructing the
        user to apply the MachineConfig Operator (MCO) before continuing.

        The validation status is asserted rather than one specific state: the
        master MachineConfig persists once applied, so a prepared cluster shows
        "verified" while an unprepared one shows the "missing" hint.
        """
        logger.test_step("Open the Scale dashboard")
        dashboard = self.nav_to_dashboard()

        logger.test_step("Open the add-nodes modal and include control plane nodes")
        modal_opened = dashboard.open_add_nodes_modal()
        logger.assertion(
            f"Add nodes modal opened: expected=True, actual={modal_opened}"
        )
        assert modal_opened, "Add nodes modal did not open"
        try:
            dashboard.enable_control_plane_nodes()

            logger.test_step("Verify a kernel-devel validation status is shown")
            status_shown = dashboard.wait_for_kernel_devel_status()
            dashboard.take_screenshot("kernel_devel_validation_status")
            dashboard.copy_dom("kernel_devel_validation_status")
            logger.assertion(
                f"Kernel-devel status shown: expected=True, actual={status_shown}"
            )
            assert status_shown, (
                "Expected the modal to show a kernel-devel validation status "
                "(verified or missing) once control plane nodes are included"
            )
        finally:
            dashboard.cancel_add_nodes_modal()

    @ui
    @skipif_ibm_cloud_managed
    @tier2
    @skipif_ocs_version("<4.23")
    @polarion_id("OCS-7761")
    def test_add_master_nodes(self, setup_ui_class):
        """
        TC5: Add control plane (master) nodes to the local cluster via the Scale
        dashboard "Add nodes to local cluster" modal.

        Worker nodes are already part of the local cluster (added during remote
        cluster mount creation), so only control plane nodes can be added here.
        Control plane nodes need the kernel-devel packages installed via the
        master MachineConfigPool. The modal reports a kernel-devel validation
        status for the selected node, so the master MCO (which reboots the
        control plane) is applied only when the packages are reported missing;
        when they are already verified the MCO is skipped. Success is confirmed
        by the local cluster node count increasing.

        Note: this test mutates shared cluster state (it adds a control plane
        node and may apply a persistent master MachineConfig) and has no teardown
        because removing a node from the Scale local cluster is not supported from
        the UI. It is therefore skipped on a re-run where all control plane nodes
        are already part of the local cluster (their row checkboxes are disabled).
        """
        logger.test_step("Open the Scale dashboard and read the local node count")
        dashboard = self.nav_to_dashboard()

        node_count_before = dashboard.get_local_node_count()
        logger.assertion(
            f"Local node count: expected=not None, actual={node_count_before}"
        )
        assert node_count_before is not None, "Could not read local cluster node count"

        logger.test_step("Open the add-nodes modal and select a control plane node")
        modal_opened = dashboard.open_add_nodes_modal()
        logger.assertion(
            f"Add nodes modal opened: expected=True, actual={modal_opened}"
        )
        assert modal_opened, "Add nodes modal did not open"
        try:
            dashboard.enable_control_plane_nodes()

            control_plane_nodes = dashboard.get_control_plane_node_names()
            logger.assertion(
                f"Control plane nodes found: expected=non-empty, "
                f"actual={control_plane_nodes}"
            )
            assert control_plane_nodes, "No control plane nodes available to add"

            # A node already in the local cluster has a disabled checkbox.
            selectable = [
                node
                for node in control_plane_nodes
                if dashboard.is_node_selectable(node)
            ]
            if not selectable:
                pytest.skip(
                    "All control plane nodes are already part of the local "
                    "cluster; test is not idempotent (node removal unsupported)"
                )
            master = selectable[0]
            dashboard.select_node(master)

            # Wait for the status to render before reading it, else "not rendered"
            # is misread as "missing" and triggers an unneeded reboot.
            logger.test_step("Apply the master MCO only if kernel-devel is missing")
            status_shown = dashboard.wait_for_kernel_devel_status()
            logger.assertion(
                f"Kernel-devel status shown: expected=True, actual={status_shown}"
            )
            assert (
                status_shown
            ), "Kernel-devel validation status was not shown for the selected node"

            if dashboard.is_kernel_devel_verified():
                logger.info(
                    "Kernel-devel packages already verified on control plane "
                    "nodes; skipping the master MCO"
                )
            else:
                logger.info(
                    "Kernel-devel packages missing on control plane nodes; "
                    "applying the master MCO and waiting for the rollout"
                )
                dashboard.cancel_add_nodes_modal()
                helpers.apply_scale_mco(
                    node_role=constants.MASTER_MACHINE, wait_for_rollout=True
                )
                dashboard = self.nav_to_dashboard()

            logger.test_step(f"Add control plane node '{master}' to the local cluster")
            add_completed = dashboard.add_nodes_to_local_cluster(
                node_names=[master], include_control_plane=True
            )
            logger.assertion(
                f"Add nodes completed: expected=True, actual={add_completed}"
            )
            assert add_completed, "Add control plane nodes flow did not complete"
        finally:
            dashboard.cancel_add_nodes_modal()

        logger.test_step("Confirm the local cluster node count increased")
        for node_count_after in TimeoutSampler(
            timeout=180, sleep=5, func=dashboard.get_local_node_count
        ):
            if node_count_after and node_count_after > node_count_before:
                break
        dashboard.take_screenshot("local_cluster_node_count")
        dashboard.copy_dom("local_cluster_node_count")
        logger.assertion(
            f"Local node count increased: expected>{node_count_before}, "
            f"actual={node_count_after}"
        )

    @ui
    @skipif_ibm_cloud_managed
    @tier2
    @skipif_ocs_version("<4.23")
    @polarion_id("OCS-7764")
    def test_remote_cluster_offboarding_remove_guard(self, setup_ui_class):
        """
        TC6 (RHSTOR-8585): Conditional guardrails for remote cluster offboarding.
        The Remove/offboard action in the Scale dashboard Actions dropdown stays
        disabled while file systems are still connected, and the explanatory
        "Cannot be removed if file systems exist" subtext is shown.
        """
        logger.test_step("Open the Scale dashboard")
        dashboard = self.nav_to_dashboard()

        logger.test_step(
            "Verify the Remove/offboard action is disabled while file systems exist"
        )
        remove_disabled = dashboard.is_remove_action_disabled()
        dashboard.take_screenshot("remove_action_guard")
        dashboard.copy_dom("remove_action_guard")
        logger.assertion(
            f"Remove action disabled: expected=True, actual={remove_disabled}"
        )
        assert (
            remove_disabled
        ), "Remove action should be disabled while file systems exist"

    @pytest.mark.skip(
        reason=(
            "Disconnect/offboard is broken by IBM Storage Scale bug "
            "https://github.ibm.com/IBMSpectrumScale/scale-core/issues/12123"
        )
    )
    @ui
    @skipif_ibm_cloud_managed
    @tier2
    @skipif_ocs_version("<4.21")
    @polarion_id("OCS-7759")
    def test_disconnect_scale(self, setup_ui_class):
        """
        Test that disconnecting Scale removes it from the External Systems page.
        """
        scale_connect_obj = PageNavigator()
        external_systems = scale_connect_obj.nav_external_systems_page()
        external_systems.disconnect_scale(
            scale_name=SCALE_CONNECTION_NAME,
            filesystem_names=[FILESYSTEM_1, FILESYSTEM_2],
        )
        time.sleep(10)
        present = external_systems.scale_present_on_page(
            scale_name=SCALE_CONNECTION_NAME
        )
        external_systems.take_screenshot("scale_disconnected")
        external_systems.copy_dom("scale_disconnected")
        logger.assertion(
            f"Scale connection present after disconnect: expected=False, "
            f"actual={present}"
        )
        assert not present, (
            f"Scale connection '{SCALE_CONNECTION_NAME}' is still present "
            f"after disconnect."
        )

        # Deleting the Scale cluster does not cascade to its Filesystem CRs, so
        # verify each associated filesystem was removed by disconnect_scale.
        fs_ocp = OCP(
            kind="filesystem.scale.spectrum.ibm.com",
            namespace=constants.IBM_STORAGE_SCALE_NAMESPACE,
        )
        for filesystem_name in [FILESYSTEM_1, FILESYSTEM_2]:
            fs_cr_name = f"{SCALE_CONNECTION_NAME}-{filesystem_name}"
            removed = fs_ocp.check_resource_existence(
                should_exist=False, resource_name=fs_cr_name
            )
            logger.assertion(
                f"Filesystem '{fs_cr_name}' removed after disconnect: "
                f"expected=True, actual={removed}"
            )
            assert removed, f"Filesystem '{fs_cr_name}' still exists after disconnect."
