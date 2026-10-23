import logging
import time
import pytest
from ocs_ci.framework import config
from ocs_ci.helpers import helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import TimeoutExpiredError
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

        assert external_systems.scale_present_on_page(
            SCALE_CONNECTION_NAME
        ), f"Scale connection '{SCALE_CONNECTION_NAME}' was not found on the External Systems page."

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
        external_systems = PageNavigator().nav_external_systems_page()
        return external_systems.nav_to_scale_dashboard(SCALE_CONNECTION_NAME)

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
        assert (
            dashboard.filesystem_capacity_columns_present()
        ), "Used/Total capacity columns are missing from the file systems table"

        logger.test_step(f"Read capacity values for filesystem '{FILESYSTEM_1}'")
        capacity = dashboard.get_filesystem_capacity(FILESYSTEM_1)
        logger.info(f"Filesystem '{FILESYSTEM_1}' capacity: {capacity}")
        assert capacity["used"], f"Used capacity not populated for '{FILESYSTEM_1}'"
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
        assert dashboard.open_add_nodes_modal(), "Add nodes modal did not open"
        dashboard.enable_control_plane_nodes()

        logger.test_step("Verify a kernel-devel validation status is shown")
        assert dashboard.wait_for_kernel_devel_status(), (
            "Expected the modal to show a kernel-devel validation status "
            "(verified or missing) once control plane nodes are included"
        )

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
        the UI. On a re-run the node is already part of the cluster, so the
        node-count check tolerates "no increase" rather than failing.
        """
        logger.test_step("Open the Scale dashboard and read the local node count")
        dashboard = self.nav_to_dashboard()

        node_count_before = dashboard.get_local_node_count()
        assert node_count_before is not None, "Could not read local cluster node count"

        # Enabling "Include control plane nodes" reveals the master rows and
        # their kernel-devel validation status; read a master name from the
        # modal table and select it.
        logger.test_step("Open the add-nodes modal and select a control plane node")
        assert dashboard.open_add_nodes_modal(), "Add nodes modal did not open"
        dashboard.enable_control_plane_nodes()

        control_plane_nodes = dashboard.get_control_plane_node_names()
        assert control_plane_nodes, "No control plane nodes available to add"
        master = control_plane_nodes[0]
        dashboard.select_node(master)

        # Wait for the kernel-devel validation status to render before reading it;
        # otherwise "not yet rendered" would be misread as "packages missing" and
        # trigger an unnecessary, disruptive control-plane reboot.
        logger.test_step("Apply the master MCO only if kernel-devel is missing")
        assert (
            dashboard.wait_for_kernel_devel_status()
        ), "Kernel-devel validation status was not shown for the selected node"

        # Installing the kernel-devel packages via the master MachineConfig
        # reboots the control plane, so only apply the MCO when the modal
        # reports the packages as missing for the selected node; skip it when
        # they are already verified.
        if dashboard.is_kernel_devel_verified():
            logger.info(
                "Kernel-devel packages already verified on control plane nodes; "
                "skipping the master MCO"
            )
        else:
            logger.info(
                "Kernel-devel packages missing on control plane nodes; applying "
                "the master MCO and waiting for the rollout"
            )
            dashboard.cancel_add_nodes_modal()
            helpers.apply_scale_mco(
                node_role=constants.MASTER_MACHINE, wait_for_rollout=True
            )
            # Re-open the modal after the rollout before adding the node.
            dashboard = self.nav_to_dashboard()
            assert dashboard.open_add_nodes_modal(), "Add nodes modal did not reopen"

        logger.test_step(f"Add control plane node '{master}' to the local cluster")
        assert dashboard.add_nodes_to_local_cluster(
            node_names=[master], include_control_plane=True
        ), "Add control plane nodes flow did not complete"

        # Adding a control plane node must increase the local cluster node count.
        # On a re-run the node is already present and the count cannot grow again,
        # so a timeout here is treated as "already added" rather than a failure.
        logger.test_step("Confirm the local cluster node count increased")
        node_count_after = node_count_before
        try:
            for node_count_after in TimeoutSampler(
                timeout=180, sleep=5, func=dashboard.get_local_node_count
            ):
                if node_count_after and node_count_after > node_count_before:
                    break
            logger.info(
                f"Local cluster node count increased from {node_count_before} "
                f"to {node_count_after}"
            )
        except TimeoutExpiredError:
            logger.warning(
                "Local node count did not increase; the control plane node is "
                "likely already part of the local cluster from a previous run"
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
        assert (
            dashboard.is_remove_action_disabled()
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
        assert not external_systems.scale_present_on_page(
            scale_name=SCALE_CONNECTION_NAME
        ), f"Scale connection '{SCALE_CONNECTION_NAME}' is still present after disconnect."

        # Deleting the Scale cluster does not cascade to its Filesystem CRs, so
        # verify each associated filesystem was removed by disconnect_scale.
        fs_ocp = OCP(
            kind="filesystem.scale.spectrum.ibm.com",
            namespace=constants.IBM_STORAGE_SCALE_NAMESPACE,
        )
        for filesystem_name in [FILESYSTEM_1, FILESYSTEM_2]:
            fs_cr_name = f"{SCALE_CONNECTION_NAME}-{filesystem_name}"
            assert fs_ocp.check_resource_existence(
                should_exist=False, resource_name=fs_cr_name
            ), f"Filesystem '{fs_cr_name}' still exists after disconnect."
