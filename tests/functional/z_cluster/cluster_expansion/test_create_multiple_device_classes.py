import logging
import random
import time

import pytest

from ocs_ci.framework.testlib import (
    ManageTest,
    ignore_leftovers,
    tier2,
    tier4a,
    brown_squad,
    skipif_no_lso,
    skipif_bm,
    skipif_hci_provider_or_client,
    polarion_id,
    ui,
    black_squad,
    skipif_external_mode,
    skipif_mcg_only,
    skipif_ocs_version,
    jira,
)
from ocs_ci.helpers.ceph_helpers import (
    get_canceled_context_retry_lines_in_operator_log,
    wait_for_mon_quorum_count,
    wait_for_mons_in_quorum,
    wait_for_ok_to_stop_retry_in_operator_log,
)
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.ocs import constants
from ocs_ci.ocs.node import get_osd_running_nodes
from ocs_ci.ocs.device_classes import (
    create_new_lvs_for_new_deviceclass,
    verify_deviceclasses_steps,
    add_disks_matching_lvs_size,
    get_default_lvs_obj,
    verify_available_pvs_for_deviceclass,
)
from ocs_ci.ocs.resources.deployment import get_mon_deployments
from ocs_ci.ocs.resources.pv import (
    wait_for_pvs_in_lvs_to_reach_status,
    get_pv_objs_in_sc,
    wait_for_new_pvs_status,
)
from ocs_ci.ocs.resources.pvc import wait_for_pvcs_in_deviceset_to_reach_status
from ocs_ci.ocs.resources.storage_cluster import (
    add_new_deviceset_in_storagecluster,
    get_storage_cluster,
    get_first_sc_name_from_storagecluster,
    get_deviceset_name_per_deviceclass,
    get_storage_cluster_resources,
    set_storage_cluster_resources,
)
from ocs_ci.helpers.helpers import (
    create_ceph_block_pool,
    create_rbd_deviceclass_storageclass,
    modify_deployment_replica_count,
)
from ocs_ci.utility.utils import ceph_health_check
from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator


log = logging.getLogger(__name__)


def get_logs_since_param(start_time):
    """
    Get the 'since' parameter for the pod logs, which represents the relative duration
    from the given start time until now.

    Args:
        start_time (float): The start time in seconds since the epoch

    Returns:
        str: The relative duration from the given start time, as expected by the
            'oc logs --since' parameter

    """
    # Add one minute to the elapsed time, so we will not miss log lines because of
    # a small time difference between the test machine and the cluster
    return f"{int(time.time() - start_time) + 60}s"


@brown_squad
@ignore_leftovers
@skipif_no_lso
@skipif_bm
@skipif_hci_provider_or_client
@skipif_external_mode
@skipif_mcg_only
class TestMultipleDeviceClasses(ManageTest):
    """
    Automate the multiple device classes tests

    """

    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance

        """
        self.sanity_helpers = Sanity()

    @pytest.fixture(autouse=True)
    def teardown(self):
        """
        Check that the ceph health is OK

        """
        log.info("Wait for the ceph health to be OK")
        ceph_health_check(tries=20)

    @tier2
    def test_add_new_ssd_device_class(
        self, pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
    ):
        """
        The test will perform the following steps:
        1. Get the osd nodes.
        2. Create a new LocalVolumeSet for the new deviceclass as defined in the function
        'create_new_lvs_for_new_deviceclass'.
        3. Wait for the PVS in the LocalVolumeSet above to be available.
        4. Add a new device set in the storagecluster for the new LocalVolumeSet above,
        which will also create a new deviceclass.
        5. Wait for the storagecluster to be ready.
        6. Create a new CephBlockPool for the device class above.
        7. Wait for the CephBlockPool to be ready.
        8. Create a new StorageClass for the pool.
        9. Run the Verification steps as defined in the function
        'verification_steps_after_adding_new_deviceclass'.
        10. Check the cluster and Ceph health.
        11. Check basic cluster functionality by creating some resources.

        """
        osd_node_names = get_osd_running_nodes()
        log.info(f"osd node names = {osd_node_names}")
        num_of_new_pvs = len(osd_node_names)
        lvs_obj = create_new_lvs_for_new_deviceclass(osd_node_names)
        log.info(
            f"Wait for the PVs in the LocalVolumeSet {lvs_obj.name} to be available"
        )
        wait_for_pvs_in_lvs_to_reach_status(
            lvs_obj.name, num_of_new_pvs, constants.STATUS_AVAILABLE
        )

        log.info(
            f"Add a new deviceset in the storagecluster for the new LocalVolumeSet {lvs_obj.name} "
            f"which will also create a new deviceclass"
        )
        res = add_new_deviceset_in_storagecluster(lvs_obj.name, lvs_obj.name)
        assert res, "Failed to patch the storagecluster with the new deviceset"
        log.info("Wait for the new deviceset PVCs to reach Bound status")
        wait_for_pvcs_in_deviceset_to_reach_status(
            lvs_obj.name, num_of_new_pvs, constants.STATUS_BOUND
        )

        sc_obj = get_storage_cluster()
        sc_obj.wait_for_resource(
            condition=constants.STATUS_READY,
            resource_name=constants.DEFAULT_CLUSTERNAME,
            column="PHASE",
            timeout=180,
            sleep=10,
        )

        log.info(f"Create a new CephBlockPool for the device class {lvs_obj.name}")
        cbp_obj = create_ceph_block_pool(device_class=lvs_obj.name)
        assert (
            cbp_obj
        ), f"Failed to create the CephBlockPool for the device class {lvs_obj.name}"
        cbp_obj.ocp.wait_for_resource(
            condition=constants.STATUS_READY,
            resource_name=cbp_obj.name,
            column="PHASE",
            timeout=180,
            sleep=10,
        )

        log.info(f"Create a new StorageClass for the pool {cbp_obj.name}")
        sc_obj = create_rbd_deviceclass_storageclass(pool_name=cbp_obj.name)
        assert sc_obj, f"Failed to create the StorageClass for the pool {cbp_obj.name}"

        log.info("Verification steps after adding a new deviceclass...")
        verify_deviceclasses_steps()
        log.info("Checking the cluster and Ceph health")
        self.sanity_helpers.health_check(cluster_check=True, tries=40)
        log.info("Check basic cluster functionality by creating some resources")
        self.sanity_helpers.create_resources(
            pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
        )

    @tier2
    @polarion_id("OCS-7431")
    def test_add_new_ssd_device_class_same_size(
        self, pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
    ):
        """
        The test will perform the following steps:
        1. Get the osd nodes.
        2. Add new disks for the new deviceclass using the same existing LocalVolumeSet
        with the same storage size.
        3. Wait for the PVS in the LocalVolumeSet above to be available.
        4. Add a new device set in the storagecluster for the existing LocalVolumeSet above
        with the same storage class name and size, which will also create a new deviceclass.
        5. Wait for the storagecluster to be ready.
        6. Create a new CephBlockPool for the device class above.
        7. Wait for the CephBlockPool to be ready.
        8. Create a new StorageClass for the pool.
        9. Run the Verification steps as defined in the function
        'verification_steps_after_adding_new_deviceclass'.
        10. Check the cluster and Ceph health.
        11. Check basic cluster functionality by creating some resources.

        """
        sc_name = get_first_sc_name_from_storagecluster()
        log.info(f"StorageCluster first storageclass name = {sc_name}")
        current_pv_objs = get_pv_objs_in_sc(sc_name)
        lvs_obj = get_default_lvs_obj()

        osd_nodes = get_osd_running_nodes()
        log.info(f"osd node names = {osd_nodes}")
        log.info("Add new disks for the new deviceclass")
        add_disks_matching_lvs_size(osd_nodes)
        num_of_new_pvs = len(osd_nodes)

        log.info("Wait for the new PVs in the existing LocalVolumeSet to be available")
        wait_for_new_pvs_status(
            current_pv_objs=current_pv_objs,
            sc_name=lvs_obj.name,
            expected_status=constants.STATUS_AVAILABLE,
            num_of_new_pvs=num_of_new_pvs,
        )

        suffix = "".join(random.choices("0123456789", k=5))
        device_class_name = f"{lvs_obj.name}-{suffix}"
        deviceset_name = device_class_name
        # The storage size defined as "1" in the deviceset as per documentation,
        # but will default to the existing OSD size
        storage_size = "1"
        log.info(
            f"Add a new deviceset in the storagecluster for the new LocalVolumeSet: {device_class_name} "
            f"which will also create a new deviceclass"
        )
        res = add_new_deviceset_in_storagecluster(
            device_class=device_class_name,
            name=deviceset_name,
            sc_name=sc_name,
            storage_size=storage_size,
        )
        assert (
            res
        ), f"Failed to patch the storagecluster with the new deviceset {device_class_name}"
        log.info("Wait for the new deviceset PVCs to reach Bound status")
        wait_for_pvcs_in_deviceset_to_reach_status(
            deviceset_name, num_of_new_pvs, constants.STATUS_BOUND
        )

        sc_obj = get_storage_cluster()
        sc_obj.wait_for_resource(
            condition=constants.STATUS_READY,
            resource_name=constants.DEFAULT_CLUSTERNAME,
            column="PHASE",
            timeout=180,
            sleep=10,
        )

        log.info(f"Create a new CephBlockPool for the device class {device_class_name}")
        cbp_obj = create_ceph_block_pool(device_class=device_class_name)
        assert (
            cbp_obj
        ), f"Failed to create the CephBlockPool for the device class {device_class_name}"
        cbp_obj.ocp.wait_for_resource(
            condition=constants.STATUS_READY,
            resource_name=cbp_obj.name,
            column="PHASE",
            timeout=180,
            sleep=10,
        )

        log.info(f"Create a new StorageClass for the pool {cbp_obj.name}")
        sc_obj = create_rbd_deviceclass_storageclass(pool_name=cbp_obj.name)
        assert sc_obj, f"Failed to create the StorageClass for the pool {cbp_obj.name}"

        log.info("Verification steps after adding a new deviceclass...")
        verify_deviceclasses_steps()
        log.info("Checking the cluster and Ceph health")
        self.sanity_helpers.health_check(cluster_check=True, tries=40)
        log.info("Check basic cluster functionality by creating some resources")
        self.sanity_helpers.create_resources(
            pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
        )


@brown_squad
@ui
@black_squad
@ignore_leftovers
@skipif_no_lso
@skipif_bm
@skipif_hci_provider_or_client
@skipif_external_mode
@skipif_mcg_only
class TestMultipleDeviceClassesUI(ManageTest):
    """
    Automate the multiple device classes tests via UI

    """

    @pytest.fixture(autouse=True)
    def init_sanity(self, pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory):
        """
        Initialize Sanity instance

        """
        self.pvc_factory = pvc_factory
        self.pod_factory = pod_factory
        self.bucket_factory = bucket_factory
        self.rgw_bucket_factory = rgw_bucket_factory
        self.sanity_helpers = Sanity()

        self.old_deviceset_name_per_deviceclass = get_deviceset_name_per_deviceclass()
        log.info(
            f"Old deviceset name per deviceclass: {self.old_deviceset_name_per_deviceclass}"
        )
        self.available_pvs_count = None
        self.new_device_class_name = None

    def verify_new_deviceset_pvcs(self):
        """
        Wait for the new deviceset PVCs to reach Bound status after adding a new device class

        """
        new_deviceset_name_per_deviceclass = get_deviceset_name_per_deviceclass()
        # Get the new deviceset name by comparing the old and new deviceset name per device class
        deviceset_name = None
        for deviceset_name, deviceclass in new_deviceset_name_per_deviceclass.items():
            if deviceclass not in self.old_deviceset_name_per_deviceclass.values():
                break
        assert (
            deviceset_name
        ), "Failed to get the new deviceset name for the new device class"

        assert deviceclass == self.new_device_class_name, (
            f"The new device class name in the deviceset {deviceclass} doesn't match the expected new "
            f"device class name {self.new_device_class_name}",
        )
        log.info(
            f"The new deviceset {deviceset_name} with the new device class {deviceclass} was "
            f"added successfully"
        )

        log.info("Wait for the new deviceset PVCs to reach Bound status")
        wait_for_pvcs_in_deviceset_to_reach_status(
            deviceset_name, self.available_pvs_count, constants.STATUS_BOUND
        )

    def post_deviceclass_checks(self, verify_new_deviceset_pvcs=True):
        """
        Run the verification steps after adding a new device class:
        1. Wait for the new deviceset PVCs to reach the Bound state.
        2. Wait for the storage cluster to be ready.
        3. Check the verification steps as defined in the function 'verify_deviceclasses_steps'.
        4. Check the cluster and Ceph health.
        5. Check basic cluster functionality by creating some resources.

        Args:
            verify_new_deviceset_pvcs (bool): Whether to verify the new deviceset PVCs. Default is True.

        """
        log.info("Verification steps after adding a new deviceclass...")
        if verify_new_deviceset_pvcs:
            self.verify_new_deviceset_pvcs()
        sc_obj = get_storage_cluster()
        sc_obj.wait_for_resource(
            condition=constants.STATUS_READY,
            resource_name=constants.DEFAULT_CLUSTERNAME,
            column="PHASE",
            timeout=360,
            sleep=20,
        )

        # Continue with the rest of the verification steps as defined in the function
        # 'verification_steps_after_adding_new_deviceclass'
        verify_deviceclasses_steps()
        log.info("Checking the cluster and Ceph health")
        self.sanity_helpers.health_check(cluster_check=True, tries=40)
        log.info("Check basic cluster functionality by creating some resources")
        self.sanity_helpers.create_resources(
            self.pvc_factory,
            self.pod_factory,
            self.bucket_factory,
            self.rgw_bucket_factory,
        )

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        """
        Check that the ceph health is OK

        """

        def finalizer():
            log.info("Wait for the ceph health to be OK")
            ceph_health_check(tries=20)

        request.addfinalizer(finalizer)

    @tier2
    @polarion_id("OCS-7512")
    def test_add_new_device_class_ui(self, setup_ui_session):
        """
        The test will perform the following steps:
        1. Verify that there are available PVs for attaching a new device class.
        2. Navigate to the 'Attach Storage' form in the UI.
        3. Fill the form with the default values and submit it, which will add a new device class.
        4. Run the verification steps as defined in the method 'post_deviceclass_checks'.

        """
        self.available_pvs_count = verify_available_pvs_for_deviceclass()
        attach_storage = PageNavigator().nav_to_attach_storage_page()
        self.new_device_class_name = attach_storage.send_form_with_default_values()
        self.post_deviceclass_checks()


@brown_squad
@ignore_leftovers
@skipif_no_lso
@skipif_bm
@skipif_hci_provider_or_client
@skipif_external_mode
@skipif_mcg_only
class TestAddDeviceClassDuringOkToStopRetry(ManageTest):
    """
    Test adding a new device class while the rook-ceph operator is in the Ceph daemon
    'ok-to-stop' retry loop.

    Before the fix of the bug DFBUGS-10987, the 'ok-to-stop' retry loop was not context
    aware. When the CephCluster spec was updated while the retry loop was running, the
    operator cancelled the orchestration but kept retrying with the cancelled context
    until the whole retry budget was consumed. As a result, the operator never reached
    the OSD code, so the new OSDs and the new device class were not created, even though
    the storagecluster reported the Ready state.

    """

    # The operator retries the 'ok-to-stop' check 10 times, with a delay of 60 seconds
    # between the retries
    OK_TO_STOP_RETRY_BUDGET = 600
    # The timeout for the new deviceset PVCs to reach the Bound state. It gives the
    # operator enough time to finish the mon update and create the new OSDs, without
    # waiting for the whole retry budget above
    NEW_DEVICESET_PVCS_TIMEOUT = 540

    @pytest.fixture(autouse=True)
    def setup(self, request):
        """
        Initialize the Sanity instance, save the original mon resources, and add a
        finalizer that restores the mon deployment and the original mon resources

        """
        self.sanity_helpers = Sanity()
        self.mon_count = len(get_mon_deployments())
        log.info(f"Number of mon deployments: {self.mon_count}")
        self.original_mon_resources = get_storage_cluster_resources("mon")
        log.info(f"The original mon resources: {self.original_mon_resources}")
        self.scaled_down_mon_names = []

        def finalizer():
            """
            Scale up the mon deployments, restore the original mon resources, and check
            that the ceph health is OK

            """
            # The operator may fail over a mon that was scaled down, so we scale up
            # only the mon deployments that still exist
            existing_mon_names = [dep.name for dep in get_mon_deployments()]
            for mon_name in self.scaled_down_mon_names:
                if mon_name in existing_mon_names:
                    log.info(f"Scale up the mon deployment {mon_name}")
                    modify_deployment_replica_count(mon_name, 1)

            log.info("Restore the original mon resources in the storagecluster")
            set_storage_cluster_resources("mon", self.original_mon_resources)
            wait_for_mons_in_quorum(self.mon_count, timeout=900)
            log.info("Wait for the ceph health to be OK")
            ceph_health_check(tries=40)

        request.addfinalizer(finalizer)

    def get_mon_resources_for_update(self):
        """
        Get mon resources that are different from the current mon resources, so that
        patching the storagecluster with them will trigger a rolling update of the mon
        deployments. The requests are lower than the limits, so that the mon pods will
        still be scheduled.

        Returns:
            dict: The mon resources to patch the storagecluster with

        """
        mon_resources = {
            "requests": {"cpu": "500m", "memory": "1Gi"},
            "limits": {"cpu": "1", "memory": "2Gi"},
        }
        if mon_resources == self.original_mon_resources:
            mon_resources["requests"]["cpu"] = "600m"

        return mon_resources

    def start_ok_to_stop_retry_loop(self):
        """
        Make the rook-ceph operator enter the Ceph daemon 'ok-to-stop' retry loop:
        1. Scale down the mon deployments that are needed, so that stopping any of the
        remaining mons will break the mon quorum, and the 'ok-to-stop' check will fail.
        2. Trigger a rolling update of the mon deployments by changing the mon resources
        in the storagecluster, so that the operator will run the 'ok-to-stop' check.
        3. Wait for the operator to enter the 'ok-to-stop' retry loop.

        Returns:
            bool: True, if the operator entered the 'ok-to-stop' retry loop.
                False, otherwise.

        """
        start_time = time.time()
        # The number of the mons we need to scale down, so that stopping one more mon
        # will break the mon quorum. For example, with 3 mons we scale down one mon.
        mon_quorum_size = self.mon_count // 2 + 1
        num_of_mons_to_scale_down = self.mon_count - mon_quorum_size
        # Scale down the last mon deployments, so that the operator will fail the
        # 'ok-to-stop' check on the first mon deployment it tries to update
        mon_deployments = get_mon_deployments()[-num_of_mons_to_scale_down:]
        for mon_deployment in mon_deployments:
            log.info(f"Scale down the mon deployment {mon_deployment.name}")
            res = modify_deployment_replica_count(mon_deployment.name, 0)
            assert res, f"Failed to scale down the mon {mon_deployment.name}"
            self.scaled_down_mon_names.append(mon_deployment.name)

        wait_for_mon_quorum_count(mon_quorum_size, timeout=600)

        log.info(
            "Change the mon resources in the storagecluster to trigger a rolling "
            "update of the mon deployments"
        )
        res = set_storage_cluster_resources("mon", self.get_mon_resources_for_update())
        assert res, "Failed to patch the storagecluster with the new mon resources"

        return wait_for_ok_to_stop_retry_in_operator_log(
            since=get_logs_since_param(start_time),
            timeout=self.OK_TO_STOP_RETRY_BUDGET,
        )

    # The fix of the bug DFBUGS-10987 is part of ODF 5.0
    # (https://github.com/red-hat-storage/rook/pull/1305)
    @tier4a
    @jira("DFBUGS-10987")
    @skipif_ocs_version("<5.0")
    def test_add_new_device_class_during_ok_to_stop_retry(
        self, pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
    ):
        """
        The test will perform the following steps:
        1. Get the osd nodes.
        2. Create a new LocalVolumeSet for the new deviceclass as defined in the function
        'create_new_lvs_for_new_deviceclass'.
        3. Wait for the PVs in the LocalVolumeSet above to be available.
        4. Make the operator enter the Ceph daemon 'ok-to-stop' retry loop as defined in
        the method 'start_ok_to_stop_retry_loop'.
        5. While the operator is in the retry loop, add a new device set in the
        storagecluster for the new LocalVolumeSet above, which will also create a new
        deviceclass. This update cancels the in-flight orchestration.
        6. Scale up the mon deployment, so that the next orchestration can complete the
        mon update and continue to the OSD creation.
        7. Wait for the new deviceset PVCs to reach the Bound state, without waiting for
        the whole 'ok-to-stop' retry budget.
        8. Check that the operator didn't keep retrying with a cancelled context.
        9. Wait for the storagecluster to be ready.
        10. Create a new CephBlockPool for the device class above and wait for it to be
        ready.
        11. Create a new StorageClass for the pool.
        12. Run the Verification steps as defined in the function
        'verify_deviceclasses_steps'.
        13. Check the cluster and Ceph health.
        14. Check basic cluster functionality by creating some resources.

        """
        start_time = time.time()
        osd_node_names = get_osd_running_nodes()
        log.info(f"osd node names = {osd_node_names}")
        num_of_new_pvs = len(osd_node_names)
        lvs_obj = create_new_lvs_for_new_deviceclass(osd_node_names)
        log.info(
            f"Wait for the PVs in the LocalVolumeSet {lvs_obj.name} to be available"
        )
        wait_for_pvs_in_lvs_to_reach_status(
            lvs_obj.name, num_of_new_pvs, constants.STATUS_AVAILABLE
        )

        if not self.start_ok_to_stop_retry_loop():
            pytest.skip(
                "The rook-ceph operator didn't enter the Ceph daemon 'ok-to-stop' retry "
                "loop, so we can't check the scenario of the bug DFBUGS-10987"
            )

        log.info(
            f"Add a new deviceset in the storagecluster for the new LocalVolumeSet "
            f"{lvs_obj.name} while the operator is in the 'ok-to-stop' retry loop"
        )
        res = add_new_deviceset_in_storagecluster(lvs_obj.name, lvs_obj.name)
        assert res, "Failed to patch the storagecluster with the new deviceset"
        deviceset_add_time = time.time()

        log.info(
            f"Scale up the mon deployments {self.scaled_down_mon_names}, so that the "
            f"next orchestration will be able to continue to the OSD creation"
        )
        for mon_name in self.scaled_down_mon_names:
            res = modify_deployment_replica_count(mon_name, 1)
            assert res, f"Failed to scale up the mon {mon_name}"

        wait_for_mons_in_quorum(self.mon_count, timeout=600)

        log.info("Wait for the new deviceset PVCs to reach Bound status")
        res = wait_for_pvcs_in_deviceset_to_reach_status(
            lvs_obj.name,
            num_of_new_pvs,
            constants.STATUS_BOUND,
            timeout=self.NEW_DEVICESET_PVCS_TIMEOUT,
        )
        assert res, (
            f"The new deviceset PVCs didn't reach the Bound state within "
            f"{self.NEW_DEVICESET_PVCS_TIMEOUT} seconds after adding the new deviceset"
        )
        log.info(
            f"The new deviceset PVCs reached the Bound state "
            f"{int(time.time() - deviceset_add_time)} seconds after adding the new deviceset"
        )

        log.info(
            "Check that the operator didn't keep retrying the 'ok-to-stop' check with "
            "a cancelled context"
        )
        canceled_context_retry_lines = get_canceled_context_retry_lines_in_operator_log(
            since=get_logs_since_param(start_time)
        )
        assert not canceled_context_retry_lines, (
            f"The rook-ceph operator kept retrying with a cancelled context: "
            f"{canceled_context_retry_lines}"
        )

        sc_obj = get_storage_cluster()
        sc_obj.wait_for_resource(
            condition=constants.STATUS_READY,
            resource_name=constants.DEFAULT_CLUSTERNAME,
            column="PHASE",
            timeout=360,
            sleep=10,
        )

        log.info(f"Create a new CephBlockPool for the device class {lvs_obj.name}")
        cbp_obj = create_ceph_block_pool(device_class=lvs_obj.name)
        assert (
            cbp_obj
        ), f"Failed to create the CephBlockPool for the device class {lvs_obj.name}"
        cbp_obj.ocp.wait_for_resource(
            condition=constants.STATUS_READY,
            resource_name=cbp_obj.name,
            column="PHASE",
            timeout=180,
            sleep=10,
        )

        log.info(f"Create a new StorageClass for the pool {cbp_obj.name}")
        sc_obj = create_rbd_deviceclass_storageclass(pool_name=cbp_obj.name)
        assert sc_obj, f"Failed to create the StorageClass for the pool {cbp_obj.name}"

        log.info("Verification steps after adding a new deviceclass...")
        verify_deviceclasses_steps()
        log.info("Checking the cluster and Ceph health")
        self.sanity_helpers.health_check(cluster_check=True, tries=40)
        log.info("Check basic cluster functionality by creating some resources")
        self.sanity_helpers.create_resources(
            pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
        )
