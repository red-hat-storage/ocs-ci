"""
Test cases for multiple mds support
"""

import logging
import random

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    brown_squad,
    ec_allowed,
    green_squad,
    tier2,
    tier4c,
    skipif_external_mode,
    skipif_hci_client,
    runs_on_provider,
    polarion_id,
)
from ocs_ci.framework import config
from ocs_ci.framework.testlib import ManageTest
from ocs_ci.helpers import helpers
from ocs_ci.ocs.cluster import (
    adjust_active_mds_count_storagecluster,
    get_active_mds_count_cephfilesystem,
    get_active_mds_pod_objs,
    get_mds_counts,
    is_ec_pool_supported,
)
from ocs_ci.ocs.resources import pod
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.ocs import node, constants
from ocs_ci.ocs.resources.pod import get_mds_pods
from ocs_ci.utility.utils import ceph_health_check, TimeoutSampler
from tests.functional.z_cluster.nodes.test_node_replacement_proactive import (
    delete_and_create_osd_node,
)


log = logging.getLogger(__name__)


def verify_active_and_standby_mds_count(target_count, timeout=180):
    """
    Get the active and standby mds pod count from ceph command and verify it matches the target count.

    Args:
        target_count (int): The desired count of active and standby mds pods.
        timeout (int): Timeout in seconds to wait for the target count.

    """
    TimeoutSampler(timeout=timeout, sleep=10, func=get_mds_counts).wait_for_func_value(
        (target_count, target_count)
    )
    log.info(f"Active and standby-replay MDS pod counts reached {target_count}.")


@brown_squad
@tier4c
@skipif_external_mode
@skipif_hci_client
class TestMultipleMds:
    """
    Tests for support multiple mds

    """

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        """
        Make sure mds pod count is set to original.

        """

        def finalizer():
            """
            Adjust the activeMetadataServers count for the Storage cluster to 1.
            """
            adjust_active_mds_count_storagecluster(
                1
            ), "Failed to set active mds count to 1"

            log.info("Validate mds pods are up and running")
            mds_pods = get_mds_pods()
            for mds_pod in mds_pods:
                helpers.wait_for_resource_state(
                    resource=mds_pod, state=constants.STATUS_RUNNING
                )

            log.info("Checking for Ceph Health OK")
            ceph_health_check()

        request.addfinalizer(finalizer)

    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance

        """
        self.sanity_helpers = Sanity()

    def test_node_replacement_multiple_mds(self):
        """
        1. Trigger the scale-up process to add new pods.
        2. Verify active and standby-replay mds count is same.
        3. Perform node replacement on a mds pod running node.
        4. Make sure all the active mds pods come to active state.

        """
        original_active_count_cephfilesystem = get_active_mds_count_cephfilesystem()

        log.info("Scale up active mds pods from 1 to 3 sequentially.")
        new_active_mds_count = original_active_count_cephfilesystem + 2
        adjust_active_mds_count_storagecluster(new_active_mds_count)

        log.info("Verify active and standby-replay mds counts")
        verify_active_and_standby_mds_count(new_active_mds_count)

        # Replace active mds node
        active_mds_pods = get_active_mds_pod_objs()
        active_mds_pod = random.choice(active_mds_pods)
        active_mds_node_name = active_mds_pod.data["spec"].get("nodeName")
        log.info(f"Replacing active mds node : {active_mds_node_name}")
        delete_and_create_osd_node(active_mds_node_name)

        log.info("Verify active and standby-replay mds counts after node replacement")
        verify_active_and_standby_mds_count(new_active_mds_count)

        log.info("Performing cluster and Ceph health checks")
        self.sanity_helpers.health_check(tries=120)

    def test_node_drain_and_fault_tolerance_for_multiple_mds(self, pod_factory):
        """
        1. Trigger the scale-up process to add new pods.
        2. Drain active mds pod running node.
        3. Verify active and standby-replay mds count is same.
        4. Fail one active mds pod [out of two] and standby pod changes to active.

        """
        original_active_count_cephfilesystem = get_active_mds_count_cephfilesystem()

        log.info("Scale up active mds pods from 1 to 2")
        new_active_mds_count = original_active_count_cephfilesystem + 1
        adjust_active_mds_count_storagecluster(new_active_mds_count)

        log.info("Get active mds node name")
        active_mds_pods = get_active_mds_pod_objs()
        active_mds_pod = random.choice(active_mds_pods)
        active_mds_pod_name = active_mds_pod.name
        selected_pod_obj = pod.get_pod_obj(
            name=active_mds_pod_name, namespace=config.ENV_DATA["cluster_namespace"]
        )
        active_mds_node_name = selected_pod_obj.data["spec"].get("nodeName")

        log.info("Drain active mds pod running node")
        node.drain_nodes([active_mds_node_name])
        # Make the node schedulable again
        node.schedule_nodes([active_mds_node_name])

        log.info("Performing cluster and Ceph health checks")
        self.sanity_helpers.health_check(tries=120)

        log.info("Verify active and standby-replay mds counts")
        verify_active_and_standby_mds_count(new_active_mds_count)

        log.info("Start IO Workload")
        pod_obj = pod_factory(interface=constants.CEPHBLOCKPOOL)
        pod_obj.run_io(direct=1, runtime=180, storage_type="fs", size="1G")

        # Fail one active mds pod [out of two]
        log.info("Fail one active mds pod")
        rand = random.randint(0, 1)
        ct_pod = pod.get_ceph_tools_pod()
        ct_pod.exec_ceph_cmd(f"ceph mds fail {rand}")

        # Verify active and standby-replay mds counts is still same.
        log.info("Verify active and standby-replay mds counts after pod failure")
        verify_active_and_standby_mds_count(new_active_mds_count)

        log.info("Wait for IO completion")
        fio_result = pod_obj.get_fio_results()
        log.info("IO completed on all pods")
        err_count = fio_result.get("jobs")[0].get("error")
        assert err_count == 0, (
            f"IO error on pod {pod_obj.name}. " f"FIO result: {fio_result}"
        )


EC_POOL_NAME = "mds-test-ec-fs"
EC_DATA_CHUNKS = 2
EC_CODING_CHUNKS = 2


@pytest.fixture()
def cephfs_ec_storageclass(request):
    """
    Create a day-2 CephFS EC data pool and StorageClass for the test.

    Always creates a new EC pool via StorageCluster patch regardless of
    whether the cluster already has a day-1 EC pool.  The pool and
    StorageClass are cleaned up after the test.

    Returns:
        tuple: (OCS StorageClass object, full Ceph pool name)
    """
    with config.RunWithProviderConfigContextIfAvailable():
        if not is_ec_pool_supported():
            pytest.skip("EC pools are not supported on this cluster")

        min_hosts = EC_DATA_CHUNKS + EC_CODING_CHUNKS
        osd_hosts = node.get_osd_running_nodes()
        if len(osd_hosts) < min_hosts:
            pytest.skip(
                f"Not enough OSD hosts for {EC_DATA_CHUNKS}+{EC_CODING_CHUNKS} "
                f"EC profile (need {min_hosts}, have {len(osd_hosts)})"
            )

        full_pool_name = helpers.create_cephfs_ec_pool(
            EC_POOL_NAME, EC_DATA_CHUNKS, EC_CODING_CHUNKS
        )
        secret_obj = helpers.create_secret(interface_type=constants.CEPHFILESYSTEM)
        sc_obj = helpers.create_storage_class(
            interface_type=constants.CEPHFILESYSTEM,
            interface_name=full_pool_name,
            secret_name=secret_obj.name,
        )
        log.info(
            f"Created CephFS EC StorageClass '{sc_obj.name}' for pool '{full_pool_name}'"
        )

    def finalizer():
        with config.RunWithProviderConfigContextIfAvailable():
            log.info("Cleaning up CephFS EC StorageClass and pool")
            sc_obj.delete()
            sc_obj.ocp.wait_for_delete(sc_obj.name)
            secret_obj.delete()
            secret_obj.ocp.wait_for_delete(secret_obj.name)
            helpers.delete_cephfs_ec_pool(EC_POOL_NAME)

    request.addfinalizer(finalizer)
    return sc_obj, full_pool_name


@ec_allowed
@green_squad
@runs_on_provider
@skipif_hci_client
@skipif_external_mode
@pytest.mark.skipif(
    not is_ec_pool_supported(),
    reason="Erasure coded pools are not supported on this cluster",
)
class TestMdsScalingWithEcCephfs(ManageTest):
    """
    Tests for MDS scaling behaviour with CephFS EC data pool IO.
    """

    @pytest.fixture(autouse=True)
    def teardown_mds(self, request):
        """Restore activeMetadataServers to 1 after each test."""

        def finalizer():
            with config.RunWithProviderConfigContextIfAvailable():
                adjust_active_mds_count_storagecluster(1)
                log.info("Validate MDS pods are up and running")
                for mds_pod in get_mds_pods():
                    helpers.wait_for_resource_state(
                        resource=mds_pod, state=constants.STATUS_RUNNING
                    )
                ceph_health_check()

        request.addfinalizer(finalizer)

    @tier2
    @polarion_id("OCS-8063")
    def test_mds_scale_with_ec_cephfs_io(
        self, cephfs_ec_storageclass, pvc_factory, pod_factory
    ):
        """
        Verify MDS scaling does not disrupt CephFS IO on an EC data pool.

        Steps:
        1. Create CephFS RWX PVC on EC pool
        2. Run IO and verify EC pool usage increases
        3. Scale MDS from 1 to 2
        4. Verify active and standby MDS counts
        5. Run IO again and verify it succeeds after scale-up
        6. Scale MDS back from 2 to 1
        7. Verify MDS counts restored
        8. Run IO once more to confirm stability after scale-down
        """
        with config.RunWithProviderConfigContextIfAvailable():
            sc_obj, full_pool_name = cephfs_ec_storageclass

            pvc_obj = pvc_factory(
                interface=constants.CEPHFILESYSTEM,
                storageclass=sc_obj,
                access_mode=constants.ACCESS_MODE_RWX,
                size=10,
            )
            log.info(f"PVC '{pvc_obj.name}' created and bound")

            pod_obj = pod_factory(interface=constants.CEPHFILESYSTEM, pvc=pvc_obj)
            log.info(f"Pod '{pod_obj.name}' created")

            baseline_usage = helpers.fetch_used_size(full_pool_name)
            log.info(f"Baseline EC pool usage: {baseline_usage} GB")

            log.info("Running initial IO before MDS scaling")
            pod_obj.run_io(storage_type="fs", size="512M", runtime=30)
            fio_result = pod_obj.get_fio_results()
            err_count = fio_result.get("jobs")[0].get("error")
            assert err_count == 0, (
                f"IO error during initial IO on pod {pod_obj.name}. "
                f"FIO result: {fio_result}"
            )

            post_io_usage = helpers.fetch_used_size(full_pool_name)
            log.info(f"Post-IO EC pool usage: {post_io_usage} GB")
            assert post_io_usage > baseline_usage, (
                f"EC pool usage did not increase. "
                f"Baseline: {baseline_usage}, Current: {post_io_usage}"
            )

            original_count = get_active_mds_count_cephfilesystem()
            new_count = original_count + 1
            log.info(f"Scaling MDS from {original_count} to {new_count}")
            adjust_active_mds_count_storagecluster(new_count)
            verify_active_and_standby_mds_count(new_count)

            log.info("Running IO after MDS scale-up")
            pod_obj.run_io(storage_type="fs", size="512M", runtime=30)
            fio_result = pod_obj.get_fio_results()
            err_count = fio_result.get("jobs")[0].get("error")
            assert err_count == 0, (
                f"IO error after MDS scale-up on pod {pod_obj.name}. "
                f"FIO result: {fio_result}"
            )

            log.info(f"Scaling MDS back from {new_count} to {original_count}")
            adjust_active_mds_count_storagecluster(original_count)
            verify_active_and_standby_mds_count(original_count, timeout=420)

            log.info("Running IO after MDS scale-down")
            pod_obj.run_io(storage_type="fs", size="512M", runtime=30)
            fio_result = pod_obj.get_fio_results()
            err_count = fio_result.get("jobs")[0].get("error")
            assert err_count == 0, (
                f"IO error after MDS scale-down on pod {pod_obj.name}. "
                f"FIO result: {fio_result}"
            )

    @tier4c
    @polarion_id("OCS-8064")
    def test_mds_failover_with_ec_cephfs_io(
        self, cephfs_ec_storageclass, pvc_factory, pod_factory
    ):
        """
        Verify MDS failover does not break CephFS IO on an EC data pool.

        Steps:
        1. Scale MDS from 1 to 2 (so a standby-replay is available)
        2. Create CephFS RWX PVC on EC pool and start long-running IO
        3. Fail the active MDS daemon via 'ceph mds fail'
        4. Verify standby takes over (active + standby counts unchanged)
        5. Verify IO completes without errors
        """
        with config.RunWithProviderConfigContextIfAvailable():
            sc_obj, full_pool_name = cephfs_ec_storageclass

            original_count = get_active_mds_count_cephfilesystem()
            new_count = original_count + 1
            log.info(f"Scaling MDS from {original_count} to {new_count}")
            adjust_active_mds_count_storagecluster(new_count)
            verify_active_and_standby_mds_count(new_count)

            pvc_obj = pvc_factory(
                interface=constants.CEPHFILESYSTEM,
                storageclass=sc_obj,
                access_mode=constants.ACCESS_MODE_RWX,
                size=10,
            )
            pod_obj = pod_factory(interface=constants.CEPHFILESYSTEM, pvc=pvc_obj)
            log.info(f"Pod '{pod_obj.name}' created on EC CephFS pool")

            log.info("Starting long-running IO")
            pod_obj.run_io(storage_type="fs", size="1G", runtime=180)

            active_mds_pods = get_active_mds_pod_objs()
            target_mds = random.choice(active_mds_pods)
            log.info(f"Failing active MDS pod '{target_mds.name}'")
            ct_pod = pod.get_ceph_tools_pod()
            mds_daemon_name = next(
                daemon["name"]
                for daemon in ct_pod.exec_ceph_cmd("ceph fs status")["mdsmap"]
                if daemon["state"] == "active" and daemon["name"] in target_mds.name
            )
            ct_pod.exec_ceph_cmd(f"ceph mds fail {mds_daemon_name}")

            log.info("Verify MDS counts are restored after failover")
            verify_active_and_standby_mds_count(new_count)

            log.info("Waiting for IO to complete")
            fio_result = pod_obj.get_fio_results()
            err_count = fio_result.get("jobs")[0].get("error")
            assert err_count == 0, (
                f"IO error after MDS failover on pod {pod_obj.name}. "
                f"FIO result: {fio_result}"
            )
