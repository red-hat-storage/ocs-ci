import logging
import time
import pytest

from ocs_ci.framework.testlib import (
    tier2,
    ignore_leftovers,
    blue_squad,
    skipif_ocs_version,
)
from ocs_ci.ocs import constants
from ocs_ci.utility import prometheus
from ocs_ci.framework import config
from ocs_ci.ocs import ocp
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.helpers import helpers
from ocs_ci.helpers.ceph_helpers import cleanup_stale_cephfs_subvolumes
from ocs_ci.ocs.benchmark_operator_fio import BenchmarkOperatorFIO, get_file_size

log = logging.getLogger(__name__)

# Helper functions

def create_stale_cephfs_subvolumes(
    storageclass_factory,
    pvc_factory,
    count=1,
):
    """
    Create stale CephFS subvolumes by using
    reclaimPolicy=Retain and deleting PVCs and PVs.
    """
    log.info(f"Creating {count} stale CephFS subvolume(s)")

    cephfs_sc = storageclass_factory(
        interface=constants.CEPHFILESYSTEM,
        reclaim_policy=constants.RECLAIM_POLICY_RETAIN,
    )

    pv_names = []
    pvc_objs = []

    for _ in range(count):
        pvc = pvc_factory(
            storageclass=cephfs_sc,
            interface=constants.CEPHFILESYSTEM,
            size=3,
            access_mode=constants.ACCESS_MODE_RWX,
            status=constants.STATUS_BOUND,
        )
        pvc_objs.append(pvc)
        pv_names.append(pvc.backed_pv_obj.name)
        log.info(f"Created PVC {pvc.name}, PV {pv_names[-1]}")

    for pvc in pvc_objs:
        pvc.delete()
        pvc.ocp.wait_for_delete(resource_name=pvc.name)

    pv_ocp = ocp.OCP(kind=constants.PV)
    for pv_name in pv_names:
        pv_ocp.delete(resource_name=pv_name)
        pv_ocp.wait_for_delete(resource_name=pv_name)
        log.info(f"Deleted PV {pv_name} to create stale subvolume")


def wait_and_validate_stale_subvolume_alert(api, timeout=600):
    """
    Wait for CephFSStaleSubvolume alert and validate its content.
    """
    log.info("Waiting for CephFSStaleSubvolume alert to be FIRING")

    alerts = api.wait_for_alert(
        name=constants.ALERT_CEPHFS_STALE_SUBVOLUME,
        state="firing",
        timeout=timeout,
        sleep=30,
    )

    prometheus.check_alert_list(
        label=constants.ALERT_CEPHFS_STALE_SUBVOLUME,
        msg=(
            "There could be stale subvolumes that are not claimed or utilized. "
            "Please investigate for possible stale or orphaned subvolumes."
        ),
        description=(
            "Number of CephFS subvolumes are greater than the number of "
            "PersistentVolumes in the cluster."
        ),
        runbook=(
            "https://github.com/openshift/runbooks/blob/master/alerts/"
            "openshift-container-storage-operator/CephFSStaleSubvolume.md"
        ),
        states=["firing"],
        severity="warning",
        alerts=alerts,
    )

    log.info("CephFSStaleSubvolume alert validated successfully")
    return alerts


def restart_metrics_exporter():
    """
    Restart the ocs-metrics-exporter pod and wait for it to recover.
    """
    pod_ocp = ocp.OCP(
        kind=constants.POD,
        namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
    )
    exporter_pods = pod_ocp.get(
        selector=constants.OCS_METRICS_EXPORTER
    )["items"]

    assert exporter_pods, "No ocs-metrics-exporter pod found"

    exporter_pod = OCS(**exporter_pods[0])
    log.info(f"Restarting metrics exporter pod {exporter_pod.name}")
    exporter_pod.delete()

    assert pod_ocp.wait_for_resource(
        condition="Running",
        selector=constants.OCS_METRICS_EXPORTER,
        resource_count=1,
        timeout=600,
    ), "ocs-metrics-exporter did not recover in time"


def restart_mds():
    """
    Restart one CephFS MDS pod and wait for recovery.
    """
    pod_ocp = ocp.OCP(
        kind=constants.POD,
        namespace=config.ENV_DATA["cluster_namespace"],
    )
    selector = "app=rook-ceph-mds"
    mds_pods = pod_ocp.get(selector=selector)["items"]

    assert mds_pods, "No CephFS MDS pods found"
    count = len(mds_pods)

    mds_pod = OCS(**mds_pods[0])
    log.info(f"Restarting CephFS MDS pod {mds_pod.name}")
    mds_pod.delete()

    assert pod_ocp.wait_for_resource(
        condition="Running",
        selector=selector,
        resource_count=count,
        timeout=600,
    ), "CephFS MDS pods did not recover"


# Test class
# Leftovers are expected as stale subvolumes are intentionally created to trigger the alert
@ignore_leftovers
@blue_squad
@tier2
@skipif_ocs_version("<4.21")
class TestCephFSStaleSubvolumeAlert:

    @pytest.fixture(scope="function", autouse=True)
    def setup(self, request, odf_cli_setup):
        """
        Setup fixture for CephFS stale subvolume alert test.

        Initializes the ODF CLI runner and registers a finalizer
        to clean up stale CephFS subvolumes created during the test.
        """
        self.odf_cli_runner = odf_cli_setup

        def finalizer():
            cleanup_stale_cephfs_subvolumes(self.odf_cli_runner, log)

        request.addfinalizer(finalizer)

    @pytest.mark.polarion_id("OCS-7470")
    def test_cephfs_stale_subvolume_alert_fires(
        self,
        storageclass_factory,
        pvc_factory,
        threading_lock,
    ):
        """
        Test Steps:
        1. Create a CephFSStorageClass  with reclaimPolicy: Retain
        2. Create a PVC with the CephFSStorageClass
        3. Verify the PVC and PV are created and store the names of the PVC and PV
        4. Delete the PVC 
        5. Delete the PV
        6. Alert should fire
        7. Verify the alert details
        """
        create_stale_cephfs_subvolumes(
            storageclass_factory, pvc_factory, count=1
        )
        api = prometheus.PrometheusAPI(threading_lock=threading_lock)
        wait_and_validate_stale_subvolume_alert(api, timeout=900)

    @pytest.mark.polarion_id("OCS-7471")
    def test_cephfs_multiple_stale_subvolumes_alert_fires(
        self,
        storageclass_factory,
        pvc_factory,
        threading_lock,
    ):
        """
        Create multiple CephFS PVCs with reclaimPolicy: Retain, delete PVCs and PVs
        to produce multiple stale subvolumes, then verify CephFSStaleSubvolume alert fires.
        """
        create_stale_cephfs_subvolumes(
            storageclass_factory, pvc_factory, count=2
        )
        api = prometheus.PrometheusAPI(threading_lock=threading_lock)
        wait_and_validate_stale_subvolume_alert(api)

    @pytest.mark.polarion_id("OCS-7472")
    def test_cephfs_stale_subvolume_alert_after_metrics_exporter_restart(
        self,
        storageclass_factory,
        pvc_factory,
        threading_lock,
    ):
        """
        Validate that 'CephFSStaleSubvolume' alert remains available after
        respinning the 'ocs-metrics-exporter' pod.

        Steps:
        1. Create CephFS StorageClass with reclaimPolicy=Retain and a PVC.
        2. Delete PVC and PV to create stale subvolume and verify alert fires.
        3. Respin `ocs-metrics-exporter` pod and wait for it to return.
        4. Verify the alert is still present after exporter restart.
        """
        create_stale_cephfs_subvolumes(
            storageclass_factory, pvc_factory, count=2
        )

        api = prometheus.PrometheusAPI(threading_lock=threading_lock)
        wait_and_validate_stale_subvolume_alert(api)

        log.info("Restarting metrics exporter")
        restart_metrics_exporter()

        # exporter uses 10 min PV reflector resync
        time.sleep(660)

        wait_and_validate_stale_subvolume_alert(api)

    @pytest.mark.polarion_id("OCS-7473")
    def test_stale_subvolume_alert_persists_across_mds_restart(
        self,
        storageclass_factory,
        pvc_factory,
        threading_lock,
    ):
        """
        Validate that the CephFSStaleSubvolume alert remains FIRING
        during and after a CephFS MDS pod restart.
        """
        create_stale_cephfs_subvolumes(
            storageclass_factory, pvc_factory, count=1
        )

        api = prometheus.PrometheusAPI(threading_lock=threading_lock)
        wait_and_validate_stale_subvolume_alert(api)

        restart_mds()

        wait_and_validate_stale_subvolume_alert(api)

    @pytest.mark.polarion_id("OCS-7479")
    def test_stale_subvolume_alert_during_mds_scale_down_and_scale_up(
        self,
        storageclass_factory,
        pvc_factory,
        threading_lock,
    ):
        """
        Verify CephFSStaleSubvolume alert remains FIRING
        during MDS scale-down to 1 and scale-up back.
        """
        create_stale_cephfs_subvolumes(
            storageclass_factory, pvc_factory, count=1
        )
        api = prometheus.PrometheusAPI(threading_lock=threading_lock)
        wait_and_validate_stale_subvolume_alert(api, timeout=900)

        cephfs_obj = ocp.OCP(
            kind="CephFilesystem",
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        )
        cephfs = cephfs_obj.get()["items"][0]
        name = cephfs["metadata"]["name"]
        initial_count = cephfs["spec"]["metadataServer"]["activeCount"]
        original_count = initial_count

        log.info(f"Detected CephFS MDS activeCount = {initial_count}")

        if original_count == 1:
            log.info("Scaling MDS to 2 to make scale-down meaningful")
            cephfs_obj.patch(
                resource_name=name,
                params='{"spec":{"metadataServer":{"activeCount":2}}}',
                format_type="merge",
            )
            time.sleep(180)
            original_count = 2

        try:
            log.info("Scaling CephFS MDS down to 1")
            cephfs_obj.patch(
                resource_name=name,
                params='{"spec":{"metadataServer":{"activeCount":1}}}',
                format_type="merge",
            )
            time.sleep(120)
            wait_and_validate_stale_subvolume_alert(api)

            log.info(f"Scaling CephFS MDS back to {original_count}")
            cephfs_obj.patch(
                resource_name=name,
                params=f'{{"spec":{{"metadataServer":{{"activeCount":{original_count}}}}}}}',
                format_type="merge",
            )
            time.sleep(120)
            wait_and_validate_stale_subvolume_alert(api)

        finally:
            if initial_count != original_count:
                log.info(f"Restoring CephFS MDS activeCount to {initial_count}")
                cephfs_obj.patch(
                    resource_name=name,
                    params=f'{{"spec":{{"metadataServer":{{"activeCount":{initial_count}}}}}}}',
                    format_type="merge",
                )
                time.sleep(120)
    
    @pytest.mark.polarion_id("OCS-7480")
    def test_stale_subvolume_alert_behavior_under_high_cluster_utilization(
        self,
        storageclass_factory,
        pvc_factory,
        threading_lock,
    ):
        """
        Validate CephFSStaleSubvolume alert behavior when cluster utilization
        is increased significantly AFTER stale subvolumes already exist.

        Focus:
        - Alert stability under load
        - No false positives
        - No alert disappearance
        """

        alert_name = constants.ALERT_CEPHFS_STALE_SUBVOLUME
        benchmark_obj = None

        # Step 1: Create stale CephFS subvolumes (reuse helper)
        create_stale_cephfs_subvolumes(
            storageclass_factory, pvc_factory, count=3
        )

        api = prometheus.PrometheusAPI(threading_lock=threading_lock)
        wait_and_validate_stale_subvolume_alert(api, timeout=900)

        # Step 2: Increase cluster utilization using benchmark-operator
        log.info("Running benchmark-operator fio workload to increase utilization")

        try:
            size = get_file_size(40)
            benchmark_obj = BenchmarkOperatorFIO()
            benchmark_obj.setup_benchmark_fio(total_size=size)
            benchmark_obj.run_fio_benchmark_operator(is_completed=False)
            time.sleep(300)

            log.info("Benchmark-operator workload started; cluster under sustained load")
            log.info("Cluster utilization increased; validating stale alert stability under load")

            # Step 3: Validate alert remains FIRING under load
            wait_and_validate_stale_subvolume_alert(api)

            log.info(
                "CephFSStaleSubvolume alert remained FIRING under high utilization"
            )

            # Step 4: Mitigation – remove stale subvolumes to verify alert resolution
            log.info("Performing mitigation: deleting stale CephFS subvolumes")
            cleanup_stale_cephfs_subvolumes(self.odf_cli_runner, log)

            # Step 5: Verify alert resolution
            log.info("Waiting for CephFSStaleSubvolume alert to stop firing")

            api.wait_for_alert(
                name=alert_name,
                state="firing",
                timeout=900,
                sleep=30,
                expect_alert=False,
            )

            log.info("CephFSStaleSubvolume alert cleared successfully")

        finally:
            if benchmark_obj:
                log.info("Cleaning up benchmark-operator resources")
                benchmark_obj.cleanup()

    @pytest.mark.polarion_id("OCS-7481")
    def test_stale_subvolume_alert_behavior_under_multi_client_operations(
        self,
        storageclass_factory,
        pvc_factory,
        pod_factory,
        threading_lock,
    ):
        """
        Validate CephFSStaleSubvolume alert stability while multiple
        CephFS clients perform concurrent IO.

        Focus:
        - Alert stability during concurrent client activity
        - No false alert disappearance
        """

        alert_name = constants.ALERT_CEPHFS_STALE_SUBVOLUME

        # Step 1: Create stale CephFS subvolumes
        log.info("Creating stale CephFS subvolumes")
        create_stale_cephfs_subvolumes(
            storageclass_factory, pvc_factory, count=2
        )

        api = prometheus.PrometheusAPI(threading_lock=threading_lock)
        wait_and_validate_stale_subvolume_alert(api, timeout=900)

        # Step 2: Create shared RWX CephFS PVC
        log.info("Creating shared RWX CephFS PVC for multi-client access")

        cephfs_sc = storageclass_factory(
            interface=constants.CEPHFILESYSTEM,
            reclaim_policy=constants.RECLAIM_POLICY_DELETE,
        )

        shared_pvc = pvc_factory(
            storageclass=cephfs_sc,
            interface=constants.CEPHFILESYSTEM,
            size=5,
            access_mode=constants.ACCESS_MODE_RWX,
            status=constants.STATUS_BOUND,
        )

        # Step 3: Launch multiple client pods with concurrent IO
        log.info("Launching multiple CephFS client pods")

        client_pods = []
        client_count = 3

        for i in range(client_count):
            pod = pod_factory(
                interface=constants.CEPHFILESYSTEM,
                pvc=shared_pvc,
                command=[
                    "sh",
                    "-c",
                    "dd if=/dev/zero of=/mnt/cephfs/file_${HOSTNAME}.dat "
                    "bs=1M count=256 status=none && sleep 120",
                ],
            )
            client_pods.append(pod)
            log.info(f"Started client pod {pod.name}")

        log.info("Allowing concurrent client IO to run")
        time.sleep(60)

        # Step 4: Validate alert remains FIRING under multi-client load
        log.info(
            "Validating CephFSStaleSubvolume alert remains FIRING "
            "during multi-client operations"
        )

        wait_and_validate_stale_subvolume_alert(api)

        # Step 5: Mitigation – remove stale subvolumes
        log.info("Performing mitigation: deleting stale CephFS subvolumes")
        cleanup_stale_cephfs_subvolumes(self.odf_cli_runner, log)

        log.info(
            "Stale CephFS subvolumes cleaned up after validating alert "
            "stability under multi-client operations"
        )
