import logging
import pytest
import time

from ocs_ci.framework.testlib import (
    tier2,
    ignore_leftovers,
    blue_squad,
    skipif_ocs_version,
)
from ocs_ci.ocs import constants
from ocs_ci.utility import prometheus
from ocs_ci.helpers import helpers
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.framework import config
from ocs_ci.ocs import ocp
from ocs_ci.ocs.resources.ocs import OCS
import threading
from ocs_ci.helpers.ceph_helpers import cleanup_stale_cephfs_subvolumes

log = logging.getLogger(__name__)


@ignore_leftovers
@blue_squad
@tier2
@skipif_ocs_version("<4.17")
class TestCephFSStaleSubvolumeAlert:
    @pytest.fixture(scope="function", autouse=True)
    def setup(self, request, odf_cli_setup):
        self.odf_cli_runner = odf_cli_setup
        
        def finalizer():
            cleanup_stale_cephfs_subvolumes(self.odf_cli_runner, log)

        request.addfinalizer(finalizer) 

    @pytest.mark.polarion_id("OCS-XXXX")
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

        log.info("Creating CephFS PVC with RETAIN reclaim policy")
        cephfs_sc_obj = storageclass_factory(
            interface=constants.CEPHFILESYSTEM,
            reclaim_policy=constants.RECLAIM_POLICY_RETAIN,
        )
        pvc_obj = pvc_factory(
            storageclass=cephfs_sc_obj,
            interface=constants.CEPHFILESYSTEM,
            size=3,
            access_mode=constants.ACCESS_MODE_RWX,
            status=constants.STATUS_BOUND,
        )

        pv_name = pvc_obj.backed_pv_obj.name
        pv_obj = pvc_obj.backed_pv_obj
        log.info(f"Created PVC: {pvc_obj.name}, PV: {pv_name}")

        #Deleting PVC
        pvc_obj.delete()
        pvc_obj.ocp.wait_for_delete(resource_name=pvc_obj.name)
        log.info("Deleting PVC")

        #Deleting PV to create stale subvolume
        #pv_obj = helpers.get_pv_objs_by_name([pv_name])[0]
        pv_obj.delete()
        pv_obj.ocp.wait_for_delete(resource_name=pv_name)
        log.info("Deleting PV to create stale subvolume")

        #log.info("Waiting for ocs-metrics-exporter resync window (10 minutes)")
        # ocs-metrics-exporter uses a PV reflector with ~10min resync
        #time.sleep(660)

        log.info("Waiting for CephFSStaleSubvolume alert to fire...")
        api = prometheus.PrometheusAPI(threading_lock=threading_lock)

        alerts = api.wait_for_alert(
            name=constants.ALERT_CEPHFS_STALE_SUBVOLUME,
            state="firing",
            timeout=900,
            sleep=30,
        )

        log.info("Validating CephFS stale subvolume alert...")
        message = "There could be stale subvolumes that are not claimed or utilized. Please investigate for possible stale or orphaned subvolumes."
        description = "Number of CephFS subvolumes are greater than the number of PersistentVolumes in the cluster."
        runbook = (
                "https://github.com/openshift/runbooks/blob/master/alerts/openshift-container-storage-operator/CephFSStaleSubvolume.md"
        )
        state = ["firing"]
        severity = "warning"

        prometheus.check_alert_list(
            label=constants.ALERT_CEPHFS_STALE_SUBVOLUME,
            msg=message,
            description=description,
            runbook=runbook,
            states=state,
            severity=severity,
            alerts=alerts,
        )
        log.info("Alert verified successfully")

    @pytest.mark.polarion_id("OCS-YYYY")
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
        log.info("Creating CephFS StorageClass with RETAIN reclaim policy")
        cephfs_sc_obj = storageclass_factory(
            interface=constants.CEPHFILESYSTEM,
            reclaim_policy=constants.RECLAIM_POLICY_RETAIN,
        )

        log.info("Creating multiple CephFS PVCs with RETAIN policy")
        pvc_objs = [
            pvc_factory(
                storageclass=cephfs_sc_obj,
                interface=constants.CEPHFILESYSTEM,
                size=3,
                access_mode=constants.ACCESS_MODE_RWX,
                status=constants.STATUS_BOUND,
            )
            for _ in range(2)
        ]
        
        # Capture PV names EARLY (before deleting PVCs)
        pv_names = []
        for pvc in pvc_objs:
            pv_name = pvc.backed_pv_obj.name
            pv_names.append(pv_name)
            log.info(f"Created PVC: {pvc.name}, PV: {pv_name}")

        # Delete PVCs (PV remains due to Retain)
        for pvc in pvc_objs:
            pvc.delete()
            pvc.ocp.wait_for_delete(resource_name=pvc.name)
            log.info(f"Deleted PVC: {pvc.name}")

        # Delete PVs to create stale subvolumes
        pv_ocp = ocp.OCP(kind=constants.PV)

        for pv_name in pv_names:
            pv_ocp.delete(resource_name=pv_name)
            pv_ocp.wait_for_delete(resource_name=pv_name)
            log.info(f"Deleted PV: {pv_name} to create stale subvolume")

        log.info("Waiting for CephFSStaleSubvolume alert to fire for multiple stale subvolumes...")
        api = prometheus.PrometheusAPI(threading_lock=threading_lock)
        alerts = api.wait_for_alert(
            name=constants.ALERT_CEPHFS_STALE_SUBVOLUME,
            state="firing",
            timeout=600,
            sleep=30,
        )

        log.info("Validating CephFS stale subvolume alert for multiple stale subvolumes...")
        message = "There could be stale subvolumes that are not claimed or utilized. Please investigate for possible stale or orphaned subvolumes."
        description = "Number of CephFS subvolumes are greater than the number of PersistentVolumes in the cluster."
        runbook = (
            "https://github.com/openshift/runbooks/blob/master/alerts/openshift-container-storage-operator/CephFSStaleSubvolume.md"
        )
        state = ["firing"]
        severity = "warning"

        prometheus.check_alert_list(
            label=constants.ALERT_CEPHFS_STALE_SUBVOLUME,
            msg=message,
            description=description,
            runbook=runbook,
            states=state,
            severity=severity,
            alerts=alerts,
        )
        log.info("Multiple stale subvolumes alert verified successfully")

    @pytest.mark.polarion_id("OCS-ABCD")
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

        log.info("Creating CephFS StorageClass with RETAIN reclaim policy")
        cephfs_sc_obj = storageclass_factory(
            interface=constants.CEPHFILESYSTEM,
            reclaim_policy=constants.RECLAIM_POLICY_RETAIN,
        )
        pv_names = []

        for _ in range(2):
            pvc = pvc_factory(
                storageclass=cephfs_sc_obj,
                interface=constants.CEPHFILESYSTEM,
                size=3,
                access_mode=constants.ACCESS_MODE_RWX,
                status=constants.STATUS_BOUND,
            )
            pv_names.append(pvc.backed_pv_obj.name)
            pvc.delete()
            pvc.ocp.wait_for_delete(resource_name=pvc.name)

        pv_ocp = ocp.OCP(kind=constants.PV)
        for pv_name in pv_names:
            pv_ocp.delete(resource_name=pv_name)
            pv_ocp.wait_for_delete(resource_name=pv_name)
        

        log.info("Waiting for metrics exporter to detect stale subvolume")
        # ocs-metrics-exporter uses a PV reflector with a 10-minute resync interval.
        # Explicit wait is required for stale subvolume detection.
        time.sleep(660)

        log.info("Waiting for CephFSStaleSubvolume alert to fire before exporter restart...")
        api = prometheus.PrometheusAPI(threading_lock=threading_lock)
        alerts_before = api.wait_for_alert(
            name=constants.ALERT_CEPHFS_STALE_SUBVOLUME,
            state="firing",
            timeout=300,
            sleep=30,
        )

        # Basic validation of alert presence
        log.info("Validating CephFS stale subvolume alert...")
        message = "There could be stale subvolumes that are not claimed or utilized. Please investigate for possible stale or orphaned subvolumes." 
        description = "Number of CephFS subvolumes are greater than the number of PersistentVolumes in the cluster."
        runbook = (
            "https://github.com/openshift/runbooks/blob/master/alerts/openshift-container-storage-operator/CephFSStaleSubvolume.md"
        )
        state = ["firing"]
        severity = "warning"

        prometheus.check_alert_list(
            label=constants.ALERT_CEPHFS_STALE_SUBVOLUME,
            msg=message,
            description=description,
            runbook=runbook,
            states=state,
            severity=severity,
            alerts=alerts_before
        )

        # Restart metrics exporter
        log.info("Respinning ocs-metrics-exporter pod")
        pod_ocp = ocp.OCP(kind=constants.POD, namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,)
        exporter_pods = pod_ocp.get(selector=constants.OCS_METRICS_EXPORTER)["items"]
        # ensure at least one exporter pod exists
        assert exporter_pods, "No ocs-metrics-exporter pod found"
        exporter_pod = OCS(**exporter_pods[0])
        log.info(f"Deleting metrics exporter pod {exporter_pod.name}")
        exporter_pod.delete()

        # Wait for exporter pod to come back
        assert pod_ocp.wait_for_resource(
            condition="Running",
            selector=constants.OCS_METRICS_EXPORTER,
            resource_count=1,
            timeout=600,
        ), "ocs-metrics-exporter did not come back up in time"
        log.info("Waiting for exporter cache rebuild after restart")
        # ocs-metrics-exporter uses a PV reflector with a 10-minute resync interval.
        # Explicit wait is required for stale subvolume detection.
        time.sleep(660)
        log.info("Verifying CephFSStaleSubvolume alert persists after exporter restart...")
        alerts_after = api.wait_for_alert(
            name=constants.ALERT_CEPHFS_STALE_SUBVOLUME,
            state="firing",
            timeout=300,
            sleep=15,
        )
        log.info("Validating CephFS stale subvolume alert...")
        message = "There could be stale subvolumes that are not claimed or utilized. Please investigate for possible stale or orphaned subvolumes."
        description = "Number of CephFS subvolumes are greater than the number of PersistentVolumes in the cluster."
        runbook = (
                "https://github.com/openshift/runbooks/blob/master/alerts/openshift-container-storage-operator/CephFSStaleSubvolume.md"
        )
        state = ["firing"]
        severity = "warning"
        prometheus.check_alert_list(
            label=constants.ALERT_CEPHFS_STALE_SUBVOLUME,
            msg=message,
            description=description,
            runbook=runbook,
            states=state,
            severity=severity,
            alerts=alerts_after
        )

        log.info("CephFSStaleSubvolume alert verified after metrics exporter restart")

    @pytest.mark.polarion_id("OCS-CEPHFS-MDS-STALE-001")
    @pytest.mark.tier2
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

        # Step 1: Create stale CephFS subvolume
        log.info("Creating CephFS StorageClass with RETAIN reclaim policy")
        cephfs_sc = storageclass_factory(
            interface=constants.CEPHFILESYSTEM,
            reclaim_policy=constants.RECLAIM_POLICY_RETAIN,
        )

        pvc = pvc_factory(
            storageclass=cephfs_sc,
            interface=constants.CEPHFILESYSTEM,
            size=3,
            access_mode=constants.ACCESS_MODE_RWX,
            status=constants.STATUS_BOUND,
        )

        pv_name = pvc.backed_pv_obj.name
        pv_obj = pvc.backed_pv_obj
        log.info(f"Created PVC {pvc.name} with PV {pv_name}")

        pvc.delete()
        pvc.ocp.wait_for_delete(resource_name=pvc.name)

        # Delete PV to create stale subvolume
        pv_obj.delete()
        pv_obj.ocp.wait_for_delete(resource_name=pv_obj.name)

        log.info("Deleted PVC and PV to create a stale subvolume")

        # Step 2: Wait for stale subvolume alert
        api = prometheus.PrometheusAPI(threading_lock=threading_lock)

        log.info("Waiting for CephFSStaleSubvolume alert to fire")
        alerts_before = api.wait_for_alert(
            name=constants.ALERT_CEPHFS_STALE_SUBVOLUME,
            state="firing",
            timeout=600,
            sleep=30,
        )
        message = (
            "There could be stale subvolumes that are not claimed or utilized. "
            "Please investigate for possible stale or orphaned subvolumes."
        )
        description = (
            "Number of CephFS subvolumes are greater than the number of PersistentVolumes in the cluster."
        )
        runbook = (
            "https://github.com/openshift/runbooks/blob/master/alerts/"
            "openshift-container-storage-operator/CephFSStaleSubvolume.md"
        )

        state = ["firing"]
        severity = "warning"

        prometheus.check_alert_list(
            label=constants.ALERT_CEPHFS_STALE_SUBVOLUME,
            msg=message,
            description=description,
            runbook=runbook,
            states=state,
            severity=severity,
            alerts=alerts_before,
        )

        log.info("CephFSStaleSubvolume alert verified before MDS restart")

        # Step 3: Restart one CephFS MDS pod
        pod_obj = ocp.OCP(kind=constants.POD, namespace=config.ENV_DATA["cluster_namespace"])
        mds_selector = "app=rook-ceph-mds"
        mds_pods = pod_obj.get(selector=mds_selector)["items"]
        assert len(mds_pods) >= 1, "No CephFS MDS pods found"
        orig_mds_count = len(mds_pods)
        mds_pod = OCS(**mds_pods[0])
        log.info(f"Restarting CephFS MDS pod {mds_pod.name}")
        mds_pod.delete()

        # Wait for MDS pods to recover
        assert pod_obj.wait_for_resource(
            condition="Running",
            selector=mds_selector,
            resource_count=orig_mds_count,
            timeout=600,
        ), "CephFS MDS pods did not recover in time"

        log.info("CephFS MDS pod restart completed successfully")

        # Step 4: Verify alert persists after MDS restart
        log.info("Verifying CephFSStaleSubvolume alert after MDS restart")
        alerts_after = api.wait_for_alert(
            name=constants.ALERT_CEPHFS_STALE_SUBVOLUME,
            state="firing",
            timeout=300,
            sleep=15,
        )
        message = "There could be stale subvolumes that are not claimed or utilized. Please investigate for possible stale or orphaned subvolumes."
        description = "Number of CephFS subvolumes are greater than the number of PersistentVolumes in the cluster."
        runbook = (
                "https://github.com/openshift/runbooks/blob/master/alerts/openshift-container-storage-operator/CephFSStaleSubvolume.md"
        )
        state = ["firing"]
        severity = "warning"
        prometheus.check_alert_list(
            label=constants.ALERT_CEPHFS_STALE_SUBVOLUME,
            msg=message,
            description=description,
            runbook=runbook,
            states=state,
            severity=severity,
            alerts=alerts_after,
        )

        log.info("CephFSStaleSubvolume alert persisted across MDS restart")

