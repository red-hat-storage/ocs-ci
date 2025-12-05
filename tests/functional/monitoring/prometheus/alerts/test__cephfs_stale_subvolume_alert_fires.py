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

log = logging.getLogger(__name__)


@ignore_leftovers
@blue_squad
@tier2
@skipif_ocs_version("<4.17")
class TestCephFSStaleSubvolumeAlert:

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
        log.info("Deleting PV to create stale subvolume")

        log.info("Waiting for CephFSStaleSubvolume alert to fire...")
        api = prometheus.PrometheusAPI(threading_lock=threading_lock)

        alerts = api.wait_for_alert(
            name=constants.ALERT_CEPHFS_STALE_SUBVOLUME,
            state="firing",
            timeout=300,
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
