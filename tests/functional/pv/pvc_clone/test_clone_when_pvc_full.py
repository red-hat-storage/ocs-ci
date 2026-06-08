import logging
import pytest

from ocs_ci.ocs import constants
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    ManageTest,
    tier2,
    polarion_id,
    skipif_ocp_version,
)
from ocs_ci.ocs.resources import pod
from ocs_ci.utility.prometheus import PrometheusAPI, check_alert_list
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.helpers.helpers import wait_for_resource_state
from ocs_ci.ocs.resources import pod as res_pod

logger = logging.getLogger(__name__)


@green_squad
@tier2
@skipif_ocs_version("<4.6")
@skipif_ocp_version("<4.6")
@polarion_id("OCS-2353")
class TestCloneWhenFull(ManageTest):
    """
    Tests to verify PVC clone when PVC is full

    """

    @pytest.fixture(autouse=True)
    def setup(self, project_factory, pvc_clone_factory, create_pvcs_and_pods):
        """
        Create PVCs and pods

        """
        self.pvc_size_gi = 3
        self.pvcs, self.pods = create_pvcs_and_pods(
            pvc_size=self.pvc_size_gi,
            access_modes_rbd=[constants.ACCESS_MODE_RWO],
            access_modes_cephfs=[constants.ACCESS_MODE_RWO],
        )

    def test_clone_when_full(self, pvc_clone_factory, pod_factory, threading_lock):
        """
        Create a clone from an existing PVC when the PVC is 100% utilized.
        Verify data integrity.
        Verify utilization alert in cloned PVC.
        Expand cloned PVC and ensure utilization alerts are stopped.

        """
        pvc_size_expanded = 6
        file_name = "fio_full"
        prometheus_api = PrometheusAPI(threading_lock=threading_lock)

        logger.test_step("Run IO on all pods to utilize 100% of PVC capacity")
        logger.info(f"Running IO on {len(self.pods)} pods to utilize 100% of PVCs")
        for pod_obj in self.pods:
            # Get available free space in M
            df_avail_size = pod_obj.exec_cmd_on_pod(
                command=f"df {pod_obj.get_storage_path()} -B M --output=avail"
            )
            # Get the numeral value of available space. eg: 3070 from '3070M'
            available_size = int(df_avail_size.strip().split()[1][0:-1])
            pod_obj.run_io(
                "fs",
                size=f"{available_size - 2}M",
                runtime=20,
                rate="100M",
                fio_filename=file_name,
                end_fsync=1,
            )
        logger.info("Started IO on all pods to utilize 100% of PVCs")

        logger.test_step("Wait for IO to finish and verify 100% utilization")
        for pod_obj in self.pods:
            pod_obj.get_fio_results()
            logger.debug(f"IO finished on pod {pod_obj.name}")

            # Verify used space on pod is 100%
            used_space = pod.get_used_space_on_mount_point(pod_obj)
            logger.assertion(
                f"Used space on pod {pod_obj.name}: expected='100%', actual='{used_space}'"
            )
            assert used_space == "100%", (
                f"The used space on pod {pod_obj.name} is not 100% " f"but {used_space}"
            )
            # Calculate md5sum of the file
            pod_obj.pvc.md5sum = pod.cal_md5sum(pod_obj, file_name)
        logger.info("IO finished and 100% utilization verified on all pods")

        logger.test_step("Create clones of the PVCs")
        cloned_pvcs = [pvc_clone_factory(pvc_obj) for pvc_obj in self.pvcs]
        logger.info(
            f"Created {len(cloned_pvcs)} clones of the PVCs. Cloned PVCs are Bound"
        )
        for pvc_obj in self.pvcs:
            if pvc_obj.backed_sc == constants.CEPHFILESYSTEM_SC:
                pv_obj = pvc_obj.backed_pv_obj
                subvolumname = (
                    pv_obj.get()
                    .get("spec")
                    .get("csi")
                    .get("volumeAttributes")
                    .get("subvolumeName")
                )
                pend_msg = f"{subvolumname}: clone from snapshot is pending"

        # Bug 2042318
        for clone_pvc in cloned_pvcs:
            if clone_pvc.backed_sc == constants.CEPHFILESYSTEM_SC:
                pv = clone_pvc.get().get("spec").get("volumeName")
                error_msg = f"{pv} failed to create clone from subvolume"
                csi_cephfsplugin_pod_objs = res_pod.get_all_pods(
                    namespace=config.ENV_DATA["cluster_namespace"],
                    selector=[
                        f"{config.ENV_DATA['cluster_namespace']}.cephfs.csi.ceph.com-ctrlplugin"
                    ],
                )
            relevant_pod_logs = None
            for pod_obj in csi_cephfsplugin_pod_objs:
                pod_log = res_pod.get_pod_logs(
                    pod_name=pod_obj.name, container="csi-cephfsplugin"
                )

                if pv in pod_log:
                    relevant_pod_logs = pod_log
                    logger.info(f"Found '{pv}' on pod {pod_obj.name}")
                    break
        logger.assertion(
            f"Error message '{error_msg}' present in logs: expected=True, "
            f"actual={error_msg in relevant_pod_logs if relevant_pod_logs else False}"
        )
        assert (
            error_msg in relevant_pod_logs
        ), f"Logs should contain the error message '{error_msg}'"
        logger.assertion(
            f"Pending message '{pend_msg}' present in logs: expected=True, "
            f"actual={pend_msg in relevant_pod_logs if relevant_pod_logs else False}"
        )
        assert (
            pend_msg in relevant_pod_logs
        ), f"Logs should contain the pending message'{pend_msg}'"
        logger.info(f"Logs contain the messages '{error_msg}' and '{pend_msg}'")

        logger.test_step("Attach cloned PVCs to pods and verify they are running")
        clone_pod_objs = []
        for clone_pvc_obj in cloned_pvcs:
            interface = (
                constants.CEPHFILESYSTEM
                if (constants.CEPHFS_INTERFACE in clone_pvc_obj.backed_sc)
                else constants.CEPHBLOCKPOOL
            )
            clone_pod_obj = pod_factory(
                interface=interface, pvc=clone_pvc_obj, status=""
            )
            logger.debug(
                f"Attached the PVC {clone_pvc_obj.name} to pod {clone_pod_obj.name}"
            )
            clone_pod_objs.append(clone_pod_obj)

        # Verify the new pods are running
        for pod_obj in clone_pod_objs:
            timeout = (
                300
                if config.ENV_DATA["platform"] == constants.IBMCLOUD_PLATFORM
                else 60
            )
            wait_for_resource_state(pod_obj, constants.STATUS_RUNNING, timeout)
        logger.info(f"All {len(clone_pod_objs)} clone pods are running")

        logger.test_step("Verify data integrity on cloned PVCs using md5sum")
        for pod_obj in clone_pod_objs:
            logger.debug(f"Verifying md5sum of {file_name} on pod {pod_obj.name}")
            pod.verify_data_integrity(pod_obj, file_name, pod_obj.pvc.parent.md5sum)
            logger.debug(
                f"Verified: md5sum of {file_name} on pod {pod_obj.name} "
                f"matches with the original md5sum"
            )
        logger.info("Data integrity check passed on all clone pods")

        logger.test_step("Wait for utilization alerts to start firing on cloned PVCs")
        # Wait till utilization alerts starts
        for response in TimeoutSampler(180, 5, prometheus_api.get, "alerts"):
            alerts = response.json()["data"]["alerts"]
            for pvc_obj in cloned_pvcs:
                alerts_pvc = [
                    alert
                    for alert in alerts
                    if alert.get("labels", {}).get("persistentvolumeclaim")
                    == pvc_obj.name
                ]
                # At least 2 alerts should be present
                if len(alerts_pvc) < 2:
                    break

                # Verify 'PersistentVolumeUsageNearFull' alert is firing
                if not getattr(pvc_obj, "near_full_alert", False):
                    try:
                        logger.debug(
                            f"Checking 'PersistentVolumeUsageNearFull' alert "
                            f"for PVC {pvc_obj.name}"
                        )
                        near_full_msg = (
                            f"PVC {pvc_obj.name} is nearing full. Data "
                            f"deletion or PVC expansion is required."
                        )
                        check_alert_list(
                            label="PersistentVolumeUsageNearFull",
                            msg=near_full_msg,
                            alerts=alerts_pvc,
                            states=["firing"],
                            severity="warning",
                        )
                        pvc_obj.near_full_alert = True
                    except AssertionError:
                        logger.debug(
                            f"'PersistentVolumeUsageNearFull' alert not "
                            f"started firing for PVC {pvc_obj.name}"
                        )

                # Verify 'PersistentVolumeUsageCritical' alert is firing
                if not getattr(pvc_obj, "critical_alert", False):
                    try:
                        logger.debug(
                            f"Checking 'PersistentVolumeUsageCritical' alert "
                            f"for PVC {pvc_obj.name}"
                        )
                        critical_msg = (
                            f"PVC {pvc_obj.name} is critically full. Data "
                            f"deletion or PVC expansion is required."
                        )
                        check_alert_list(
                            label="PersistentVolumeUsageCritical",
                            msg=critical_msg,
                            alerts=alerts_pvc,
                            states=["firing"],
                            severity="error",
                        )
                        pvc_obj.critical_alert = True
                    except AssertionError:
                        logger.debug(
                            f"'PersistentVolumeUsageCritical' alert not "
                            f"started firing for PVC {pvc_obj.name}"
                        )

            # Collect list of PVCs for which alerts are not firing
            not_near_full_pvc = [
                pvc_ob.name
                for pvc_ob in cloned_pvcs
                if not getattr(pvc_ob, "near_full_alert", False)
            ]
            not_critical_pvc = [
                pvc_ob.name
                for pvc_ob in cloned_pvcs
                if not getattr(pvc_ob, "critical_alert", False)
            ]

            if (not not_near_full_pvc) and (not not_critical_pvc):
                logger.info(
                    "'PersistentVolumeUsageNearFull' and "
                    "'PersistentVolumeUsageCritical' alerts are firing "
                    "for all cloned PVCs."
                )
                break
        logger.info("Verified: Utilization alerts are firing")

        logger.test_step(f"Expand cloned PVCs to {pvc_size_expanded}Gi")
        for pvc_obj in cloned_pvcs:
            logger.debug(
                f"Expanding size of PVC {pvc_obj.name} to {pvc_size_expanded}Gi"
            )
            # Expand PVC
            pvc_obj.resize_pvc(pvc_size_expanded, True)
        logger.info(f"All cloned PVCs expanded to {pvc_size_expanded}Gi")

        logger.test_step("Verify utilization alerts stop firing after PVC expansion")
        # Verify utilization alerts are stopped
        for response in TimeoutSampler(180, 5, prometheus_api.get, "alerts"):
            alerts = response.json()["data"]["alerts"]
            for pvc_obj in cloned_pvcs:
                alerts_pvc = [
                    alert
                    for alert in alerts
                    if alert.get("labels", {}).get("persistentvolumeclaim")
                    == pvc_obj.name
                ]
                if not alerts_pvc:
                    pvc_obj.near_full_alert = False
                    pvc_obj.critical_alert = False
                    continue

                # Verify 'PersistentVolumeUsageNearFull' alert stopped firing
                if getattr(pvc_obj, "near_full_alert"):
                    try:
                        logger.debug(
                            f"Checking 'PersistentVolumeUsageNearFull' alert "
                            f"is cleared for PVC {pvc_obj.name}"
                        )
                        near_full_msg = (
                            f"PVC {pvc_obj.name} is nearing full. Data "
                            f"deletion or PVC expansion is required."
                        )
                        check_alert_list(
                            label="PersistentVolumeUsageNearFull",
                            msg=near_full_msg,
                            alerts=alerts_pvc,
                            states=["firing"],
                            severity="warning",
                        )
                        logger.debug(
                            f"'PersistentVolumeUsageNearFull' alert is not "
                            f"stopped for PVC {pvc_obj.name}"
                        )
                    except AssertionError:
                        pvc_obj.near_full_alert = False
                        logger.debug(
                            f"'PersistentVolumeUsageNearFull' alert stopped "
                            f"firing for PVC {pvc_obj.name}"
                        )

                # Verify 'PersistentVolumeUsageCritical' alert stopped firing
                if getattr(pvc_obj, "critical_alert"):
                    try:
                        logger.debug(
                            f"Checking 'PersistentVolumeUsageCritical' alert "
                            f"is cleared for PVC {pvc_obj.name}"
                        )
                        critical_msg = (
                            f"PVC {pvc_obj.name} is critically full. Data "
                            f"deletion or PVC expansion is required."
                        )
                        check_alert_list(
                            label="PersistentVolumeUsageCritical",
                            msg=critical_msg,
                            alerts=alerts_pvc,
                            states=["firing"],
                            severity="error",
                        )
                        logger.debug(
                            f"'PersistentVolumeUsageCritical' alert is not "
                            f"stopped for PVC {pvc_obj.name}"
                        )
                    except AssertionError:
                        pvc_obj.critical_alert = False
                        logger.debug(
                            f"'PersistentVolumeUsageCritical' alert stopped "
                            f"firing for PVC {pvc_obj.name}"
                        )

            # Collect list of PVCs for which alerts are still firing
            near_full_pvcs = [
                pvc_ob.name
                for pvc_ob in cloned_pvcs
                if getattr(pvc_ob, "near_full_alert")
            ]
            critical_pvcs = [
                pvc_ob.name
                for pvc_ob in cloned_pvcs
                if getattr(pvc_ob, "critical_alert")
            ]

            if (not near_full_pvcs) and (not critical_pvcs):
                logger.info(
                    "'PersistentVolumeUsageNearFull' and "
                    "'PersistentVolumeUsageCritical' alerts are cleared for "
                    "all cloned PVCs."
                )
                break

        logger.info("Verified: Utilization alerts stopped firing")
