import logging
import pytest

from ocs_ci.framework.logger_helper import log_step
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.resources.pod import get_used_space_on_mount_point
from ocs_ci.framework.pytest_customization.marks import green_squad, post_upgrade
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    ManageTest,
    tier2,
    polarion_id,
    skipif_upgraded_from,
)
from ocs_ci.utility.prometheus import PrometheusAPI, check_alert_list
from ocs_ci.utility.utils import TimeoutSampler

log = logging.getLogger(__name__)


@green_squad
@tier2
@skipif_ocs_version("<4.5")
@skipif_upgraded_from(["4.4"])
@polarion_id("OCS-301")
class TestPvcExpansionWhenFull(ManageTest):
    """
    Tests to verify PVC expansion when the PVC is 100% utilized.
    Verify utilization alert will stop firing after volume expansion.
    """

    @pytest.fixture(autouse=True)
    def setup(self, create_pvcs_and_pods):
        """
        Create PVCs and pods
        """
        self.pvc_size = 4
        self.pvcs, self.pods = create_pvcs_and_pods(
            pvc_size=self.pvc_size,
            access_modes_rbd=[constants.ACCESS_MODE_RWO],
            access_modes_cephfs=[constants.ACCESS_MODE_RWO],
        )
        self.near_full_threshold_violated = 0.91
        self.critical_full_threshold_violated = 0.96

    @post_upgrade
    def test_pvc_expansion_when_full(self, threading_lock):
        """
        Verify PVC expansion when the PVC is 100% utilized.
        Verify utilization alert will stop firing after volume expansion.

        The test based on TimeoutSampler mechanism. It will fire TimeoutExpiredError if the alerts not present when
        conditions met or still firing after the timeout.
        The test has assertions on the fio job results.

        Test Steps:
            1. Run IO on PVCs to utilize 91% of available storage. Check PersistentVolumeUsageNearFull alert.
            2. Run IO on PVCs to utilize 96% of available storage. Check PersistentVolumeUsageCritical alert.
            3. Run IO on all to utilize 100% of PVCs storage capacity.
            4. Expand PVCs.
            5. Verify no PVC full alerts after PVC expansion.
            6. Run IO after PVC expansion.
            7. Verify no PVC full alerts after PVC expansion and additional IO.

        """
        pvc_size_expanded = 10
        pvc_fill_up_after_resize = 3
        timeout_alerts = 300
        fill_up_near_full_mb = round(
            self.pvc_size * self.near_full_threshold_violated * 1024
        )
        fill_up_critical_full_mb = (
            round(self.pvc_size * self.critical_full_threshold_violated * 1024)
            - fill_up_near_full_mb
        )
        fill_up_full_mb = self.pvc_size * 1024 - (
            fill_up_critical_full_mb + fill_up_near_full_mb
        )

        prometheus_api = PrometheusAPI(threading_lock=threading_lock)

        log_step(
            "Run IO on PVCs to utilize 91% of available storage. Check PersistentVolumeUsageNearFull alert."
        )
        self._run_io_and_check_alerts(
            fill_up_near_full_mb,
            prometheus_api,
            alert_type="PersistentVolumeUsageNearFull",
            alert_msg="is nearing full. Data deletion or PVC expansion is required.",
            threshold_attr="near_full_alert",
        )

        log_step(
            "Run IO on PVCs to utilize 96% of available storage. Check PersistentVolumeUsageCritical alert."
        )
        self._run_io_and_check_alerts(
            fill_up_critical_full_mb,
            prometheus_api,
            alert_type="PersistentVolumeUsageCritical",
            alert_msg="is critically full. Data deletion or PVC expansion is required.",
            threshold_attr="critical_alert",
        )

        log_step("Run IO on all to utilize 100% of PVCs storage capacity.")
        self.fill_up_pvcs(fill_up_full_mb, pvc_full_error_expected=True)

        log_step("Run IO on PVCs to utilise 100% of PVCs storage capacity.")
        self._verify_used_space_on_pods("100%")

        log_step("Expanding PVCs.")
        for pvc_obj in self.pvcs:
            log.info(f"Expanding size of PVC {pvc_obj.name} to {pvc_size_expanded}Gi")
            assert pvc_obj.resize_pvc(
                pvc_size_expanded, True
            ), f"Failed to resize PVC '{pvc_obj.name}'"
        log.info(f"All PVCs are expanded to {pvc_size_expanded}Gi")

        log_step("Verify no PVC full alerts after PVC expansion.")
        self.verify_no_pvc_alerts(prometheus_api, round(timeout_alerts / 2))

        log_step("Run IO after PVC expansion.")
        self.fill_up_pvcs(
            fill_up_near_full_mb
            + fill_up_critical_full_mb
            + fill_up_full_mb
            + (pvc_fill_up_after_resize * 1024)
        )

        log_step("Verify no PVC full alerts after PVC expansion and additional IO.")
        self.verify_no_pvc_alerts(prometheus_api, round(timeout_alerts / 2))

    def _run_io_and_check_alerts(
        self, fill_up_mb, prometheus_api, alert_type, alert_msg, threshold_attr
    ):
        """
        Run IO on pods to utilize the PVCs until near_full or critical_full threshold is reached.

        Args:
            fill_up_mb (int): The amount of data to write to the PVCs
            prometheus_api (PrometheusAPI): Prometheus API object
            alert_type (str): The type of alert to check
            alert_msg (str): The message to check in the alert
            threshold_attr (str): The attribute to set when the alert is fired
        """
        log.info(f"Run IO to utilize PVCs until {alert_type} threshold is reached.")
        self.fill_up_pvcs(fill_up_mb)

        log.info(f"Wait for {alert_type} alerts to start firing for PVCs.")
        for response in TimeoutSampler(300, 5, prometheus_api.get, "alerts"):
            alerts = response.json()["data"]["alerts"]
            if self._check_pvc_alerts(alerts, alert_type, alert_msg, threshold_attr):
                log.info(f"{alert_type} alerts fired for all PVCs")
                break

    def _check_pvc_alerts(self, alerts, alert_type, alert_msg, threshold_attr):
        """
        Check if the alerts are firing for all PVCs
        Args:
            alerts (list): List of alerts
            alert_type (str): The type of alert to check
            alert_msg (str): The message to check in the alert
            threshold_attr (str): The attribute to set when the alert is fired
        """
        for pvc_obj in self.pvcs:
            alerts_pvc = [
                alert
                for alert in alerts
                if alert.get("labels", {}).get("persistentvolumeclaim") == pvc_obj.name
            ]
            if not getattr(pvc_obj, threshold_attr, False):
                try:
                    log.info(f"Checking '{alert_type}' alert for PVC {pvc_obj.name}")
                    check_alert_list(
                        label=alert_type,
                        msg=f"PVC {pvc_obj.name} {alert_msg}",
                        alerts=alerts_pvc,
                        states=["firing"],
                        severity="warning" if "NearFull" in alert_type else "error",
                    )
                    setattr(pvc_obj, threshold_attr, True)
                except AssertionError:
                    log.info(
                        f"'{alert_type}' alert not started firing for PVC {pvc_obj.name}"
                    )

        return all([getattr(pvc_obj, threshold_attr, False) for pvc_obj in self.pvcs])

    def _verify_used_space_on_pods(self, expected_used_space):
        """
        Verify used space on pods

        Args:
            expected_used_space (str): The expected used space on the pods
        """
        for pod_obj in self.pods:
            used_space = get_used_space_on_mount_point(pod_obj)
            assert (
                used_space == expected_used_space
            ), f"The used space on pod {pod_obj.name} is not {expected_used_space} but {used_space}"
            log.info(
                f"Verified: Used space on pod {pod_obj.name} is {expected_used_space}"
            )

    def verify_no_pvc_alerts(self, prometheus_api, timeout_alerts):
        """
        Verify utilization alerts are stopped
        TimeOutSampler will raise an exception if the alerts are still firing after the timeout

        Args:
            prometheus_api (PrometheusAPI): Prometheus API object
            timeout_alerts (int): Timeout to wait for alerts to stop firing
        """
        for response in TimeoutSampler(timeout_alerts, 5, prometheus_api.get, "alerts"):
            alerts = response.json()["data"]["alerts"]
            near_full_pvcs, critical_pvcs = [], []
            for pvc_obj in self.pvcs:
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

                if getattr(pvc_obj, "near_full_alert", False):
                    try:
                        self._verify_alert_stopped(
                            alerts_pvc,
                            pvc_obj,
                            "PersistentVolumeUsageNearFull",
                            "warning",
                        )
                    except AssertionError:
                        near_full_pvcs.append(pvc_obj.name)

                if getattr(pvc_obj, "critical_alert", False):
                    try:
                        self._verify_alert_stopped(
                            alerts_pvc,
                            pvc_obj,
                            "PersistentVolumeUsageCritical",
                            "error",
                        )
                    except AssertionError:
                        critical_pvcs.append(pvc_obj.name)

            if not near_full_pvcs and not critical_pvcs:
                log.info(
                    "'PersistentVolumeUsageNearFull' and 'PersistentVolumeUsageCritical' alerts "
                    "are not present for all PVCs."
                )
                break

    def _verify_alert_stopped(self, alerts_pvc, pvc_obj, alert_label, severity):
        """
        Verify the alert is not present for the PVC

        Args:
            alerts_pvc (list): List of alerts for the PVC
            pvc_obj (PVC): PVC object
            alert_label (str): The alert label to check
            severity (str): The severity of the alert
        """
        log.info(
            f"Checking '{alert_label}' alert is not present for PVC {pvc_obj.name}"
        )
        check_alert_list(
            label=alert_label,
            msg=f"PVC {pvc_obj.name} is {alert_label.split('PersistentVolumeUsage')[-1].lower()}.",
            alerts=alerts_pvc,
            states=["firing"],
            severity=severity,
        )
        log.info(f"'{alert_label}' alert is present for PVC {pvc_obj.name}")

    def fill_up_pvcs(self, fill_up_mb, pvc_full_error_expected=False):
        """
        Run IO on pods to utilize the PVCs until near_full threshold

        Args:
            fill_up_mb (int): The amount of data to write to the PVCs
            pvc_full_error_expected (bool): PVC full error is expected
        """
        for pod_obj in self.pods:
            pod_obj.run_io(
                "fs",
                size=f"{fill_up_mb}M",
                io_direction="write",
                runtime=30,
                rate="100M",
                end_fsync=1,
            )
        for pod_obj in self.pods:
            try:
                fio_result = pod_obj.get_fio_results()
                err_count = fio_result.get("jobs")[0].get("error")

                assert (
                    err_count == 0
                ), f"IO error on pod {pod_obj.name}. FIO result: {fio_result}"
            except CommandFailed as cfe:
                if "No space left on device" in str(cfe) and pvc_full_error_expected:
                    continue
                else:
                    raise

        log.info(f"Verified IO on pods to utilize {fill_up_mb}MB")
