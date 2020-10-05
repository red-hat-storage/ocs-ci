import logging
import pytest

from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.resources.pod import get_used_space_on_mount_point
from ocs_ci.framework.testlib import (
    skipif_ocs_version, ManageTest, tier2, polarion_id, skipif_upgraded_from
)
from ocs_ci.utility.prometheus import PrometheusAPI, check_alert_list
from ocs_ci.utility.utils import TimeoutSampler

log = logging.getLogger(__name__)


@tier2
@skipif_ocs_version('<4.5')
@skipif_upgraded_from(['4.4'])
@polarion_id('OCS-301')
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
            access_modes_cephfs=[constants.ACCESS_MODE_RWO]
        )

    def test_pvc_expansion_when_full(self):
        """
        Verify PVC expansion when the PVC is 100% utilized.
        Verify utilization alert will stop firing after volume expansion.

        """
        pvc_size_expanded = 10

        # Run IO to utilise 100% of volume
        log.info("Run IO on all to utilise 100% of PVCs")
        for pod_obj in self.pods:
            pod_obj.run_io(
                'fs', size=f'{self.pvc_size}G', io_direction='write',
                runtime=30, rate='100M', fio_filename=f'{pod_obj.name}_f1'
            )
        log.info("Started IO on all to utilise 100% of PVCs")
        # Wait for IO to finish
        log.info("Wait for IO to finish on pods")
        for pod_obj in self.pods:
            try:
                pod_obj.get_fio_results()
            except CommandFailed as cfe:
                if "No space left on device" not in str(cfe):
                    raise
            log.info(f"IO finished on pod {pod_obj.name}")
            # Verify used space on pod is 100%
            used_space = get_used_space_on_mount_point(pod_obj)
            assert used_space == '100%', (
                f"The used space on pod {pod_obj.name} is not 100% "
                f"but {used_space}"
            )
            log.info(f"Verified: Used space on pod {pod_obj.name} is 100%")

        prometheus_api = PrometheusAPI()

        # Wait till utilization alerts starts
        for response in TimeoutSampler(140, 5, prometheus_api.get, 'alerts'):
            alerts = response.json()['data']['alerts']
            for pvc_obj in self.pvcs:
                alerts_pvc = [
                    alert for alert in alerts if alert.get('labels', {}).get(
                        'persistentvolumeclaim'
                    ) == pvc_obj.name
                ]
                # At least 2 alerts should be present
                if len(alerts_pvc) < 2:
                    break

                # Verify 'PersistentVolumeUsageNearFull' alert is firing
                if not getattr(pvc_obj, 'near_full_alert', False):
                    try:
                        log.info(
                            f"Checking 'PersistentVolumeUsageNearFull' alert "
                            f"for PVC {pvc_obj.name}"
                        )
                        near_full_msg = (
                            f"PVC {pvc_obj.name} is nearing full. Data "
                            f"deletion or PVC expansion is required."
                        )
                        check_alert_list(
                            label='PersistentVolumeUsageNearFull',
                            msg=near_full_msg, alerts=alerts_pvc,
                            states=['firing'], severity='warning'
                        )
                        pvc_obj.near_full_alert = True
                    except AssertionError:
                        log.info(
                            f"'PersistentVolumeUsageNearFull' alert not "
                            f"started firing for PVC {pvc_obj.name}"
                        )

                # Verify 'PersistentVolumeUsageCritical' alert is firing
                if not getattr(pvc_obj, 'critical_alert', False):
                    try:
                        log.info(
                            f"Checking 'PersistentVolumeUsageCritical' alert "
                            f"for PVC {pvc_obj.name}"
                        )
                        critical_msg = (
                            f"PVC {pvc_obj.name} is critically full. Data "
                            f"deletion or PVC expansion is required."
                        )
                        check_alert_list(
                            label='PersistentVolumeUsageCritical',
                            msg=critical_msg, alerts=alerts_pvc,
                            states=['firing'], severity='error'
                        )
                        pvc_obj.critical_alert = True
                    except AssertionError:
                        log.info(
                            f"'PersistentVolumeUsageCritical' alert not "
                            f"started firing for PVC {pvc_obj.name}"
                        )

            # Collect list of PVCs for which alerts are not firing
            not_near_full_pvc = [
                pvc_ob.name for pvc_ob in self.pvcs if not getattr(
                    pvc_ob, 'near_full_alert', False
                )
            ]
            not_critical_pvc = [
                pvc_ob.name for pvc_ob in self.pvcs if not getattr(
                    pvc_ob, 'critical_alert', False
                )
            ]

            if (not not_near_full_pvc) and (not not_critical_pvc):
                log.info(
                    "'PersistentVolumeUsageNearFull' and "
                    "'PersistentVolumeUsageCritical' alerts are firing "
                    "for all PVCs."
                )
                break

        log.info("Expanding PVCs.")
        for pvc_obj in self.pvcs:
            log.info(
                f"Expanding size of PVC {pvc_obj.name} to "
                f"{pvc_size_expanded}Gi"
            )
            pvc_obj.resize_pvc(pvc_size_expanded, True)
        log.info(f"All PVCs are expanded to {pvc_size_expanded}Gi")

        # Verify utilization alerts are stopped
        for response in TimeoutSampler(140, 5, prometheus_api.get, 'alerts'):
            alerts = response.json()['data']['alerts']
            for pvc_obj in self.pvcs:
                alerts_pvc = [
                    alert for alert in alerts if alert.get('labels', {}).get(
                        'persistentvolumeclaim'
                    ) == pvc_obj.name
                ]
                if not alerts_pvc:
                    pvc_obj.near_full_alert = False
                    pvc_obj.critical_alert = False
                    continue

                # Verify 'PersistentVolumeUsageNearFull' alert stopped firing
                if getattr(pvc_obj, 'near_full_alert'):
                    try:
                        log.info(
                            f"Checking 'PrsistentVolumeUsageNearFull' alert "
                            f"is cleared for PVC {pvc_obj.name}"
                        )
                        near_full_msg = (
                            f"PVC {pvc_obj.name} is nearing full. Data "
                            f"deletion or PVC expansion is required."
                        )
                        check_alert_list(
                            label='PersistentVolumeUsageNearFull',
                            msg=near_full_msg, alerts=alerts_pvc,
                            states=['firing'], severity='warning'
                        )
                        log.info(
                            f"'PersistentVolumeUsageNearFull' alert is not "
                            f"stopped for PVC {pvc_obj.name}"
                        )
                    except AssertionError:
                        pvc_obj.near_full_alert = False
                        log.info(
                            f"'PersistentVolumeUsageNearFull' alert stopped "
                            f"firing for PVC {pvc_obj.name}"
                        )

                # Verify 'PersistentVolumeUsageCritical' alert stopped firing
                if getattr(pvc_obj, 'critical_alert'):
                    try:
                        log.info(
                            f"Checking 'PersistentVolumeUsageCritical' alert "
                            f"is cleared for PVC {pvc_obj.name}"
                        )
                        critical_msg = (
                            f"PVC {pvc_obj.name} is critically full. Data "
                            f"deletion or PVC expansion is required."
                        )
                        check_alert_list(
                            label='PersistentVolumeUsageCritical',
                            msg=critical_msg, alerts=alerts_pvc,
                            states=['firing'], severity='error'
                        )
                        log.info(
                            f"'PersistentVolumeUsageCritical' alert is not "
                            f"stopped for PVC {pvc_obj.name}"
                        )
                    except AssertionError:
                        pvc_obj.critical_alert = False
                        log.info(
                            f"'PersistentVolumeUsageCritical' alert stopped "
                            f"firing for PVC {pvc_obj.name}"
                        )

            # Collect list of PVCs for which alerts are still firing
            near_full_pvcs = [
                pvc_ob.name for pvc_ob in self.pvcs if getattr(
                    pvc_ob, 'near_full_alert'
                )
            ]
            critical_pvcs = [
                pvc_ob.name for pvc_ob in self.pvcs if getattr(
                    pvc_ob, 'critical_alert'
                )
            ]

            if (not near_full_pvcs) and (not critical_pvcs):
                log.info(
                    "'PersistentVolumeUsageNearFull' and "
                    "'PersistentVolumeUsageCritical' alerts are cleared for "
                    "all PVCs."
                )
                break

        # Run IO to verify the expanded capacity can be utilized
        log.info("Run IO after PVC expansion.")
        for pod_obj in self.pods:
            pod_obj.run_io(
                'fs', size='3G', io_direction='write', runtime=60,
                fio_filename=f'{pod_obj.name}_f2'
            )

        # Wait for IO to complete
        log.info("Waiting for IO to complete on pods.")
        for pod_obj in self.pods:
            fio_result = pod_obj.get_fio_results()
            err_count = fio_result.get('jobs')[0].get('error')
            assert err_count == 0, (
                f"IO error on pod {pod_obj.name}. FIO result: {fio_result}"
            )
            log.info(f"Verified IO on pod {pod_obj.name} after expanding PVC.")
