import pytest
from ocs_ci.framework.pytest_customization.marks import tier4c
from ocs_ci.helpers.helpers import wait_for_resource_state
from ocs_ci.ocs import constants
from ocs_ci.ocs.constants import STATUS_RUNNING
from ocs_ci.ocs.resources.pod import delete_pods, get_osd_pods_having_ids
from ocs_ci.utility import prometheus


@pytest.mark.polarion_id("")  # TODO
@pytest.mark.bugzilla("1935342")
@tier4c
def test_osd_flapping_alert(measure_stop_ceph_osd):
    """
    Prometheus alert for OSD restart. alert notifies if an OpenShift Container
    Storage OSD restarts more than 5 times in 5 minutes.

    """
    expected_alert_msg = (
        f"Storage daemon osd.0 has restarted 5 times in the last 5 minutes. Please check "
        f"the pod events or ceph status to find out the cause."
    )

    target_osd = get_osd_pods_having_ids([0])
    for i in range(6):
        delete_pods(target_osd[0])
        target_osd = get_osd_pods_having_ids([0])
        wait_for_resource_state(target_osd, STATUS_RUNNING)

    prometheus.check_alert_list(
        label=constants.ALERT_CEPHOSDFLAPPING,
        msg=expected_alert_msg,
        alerts=[constants.ALERT_CEPHOSDFLAPPING],
        states=["pending", "firing"],
        severity="critical",
    )

    wait_for_resource_state(target_osd, STATUS_RUNNING)
