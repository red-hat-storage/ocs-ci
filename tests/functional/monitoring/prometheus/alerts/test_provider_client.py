import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import blue_squad
from ocs_ci.framework.testlib import (
    tier4c,
    runs_on_provider,
    hci_provider_and_client_required,
)
from ocs_ci.ocs import constants
from ocs_ci.utility import prometheus
from ocs_ci.ocs.ocp import OCP


log = logging.getLogger(__name__)


@blue_squad
@tier4c
@runs_on_provider
@hci_provider_and_client_required
@pytest.mark.polarion_id("OCS-5392")
def test_change_client_ocs_version_and_stop_heartbeat(
    measure_change_client_ocs_version_and_stop_heartbeat, threading_lock
):
    """
    Test that there are appropriate alerts raised when ocs version of client
    is changed to a different version and those alerts are cleared when the
    heartbeat is resumed. During the test is stopped heartbeat cronjob on
    client in order to stop overwritting the version set for testing. When the
    heartbeat is resumed thereshould be also resumed version reporting so the
    version should contain previous version.

    """
    api = prometheus.PrometheusAPI(threading_lock=threading_lock)

    # get alerts from time when manager deployment was scaled down
    alerts = measure_change_client_ocs_version_and_stop_heartbeat.get(
        "prometheus_alerts"
    )
    client_name = measure_change_client_ocs_version_and_stop_heartbeat.get(
        "metadata"
    ).get("client_name")
    target_alerts = [
        {
            "label": constants.ALERT_STORAGECLIENTHEARTBEATMISSED,
            "msg": f"Storage Client ({client_name}) heartbeat missed for more than 120 (s).",
        },
        {
            "label": constants.ALERT_STORAGECLIENTINCOMPATIBLEOPERATORVERSION,
            "msg": f"Storage Client Operator ({client_name}) differs by more "
            "than 1 minor version. Client configuration may be incompatible and unsupported",
        },
    ]
    states = ["firing"]

    for target_alert in target_alerts:
        prometheus.check_alert_list(
            label=target_alert["label"],
            msg=target_alert["msg"],
            alerts=alerts,
            states=states,
            severity="error",
        )
        prometheus.check_alert_list(
            label=target_alert["label"],
            msg=target_alert["msg"],
            alerts=alerts,
            states=states,
            severity="warning",
        )
        api.check_alert_cleared(
            label=target_alert["label"],
            measure_end_time=measure_change_client_ocs_version_and_stop_heartbeat.get(
                "stop"
            ),
            time_min=300,
        )


def teardown_module():
    ocs_obj = OCP()
    ocs_obj.login_as_sa()
