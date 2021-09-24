import logging
import pytest

from ocs_ci.framework.testlib import bugzilla, skipif_not_managed_service, tier4, tier4a
from ocs_ci.ocs import constants
from ocs_ci.utility import pagerduty


log = logging.getLogger(__name__)


@tier4
@tier4a
#@skipif_not_managed_service
@bugzilla("1998056")
@pytest.mark.polarion_id("OCS-1052")
def test_node_stopped_pd(measure_stop_worker_node):
    """
    Test that there is appropriate incident in PagerDuty when ceph manager
    is unavailable and that this incident is cleared when the manager
    is back online.
    """
    api = pagerduty.PagerDutyAPI()

    # get incidents from time when manager deployment was scaled down
    incidents = measure_stop_worker_node.get("pagerduty_incidents")
    target_label = constants.ALERT_OSDDISCNOTRESPONDING

    # TODO(fbalak): check the whole string in summary and incident alerts
    assert pagerduty.check_incident_list(
        summary=target_label,
        incidents=incidents,
        urgency="high",
    )
    api.check_incident_cleared(
        summary=target_label, measure_end_time=measure_stop_worker_node.get("stop")
    )
