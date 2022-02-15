import logging
import pytest

from ocs_ci.framework.testlib import bugzilla, managed_service_required, tier4, tier4a
from ocs_ci.ocs import constants
from ocs_ci.utility import pagerduty


log = logging.getLogger(__name__)


@tier4
@tier4a
@managed_service_required
@pytest.mark.polarion_id("OCS-903")
def test_corrupt_pg_pd(measure_corrupt_pg):
    """
    Test that there is appropriate incident in PagerDuty when Placement group
    on one OSD is corrupted and that this incident is cleared when the corrupted
    ceph pool is removed.
    """
    api = pagerduty.PagerDutyAPI()

    # get incidents from time when manager deployment was scaled down
    incidents = measure_corrupt_pg.get("pagerduty_incidents")
    target_label = constants.ALERT_CLUSTERERRORSTATE

    # TODO(fbalak): check the whole string in summary and incident alerts
    assert pagerduty.check_incident_list(
        summary=target_label,
        incidents=incidents,
        urgency="high",
    )
    api.check_incident_cleared(
        summary=target_label, measure_end_time=measure_stop_ceph_mgr.get("stop")
    )
