import logging
import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import blue_squad
from ocs_ci.framework.testlib import (
    managed_service_required,
    skipif_ms_consumer,
    tier4,
    tier4a,
)
from ocs_ci.ocs import constants
from ocs_ci.utility import pagerduty


log = logging.getLogger(__name__)


@blue_squad
@pytest.mark.skip(
    reason="measure_corrupt_pg is unstable and may turn cluster into segfault state"
)
@tier4
@tier4a
@managed_service_required
@skipif_ms_consumer
@pytest.mark.polarion_id("OCS-2771")
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
        summary=target_label,
        measure_end_time=measure_corrupt_pg.get("stop"),
        pagerduty_service_ids=[pagerduty.get_pagerduty_service_id()],
    )
