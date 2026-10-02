import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import blue_squad
from ocs_ci.framework.testlib import (
    managed_service_required,
    skipif_ms_consumer,
    tier4,
    tier4a,
)
from ocs_ci.ocs import constants
from ocs_ci.utility import pagerduty


logger = logging.getLogger(__name__)


@blue_squad
@pytest.mark.skip(
    reason="measure_corrupt_pg is unstable and may turn cluster into segfault state"
)
@tier4
@tier4a
@managed_service_required
@skipif_ms_consumer
@pytest.mark.polarion_id("OCS-2771")
def deprecated_test_corrupt_pg_pd(measure_corrupt_pg):
    """
    Test that there is appropriate incident in PagerDuty when Placement group
    on one OSD is corrupted and that this incident is cleared when the corrupted
    ceph pool is removed.
    """
    logger.test_step("Initialize PagerDuty API client")
    api = pagerduty.PagerDutyAPI()

    logger.test_step("Retrieve PagerDuty incidents from corrupted PG time window")
    incidents = measure_corrupt_pg.get("pagerduty_incidents")
    target_label = constants.ALERT_CLUSTERERRORSTATE
    logger.info(f"Target alert label: {target_label}")
    logger.info(f"Number of incidents retrieved: {len(incidents) if incidents else 0}")

    logger.test_step("Verify high-urgency incident exists for ClusterErrorState alert")
    # TODO(fbalak): check the whole string in summary and incident alerts
    incident_found = pagerduty.check_incident_list(
        summary=target_label,
        incidents=incidents,
        urgency="high",
    )
    logger.assertion(
        f"ClusterErrorState incident check: expected=True, actual={incident_found}, "
        f"urgency=high, alert={target_label}"
    )
    assert incident_found, f"No high-urgency incident found for {target_label}"

    logger.test_step("Verify incident is cleared after corrupted pool removal")
    api.check_incident_cleared(
        summary=target_label,
        measure_end_time=measure_corrupt_pg.get("stop"),
        pagerduty_service_ids=[pagerduty.get_pagerduty_service_id()],
    )
    logger.info("Incident verified as cleared in PagerDuty")
