import logging

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    blue_squad,
    post_upgrade,
)
from ocs_ci.ocs import constants
from ocs_ci.utility.prometheus import get_alert_names, get_unexpected_alerts

log = logging.getLogger(__name__)


def get_expected_alerts():
    """
    Get names of alerts that are not reported as unexpected when fired during
    an upgrade.

    Returns:
        list: Names of expected alerts

    """
    return constants.EXPECTED_UPGRADE_ALERTS + config.UPGRADE.get("expected_alerts", [])


@post_upgrade
@blue_squad
@pytest.mark.parametrize(
    argnames="upgrade_type",
    argvalues=[
        pytest.param("ocp_upgrade", marks=pytest.mark.polarion_id("OCS-8298")),
        pytest.param("odf_upgrade", marks=pytest.mark.polarion_id("OCS-8297")),
    ],
)
def test_no_unexpected_alerts(upgrade_stats, upgrade_type):
    """
    Test that no unexpected alert was fired during the upgrade. Alerts that
    are known to be fired during an upgrade are listed in
    constants.EXPECTED_UPGRADE_ALERTS and can be extended by UPGRADE/
    expected_alerts config option.
    """
    alerts = upgrade_stats[upgrade_type].get("alerts")
    if alerts is None:
        pytest.skip(
            f"No alerts were collected during {upgrade_type} which means that "
            "the upgrade was not executed in this test run"
        )

    log.info(f"Alerts fired during {upgrade_type}: {get_alert_names(alerts)}")
    log.debug(f"Alerts collected during {upgrade_type}: {alerts}")
    ignored_severities = config.UPGRADE.get("ignored_alert_severities", [])
    unexpected_alerts = get_unexpected_alerts(
        alerts,
        expected_alerts=get_expected_alerts(),
        ignored_severities=ignored_severities,
    )
    for alert in unexpected_alerts:
        log.error(f"Unexpected alert fired during {upgrade_type}: {alert}")

    assert not unexpected_alerts, (
        f"Unexpected alerts {get_alert_names(unexpected_alerts)} were fired "
        f"during {upgrade_type}"
    )
