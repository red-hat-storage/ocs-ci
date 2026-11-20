"""
Tests for RHSTOR-8681: Provider-to-client upgrade notification behaviour.

The main validation flow (subscription channel unchanged → alerts fire →
per-client upgrade approval → alerts cleared) is embedded in
OperatorUpgrade.validate_and_upgrade_clients(), which runs as part of
test_provider_cluster_upgrade → run_provider_upgrade().

This file covers the one scenario that requires a dedicated environment:
alert severity escalation to 'critical' when a client lags by more than
one minor ODF version behind the provider.
"""

import functools
import logging

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import yellow_squad, runs_on_provider
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    skipif_external_mode,
    skipif_managed_service,
    post_upgrade,
)
from ocs_ci.ocs.provider_client_upgrade import (
    verify_incompatible_operator_version_alert,
)

log = logging.getLogger(__name__)


@yellow_squad
@post_upgrade
@runs_on_provider
@skipif_ocs_version("<5.2")
@skipif_external_mode
@skipif_managed_service
def test_alert_severity_escalates_to_critical():
    """
    Verify that the StorageClientIncompatibleOperatorVersion alert severity
    escalates to 'critical' when the client lags by more than one minor ODF
    version behind the provider (e.g. provider at 5.2, client still at 5.0).

    Test precondition: at least one client cluster has
    ``two_minor_versions_behind: true`` set in its ENV_DATA config, meaning
    it was not upgraded during the previous minor-version upgrade cycle.
    """
    behind_indexes = [
        i
        for i in range(config.nclusters)
        if config.clusters[i].ENV_DATA.get("two_minor_versions_behind", False)
    ]
    if not behind_indexes:
        pytest.skip(
            "No client configured with 'two_minor_versions_behind: true' in cluster config"
        )

    for idx in behind_indexes:
        cluster_name = config.get_cluster_name_by_index(idx)
        log.info(
            f"Verifying critical severity alert on client '{cluster_name}' "
            f"which is >1 minor version behind the provider"
        )
        verify_incompatible_operator_version_alert(
            timeout=1200,
            expected_severity="critical",
            cluster_context=functools.partial(config.RunWithConfigContext, idx),
        )
        log.info(f"Critical severity alert confirmed on '{cluster_name}'")
