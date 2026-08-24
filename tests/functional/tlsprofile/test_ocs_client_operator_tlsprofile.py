import logging
from datetime import datetime, timezone

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    brown_squad,
    ignore_leftover_label,
    skipif_external_mode,
    skipif_fips_enabled,
    skipif_managed_service,
    skipif_ocs_version,
    tier3,
)
from ocs_ci.framework.testlib import ManageTest
from ocs_ci.ocs import constants
from ocs_ci.helpers.tlsprofile_helper import (
    CLIENT_OPERATOR_HTTPS_PORTS,
    TLS_PROFILE_SELECTOR_CLIENT_OPERATOR_METRICS,
    TLS_PROFILE_SELECTOR_CLIENT_OPERATOR_WEBHOOK,
    TLS_PROFILE_V12_CIPHERS,
    TLS_PROFILE_V12_GROUPS,
    TLS_PROFILE_V13_CIPHERS,
    TLS_PROFILE_V13_GROUPS,
    TLSProfile,
    assert_no_tls_errors_in_relevant_pod_logs,
    assert_ocs_client_operator_https_tls_applied,
    ocs_client_operator_is_deployed,
    scan_cluster,
    tlsprofile_crd_exists,
    wait_for_ocs_client_operator_ready,
    wait_for_tlsprofile_config_version,
)

log = logging.getLogger(__name__)

_CLIENT_OPERATOR_COMPONENT = "ocs-client-operator"
_CLIENT_OPERATOR_SELECTORS = [
    TLS_PROFILE_SELECTOR_CLIENT_OPERATOR_WEBHOOK,
    TLS_PROFILE_SELECTOR_CLIENT_OPERATOR_METRICS,
]


@pytest.fixture(scope="module", autouse=True)
def require_tlsprofile_crd():
    if not tlsprofile_crd_exists():
        pytest.skip(
            "TLSProfile CRD tlsprofiles.ocs.openshift.io not found on this cluster"
        )


@brown_squad
@tier3
@skipif_ocs_version("<4.22")
@skipif_fips_enabled
@skipif_external_mode
@skipif_managed_service
@ignore_leftover_label(constants.OCS_CLIENT_OPERATOR_LABEL)
class TestOCSClientOperatorTLSProfile(ManageTest):
    """
    Lifecycle tests for centralized ``TLSProfile`` on ocs-client-operator
    (DF 4.22+): selectors ``ocs.openshift.io/webhook`` and
    ``ocs.openshift.io/metrics`` in one rule, TLS 1.3 then TLS 1.2, in-cluster
    scantls of webhook (7443) and metrics (8443) HTTPS endpoints, then delete
    ``ocs-tls-profile``.

    Both servers must pick up the ODF TLS API without manual restarts. Skips
    on FIPS (PQ / ChaCha in our cipher lists). Deletes the CR at the end—only
    run where that is safe. An autouse fixture also deletes a leftover
    ``ocs-tls-profile`` if the test aborts early. The operator pod restarts
    when TLS settings change.
    """

    @pytest.fixture(autouse=True)
    def cleanup_tlsprofile(self, request):
        tls = TLSProfile()

        def _cleanup():
            try:
                if tls.is_tls_profile_available(silent=True):
                    log.info("Teardown: deleting leftover ocs-tls-profile")
                    tls.delete_tls_profile(wait=True, force=True)
            except Exception:
                log.exception("Teardown: failed to delete TLSProfile")
                raise

        request.addfinalizer(_cleanup)

    def test_ocs_client_operator_tls_profile_version_lifecycle(self):
        """
        ocs-client-operator TLSProfile: TLS 1.3 then TLS 1.2 on webhook and
        metrics endpoints.

        Steps:
        1. Skip if ocs-client-operator is not deployed.
        2. Apply centralized TLS configuration with selectors
           ``ocs.openshift.io/webhook`` and ``ocs.openshift.io/metrics`` using
           TLS 1.3 and the required cipher suites and TLS groups.
        3. Wait for reconciliation.
        4. Run scantls against the webhook and metrics endpoints and verify
           TLS 1.3 (and only the configured ciphers/groups) is used.
        5. Repeat with a TLS 1.2 profile; both servers must update without
           manual intervention.
        6. Delete the TLSProfile and scan operator logs for TLS-related errors.
        """
        test_start_time = datetime.now(timezone.utc)
        namespace = config.ENV_DATA["cluster_namespace"]

        if not ocs_client_operator_is_deployed(namespace):
            pytest.skip(
                f"ocs-client-operator is not deployed in {namespace}; "
                "TLSProfile selectors ocs.openshift.io/webhook and "
                "ocs.openshift.io/metrics require it"
            )

        tls = TLSProfile()
        log.assertion(
            f"TLSProfile metadata.name: expected='ocs-tls-profile', actual='{tls.name}'"
        )
        assert (
            tls.name == "ocs-tls-profile"
        ), "TLSProfile metadata.name must be ocs-tls-profile"

        log.test_step(
            "Apply TLSProfile selectors ocs.openshift.io/webhook and "
            "ocs.openshift.io/metrics with TLSv1.3, required cipher suites, "
            "and TLS groups"
        )
        if not tls.is_tls_profile_available():
            log.info("TLSProfile absent; creating with TLSv1.3 for ocs-client-operator")
            tls.create_tls_profile(
                selectors=_CLIENT_OPERATOR_SELECTORS,
                tls_version="TLSv1.3",
                ciphers=TLS_PROFILE_V13_CIPHERS,
                groups=TLS_PROFILE_V13_GROUPS,
            )
        else:
            log.info(
                "TLSProfile exists; replacing rules with TLSv1.3 for "
                "ocs-client-operator"
            )
            tls.replace_rules(
                _CLIENT_OPERATOR_SELECTORS,
                "TLSv1.3",
                TLS_PROFILE_V13_CIPHERS,
                TLS_PROFILE_V13_GROUPS,
            )

        log.test_step("Wait for TLSProfile reconciliation (TLSv1.3)")
        wait_for_tlsprofile_config_version(tls, "TLSv1.3")
        actual_version = tls.get_config_version()
        log.assertion(
            f"TLSProfile version: expected='TLSv1.3', actual='{actual_version}'"
        )
        assert actual_version == "TLSv1.3"
        wait_for_ocs_client_operator_ready(namespace)

        log.test_step(
            "scantls ocs-client-operator HTTPS ports "
            f"{list(CLIENT_OPERATOR_HTTPS_PORTS)} (metrics 8443, webhook 7443): "
            "expect tls1.3 only, configured ciphers/groups"
        )
        scan_after_v13 = scan_cluster(
            component=_CLIENT_OPERATOR_COMPONENT, namespaces=[namespace]
        )
        assert_ocs_client_operator_https_tls_applied(
            scan_after_v13,
            "TLSv1.3",
            expected_ciphers=TLS_PROFILE_V13_CIPHERS,
            expected_groups=TLS_PROFILE_V13_GROUPS,
            context="TLSProfile TLSv1.3, component=ocs-client-operator",
        )

        log.test_step(
            "Patch TLSProfile to TLSv1.2; webhook and metrics must update "
            "without manual intervention"
        )
        tls.replace_rules(
            _CLIENT_OPERATOR_SELECTORS,
            "TLSv1.2",
            TLS_PROFILE_V12_CIPHERS,
            TLS_PROFILE_V12_GROUPS,
        )
        log.test_step("Wait for TLSProfile reconciliation (TLSv1.2)")
        wait_for_tlsprofile_config_version(tls, "TLSv1.2")
        actual_version = tls.get_config_version()
        log.assertion(
            f"TLSProfile version: expected='TLSv1.2', actual='{actual_version}'"
        )
        assert actual_version == "TLSv1.2"
        wait_for_ocs_client_operator_ready(namespace)

        log.test_step(
            "scantls ocs-client-operator HTTPS ports "
            f"{list(CLIENT_OPERATOR_HTTPS_PORTS)} (metrics 8443, webhook 7443): "
            "expect tls1.2 only, configured ciphers/groups"
        )
        scan_after_v12 = scan_cluster(
            component=_CLIENT_OPERATOR_COMPONENT, namespaces=[namespace]
        )
        assert_ocs_client_operator_https_tls_applied(
            scan_after_v12,
            "TLSv1.2",
            expected_ciphers=TLS_PROFILE_V12_CIPHERS,
            expected_groups=TLS_PROFILE_V12_GROUPS,
            context="TLSProfile TLSv1.2, component=ocs-client-operator",
        )

        tls.delete_tls_profile(wait=True, force=False)
        still_present = tls.is_tls_profile_available()
        log.assertion(
            f"TLSProfile after delete: expected=absent, actual_present={still_present}"
        )
        assert not still_present, "TLSProfile should be absent after delete"
        wait_for_ocs_client_operator_ready(namespace)

        elapsed_s = max(
            120,
            int((datetime.now(timezone.utc) - test_start_time).total_seconds()) + 30,
        )
        log.test_step(
            f"Scan operator/ocs-client-operator pod logs for TLS-related errors "
            f"(since last {elapsed_s}s, from test start)"
        )
        assert_no_tls_errors_in_relevant_pod_logs(
            namespace, _CLIENT_OPERATOR_COMPONENT, since=f"{elapsed_s}s"
        )
