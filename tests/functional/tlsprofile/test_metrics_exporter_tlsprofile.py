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
    METRICS_EXPORTER_HTTPS_PORTS,
    TLS_PROFILE_SELECTOR_METRICS_EXPORTER,
    TLS_PROFILE_V12_CIPHERS,
    TLS_PROFILE_V12_GROUPS,
    TLS_PROFILE_V13_CIPHERS,
    TLS_PROFILE_V13_GROUPS,
    TLSProfile,
    assert_metrics_exporter_https_tls_applied,
    assert_no_tls_errors_in_relevant_pod_logs,
    metrics_exporter_is_deployed,
    scan_cluster,
    tlsprofile_crd_exists,
    wait_for_metrics_exporter_ready,
    wait_for_tlsprofile_config_version,
)

log = logging.getLogger(__name__)

_METRICS_EXPORTER_COMPONENT = "metrics-exporter"
_METRICS_EXPORTER_SELECTORS = [TLS_PROFILE_SELECTOR_METRICS_EXPORTER]


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
@ignore_leftover_label(constants.OCS_METRICS_EXPORTER)
class TestMetricsExporterTLSProfile(ManageTest):
    """
    Lifecycle tests for centralized ``TLSProfile`` on ocs-metrics-exporter
    (DF 4.22+): selector ``ocs.openshift.io/metrics-exporter``, TLS 1.3 / 1.2
    rules, in-cluster TLS scan of exporter HTTPS ports 8443 (https-main)
    and 9443 (https-self), then delete ``ocs-tls-profile``.

    Skips on FIPS (PQ / ChaCha in our cipher lists). Deletes the CR at the
    end—only run where that is safe. An autouse fixture also deletes a leftover
    ``ocs-tls-profile`` if the test aborts before the in-test delete. Exporter
    pods may roll when TLS settings change.
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

    def test_metrics_exporter_tls_profile_version_lifecycle(self):
        """
        ocs-metrics-exporter TLSProfile: version toggle (1.3 / 1.2 / 1.3), scan,
        delete, log check.

        Steps:
        1. Skip if ocs-metrics-exporter is not deployed.
        2. Detect whether ``ocs-tls-profile`` exists (required metadata name).
        3. If missing, create it with selector ``ocs.openshift.io/metrics-exporter``
           and TLS 1.3; otherwise normalize the rule to TLS 1.3.
        4. Wait until the exporter is ready; scan HTTPS ports 8443 and 9443
           and assert both speak TLS with ``tls1.3`` only.
        5. Patch to TLS 1.2, wait for the exporter, scan 8443/9443 and assert
           both speak ``tls1.2`` only.
        6. Restore TLS 1.3; scan 8443/9443 and assert both speak ``tls1.3`` only.
        7. Delete the TLSProfile and confirm it is gone.
        8. Confirm the exporter remains ready after delete.
        9. Scan operator / exporter pod logs for TLS-related error lines.
        """
        test_start_time = datetime.now(timezone.utc)
        namespace = config.ENV_DATA["cluster_namespace"]

        if not metrics_exporter_is_deployed(namespace):
            pytest.skip(
                f"ocs-metrics-exporter is not deployed in {namespace}; "
                "TLSProfile selector ocs.openshift.io/metrics-exporter requires it"
            )

        tls = TLSProfile()
        log.assertion(
            f"TLSProfile metadata.name: expected='ocs-tls-profile', actual='{tls.name}'"
        )
        assert (
            tls.name == "ocs-tls-profile"
        ), "TLSProfile metadata.name must be ocs-tls-profile"

        log.test_step(
            "Create or normalize TLSProfile with ocs.openshift.io/metrics-exporter "
            "selector and TLSv1.3"
        )
        if not tls.is_tls_profile_available():
            log.info("TLSProfile absent; creating with TLSv1.3 for metrics-exporter")
            tls.create_tls_profile(
                selectors=_METRICS_EXPORTER_SELECTORS,
                tls_version="TLSv1.3",
                ciphers=TLS_PROFILE_V13_CIPHERS,
                groups=TLS_PROFILE_V13_GROUPS,
            )
        else:
            log.info(
                "TLSProfile exists; normalizing rule to TLSv1.3 for metrics-exporter"
            )
            tls.replace_rules(
                _METRICS_EXPORTER_SELECTORS,
                "TLSv1.3",
                TLS_PROFILE_V13_CIPHERS,
                TLS_PROFILE_V13_GROUPS,
            )

        wait_for_tlsprofile_config_version(tls, "TLSv1.3")
        actual_version = tls.get_config_version()
        log.assertion(
            f"TLSProfile version: expected='TLSv1.3', actual='{actual_version}'"
        )
        assert actual_version == "TLSv1.3"
        wait_for_metrics_exporter_ready(namespace)

        log.test_step(
            "Scan ocs-metrics-exporter HTTPS ports "
            f"{list(METRICS_EXPORTER_HTTPS_PORTS)}: expect tls1.3"
        )
        scan_after_v13 = scan_cluster(
            component=_METRICS_EXPORTER_COMPONENT, namespaces=[namespace]
        )
        assert_metrics_exporter_https_tls_applied(
            scan_after_v13,
            "TLSv1.3",
            context="TLSProfile TLSv1.3, component=metrics-exporter",
        )

        log.test_step("Patch TLSProfile to TLSv1.2 and validate metrics-exporter")
        tls.replace_rules(
            _METRICS_EXPORTER_SELECTORS,
            "TLSv1.2",
            TLS_PROFILE_V12_CIPHERS,
            TLS_PROFILE_V12_GROUPS,
        )
        wait_for_tlsprofile_config_version(tls, "TLSv1.2")
        actual_version = tls.get_config_version()
        log.assertion(
            f"TLSProfile version: expected='TLSv1.2', actual='{actual_version}'"
        )
        assert actual_version == "TLSv1.2"
        wait_for_metrics_exporter_ready(namespace)

        log.test_step(
            "Scan ocs-metrics-exporter HTTPS ports "
            f"{list(METRICS_EXPORTER_HTTPS_PORTS)}: expect tls1.2"
        )
        scan_after_v12 = scan_cluster(
            component=_METRICS_EXPORTER_COMPONENT, namespaces=[namespace]
        )
        assert_metrics_exporter_https_tls_applied(
            scan_after_v12,
            "TLSv1.2",
            context="TLSProfile TLSv1.2, component=metrics-exporter",
        )

        log.test_step("Restore TLSv1.3 on TLSProfile, then delete the resource")
        tls.replace_rules(
            _METRICS_EXPORTER_SELECTORS,
            "TLSv1.3",
            TLS_PROFILE_V13_CIPHERS,
            TLS_PROFILE_V13_GROUPS,
        )
        wait_for_tlsprofile_config_version(tls, "TLSv1.3")
        wait_for_metrics_exporter_ready(namespace)

        log.test_step(
            "Scan ocs-metrics-exporter HTTPS ports "
            f"{list(METRICS_EXPORTER_HTTPS_PORTS)} after restore: expect tls1.3"
        )
        scan_after_restore = scan_cluster(
            component=_METRICS_EXPORTER_COMPONENT, namespaces=[namespace]
        )
        assert_metrics_exporter_https_tls_applied(
            scan_after_restore,
            "TLSv1.3",
            context="TLSProfile restored to TLSv1.3, component=metrics-exporter",
        )

        tls.delete_tls_profile(wait=True, force=False)
        still_present = tls.is_tls_profile_available()
        log.assertion(
            f"TLSProfile after delete: expected=absent, actual_present={still_present}"
        )
        assert not still_present, "TLSProfile should be absent after delete"
        wait_for_metrics_exporter_ready(namespace)

        elapsed_s = max(
            120,
            int((datetime.now(timezone.utc) - test_start_time).total_seconds()) + 30,
        )
        log.test_step(
            f"Scan operator/exporter pod logs for TLS-related errors "
            f"(since last {elapsed_s}s, from test start)"
        )
        assert_no_tls_errors_in_relevant_pod_logs(
            namespace, _METRICS_EXPORTER_COMPONENT, since=f"{elapsed_s}s"
        )
