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
    TLS_PROFILE_SELECTOR_NOOBAA_DOMAIN,
    TLS_PROFILE_SELECTOR_RGW_DOMAIN,
    TLS_PROFILE_V12_CIPHERS,
    TLS_PROFILE_V12_GROUPS,
    TLS_PROFILE_V13_CIPHERS,
    TLS_PROFILE_V13_GROUPS,
    TLSProfile,
    assert_no_tls_errors_in_relevant_pod_logs,
    assert_tls_scan_results_include_version,
    get_first_cephobjectstore_name,
    scan_cluster,
    tlsprofile_crd_exists,
    wait_for_cephobjectstore_security_cleared,
    wait_for_cephobjectstore_tls_ciphers_substring,
    wait_for_noobaa_api_server_security_absent,
    wait_for_noobaa_tls_min_version_substring,
    wait_for_tlsprofile_config_version,
)

log = logging.getLogger(__name__)


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
@ignore_leftover_label(constants.NOOBAA_ENDPOINT_POD_LABEL)
class TestCentralizedTLSProfileConfiguration(ManageTest):
    """
    Lifecycle tests for centralized ``TLSProfile`` (DF 4.22+): TLS 1.3 / 1.2 rules,
    operand checks, in-cluster TLS scan, then delete ``ocs-tls-profile``.

    Skips on FIPS (PQ / ChaCha in our cipher lists). Deletes the CR at the end—only
    run where that is safe. An autouse fixture also deletes a leftover ``ocs-tls-profile``
    if the test aborts before the in-test delete (failures, timeouts). NooBaa endpoint
    pods may roll; leftover ignore matches other MCG tests.
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

    @pytest.mark.parametrize(
        argnames="component,selectors",
        argvalues=[
            pytest.param(
                "all",
                ["*"],
                marks=pytest.mark.polarion_id("OCS-7935"),
                id="Centralized TLSProfile: wildcard selector applies to MCG and RGW",
            ),
            pytest.param(
                "noobaa",
                [TLS_PROFILE_SELECTOR_NOOBAA_DOMAIN],
                marks=pytest.mark.polarion_id("OCS-7936"),
                id="Centralized TLSProfile: noobaa.io selector",
            ),
            pytest.param(
                "rgw",
                [TLS_PROFILE_SELECTOR_RGW_DOMAIN],
                marks=[
                    pytest.mark.skipif(
                        config.ENV_DATA.get("mcg_only_deployment"),
                        reason="No RGW/CephObjectStore in mcg-only deployments",
                    ),
                    pytest.mark.polarion_id("OCS-7937"),
                ],
                id="Centralized TLSProfile: ceph.rook.io (RGW) selector",
            ),
        ],
    )
    def test_tls_profile_version_lifecycle(self, component, selectors):
        """
        Centralized TLSProfile: version toggle (1.3 / 1.2 / 1.3), scan, delete, log check.

        Steps:
        1. Detect whether ``ocs-tls-profile`` exists (required metadata name per product).
        2. If missing, create it with TLS 1.3 and DF-supported cipher/group sets.
        3. Confirm spec shows TLSv1.3 and (where applicable) operands follow; run an
            in-cluster port scan and assert ``tls1.3`` appears on at least one ``OK``
            endpoint (other ports may use TLS 1.2 only).
        4. Patch to TLS 1.2, verify operands where applicable; scan and assert ``tls1.2``
            appear on at least one ``OK`` endpoint.
        5. Patch back to TLS 1.3; scan and assert ``tls1.3`` on at least one ``OK``
            appear on at least one ``OK`` endpoint.
        6. Delete the TLSProfile.
        7. Confirm operator default (NooBaa/RGW TLSProfile propagation cleared).
        8. Scan operator/workload pod logs for TLS-related error lines (heuristic).
        """
        test_start_time = datetime.now(timezone.utc)
        namespace = config.ENV_DATA["cluster_namespace"]
        mcg_only = config.ENV_DATA.get("mcg_only_deployment")

        verify_nb = component in ("noobaa", "all")
        verify_rgw = component in ("rgw", "all") and not mcg_only
        cos_name = get_first_cephobjectstore_name(namespace) if verify_rgw else None
        if verify_rgw and cos_name is None:
            if component == "rgw":
                pytest.skip(
                    f"No CephObjectStore in {namespace}; RGW parametrization requires an "
                    "object store (check namespace, ODF build, or that RGW is deployed)."
                )
            log.warning(
                "No CephObjectStore in %s; skipping RGW-side assertions for selector-all",
                namespace,
            )
            verify_rgw = False

        tls = TLSProfile()
        assert (
            tls.name == "ocs-tls-profile"
        ), "TLSProfile metadata.name must be ocs-tls-profile"

        if not tls.is_tls_profile_available():
            log.info("TLSProfile absent; creating with TLSv1.3")
            tls.create_tls_profile(
                selectors=selectors,
                tls_version="TLSv1.3",
                ciphers=TLS_PROFILE_V13_CIPHERS,
                groups=TLS_PROFILE_V13_GROUPS,
            )
        else:
            log.info("TLSProfile exists; normalizing rule to TLSv1.3 before checks")
            tls.replace_rules(
                selectors,
                "TLSv1.3",
                TLS_PROFILE_V13_CIPHERS,
                TLS_PROFILE_V13_GROUPS,
            )

        wait_for_tlsprofile_config_version(tls, "TLSv1.3")
        assert tls.get_config_version() == "TLSv1.3"

        if verify_nb:
            wait_for_noobaa_tls_min_version_substring(namespace, "1.3")

        log.info(
            "In-cluster TLS scan: expect tls1.3 on ports that speak TLS (param %r)",
            component,
        )
        scan_after_v13 = scan_cluster(component=component, namespaces=[namespace])
        assert_tls_scan_results_include_version(
            scan_after_v13,
            "TLSv1.3",
            context=f"TLSProfile TLSv1.3, component={component}",
        )

        log.info("Patch TLSProfile to TLSv1.2 and validate")
        tls.replace_rules(
            selectors,
            "TLSv1.2",
            TLS_PROFILE_V12_CIPHERS,
            TLS_PROFILE_V12_GROUPS,
        )
        wait_for_tlsprofile_config_version(tls, "TLSv1.2")
        assert tls.get_config_version() == "TLSv1.2"

        if verify_nb:
            wait_for_noobaa_tls_min_version_substring(namespace, "1.2")

        if verify_rgw:
            wait_for_cephobjectstore_tls_ciphers_substring(
                namespace, cos_name, "ECDHE", timeout=600, sleep=15
            )

        log.info(
            "In-cluster TLS scan: expect tls1.2 on ports that speak TLS (param %r)",
            component,
        )
        scan_after_v12 = scan_cluster(component=component, namespaces=[namespace])
        assert_tls_scan_results_include_version(
            scan_after_v12,
            "TLSv1.2",
            context=f"TLSProfile TLSv1.2, component={component}",
        )

        log.info("Restore TLSv1.3 on TLSProfile, then delete resource")
        tls.replace_rules(
            selectors,
            "TLSv1.3",
            TLS_PROFILE_V13_CIPHERS,
            TLS_PROFILE_V13_GROUPS,
        )
        wait_for_tlsprofile_config_version(tls, "TLSv1.3")

        log.info(
            "In-cluster TLS scan: expect tls1.3 after restore (param %r)",
            component,
        )
        scan_after_restore = scan_cluster(component=component, namespaces=[namespace])
        assert_tls_scan_results_include_version(
            scan_after_restore,
            "TLSv1.3",
            context=f"TLSProfile restored to TLSv1.3, component={component}",
        )

        tls.delete_tls_profile(wait=True, force=False)
        assert (
            not tls.is_tls_profile_available()
        ), "TLSProfile should be absent after delete"

        if verify_nb:
            wait_for_noobaa_api_server_security_absent(namespace)

        if verify_rgw:
            wait_for_cephobjectstore_security_cleared(
                namespace, cos_name, timeout=600, sleep=15
            )

        elapsed_s = max(
            120,
            int((datetime.now(timezone.utc) - test_start_time).total_seconds()) + 30,
        )
        log.info(
            "Scanning operator/workload pod logs for TLS-related errors "
            "(since last %ss, from test start)",
            elapsed_s,
        )
        assert_no_tls_errors_in_relevant_pod_logs(
            namespace, component, since=f"{elapsed_s}s"
        )
