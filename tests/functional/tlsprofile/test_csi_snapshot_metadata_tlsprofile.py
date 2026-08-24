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
    CSI_SNAPSHOT_METADATA_HTTPS_PORTS,
    TLS_PROFILE_SELECTOR_CSI_SNAPSHOT_METADATA,
    TLS_PROFILE_V12_CIPHERS,
    TLS_PROFILE_V12_GROUPS,
    TLS_PROFILE_V13_CIPHERS,
    TLS_PROFILE_V13_GROUPS,
    TLSProfile,
    assert_csi_snapshot_metadata_https_tls_applied,
    assert_no_tls_errors_in_relevant_pod_logs,
    csi_snapshot_metadata_is_deployed,
    scan_cluster,
    snapshot_tlsprofile_state,
    teardown_tlsprofile,
    tlsprofile_crd_exists,
    wait_for_csi_snapshot_metadata_ready,
    wait_for_tlsprofile_config_version,
)

log = logging.getLogger(__name__)

_CSI_SNAPSHOT_METADATA_COMPONENT = "csi-snapshot-metadata"
_CSI_SNAPSHOT_METADATA_SELECTORS = [TLS_PROFILE_SELECTOR_CSI_SNAPSHOT_METADATA]


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
@ignore_leftover_label(
    constants.CSI_SNAPSHOT_METADATA,
    constants.CSI_RBDPLUGIN_PROVISIONER_LABEL,
    constants.CSI_RBDPLUGIN_PROVISIONER_LABEL_419,
)
class TestCSISnapshotMetadataTLSProfile(ManageTest):
    """
    Lifecycle tests for centralized ``TLSProfile`` on csi-snapshot-metadata
    (DF 4.22+): selector ``cbt.storage.k8s.io``, TLS 1.3 then TLS 1.2, in-cluster
    scantls of gRPC HTTPS port 50051, then delete ``ocs-tls-profile``.

    The CSI Snapshot Metadata service must pick up the ODF TLS API without
    manual restarts. Skips on FIPS (PQ / ChaCha in our cipher lists). Deletes
    the CR at the end—only run where that is safe. An autouse fixture also
    deletes a leftover ``ocs-tls-profile`` if the test aborts early. CSI
    provisioner / snapshot-metadata pods may roll when TLS settings change.
    """

    @pytest.fixture(autouse=True)
    def cleanup_tlsprofile(self, request):
        tls = TLSProfile()
        existed_before, original_rules = snapshot_tlsprofile_state(tls)

        def _cleanup():
            try:
                teardown_tlsprofile(tls, existed_before, original_rules)
            except Exception:
                log.exception("Teardown: failed to restore or delete TLSProfile")
                raise

        request.addfinalizer(_cleanup)

    def test_csi_snapshot_metadata_tls_profile_version_lifecycle(self):
        """
        csi-snapshot-metadata TLSProfile: TLS 1.3 then TLS 1.2 on port 50051.

        Steps:
        1. Skip if csi-snapshot-metadata is not deployed.
        2. Apply centralized TLS configuration with selector ``cbt.storage.k8s.io``
           using TLS 1.3 and the required cipher suites and TLS groups.
        3. Wait for reconciliation.
        4. Run scantls against port 50051 and verify TLS 1.3 (and only the
           configured ciphers/groups) is used.
        5. Repeat with a TLS 1.2 profile; the service must update without
           manual intervention.
        6. Delete the TLSProfile and scan operator / snapshot-metadata logs
           for TLS-related errors.
        """
        test_start_time = datetime.now(timezone.utc)
        namespace = config.ENV_DATA["cluster_namespace"]

        if not csi_snapshot_metadata_is_deployed(namespace):
            pytest.skip(
                f"csi-snapshot-metadata is not deployed in {namespace}; "
                "TLSProfile selector cbt.storage.k8s.io requires it"
            )

        tls = TLSProfile()
        log.assertion(
            f"TLSProfile metadata.name: expected='ocs-tls-profile', actual='{tls.name}'"
        )
        assert (
            tls.name == "ocs-tls-profile"
        ), "TLSProfile metadata.name must be ocs-tls-profile"

        log.test_step(
            "Apply TLSProfile selector cbt.storage.k8s.io with TLSv1.3, "
            "required cipher suites, and TLS groups"
        )
        if not tls.is_tls_profile_available():
            log.info(
                "TLSProfile absent; creating with TLSv1.3 for csi-snapshot-metadata"
            )
            tls.create_tls_profile(
                selectors=_CSI_SNAPSHOT_METADATA_SELECTORS,
                tls_version="TLSv1.3",
                ciphers=TLS_PROFILE_V13_CIPHERS,
                groups=TLS_PROFILE_V13_GROUPS,
            )
        else:
            log.info(
                "TLSProfile exists; replacing rules with TLSv1.3 for "
                "csi-snapshot-metadata"
            )
            tls.replace_rules(
                _CSI_SNAPSHOT_METADATA_SELECTORS,
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
        wait_for_csi_snapshot_metadata_ready(namespace)

        log.test_step(
            "scantls csi-snapshot-metadata HTTPS port "
            f"{list(CSI_SNAPSHOT_METADATA_HTTPS_PORTS)}: expect tls1.3 only, "
            "configured ciphers/groups"
        )
        scan_after_v13 = scan_cluster(
            component=_CSI_SNAPSHOT_METADATA_COMPONENT, namespaces=[namespace]
        )
        assert_csi_snapshot_metadata_https_tls_applied(
            scan_after_v13,
            "TLSv1.3",
            expected_ciphers=TLS_PROFILE_V13_CIPHERS,
            expected_groups=TLS_PROFILE_V13_GROUPS,
            context="TLSProfile TLSv1.3, component=csi-snapshot-metadata",
        )

        log.test_step(
            "Patch TLSProfile to TLSv1.2; service must update without "
            "manual intervention"
        )
        tls.replace_rules(
            _CSI_SNAPSHOT_METADATA_SELECTORS,
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
        wait_for_csi_snapshot_metadata_ready(namespace)

        log.test_step(
            "scantls csi-snapshot-metadata HTTPS port "
            f"{list(CSI_SNAPSHOT_METADATA_HTTPS_PORTS)}: expect tls1.2 only, "
            "configured ciphers/groups"
        )
        scan_after_v12 = scan_cluster(
            component=_CSI_SNAPSHOT_METADATA_COMPONENT, namespaces=[namespace]
        )
        assert_csi_snapshot_metadata_https_tls_applied(
            scan_after_v12,
            "TLSv1.2",
            expected_ciphers=TLS_PROFILE_V12_CIPHERS,
            expected_groups=TLS_PROFILE_V12_GROUPS,
            context="TLSProfile TLSv1.2, component=csi-snapshot-metadata",
        )

        tls.delete_tls_profile(wait=True, force=False)
        still_present = tls.is_tls_profile_available()
        log.assertion(
            f"TLSProfile after delete: expected=absent, actual_present={still_present}"
        )
        assert not still_present, "TLSProfile should be absent after delete"
        wait_for_csi_snapshot_metadata_ready(namespace)

        elapsed_s = max(
            120,
            int((datetime.now(timezone.utc) - test_start_time).total_seconds()) + 30,
        )
        log.test_step(
            f"Scan operator/csi-snapshot-metadata pod logs for TLS-related errors "
            f"(since last {elapsed_s}s, from test start)"
        )
        assert_no_tls_errors_in_relevant_pod_logs(
            namespace, _CSI_SNAPSHOT_METADATA_COMPONENT, since=f"{elapsed_s}s"
        )
