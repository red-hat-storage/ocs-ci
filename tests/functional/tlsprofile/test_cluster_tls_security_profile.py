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
    CSI_ADDONS_GRPC_PORTS,
    METRICS_EXPORTER_HTTPS_PORTS,
    TLS_PROFILE_VERSION_TO_GO_MIN,
    TLSProfile,
    assert_csi_addons_grpc_tls_applied,
    assert_metrics_exporter_https_tls_applied,
    assert_no_tls_errors_in_relevant_pod_logs,
    csi_addons_grpc_tls_is_enabled,
    csi_addons_sidecar_is_deployed,
    list_labeled_container_cli,
    maybe_assert_tls_cli_flags,
    metrics_exporter_is_deployed,
    restore_apiserver_tls_security_profile,
    scan_cluster,
    set_apiserver_tls_security_profile,
    snapshot_apiserver_tls_security_profile,
    wait_for_csi_addons_sidecar_ready,
    wait_for_kube_apiserver_tls_profile_rollout,
    wait_for_metrics_exporter_ready,
)

log = logging.getLogger(__name__)

_CSI_ADDONS_SIDECAR_LABELS = [
    constants.CSI_RBD_ADDON_NODEPLUGIN_LABEL_420,
    constants.CSI_CEPHFS_ADDON_NODEPLUGIN_LABEL_420,
    constants.RBD_CTRLPLUGIN_LABEL,
    constants.CEPHFS_CTRLPLUGIN_LABEL,
]


@pytest.fixture(scope="class")
def cluster_modern_tls_profile(request):
    """Set APIServer tlsSecurityProfile to Modern; restore after the class."""
    namespace = config.ENV_DATA["cluster_namespace"]
    tls = TLSProfile()
    if tls.is_tls_profile_available(silent=True):
        log.info("Deleting ocs-tls-profile so operands inherit the cluster TLS profile")
        tls.delete_tls_profile(wait=True, force=True)

    original = snapshot_apiserver_tls_security_profile()
    original_type = (original or {}).get("type")
    log.info("Setting APIServer/cluster tlsSecurityProfile to Modern")
    set_apiserver_tls_security_profile("Modern")
    wait_for_kube_apiserver_tls_profile_rollout("Modern")
    if metrics_exporter_is_deployed(namespace):
        wait_for_metrics_exporter_ready(namespace, timeout=900)
    if csi_addons_sidecar_is_deployed(namespace):
        wait_for_csi_addons_sidecar_ready(namespace, timeout=900)

    def _restore():
        try:
            restore_apiserver_tls_security_profile(original)
            wait_for_kube_apiserver_tls_profile_rollout(original_type)
        except Exception:
            log.exception("Failed to restore APIServer tlsSecurityProfile")
            raise

    request.addfinalizer(_restore)
    return original


@brown_squad
@tier3
@skipif_ocs_version("<4.22")
@skipif_fips_enabled
@skipif_external_mode
@skipif_managed_service
@ignore_leftover_label(
    constants.OCS_METRICS_EXPORTER,
    constants.OCS_CLIENT_OPERATOR_LABEL,
    constants.RBD_CTRLPLUGIN_LABEL,
    constants.CEPHFS_CTRLPLUGIN_LABEL,
    constants.CSI_ADDONS_CONTROLLER_MANAGER_LABEL,
    constants.CSI_RBD_ADDON_NODEPLUGIN_LABEL_420,
    constants.CSI_CEPHFS_ADDON_NODEPLUGIN_LABEL_420,
    # kube-apiserver TLS profile rolls recreate these node-local rook pods
    constants.CRASHCOLLECTOR_APP_LABEL,
    constants.EXPORTER_APP_LABEL,
)
class TestClusterTLSSecurityProfile(ManageTest):
    """
    Cluster APIServer tlsSecurityProfile Modern: ocs-metrics-exporter must
    follow the cluster TLS profile when no ocs-tls-profile CR overrides it.
    CSI-Addons sidecar gRPC is checked only when the sidecar is started with
    --enable-auth (otherwise the ports are plaintext).
    """

    def test_metrics_exporter_cluster_modern_tls_profile(
        self, cluster_modern_tls_profile
    ):
        """
        ocs-metrics-exporter accepts TLS 1.3 only when the cluster TLS profile
        is Modern (no ocs-tls-profile CR).
        """
        test_start_time = datetime.now(timezone.utc)
        namespace = config.ENV_DATA["cluster_namespace"]
        if not metrics_exporter_is_deployed(namespace):
            pytest.skip(
                f"ocs-metrics-exporter is not deployed in {namespace}; "
                "cluster Modern TLS profile check requires it"
            )

        log.test_step("Check exporter TLS CLI flags if the operand sets them")
        cli_rows = list_labeled_container_cli(
            namespace, constants.OCS_METRICS_EXPORTER, "ocs-metrics-exporter"
        )
        maybe_assert_tls_cli_flags(
            cli_rows,
            min_version=TLS_PROFILE_VERSION_TO_GO_MIN["TLSv1.3"],
        )

        log.test_step(
            f"scantls metrics-exporter ports {list(METRICS_EXPORTER_HTTPS_PORTS)}: "
            "expect tls1.3 only (TLS 1.2 rejected)"
        )
        results = scan_cluster(component="metrics-exporter", namespaces=[namespace])
        assert_metrics_exporter_https_tls_applied(
            results,
            "TLSv1.3",
            context="cluster tlsSecurityProfile=Modern",
            verify_ciphers_groups=False,
        )

        elapsed_s = max(
            120,
            int((datetime.now(timezone.utc) - test_start_time).total_seconds()) + 30,
        )
        log.test_step("Scan exporter logs for TLS-related errors")
        assert_no_tls_errors_in_relevant_pod_logs(
            namespace, "metrics-exporter", since=f"{elapsed_s}s"
        )

    def test_csi_addons_cluster_modern_tls_profile(self, cluster_modern_tls_profile):
        """
        CSI-Addons sidecar gRPC follows the cluster Modern TLS profile when
        --enable-auth is set: TLS 1.2 rejected, TLS 1.3 accepted.

        Sidecars without --enable-auth speak plaintext on 9070/9071/9080.
        """
        test_start_time = datetime.now(timezone.utc)
        namespace = config.ENV_DATA["cluster_namespace"]
        if not csi_addons_sidecar_is_deployed(namespace):
            pytest.skip(
                f"csi-addons sidecar is not deployed in {namespace}; "
                "cluster Modern TLS profile check requires it"
            )
        if not csi_addons_grpc_tls_is_enabled(namespace):
            pytest.skip(
                "csi-addons sidecar gRPC is plaintext (--enable-auth is not set); "
                "cluster Modern TLS profile does not apply until auth is enabled"
            )

        log.test_step("Check CSI-Addons sidecar TLS CLI after Modern profile")
        cli_rows = list_labeled_container_cli(
            namespace, _CSI_ADDONS_SIDECAR_LABELS, "csi-addons"
        )
        log.assertion(
            f"csi-addons sidecar containers: expected>=1, actual={len(cli_rows)}"
        )
        assert cli_rows, "csi-addons sidecar container args were not found"
        maybe_assert_tls_cli_flags(
            cli_rows,
            min_version=TLS_PROFILE_VERSION_TO_GO_MIN["TLSv1.3"],
        )

        log.test_step(
            f"scantls csi-addons gRPC {list(CSI_ADDONS_GRPC_PORTS)}: "
            "expect tls1.3 only (TLS 1.2 rejected)"
        )
        results = scan_cluster(component="csi-addons", namespaces=[namespace])
        assert_csi_addons_grpc_tls_applied(
            results,
            "TLSv1.3",
            context="cluster tlsSecurityProfile=Modern",
            exact_version=True,
            verify_ciphers_groups=False,
        )

        elapsed_s = max(
            120,
            int((datetime.now(timezone.utc) - test_start_time).total_seconds()) + 30,
        )
        log.test_step("Scan CSI-Addons logs for TLS-related errors")
        assert_no_tls_errors_in_relevant_pod_logs(
            namespace, "csi-addons", since=f"{elapsed_s}s"
        )
