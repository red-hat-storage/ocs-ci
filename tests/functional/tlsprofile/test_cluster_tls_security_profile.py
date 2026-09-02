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
    assert_no_tls_errors_in_relevant_pod_logs,
    csi_addons_grpc_tls_is_enabled,
    csi_addons_sidecar_is_deployed,
    filter_tls_scan_results_by_ports,
    get_metrics_exporter_tls_profile_generations,
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
        log.info(
            "Deleting ocs-tls-profile so the cluster APIServer profile is "
            "not overridden by a CR"
        )
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
    Cluster APIServer tlsSecurityProfile Modern with no ocs-tls-profile CR.

    ocs-metrics-exporter applies TLS only from ``ocs-tls-profile``
    (``TLS_PROFILE_GENERATION``). Cluster Modern does not restrict the
    exporter listener. CSI-Addons sidecar gRPC is checked only when the
    sidecar is started with ``--enable-auth`` (otherwise the ports are
    plaintext).
    """

    @pytest.mark.polarion_id("OCS-8250")
    def test_metrics_exporter_cluster_modern_tls_profile(
        self, cluster_modern_tls_profile
    ):
        """
        Without ocs-tls-profile, cluster Modern does not make
        ocs-metrics-exporter TLS 1.3-only. HTTPS stays on the operand
        default (tls1.2 and tls1.3) and TLS_PROFILE_GENERATION stays 0.
        """
        test_start_time = datetime.now(timezone.utc)
        namespace = config.ENV_DATA["cluster_namespace"]
        if not metrics_exporter_is_deployed(namespace):
            pytest.skip(
                f"ocs-metrics-exporter is not deployed in {namespace}; "
                "cluster Modern TLS profile check requires it"
            )

        log.test_step(
            "Confirm exporter has no ocs-tls-profile generation bump and "
            "no TLS 1.3-only CLI flags"
        )
        generations = get_metrics_exporter_tls_profile_generations(namespace)
        log.assertion(
            f"TLS_PROFILE_GENERATION: expected all '0' (no ocs-tls-profile), "
            f"actual={generations}"
        )
        assert generations, "ocs-metrics-exporter pods were not found"
        nonzero = [
            (name, gen)
            for name, gen in generations
            if str(gen if gen is not None else "0") != "0"
        ]
        assert not nonzero, (
            "ocs-metrics-exporter TLS_PROFILE_GENERATION is non-zero without "
            f"ocs-tls-profile; cluster Modern is not the TLSProfile CR path: "
            f"{nonzero}"
        )
        cli_rows = list_labeled_container_cli(
            namespace, constants.OCS_METRICS_EXPORTER, "ocs-metrics-exporter"
        )
        # Flags are optional; if present they must not claim TLS 1.3-only.
        maybe_assert_tls_cli_flags(cli_rows)

        log.test_step(
            f"scantls metrics-exporter ports {list(METRICS_EXPORTER_HTTPS_PORTS)}: "
            "expect tls1.2 and tls1.3 (cluster Modern is not inherited)"
        )
        results = scan_cluster(component="metrics-exporter", namespaces=[namespace])
        for port in METRICS_EXPORTER_HTTPS_PORTS:
            rows = filter_tls_scan_results_by_ports(results, (port,))
            ok_rows = [row for row in rows if row.get("status") == "OK"]
            log.assertion(
                f"exporter port {port}: expected HTTPS OK, actual_ok={len(ok_rows)}"
            )
            assert ok_rows, (
                f"ocs-metrics-exporter port {port} is not serving HTTPS after "
                f"cluster Modern (rows={rows})"
            )
            versions = set()
            for row in ok_rows:
                versions.update(row.get("tls_versions") or [])
            log.assertion(
                f"exporter port {port} tls_versions: expected tls1.2 and "
                f"tls1.3, actual={sorted(versions)}"
            )
            assert "tls1.2" in versions and "tls1.3" in versions, (
                "without ocs-tls-profile, cluster Modern must not restrict "
                f"ocs-metrics-exporter port {port}; got {sorted(versions)}"
            )

        elapsed_s = max(
            120,
            int((datetime.now(timezone.utc) - test_start_time).total_seconds()) + 30,
        )
        log.test_step("Scan exporter logs for TLS-related errors")
        assert_no_tls_errors_in_relevant_pod_logs(
            namespace, "metrics-exporter", since=f"{elapsed_s}s"
        )

    @pytest.mark.polarion_id("OCS-8251")
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
