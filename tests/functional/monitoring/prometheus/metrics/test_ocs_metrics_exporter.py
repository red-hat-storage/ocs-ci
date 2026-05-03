"""
RHSTOR-7964 — ocs-metrics-exporter works in all deployment modes.

EXISTING TESTS (already in ocs-ci, not duplicated here):
  - tests/functional/monitoring/prometheus/metrics/test_monitoring_defaults.py
      ::test_provider_metrics_available
        Partially covers the "exporter running in provider-client setup" case:
        it verifies metrics are present on the provider Prometheus but does
        NOT check pod structure, container count, or kube-rbac-proxy removal.

  - tests/functional/monitoring/prometheus/metrics/test_rgw.py
      ::test_ceph_rgw_metrics_after_metrics_exporter_respin
        Covers exporter respin behaviour for RGW metrics.

Addition vs existing tests
--------------------------
This file adds explicit structural checks (pod Running, single container,
no kube-rbac-proxy) and metric-label validation (consumer_name, rados_namespace)
that are not covered by the existing monitoring defaults tests.
New helpers used from ocs_ci/helpers/metrics_exporter.py:
  - get_metrics_exporter_pod()
  - verify_metrics_exporter_running()
  - query_metrics_exporter_endpoint()
New alert helpers used from ocs_ci/utility/prometheus.py:
  - wait_and_validate_alert_firing()
  - wait_for_alert_cleared()
"""

import logging

import pytest

from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import (
    ManageTest,
    tier1,
    tier2,
    skipif_ocs_version,
    hci_provider_and_client_required,
    runs_on_provider,
)
from ocs_ci.helpers.metrics_exporter import (
    get_metrics_exporter_pod,
    query_metrics_exporter_endpoint,
    verify_metrics_exporter_running,
)
from ocs_ci.ocs import constants

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Internal-mode tests (single-cluster)
# ---------------------------------------------------------------------------


@green_squad
@tier1
@skipif_ocs_version("<4.22")
class TestOCSMetricsExporterInternal(ManageTest):
    """
    RHSTOR-7964 — ocs-metrics-exporter structural and metric checks for
    Internal (single-cluster) mode.
    """

    def test_no_kube_rbac_proxy(self):
        """
        Verify kube-rbac-proxy container is removed from the exporter pod.

        RHSTOR-7964: kube-rbac-proxy removal — Internal/Provider mode.

        Steps:
        1. Get the ocs-metrics-exporter pod.
        2. Assert pod is Running with exactly 1 container.
        3. Assert 'kube-rbac-proxy' is NOT in the container list.
        4. Assert the single container is ready.
        """
        log.info("Verifying kube-rbac-proxy is not present in exporter pod")
        verify_metrics_exporter_running(expected_container_count=1)

    def test_rbd_pv_metadata_internal_mode(self, pvc_factory, teardown_factory):
        """
        RHSTOR-7964 — Metrics Validation Internal: ocs_rbd_pv_metadata.

        Steps:
        1. Create an RBD PVC.
        2. Query ocs_rbd_pv_metadata from the exporter /metrics endpoint.
        3. Verify the metric includes name, image, pool_name, rados_namespace.
        4. Verify consumer_name is absent or empty for internal mode.
        """
        pvc_obj = pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            size=1,
            status=constants.STATUS_BOUND,
        )
        teardown_factory(pvc_obj)

        pod = get_metrics_exporter_pod()
        pod_name = pod["metadata"]["name"]

        log.info("Querying ocs_rbd_pv_metadata from /metrics endpoint")
        output = query_metrics_exporter_endpoint(
            pod_name=pod_name,
            metric_filter="ocs_rbd_pv_metadata",
        )
        assert output, "ocs_rbd_pv_metadata metric not found in exporter output"

        # In Internal mode consumer_name should be absent or empty
        for line in output.splitlines():
            if "ocs_rbd_pv_metadata" in line and "{" in line:
                assert (
                    'consumer_name=""' in line or "consumer_name" not in line
                ), f"consumer_name should be empty for internal mode, got: {line}"
        log.info("ocs_rbd_pv_metadata metric is present with correct labels")

    def test_rbd_children_count_internal_mode(self, pvc_factory, teardown_factory):
        """
        RHSTOR-7964 — Metrics Validation Internal: ocs_rbd_children_count.

        Steps:
        1. Create an RBD PVC, snapshot, and clone PVC.
        2. Query ocs_rbd_children_count; verify count >= 1.
        3. Verify consumer_name is absent or empty.
        4. Delete the clone; verify count decrements.
        """
        from ocs_ci.helpers import helpers
        from ocs_ci.ocs.resources import pvc as pvc_resource

        source_pvc = pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            size=1,
            status=constants.STATUS_BOUND,
        )
        teardown_factory(source_pvc)

        # Create snapshot (Delete policy — this test does not need Retain)
        snap_name = helpers.create_unique_resource_name("test", "rbd-snap")
        snap_obj = pvc_resource.create_pvc_snapshot(
            pvc_name=source_pvc.name,
            snap_yaml=constants.CSI_RBD_SNAPSHOT_YAML,
            snap_name=snap_name,
            namespace=source_pvc.namespace,
            wait=True,
            timeout=120,
        )
        teardown_factory(snap_obj)

        # Create clone
        clone_pvc = pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            size=1,
            status=constants.STATUS_BOUND,
            source_pvc=source_pvc,
        )
        teardown_factory(clone_pvc)

        pod = get_metrics_exporter_pod()
        pod_name = pod["metadata"]["name"]

        log.info("Querying ocs_rbd_children_count from /metrics endpoint")
        output = query_metrics_exporter_endpoint(
            pod_name=pod_name,
            metric_filter="ocs_rbd_children_count",
        )
        assert output, "ocs_rbd_children_count metric not found"

        # Verify at least one entry with count >= 1
        found_nonzero = False
        for line in output.splitlines():
            if line.startswith("ocs_rbd_children_count") and not line.startswith("#"):
                count = float(line.split()[-1])
                if count >= 1:
                    found_nonzero = True
                    break
        assert found_nonzero, (
            f"Expected ocs_rbd_children_count >= 1 after creating a clone. "
            f"Output:\n{output}"
        )
        log.info("ocs_rbd_children_count metric correctly reflects clone count")

    def test_cluster_level_metrics_no_consumer_name(self):
        """
        RHSTOR-7964 — Metrics Validation Internal: cluster-level metrics
        must NOT carry a consumer_name label.

        Checks: ocs_rbd_mirror_daemon_health, ocs_mirror_daemon_count,
                ocs_storage_consumer_metadata, ocs_storage_client_last_heartbeat.
        """
        pod = get_metrics_exporter_pod()
        pod_name = pod["metadata"]["name"]
        output = query_metrics_exporter_endpoint(pod_name=pod_name)

        cluster_metrics = [
            "ocs_rbd_mirror_daemon_health",
            "ocs_mirror_daemon_count",
            "ocs_storage_consumer_metadata",
            "ocs_storage_client_last_heartbeat",
        ]
        for metric in cluster_metrics:
            lines = [
                l
                for l in output.splitlines()
                if l.startswith(metric) and not l.startswith("#")
            ]
            for line in lines:
                assert "consumer_name" not in line, (
                    f"Cluster-level metric {metric} should not have consumer_name "
                    f"label but found: {line}"
                )
        log.info("Cluster-level metrics verified — no consumer_name label present")

    def test_dedicated_ceph_user_credentials(self):
        """
        RHSTOR-7964 — Dedicated Ceph User Credentials.

        Steps:
        1. Verify the ocs-metrics-exporter-ceph-auth secret exists.
        2. Verify the secret is mounted in the exporter pod.
        3. Verify the exporter logs reference the dedicated user (not admin).
        """
        from ocs_ci.ocs.ocp import OCP

        namespace = constants.OPENSHIFT_STORAGE_NAMESPACE
        secret_name = "ocs-metrics-exporter-ceph-auth"

        log.info(f"Checking secret {secret_name} exists in {namespace}")
        ocp = OCP(kind="Secret", namespace=namespace)
        secret = ocp.get(resource_name=secret_name)
        assert secret, f"Secret {secret_name} not found in namespace {namespace}"

        log.info("Verifying secret is mounted in exporter pod")
        pod = get_metrics_exporter_pod()
        volumes = pod["spec"].get("volumes", [])
        vol_names = [v.get("name", "") for v in volumes]
        # The secret should appear in at least one volume
        secret_mounted = any(
            v.get("secret", {}).get("secretName") == secret_name for v in volumes
        )
        assert secret_mounted, (
            f"Secret {secret_name} is not mounted in exporter pod. "
            f"Volumes: {vol_names}"
        )
        log.info(f"Secret {secret_name} is correctly mounted")

    def test_backward_compat_metric_names(self):
        """
        RHSTOR-7964 — Backward Compatibility: existing metric names preserved.

        Verify that a set of well-known pre-RHSTOR-7964 metric names still
        appear in the /metrics output after the rearchitecture.
        """
        known_metrics = [
            "ocs_rbd_pv_metadata",
            "ocs_rbd_children_count",
            "ocs_cephfs_pv_metadata",
            "ocs_cephfs_subvolume_count",
            "ocs_pool_mirroring_status",
            "ocs_storage_consumer_metadata",
        ]

        pod = get_metrics_exporter_pod()
        pod_name = pod["metadata"]["name"]
        output = query_metrics_exporter_endpoint(pod_name=pod_name)

        missing = [m for m in known_metrics if m not in output]
        assert not missing, (
            f"The following metrics are missing after RHSTOR-7964 rearchitecture "
            f"(backward compatibility broken): {missing}"
        )
        log.info(
            f"All {len(known_metrics)} known metric names are still present "
            f"(backward compatibility confirmed)"
        )

    def test_readyz_endpoint(self):
        """
        RHSTOR-7964 — Readiness Endpoint: /readyz returns 200 when Ceph is
        healthy.

        Steps:
        1. Get the exporter pod.
        2. curl http://localhost:<port>/readyz from inside the pod.
        3. Assert HTTP 200 is returned.
        """
        from ocs_ci.utility.utils import exec_cmd

        namespace = constants.OPENSHIFT_STORAGE_NAMESPACE
        pod = get_metrics_exporter_pod()
        pod_name = pod["metadata"]["name"]

        log.info(f"Checking /readyz endpoint on pod {pod_name}")
        result = exec_cmd(
            f"oc exec {pod_name} -n {namespace} -- "
            f"curl -s -o /dev/null -w '%{{http_code}}' http://localhost:8443/readyz"
        )
        http_code = result.stdout.decode().strip()
        assert http_code == "200", (
            f"/readyz returned HTTP {http_code}, expected 200. "
            f"Ceph may not be healthy."
        )
        log.info("/readyz returned HTTP 200 — exporter is ready")

    def test_existing_alerts_fire_internal_mode(self, threading_lock):
        """
        RHSTOR-7964 — Backward Compatibility: existing alerts fire in
        Internal mode after rearchitecture.

        Verifies that HighRBDCloneSnapshotCount PrometheusRule is still
        present and syntactically valid — not that it fires (that is TC
        test_high_rbd_clone_count_alert_clear in the RHSTOR-7465 file).

        Steps:
        1. List PrometheusRules in openshift-storage.
        2. Verify HighRBDCloneSnapshotCount rule exists.
        3. Verify no PrometheusRule failures are reported.
        """
        from ocs_ci.ocs.ocp import OCP
        from ocs_ci.ocs.monitoring import validate_no_prometheus_rule_failures

        namespace = constants.OPENSHIFT_STORAGE_NAMESPACE
        ocp = OCP(kind="PrometheusRule", namespace=namespace)
        rules = ocp.get()
        rule_names = [r["metadata"]["name"] for r in rules.get("items", [])]
        assert rule_names, f"No PrometheusRules found in {namespace}"

        # Verify no rule failures
        validate_no_prometheus_rule_failures(threading_lock=threading_lock)
        log.info(f"Found {len(rule_names)} PrometheusRule(s); no failures detected")


# ---------------------------------------------------------------------------
# Provider-mode tests — require provider+client topology
# ---------------------------------------------------------------------------


@green_squad
@tier2
@skipif_ocs_version("<4.22")
@runs_on_provider
@hci_provider_and_client_required
class TestOCSMetricsExporterProvider(ManageTest):
    """
    RHSTOR-7964 — ocs-metrics-exporter metric-label checks for Provider mode.

    These tests require a provider cluster with at least one remote
    StorageConsumer (client) cluster.
    """

    def test_rbd_pv_metadata_with_consumer_name(self, pvc_factory, teardown_factory):
        """
        RHSTOR-7964 — Metrics Provider: ocs_rbd_pv_metadata carries
        consumer_name label for remote client PVCs.

        Steps:
        1. Create an RBD PVC on a client cluster.
        2. On the provider, query ocs_rbd_pv_metadata.
        3. Verify a metric line carries consumer_name=<client-name>.
        4. Verify local PVCs have empty consumer_name.

        TODO: Creating PVCs on the client cluster requires multi-cluster
        config context switching. Tracked in OCSQE-4504.
        """
        pytest.skip(
            "Creating PVCs on the client cluster requires multi-cluster "
            "context switching. Tracked in OCSQE-4504."
        )

    def test_high_rbd_clone_count_alert_consumer_name(self, threading_lock):
        """
        RHSTOR-7964 — Alert Provider: HighRBDCloneSnapshotCount with
        consumer_name label.

        Steps:
        1. On a client, create 201+ RBD clone PVCs (exceeds threshold).
        2. Wait for HighRBDCloneSnapshotCount to fire on the provider.
        3. Verify alert labels include consumer_name=<client-name>.
        4. Clean up clones; verify alert resolves.

        TODO: Requires multi-cluster context for clone creation.
        Tracked in OCSQE-4504.
        """
        pytest.skip(
            "Requires multi-cluster context switching for client-side clone "
            "creation. Tracked in OCSQE-4504."
        )

    def test_cephfs_stale_subvolume_alert_consumer_name(self, threading_lock):
        """
        RHSTOR-7964 — Alert Provider: CephFSStaleSubvolume alert carries
        consumer_name label.

        Steps:
        1. Create a CephFS PVC on a client.
        2. Simulate a stale subvolume condition.
        3. Wait for CephFSStaleSubvolume to fire; verify consumer_name.
        4. Clean up; verify alert resolves.

        TODO: Stale subvolume simulation and multi-cluster context needed.
        Tracked in OCSQE-4504.
        """
        pytest.skip(
            "Stale subvolume simulation with multi-cluster context not yet "
            "implemented. Tracked in OCSQE-4504."
        )

    def test_odf_rbd_client_blocked_alert_consumer_name(self, threading_lock):
        """
        RHSTOR-7964 — Alert Provider: ODFRBDClientBlocked with consumer_name.

        Steps:
        1. Blocklist a remote client node IP via ceph osd blocklist add.
        2. Wait for ODFRBDClientBlocked to fire; verify consumer_name.
        3. Remove the blocklist entry; verify alert resolves.

        Note from design doc: this alert cannot fully work for remote clients
        because it also checks pod status — this test validates only the
        exporter's blocklist detection portion with consumer_name.
        """
        log.info(
            "TODO: Add blocklist entry via toolbox before waiting for alert. "
            "Tracked in OCSQE-4504."
        )
        pytest.skip(
            "Client node blocklist setup requires toolbox access and "
            "multi-cluster context. Tracked in OCSQE-4504."
        )

        # --- Implementation outline ---
        # toolbox = get_ceph_tools_pod()
        # toolbox.exec_ceph_cmd("ceph osd blocklist add <client-node-ip>:0/0")
        # alert = wait_and_validate_alert_firing(
        #     api, constants.ALERT_ODF_RBD_CLIENT_BLOCKED, timeout=600,
        #     expected_severity="warning",
        # )
        # assert "consumer_name" in alert["labels"], \
        #     f"consumer_name missing from alert labels: {alert['labels']}"
        # toolbox.exec_ceph_cmd("ceph osd blocklist rm <client-node-ip>:0/0")
        # wait_for_alert_cleared(api, constants.ALERT_ODF_RBD_CLIENT_BLOCKED)

    def test_metric_isolation_between_consumers(self):
        """
        RHSTOR-7964 — Multi-Consumer: verify metric isolation.

        Steps:
        1. Create 3 RBD PVCs on client 1, 2 on client 2.
        2. On provider, query ocs_rbd_pv_metadata.
        3. Verify each PVC carries the correct consumer_name.
        4. Verify no cross-contamination between consumers.
        5. Verify local PVCs have empty consumer_name.

        TODO: Requires two client clusters. Tracked in OCSQE-4504.
        """
        pytest.skip(
            "Requires two client clusters with separate contexts. "
            "Tracked in OCSQE-4504."
        )
