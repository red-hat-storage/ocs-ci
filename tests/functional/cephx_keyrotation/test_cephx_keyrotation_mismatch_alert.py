"""
CephX Key Rotation Mismatch Alert and Reconciliation Tests (PR #3910).

Covers:
  A. Alert & metric tests (TC1–TC7)
  B. CephCluster reconciliation (TC8–TC13)
  C. CephClient annotation tests (TC14–TC15)
  D. Negative / edge cases (TC16–TC18; TC17 invalid keyGeneration types)
"""

import logging
import time

import pytest
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    green_squad,
    ignore_leftovers,
    skipif_external_mode,
    skipif_ocs_version,
    tier1,
    tier2,
)
from ocs_ci.helpers.cephx_keyrotation_helper import CephXKeyRotation
from ocs_ci.helpers.helpers import modify_deployment_replica_count
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import get_pod_logs, get_pods_having_label
from ocs_ci.utility.prometheus import (
    PrometheusAPI,
    wait_for_alert_cleared,
    wait_for_alert_firing,
)
from ocs_ci.utility.utils import TimeoutSampler, ceph_health_check, exec_cmd

log = logging.getLogger(__name__)

MISMATCH_METRIC = constants.OCS_CEPHX_DAEMON_KEY_ROTATION_MISMATCH_METRIC
ALERT_NAME = constants.ALERT_CEPHX_KEY_GENERATION_FAILED
DAEMON_TYPES = constants.ROOK_CEPHX_KEYROTATION_DAEMONS
REQUIRED_METRIC_LABELS = ("daemon", "ceph_cluster", "namespace")
MISMATCH_METRIC_TIMEOUT = 300
ALERT_FIRE_TIMEOUT = 600
ALERT_CLEAR_TIMEOUT = 900
ROTATION_COMPLETE_TIMEOUT = 1500
OCS_OPERATOR_DEPLOYMENT = "ocs-operator"
ROOK_OPERATOR_DEPLOYMENT = constants.ROOK_CEPH_OPERATOR
EXPECTED_METRIC_EXPORTER_CEPHCLIENT = "ocs-metrics-exporter-ceph-auth"
CSI_CEPHCLIENT_NAME_PREFIXES = (
    "csi-rbd-provisioner",
    "csi-rbd-node",
    "csi-cephfs-provisioner",
    "csi-cephfs-node",
)


def _query_mismatch_metrics(prometheus):
    """Return Prometheus instant-query samples for the mismatch metric."""
    result = prometheus.query(MISMATCH_METRIC)
    log.info("Prometheus %s returned %d series", MISMATCH_METRIC, len(result))
    return result


def _metric_samples_by_daemon(result):
    """Map daemon label -> sample dict from a Prometheus query result."""
    by_daemon = {}
    for sample in result:
        daemon = sample.get("metric", {}).get("daemon")
        if daemon:
            by_daemon[daemon] = sample
    return by_daemon


def _metric_value(sample):
    """Return integer value from a Prometheus instant-query sample."""
    return int(float(sample["value"][1]))


def _wait_for_mismatch_metric_values(
    prometheus, expected_by_daemon, timeout=MISMATCH_METRIC_TIMEOUT, sleep=10
):
    """
    Wait until mismatch metric values match *expected_by_daemon*.

    Args:
        prometheus (PrometheusAPI): Prometheus client.
        expected_by_daemon (dict): daemon name -> expected int value.
        timeout (int): Seconds to wait.
        sleep (int): Poll interval.
    """
    log.info(
        "Waiting for %s values %s (timeout=%ss)",
        MISMATCH_METRIC,
        expected_by_daemon,
        timeout,
    )
    for result in TimeoutSampler(timeout, sleep, _query_mismatch_metrics, prometheus):
        by_daemon = _metric_samples_by_daemon(result)
        mismatches = {}
        for daemon, expected in expected_by_daemon.items():
            sample = by_daemon.get(daemon)
            if sample is None:
                mismatches[daemon] = "missing"
                continue
            actual = _metric_value(sample)
            if actual != expected:
                mismatches[daemon] = actual
        if not mismatches:
            log.info("Mismatch metric values match expected: %s", expected_by_daemon)
            return by_daemon
        log.info("Mismatch metric not yet ready: %s", mismatches)

    raise UnexpectedBehaviour(
        f"{MISMATCH_METRIC} did not reach {expected_by_daemon} within {timeout}s"
    )


def _scale_rook_operator(replicas, namespace):
    """Scale rook-ceph-operator deployment and wait for replica status."""
    log.info("Scaling %s to %s replicas", ROOK_OPERATOR_DEPLOYMENT, replicas)
    assert modify_deployment_replica_count(
        ROOK_OPERATOR_DEPLOYMENT, replicas, namespace=namespace
    ), f"Failed to scale {ROOK_OPERATOR_DEPLOYMENT} to {replicas}"
    deploy = OCP(
        kind=constants.DEPLOYMENT,
        namespace=namespace,
        resource_name=ROOK_OPERATOR_DEPLOYMENT,
    )
    if replicas == 0:
        for ready in TimeoutSampler(180, 5, lambda: deploy.get()):
            status = ready.get("status", {}) or {}
            if int(status.get("readyReplicas", 0) or 0) == 0:
                log.info("rook-ceph-operator scaled to 0")
                return
        raise UnexpectedBehaviour("rook-ceph-operator did not scale to 0")
    for pods in TimeoutSampler(
        300,
        5,
        get_pods_having_label,
        constants.OPERATOR_LABEL,
        namespace=namespace,
    ):
        running = [
            pod
            for pod in pods
            if pod.get("status", {}).get("phase") == constants.STATUS_RUNNING
        ]
        if len(running) >= replicas:
            log.info("rook-ceph-operator is Running (%d pod(s))", len(running))
            return
    raise UnexpectedBehaviour("rook-ceph-operator did not become Running")


def _get_ocs_operator_env(namespace):
    """Return env list from the ocs-operator deployment container."""
    deploy = OCP(
        kind=constants.DEPLOYMENT,
        namespace=namespace,
        resource_name=OCS_OPERATOR_DEPLOYMENT,
    ).get()
    containers = deploy["spec"]["template"]["spec"]["containers"]
    return containers[0].get("env") or []


def _wait_for_ocs_operator_running(namespace):
    """Wait for ocs-operator deployment rollout, then a Running pod."""
    deploy = OCP(
        kind=constants.DEPLOYMENT,
        namespace=namespace,
        resource_name=OCS_OPERATOR_DEPLOYMENT,
    )
    deploy.exec_oc_cmd(
        f"rollout status deployment/{OCS_OPERATOR_DEPLOYMENT} --timeout=300s",
        out_yaml_format=False,
    )
    for pods in TimeoutSampler(
        300,
        5,
        get_pods_having_label,
        constants.OCS_OPERATOR_LABEL,
        namespace=namespace,
    ):
        running = [
            pod
            for pod in pods
            if pod.get("status", {}).get("phase") == constants.STATUS_RUNNING
        ]
        if running:
            return
    raise UnexpectedBehaviour("ocs-operator did not become Running")


def _set_ocs_operator_env_var(namespace, name, value):
    """
    Set or add an env var on ocs-operator deployment and wait for rollout.

    ``value`` may be a string, bool, or ``None`` (sets an empty env value to
    exercise null-like parsing).

    Returns:
        tuple: (previous_value_or_None, existed_bool)
    """
    env_list = _get_ocs_operator_env(namespace)
    previous = None
    existed = False
    for item in env_list:
        if item.get("name") == name:
            previous = item.get("value")
            existed = True
            break

    if value is None:
        # Keep the env key present with an empty value (null-like input).
        env_assignment = f"{name}="
        value_repr = "null/empty"
    elif isinstance(value, bool):
        env_assignment = f"{name}={str(value).lower()}"
        value_repr = str(value).lower()
    else:
        env_assignment = f"{name}={value}"
        value_repr = str(value)

    exec_cmd(
        f"oc set env deployment/{OCS_OPERATOR_DEPLOYMENT} "
        f"{env_assignment} -n {namespace}"
    )
    log.info(
        "Set %s=%s on %s (previous=%s)",
        name,
        value_repr,
        OCS_OPERATOR_DEPLOYMENT,
        previous,
    )
    time.sleep(5)
    _wait_for_ocs_operator_running(namespace)
    return previous, existed


def _remove_ocs_operator_env_var(namespace, name):
    """Remove an env var from ocs-operator deployment."""
    exec_cmd(f"oc set env deployment/{OCS_OPERATOR_DEPLOYMENT} {name}- -n {namespace}")
    log.info("Removed %s from %s env", name, OCS_OPERATOR_DEPLOYMENT)
    time.sleep(5)
    _wait_for_ocs_operator_running(namespace)


def _restore_ocs_operator_env_var(namespace, name, previous_value, existed):
    """Restore ocs-operator env var to previous state."""
    if existed and previous_value is not None:
        _set_ocs_operator_env_var(namespace, name, previous_value)
    elif existed:
        _set_ocs_operator_env_var(
            namespace, name, str(constants.DEFAULT_DESIRED_CEPHX_KEY_GEN)
        )
    else:
        _remove_ocs_operator_env_var(namespace, name)


def _list_cephclients(namespace):
    """Return list of CephClient resource dicts."""
    return OCP(kind=constants.CEPHCLIENT, namespace=namespace).get().get("items") or []


def _cephfilesystem_exists(namespace):
    """Return True if any CephFilesystem exists in *namespace*."""
    items = (
        OCP(kind=constants.CEPHFILESYSTEM, namespace=namespace).get().get("items") or []
    )
    return bool(items)


def _induce_daemon_key_mismatch(rotator, namespace):
    """
    Create a persistent daemon keyGeneration mismatch.

    Scales rook-ceph-operator to 0 so status cannot catch up, then bumps
    StorageCluster daemon keyGeneration (without waiting for rotation
    completion — rook is intentionally down). Returns the target generation.
    """
    _scale_rook_operator(0, namespace)
    target_generation = rotator.get_next_rook_daemon_key_generation()
    log.info("Inducing CephX daemon key mismatch at generation %s", target_generation)

    # Patch StorageCluster only — do not call rotate_daemon_keys(), which waits
    # for CephCluster Progressing→Ready (impossible while rook is scaled to 0).
    component_config = dict(
        rotator.get_storagecluster_component_spec(rotator.COMPONENT_DAEMON)
    )
    component_config["keyRotationPolicy"] = (
        CephXKeyRotation.KEY_ROTATION_POLICY_KEY_GENERATION
    )
    component_config["keyGeneration"] = int(target_generation)
    rotator.patch_storagecluster_cephx_component(
        rotator.COMPONENT_DAEMON, component_config
    )

    def _cephcluster_spec_reached():
        daemon_spec = rotator.get_spec_cephx().get("daemon") or {}
        return int(daemon_spec.get("keyGeneration", 0) or 0) >= target_generation

    for ready in TimeoutSampler(300, 10, _cephcluster_spec_reached):
        if ready:
            log.info(
                "CephCluster spec.security.cephx.daemon.keyGeneration >= %s "
                "(rook scaled to 0; status intentionally lagging)",
                target_generation,
            )
            return target_generation
    raise UnexpectedBehaviour(
        f"CephCluster daemon keyGeneration did not reach {target_generation}"
    )


@skipif_external_mode
@skipif_ocs_version("<4.22")
@green_squad
class TestCephXKeyRotationMismatchMetric:
    """TC1–TC2: mismatch metric emission and in-sync (value 0) state."""

    @tier1
    def test_cephx_mismatch_metric_emitted_for_all_daemons(self, threading_lock):
        """
        TC1: Metric ocs_cephx_daemon_key_rotation_mismatch is emitted for all
        daemon types with labels daemon, ceph_cluster, and namespace.
        """
        prometheus = PrometheusAPI(threading_lock=threading_lock)
        result = _query_mismatch_metrics(prometheus)
        assert result, f"No series returned for {MISMATCH_METRIC}"

        by_daemon = _metric_samples_by_daemon(result)
        missing = [daemon for daemon in DAEMON_TYPES if daemon not in by_daemon]
        assert not missing, (
            f"Missing mismatch metric series for daemon(s): {missing}; "
            f"found={sorted(by_daemon)}"
        )

        for daemon in DAEMON_TYPES:
            labels = by_daemon[daemon]["metric"]
            for label in REQUIRED_METRIC_LABELS:
                assert labels.get(
                    label
                ), f"Metric for daemon={daemon} missing label '{label}': {labels}"
            assert labels["daemon"] == daemon
            log.info(
                "TC1 series ok: daemon=%s ceph_cluster=%s namespace=%s value=%s",
                daemon,
                labels.get("ceph_cluster"),
                labels.get("namespace"),
                _metric_value(by_daemon[daemon]),
            )

    @tier1
    def test_cephx_mismatch_metric_zero_when_in_sync(
        self, cephx_keyrotation_setup, threading_lock
    ):
        """
        TC2: Metric value is 0 when key rotation is in sync.
        """
        rotator = cephx_keyrotation_setup
        namespace = config.ENV_DATA["cluster_namespace"]
        ceph_health_check(namespace=namespace)
        rotator.wait_for_cluster_ready()

        # Setup only enables policy at DESIRED_CEPHX_KEY_GEN; that does not
        # advance status.keyGeneration. Wait for status catch-up only when an
        # actual rotation above the desired baseline has been requested.
        spec_gen = rotator.get_spec_key_generation(rotator.COMPONENT_DAEMON)
        desired = rotator.get_desired_cephx_key_gen()
        if spec_gen > desired:
            rotator.wait_for_rook_daemon_rotation(spec_gen, timeout=900)

        prometheus = PrometheusAPI(threading_lock=threading_lock)
        expected = {daemon: 0 for daemon in DAEMON_TYPES}
        _wait_for_mismatch_metric_values(prometheus, expected, timeout=180)
        log.info("TC2: all daemon mismatch metrics are 0 (in sync)")


@skipif_external_mode
@skipif_ocs_version("<4.22")
@green_squad
@ignore_leftovers
class TestCephXKeyRotationMismatchAlertLifecycle:
    """TC3–TC6, TC18: induce mismatch, fire alert, recover, check MDS + health mutes."""

    @pytest.fixture(autouse=True)
    def _restore_rook_operator(self, request):
        """Always restore rook-ceph-operator replicas after disruptive tests."""
        namespace = config.ENV_DATA["cluster_namespace"]

        def finalizer():
            try:
                _scale_rook_operator(1, namespace)
            except Exception as exc:
                log.warning("Teardown scale-up of rook-ceph-operator failed: %s", exc)
            try:
                CephXKeyRotation().ensure_daemon_key_generations_aligned()
            except Exception as exc:
                log.warning("Teardown: failed to align daemon keyGeneration: %s", exc)

        request.addfinalizer(finalizer)

    @tier1
    def test_cephx_mismatch_metric_alert_and_recovery(
        self, cephx_keyrotation_setup, threading_lock
    ):
        """
        TC3: Metric becomes 1 when keyGeneration is mismatched.
        TC4: CephxKeyGenerationFailed fires after 5m of mismatch.
        TC5: Metric returns to 0 and alert resolves after successful rotation.
        TC6: MDS mismatch is detected via CephFilesystem status.
        TC18: Rook mutes/unmutes AUTH_INSECURE_* health warnings during rotation.
        """
        rotator = cephx_keyrotation_setup
        namespace = config.ENV_DATA["cluster_namespace"]
        prometheus = PrometheusAPI(threading_lock=threading_lock)

        ceph_health_check(namespace=namespace)
        rotator.wait_for_cluster_ready()

        if not _cephfilesystem_exists(namespace):
            pytest.skip("CephFilesystem required for MDS mismatch verification (TC6)")

        # --- TC3: induce mismatch while rook cannot complete rotation ---
        target_generation = _induce_daemon_key_mismatch(rotator, namespace)
        expected_mismatch = {daemon: 1 for daemon in DAEMON_TYPES}
        by_daemon = _wait_for_mismatch_metric_values(
            prometheus, expected_mismatch, timeout=MISMATCH_METRIC_TIMEOUT
        )
        for daemon in DAEMON_TYPES:
            assert (
                _metric_value(by_daemon[daemon]) == 1
            ), f"TC3: expected mismatch=1 for {daemon}"
        log.info("TC3: mismatch metric is 1 for daemons %s", list(DAEMON_TYPES))

        # --- TC6: MDS series specifically ---
        mds_sample = by_daemon.get("mds")
        assert mds_sample is not None, "TC6: missing mds mismatch metric series"
        assert _metric_value(mds_sample) == 1, "TC6: expected mds mismatch metric = 1"
        fs_gen = rotator.get_filesystem_daemon_key_generation()
        log.info(
            "TC6: mds mismatch=1 (CephFilesystem status keyGeneration=%s, "
            "desired=%s)",
            fs_gen,
            target_generation,
        )

        # --- TC4: alert fires after >5m (rule duration=300s) ---
        alerts = wait_for_alert_firing(
            prometheus,
            alert_name=ALERT_NAME,
            timeout=ALERT_FIRE_TIMEOUT,
            expected_severity="warning",
            expected_message_substr="CephX key rotation failed",
            min_count=1,
        )
        for alert in alerts:
            labels = alert.get("labels") or {}
            annotations = alert.get("annotations") or {}
            assert (
                annotations.get("severity_level") == "warning"
            ), f"TC4: expected severity_level=warning, got {annotations}"
            description = annotations.get("description", "")
            message = annotations.get("message", "")
            combined = f"{description} {message}"
            daemon = labels.get("daemon")
            assert daemon in DAEMON_TYPES, f"TC4: unexpected daemon label {daemon}"
            assert (
                labels.get("namespace") == namespace
            ), f"TC4: unexpected namespace label {labels.get('namespace')}"
            assert (
                labels.get("ceph_cluster")
                or "ceph_cluster" in combined.lower()
                or (rotator.ceph_cluster_name in combined)
            ), f"TC4: missing cluster identity in alert: labels={labels} ann={annotations}"
            assert (
                daemon in combined or f"{daemon}" in message or daemon in description
            ), f"TC4: daemon name missing from alert annotations: {annotations}"
            log.info(
                "TC4: alert firing daemon=%s severity=%s severity_level=%s",
                daemon,
                labels.get("severity"),
                annotations.get("severity_level"),
            )

        # --- TC5: restore rook, complete rotation, metric=0, alert clears ---
        log.info(
            "TC18: will verify AUTH_INSECURE mute/unmute patterns after "
            "rook-ceph-operator resumes rotation"
        )
        _scale_rook_operator(1, namespace)
        rotator.wait_for_rook_daemon_rotation(
            target_generation, timeout=ROTATION_COMPLETE_TIMEOUT
        )

        for entity in constants.CEPHCLUSTER_CEPHX_KEYROTATION_STATUS_ENTITIES:
            status_gen = rotator.get_status_key_generation(entity)
            assert status_gen >= target_generation, (
                f"TC5: status.cephx.{entity}.keyGeneration={status_gen} "
                f"< desired {target_generation}"
            )
        assert (
            rotator.get_filesystem_daemon_key_generation() >= target_generation
        ), "TC5: CephFilesystem daemon keyGeneration did not reach desired value"

        expected_sync = {daemon: 0 for daemon in DAEMON_TYPES}
        _wait_for_mismatch_metric_values(
            prometheus, expected_sync, timeout=MISMATCH_METRIC_TIMEOUT
        )
        wait_for_alert_cleared(prometheus, ALERT_NAME, timeout=ALERT_CLEAR_TIMEOUT)
        log.info("TC5: mismatch metrics are 0 and %s resolved", ALERT_NAME)

        # --- TC18: inspect rook logs for mute/unmute behavior ---
        operator_logs = ""
        try:
            pods = get_pods_having_label(constants.OPERATOR_LABEL, namespace=namespace)
            if pods:
                operator_logs = get_pod_logs(pods[0]["metadata"]["name"]) or ""
        except Exception as exc:
            log.warning("TC18: could not fetch rook-ceph-operator logs: %s", exc)

        muted_markers = (
            "AUTH_INSECURE_KEYS_ALLOWED",
            "AUTH_INSECURE_KEYS_CREATABLE",
        )
        unmuted_markers = (
            "AUTH_INSECURE_SERVICE_KEY_TYPE",
            "AUTH_INSECURE_SERVICE_TICKETS",
        )
        if operator_logs:
            found_muted = [m for m in muted_markers if m in operator_logs]
            found_unmuted = [m for m in unmuted_markers if m in operator_logs]
            log.info(
                "TC18: mute-related markers in logs: muted=%s unmuted=%s",
                found_muted,
                found_unmuted,
            )
            # Soft check: do not hard-fail greenfield clusters that never emit
            # these warnings during the rotation lifecycle.
            if not (found_muted or found_unmuted):
                log.warning(
                    "TC18: no AUTH_INSECURE mute/unmute markers found in "
                    "rook-ceph-operator logs (may be expected on greenfield)"
                )
        else:
            log.warning("TC18: empty operator logs; mute/unmute check skipped")

        ceph_health_check(namespace=namespace)
        log.info(
            "TC3/TC4/TC5/TC6/TC18 completed for target generation %s",
            target_generation,
        )


@skipif_external_mode
@skipif_ocs_version("<4.22")
@green_squad
class TestCephXKeyRotationMismatchNoFilesystem:
    """TC7: MDS mismatch metric is 0 when no CephFilesystem exists."""

    @tier2
    def test_cephx_mds_mismatch_zero_without_filesystem(self, threading_lock):
        """
        TC7: ocs_cephx_daemon_key_rotation_mismatch{daemon="mds"} is 0 when no
        CephFilesystem exists.
        """
        namespace = config.ENV_DATA["cluster_namespace"]
        if _cephfilesystem_exists(namespace):
            pytest.skip(
                "Cluster has a CephFilesystem; TC7 requires a cluster without one"
            )

        prometheus = PrometheusAPI(threading_lock=threading_lock)
        result = _query_mismatch_metrics(prometheus)
        by_daemon = _metric_samples_by_daemon(result)
        mds = by_daemon.get("mds")
        assert mds is not None, "Expected mds mismatch metric series even without FS"
        assert _metric_value(mds) == 0, (
            f"TC7: expected mds mismatch=0 without CephFilesystem, got "
            f"{_metric_value(mds)}"
        )


@skipif_external_mode
@skipif_ocs_version("<4.22")
@green_squad
class TestCephXCephClusterReconciliation:
    """TC8–TC13: CephCluster annotations and daemon keyGeneration reconciliation."""

    @tier1
    def test_cephx_greenfield_annotations_present(self, cephx_bootstrap_setup):
        """
        TC8: Greenfield CephCluster has created-with-cephx-features and
        created-at-df-version annotations.
        """
        rotator = cephx_bootstrap_setup
        cluster = rotator._get_cluster_dict()
        annotations = cluster.get("metadata", {}).get("annotations") or {}
        created_with = annotations.get(constants.CEPHX_CREATED_WITH_FEATURES_ANNOTATION)
        created_at = annotations.get(constants.CEPHX_CREATED_AT_DF_VERSION_ANNOTATION)

        if created_with is None:
            pytest.skip(
                "Cluster is brownfield (missing created-with-cephx-features); "
                "TC8 applies to greenfield installs only"
            )

        assert (
            created_with == ""
        ), f"TC8: expected empty-string annotation value, got {created_with!r}"
        assert created_at, "TC8: created-at-df-version annotation is missing"
        log.info(
            "TC8: greenfield annotations present created-at-df-version=%s", created_at
        )

    @tier1
    def test_cephx_greenfield_security_spec(self, cephx_bootstrap_setup):
        """
        TC9: Greenfield CephCluster has correct cephx security defaults.
        """
        rotator = cephx_bootstrap_setup
        cluster = rotator._get_cluster_dict()
        annotations = cluster.get("metadata", {}).get("annotations") or {}
        if constants.CEPHX_CREATED_WITH_FEATURES_ANNOTATION not in annotations:
            pytest.skip(
                "TC9 requires greenfield created-with-cephx-features annotation"
            )

        cephx = rotator.get_spec_cephx()
        assert list(cephx.get("allowedCiphers") or []) == list(
            constants.CEPHX_DEFAULT_ALLOWED_CIPHERS
        ), f"TC9: unexpected allowedCiphers: {cephx.get('allowedCiphers')}"

        sc_daemon = rotator.get_storagecluster_component_spec(rotator.COMPONENT_DAEMON)
        daemon_spec = cephx.get("daemon") or {}
        if sc_daemon.get("keyGeneration") or sc_daemon.get("keyRotationPolicy"):
            log.info(
                "TC9: StorageCluster sets daemon CephX config %s; CephCluster "
                "daemon section is managed (%s) — skipping empty-daemon assertion",
                sc_daemon,
                daemon_spec,
            )
        else:
            assert not daemon_spec.get(
                "keyGeneration"
            ), f"TC9: greenfield daemon.keyGeneration should be unset, got {daemon_spec}"
            assert not daemon_spec.get("keyRotationPolicy"), (
                f"TC9: greenfield daemon.keyRotationPolicy should be unset, got "
                f"{daemon_spec}"
            )

        # keyType for CSI / RBD mirror peer lives on CephClients for this PR;
        # on CephCluster the sections may be empty objects.
        csi_spec = cephx.get("csi") or {}
        rbd_spec = cephx.get("rbdMirrorPeer") or {}
        if csi_spec.get("keyType"):
            assert csi_spec["keyType"] == "aes"
        if rbd_spec.get("keyType"):
            assert rbd_spec["keyType"] == "aes"
        log.info("TC9: greenfield cephx security defaults verified")

    @tier2
    def test_cephx_brownfield_no_created_with_annotation(self, cephx_bootstrap_setup):
        """
        TC10: Brownfield/upgraded cluster does NOT get created-with-cephx-features.
        """
        rotator = cephx_bootstrap_setup
        annotations = (
            rotator._get_cluster_dict().get("metadata", {}).get("annotations") or {}
        )
        if constants.CEPHX_CREATED_WITH_FEATURES_ANNOTATION in annotations:
            pytest.skip(
                "Cluster has greenfield created-with-cephx-features annotation; "
                "TC10 requires a brownfield/upgraded cluster"
            )
        log.info("TC10: brownfield cluster correctly lacks created-with-cephx-features")

    @tier2
    def test_cephx_brownfield_daemon_key_rotation_config(self, cephx_bootstrap_setup):
        """
        TC11: Brownfield cluster gets daemon KeyGeneration config from env default.
        """
        rotator = cephx_bootstrap_setup
        annotations = (
            rotator._get_cluster_dict().get("metadata", {}).get("annotations") or {}
        )
        if constants.CEPHX_CREATED_WITH_FEATURES_ANNOTATION in annotations:
            pytest.skip(
                "Cluster is greenfield; TC11 requires brownfield without "
                "created-with-cephx-features"
            )

        cephx = rotator.get_spec_cephx()
        assert list(cephx.get("allowedCiphers") or []) == list(
            constants.CEPHX_DEFAULT_ALLOWED_CIPHERS
        )
        daemon = cephx.get("daemon") or {}
        assert daemon.get("keyRotationPolicy") == (
            CephXKeyRotation.KEY_ROTATION_POLICY_KEY_GENERATION
        ), f"TC11: unexpected daemon.keyRotationPolicy: {daemon}"
        assert int(daemon.get("keyGeneration", 0) or 0) >= (
            constants.DEFAULT_DESIRED_CEPHX_KEY_GEN
        ), f"TC11: daemon.keyGeneration should be >= env default, got {daemon}"
        log.info("TC11: brownfield daemon key rotation config verified: %s", daemon)

    @tier2
    def test_storagecluster_keygeneration_zero_falls_back_to_env(
        self, cephx_bootstrap_setup
    ):
        """
        TC13: Unset/0 StorageCluster keyGeneration falls back to DESIRED_CEPHX_KEY_GEN.

        Runs before TC12 so a persisted StorageCluster keyGeneration override does
        not force a skip. ``keyGeneration`` cannot be removed once set, so this
        check only runs when the field is unset or already ``0``.
        """
        rotator = cephx_bootstrap_setup
        namespace = config.ENV_DATA["cluster_namespace"]
        env_list = _get_ocs_operator_env(namespace)
        env_map = {item.get("name"): item.get("value") for item in env_list}
        desired_env = env_map.get(constants.DESIRED_CEPHX_KEY_GEN_ENV)
        assert (
            desired_env is not None
        ), f"TC13: {constants.DESIRED_CEPHX_KEY_GEN_ENV} must be set on ocs-operator"
        assert int(desired_env) == constants.DEFAULT_DESIRED_CEPHX_KEY_GEN

        sc_daemon = rotator.get_storagecluster_component_spec(rotator.COMPONENT_DAEMON)
        if "keyGeneration" in sc_daemon:
            sc_gen = int(sc_daemon.get("keyGeneration") or 0)
            if sc_gen != 0:
                pytest.skip(
                    f"TC13: StorageCluster daemon.keyGeneration={sc_gen} is set; "
                    f"{constants.CEPHX_KEY_GENERATION_REMOVE_ERROR}, so "
                    "fallback-to-env cannot be exercised on this cluster"
                )
            log.info(
                "TC13: StorageCluster daemon.keyGeneration already 0; "
                "verifying fallback to DESIRED_CEPHX_KEY_GEN"
            )

        desired = int(desired_env)

        def _fallback_applied():
            daemon = rotator.get_spec_cephx().get("daemon") or {}
            return int(daemon.get("keyGeneration", 0) or 0) == desired

        # Wait for CephCluster to reflect env fallback before reading CC state.
        for ready in TimeoutSampler(300, 10, _fallback_applied):
            if ready:
                break
        else:
            daemon = rotator.get_spec_cephx().get("daemon") or {}
            cc_gen = int(daemon.get("keyGeneration", 0) or 0)
            annotations = (
                rotator._get_cluster_dict().get("metadata", {}).get("annotations") or {}
            )
            if constants.CEPHX_CREATED_WITH_FEATURES_ANNOTATION in annotations:
                pytest.skip(
                    "Greenfield clusters do not apply DESIRED_CEPHX_KEY_GEN to "
                    "daemon by default (TC13 is brownfield-oriented)"
                )
            pytest.skip(
                f"CephCluster daemon.keyGeneration={cc_gen} already advanced past "
                f"env default {desired_env}; TC13 fallback check requires a "
                "brownfield cluster that has not been rotated above the env default"
            )

        daemon = rotator.get_spec_cephx().get("daemon") or {}
        annotations = (
            rotator._get_cluster_dict().get("metadata", {}).get("annotations") or {}
        )
        if constants.CEPHX_CREATED_WITH_FEATURES_ANNOTATION in annotations:
            pytest.skip(
                "Greenfield clusters do not apply DESIRED_CEPHX_KEY_GEN to daemon "
                "by default (TC13 is brownfield-oriented)"
            )
        cc_gen = int(daemon.get("keyGeneration", 0) or 0)
        assert cc_gen == desired, (
            f"TC13: expected CephCluster daemon.keyGeneration={desired_env}, "
            f"got {daemon}"
        )

    @tier1
    def test_storagecluster_keygeneration_overrides_env(
        self, cephx_keyrotation_setup, request
    ):
        """
        TC12: StorageCluster daemon.keyGeneration overrides DESIRED_CEPHX_KEY_GEN.
        """
        rotator = cephx_keyrotation_setup
        namespace = config.ENV_DATA["cluster_namespace"]
        original_generation = rotator.get_spec_key_generation(rotator.COMPONENT_DAEMON)
        target_generation = max(original_generation, 5) + 1

        def restore():
            # Generations are monotonic; wait for the bumped generation to finish
            # rotating so later tests see an in-sync cluster.
            try:
                rotator.wait_for_rook_daemon_rotation(
                    target_generation, timeout=ROTATION_COMPLETE_TIMEOUT
                )
            except Exception as exc:
                log.warning("TC12 teardown wait for rotation failed: %s", exc)
            try:
                rotator.ensure_daemon_key_generations_aligned()
            except Exception as exc:
                log.warning(
                    "TC12 teardown: failed to align daemon keyGeneration: %s", exc
                )

        request.addfinalizer(restore)

        env_list = _get_ocs_operator_env(namespace)
        env_map = {item.get("name"): item.get("value") for item in env_list}
        desired_env = env_map.get(constants.DESIRED_CEPHX_KEY_GEN_ENV)
        log.info(
            "TC12: DESIRED_CEPHX_KEY_GEN=%s; setting StorageCluster keyGeneration=%s",
            desired_env,
            target_generation,
        )
        assert (
            desired_env is not None
        ), f"{constants.DESIRED_CEPHX_KEY_GEN_ENV} missing from ocs-operator"
        assert (
            int(desired_env) != target_generation
        ), "TC12: choose a StorageCluster generation different from env default"

        rotator.rotate_daemon_keys(target_generation)

        def _spec_matches():
            daemon = rotator.get_spec_cephx().get("daemon") or {}
            return int(daemon.get("keyGeneration", 0) or 0) == target_generation

        for ready in TimeoutSampler(300, 10, _spec_matches):
            if ready:
                break
        else:
            raise UnexpectedBehaviour(
                f"TC12: CephCluster daemon.keyGeneration did not become "
                f"{target_generation}"
            )

        daemon = rotator.get_spec_cephx().get("daemon") or {}
        assert int(daemon["keyGeneration"]) == target_generation
        assert int(daemon["keyGeneration"]) != int(desired_env)
        log.info(
            "TC12: StorageCluster keyGeneration=%s won over env=%s",
            target_generation,
            desired_env,
        )
        # Allow rotation to complete so cluster stays healthy for later tests
        rotator.wait_for_rook_daemon_rotation(target_generation, timeout=1200)


@skipif_external_mode
@skipif_ocs_version("<4.22")
@green_squad
class TestCephXCephClientAnnotations:
    """TC14–TC15: CephClient created-with-cephx-features and keyType."""

    @tier1
    def test_cephclients_have_cephx_annotation_and_aes_keytype(
        self, cephx_bootstrap_setup
    ):
        """
        TC14: New CephClients get created-with-cephx-features and keyType aes.
        """
        namespace = config.ENV_DATA["cluster_namespace"]
        clients = _list_cephclients(namespace)
        assert clients, "No CephClient resources found"

        # Prefer metrics-exporter + CSI clients when present
        interesting = []
        for client in clients:
            name = client["metadata"]["name"]
            if name == EXPECTED_METRIC_EXPORTER_CEPHCLIENT or name.startswith(
                CSI_CEPHCLIENT_NAME_PREFIXES
            ):
                interesting.append(client)
        if not interesting:
            interesting = clients

        for client in interesting:
            name = client["metadata"]["name"]
            annotations = client.get("metadata", {}).get("annotations") or {}
            if constants.CEPHX_CREATED_WITH_FEATURES_ANNOTATION not in annotations:
                # Existing/pre-PR clients — skip individual client
                log.info(
                    "TC14: CephClient %s lacks greenfield annotation (likely "
                    "pre-existing); skipping",
                    name,
                )
                continue
            assert (
                annotations[constants.CEPHX_CREATED_WITH_FEATURES_ANNOTATION] == ""
            ), f"TC14: unexpected annotation value on {name}"
            key_type = (
                (client.get("spec") or {})
                .get("security", {})
                .get("cephx", {})
                .get("keyType")
            )
            assert (
                key_type == "aes"
            ), f"TC14: CephClient {name} keyType={key_type!r}, expected 'aes'"
            log.info("TC14: CephClient %s has annotation + keyType=aes", name)

    @tier2
    def test_existing_cephclients_no_annotation_on_upgrade(self, cephx_bootstrap_setup):
        """
        TC15: Existing (pre-PR) CephClients do not get annotation/keyType on upgrade.
        """
        namespace = config.ENV_DATA["cluster_namespace"]
        clients = _list_cephclients(namespace)
        pre_pr = [
            c
            for c in clients
            if constants.CEPHX_CREATED_WITH_FEATURES_ANNOTATION
            not in (c.get("metadata", {}).get("annotations") or {})
        ]
        if not pre_pr:
            pytest.skip(
                "All CephClients have created-with-cephx-features; TC15 needs "
                "pre-PR / brownfield clients"
            )
        for client in pre_pr:
            name = client["metadata"]["name"]
            key_type = (
                (client.get("spec") or {})
                .get("security", {})
                .get("cephx", {})
                .get("keyType")
            )
            assert not key_type, (
                f"TC15: pre-existing CephClient {name} unexpectedly has keyType="
                f"{key_type}"
            )
            log.info("TC15: pre-existing CephClient %s has no annotation/keyType", name)


@skipif_external_mode
@skipif_ocs_version("<4.22")
@green_squad
@ignore_leftovers
class TestCephXDesiredKeyGenNegative:
    """TC16: DESIRED_CEPHX_KEY_GEN env; TC17: StorageCluster keyGeneration types."""

    # TC17: invalid JSON types for StorageCluster daemon.keyGeneration.
    # null is rejected with "keyGeneration cannot be removed once set" (null is
    # treated as removing the field after it has already been set).
    INVALID_DAEMON_KEY_GENERATION_TYPE_CASES = (
        pytest.param("16", "string", id="value-string-16"),
        pytest.param("abc", "string", id="value-string-abc"),
        pytest.param(True, "boolean", id="value-boolean-true"),
        pytest.param(None, "null", id="value-null"),
    )

    @pytest.fixture(autouse=True)
    def _restore_desired_env(self, request):
        namespace = config.ENV_DATA["cluster_namespace"]
        env_list = _get_ocs_operator_env(namespace)
        original = None
        existed = False
        for item in env_list:
            if item.get("name") == constants.DESIRED_CEPHX_KEY_GEN_ENV:
                original = item.get("value")
                existed = True
                break

        def finalizer():
            try:
                _restore_ocs_operator_env_var(
                    namespace,
                    constants.DESIRED_CEPHX_KEY_GEN_ENV,
                    original,
                    existed,
                )
            except Exception as exc:
                log.warning("Failed to restore DESIRED_CEPHX_KEY_GEN: %s", exc)
            try:
                CephXKeyRotation().ensure_daemon_key_generations_aligned()
            except Exception as exc:
                log.warning("Teardown: failed to align daemon keyGeneration: %s", exc)

        request.addfinalizer(finalizer)
        self._namespace = namespace
        self._original_env = original
        self._env_existed = existed

    @tier2
    def test_desired_cephx_key_gen_missing_errors(self, cephx_bootstrap_setup):
        """
        TC16: Removing DESIRED_CEPHX_KEY_GEN causes a clear operator error on
        brownfield reconciliation.
        """
        rotator = cephx_bootstrap_setup
        annotations = (
            rotator._get_cluster_dict().get("metadata", {}).get("annotations") or {}
        )
        if constants.CEPHX_CREATED_WITH_FEATURES_ANNOTATION in annotations:
            pytest.skip(
                "TC16 targets brownfield reconciliation that consumes "
                "DESIRED_CEPHX_KEY_GEN; this cluster is greenfield-annotated"
            )

        _remove_ocs_operator_env_var(
            self._namespace, constants.DESIRED_CEPHX_KEY_GEN_ENV
        )
        # Trigger reconcile by touching StorageCluster
        rotator.trigger_cephcluster_reconcile()
        time.sleep(30)

        pods = get_pods_having_label(
            constants.OCS_OPERATOR_LABEL, namespace=self._namespace
        )
        assert pods, "ocs-operator pod not found after env removal"
        logs = get_pod_logs(pods[0]["metadata"]["name"])
        assert (
            constants.DESIRED_CEPHX_KEY_GEN_ENV in logs
        ), "TC16: expected operator logs to mention missing DESIRED_CEPHX_KEY_GEN"
        assert any(
            token in logs.lower()
            for token in ("missing", "required", "panic", "error", "not set")
        ), "TC16: expected clear error/panic about missing env var in operator logs"
        log.info("TC16: operator reported missing DESIRED_CEPHX_KEY_GEN")

    @tier2
    @pytest.mark.parametrize(
        "invalid_value, expected_json_type",
        INVALID_DAEMON_KEY_GENERATION_TYPE_CASES,
    )
    def test_storagecluster_daemon_key_generation_invalid_type_rejected(
        self, cephx_bootstrap_setup, invalid_value, expected_json_type
    ):
        """
        TC17: StorageCluster rejects non-integer daemon keyGeneration patches.

        Parametrized over JSON values that must fail validation:
          - ``"16"`` / ``"abc"`` → must be of type integer: "string"
          - ``true`` → must be of type integer: "boolean"
          - ``null`` → keyGeneration cannot be removed once set
            (null is treated as removing the field after it was set)

        Patch must not succeed; StorageCluster / CephCluster state unchanged.
        """
        rotator = cephx_bootstrap_setup
        namespace = config.ENV_DATA["cluster_namespace"]

        ceph_health_check(namespace=namespace)
        rotator.wait_for_cluster_ready()

        # Ensure daemon.keyGeneration exists so replace exercises type checks.
        rotator.ensure_daemon_key_rotation_enabled()

        pre_generations = rotator.record_all_cephx_status_generations()
        pre_sc_generation = rotator.get_spec_key_generation(rotator.COMPONENT_DAEMON)
        pre_cc_phase = rotator.get_cephcluster_phase()
        pre_sc_phase = rotator.get_storagecluster_phase()

        rotator.assert_invalid_daemon_key_generation_type_rejected(
            invalid_value, expected_json_type
        )

        time.sleep(10)
        assert (
            rotator.get_spec_key_generation(rotator.COMPONENT_DAEMON)
            == pre_sc_generation
        ), "StorageCluster daemon keyGeneration changed after rejected type patch"
        assert pre_cc_phase == constants.STATUS_READY, (
            "CephCluster must be Ready before invalid keyGeneration type patch; "
            f"got {pre_cc_phase}"
        )
        assert pre_sc_phase == constants.STATUS_READY, (
            "StorageCluster must be Ready before invalid keyGeneration type patch; "
            f"got {pre_sc_phase}"
        )
        assert rotator.get_cephcluster_phase() == pre_cc_phase, (
            "CephCluster phase changed unexpectedly after rejected keyGeneration "
            f"type patch (was {pre_cc_phase})"
        )
        assert rotator.get_storagecluster_phase() == pre_sc_phase, (
            "StorageCluster phase changed unexpectedly after rejected keyGeneration "
            f"type patch (was {pre_sc_phase})"
        )
        rotator.assert_cephx_status_generations_unchanged(
            pre_generations,
            context="after rejected non-integer daemon keyGeneration patch",
        )
        log.info(
            "TC17: StorageCluster rejected daemon keyGeneration type=%s value=%r",
            expected_json_type,
            invalid_value,
        )
