"""
Tests for PVC volume health annotation feature (RHSTOR-7596).
"""

import json
import logging
import time
from datetime import datetime

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    green_squad,
    skipif_ocs_version,
    skipif_mcg_only,
    skipif_managed_service,
    skipif_rosa_hcp,
    skipif_external_mode,
)
from ocs_ci.framework.testlib import ManageTest, tier1, tier2
from ocs_ci.framework import config
from ocs_ci.helpers.helpers import (
    assert_pvc_volume_health_event,
    blocklist_cephfs_client,
    remove_cephfs_client_blocklist,
    modify_deployment_replica_count,
)
from ocs_ci.ocs.resources.events import count_pvc_volume_health_events
from ocs_ci.ocs import constants, ocp, node
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.resources.csi_addons import (
    get_csi_addon_pod_on_node,
)
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.utility.utils import ceph_health_check, TimeoutSampler

logger = logging.getLogger(__name__)

ANNOTATION_POLL_TIMEOUT = 180
ANNOTATION_POLL_INTERVAL = 15
REPORTER_TICK_WAIT = 60
UNHEALTHY_POLL_TIMEOUT = 300
RECOVERY_POLL_TIMEOUT = 300


@tier1
@green_squad
@skipif_mcg_only
@skipif_external_mode
@skipif_ocs_version("<4.23")
@pytest.mark.parametrize(
    argnames=["interface"],
    argvalues=[
        pytest.param(
            constants.CEPHFILESYSTEM,
            marks=pytest.mark.polarion_id("OCS-8219"),
        ),
        pytest.param(
            constants.CEPHBLOCKPOOL,
            marks=pytest.mark.polarion_id("OCS-8220"),
        ),
    ],
)
class TestPVCVolumeHealthAnnotation(ManageTest):
    """
    Test PVC volume health annotations written by CSI-Addons
    sidecars for both CephFS and RBD storage interfaces.
    """

    def test_pvc_healthy_annotation(self, interface, pvc_factory, pod_factory):
        """
        Verify healthy volumehealth annotations on a PVC.

        CephFS: RWX PVC with 2 pods on different nodes.
        RBD: RWO PVC (Filesystem volumeMode) with 1 pod.

        Steps:
            1. Verify Ceph health is HEALTH_OK.
            2. Create PVC (5Gi), wait for Bound.
            3. Create pod(s) mounting the PVC.
            4. Run FIO I/O (fs, 512M) on all pods.
            5. Wait for reporter tick (~60s).
            6. Poll and assert volumehealth annotation count.
            7. Validate JSON and node UID match.
            8. Assert VolumeConditionHealthy K8s event.
            9. Verify PV name in csi-addons sidecar logs.
        """
        ns = config.ENV_DATA["cluster_namespace"]

        if interface == constants.CEPHFILESYSTEM:
            access_mode = constants.ACCESS_MODE_RWX
            expected_count = 2
            driver = "cephfs"
        else:
            access_mode = constants.ACCESS_MODE_RWO
            expected_count = 1
            driver = "rbd"
        logger.test_step("Verify Ceph health is HEALTH_OK")
        ceph_health_check(tries=3, delay=10)
        logger.test_step(f"Create {driver.upper()} PVC ({access_mode}, 5Gi)")
        pvc_obj = pvc_factory(
            interface=interface,
            size=5,
            access_mode=access_mode,
        )
        logger.info(f"PVC {pvc_obj.name} created and Bound")
        pod_objs = []
        if interface == constants.CEPHFILESYSTEM:
            logger.test_step("Create 2 pods on different nodes")
            worker_nodes = node.get_worker_nodes()
            logger.assertion(
                f"Worker node count: expected >= 2, actual={len(worker_nodes)}"
            )
            assert (
                len(worker_nodes) >= 2
            ), f"Need >= 2 worker nodes, found {len(worker_nodes)}"
            for i in range(2):
                p = pod_factory(
                    pvc=pvc_obj,
                    interface=interface,
                    node_name=worker_nodes[i],
                )
                pod_objs.append(p)
            node1 = pod_objs[0].get()["spec"]["nodeName"]
            node2 = pod_objs[1].get()["spec"]["nodeName"]
            logger.info(
                f"Pod {pod_objs[0].name} on {node1}, "
                f"Pod {pod_objs[1].name} on {node2}"
            )
            logger.assertion(f"Pods on different nodes: {node1} != {node2}")
            assert node1 != node2, f"Both pods on same node: {node1}"
        else:
            logger.test_step("Create 1 pod mounting the PVC")
            p = pod_factory(
                pvc=pvc_obj,
                interface=interface,
            )
            pod_objs.append(p)
            pod_node = p.get()["spec"]["nodeName"]
            logger.info(f"Pod {p.name} on {pod_node}")
        logger.test_step("Run FIO I/O on all pods")
        for p in pod_objs:
            p.run_io(
                storage_type="fs",
                size="512M",
                fio_filename=p.name,
            )
        for p in pod_objs:
            pod.get_fio_rw_iops(p)
        logger.info("FIO I/O completed")
        logger.test_step(f"Wait {REPORTER_TICK_WAIT}s for reporter tick")
        time.sleep(REPORTER_TICK_WAIT)

        logger.test_step(f"Poll for {expected_count} volumehealth annotation(s)")
        health_annotations = pvc_obj.wait_for_volume_health_state(
            expected_state="healthy",
            timeout=ANNOTATION_POLL_TIMEOUT,
            interval=ANNOTATION_POLL_INTERVAL,
            expected_count=expected_count,
        )
        logger.assertion(f"Exactly {expected_count} volumehealth annotation(s) present")
        assert (
            len(health_annotations) == expected_count
        ), f"Expected {expected_count}, found {len(health_annotations)}"
        pod_node_names = [p.get()["spec"]["nodeName"] for p in pod_objs]
        expected_node_uids = set()
        for node_name in pod_node_names:
            node_ocp = ocp.OCP(kind="node", resource_name=node_name)
            uid = node_ocp.get()["metadata"]["uid"]
            expected_node_uids.add(uid)
            expected_key = f"{constants.VOLUME_HEALTH_ANNOTATION_PREFIX}{uid}"
            logger.assertion(f"Annotation key for node {node_name} (uid={uid}) exists")
            assert expected_key in health_annotations, (
                f"Missing key {expected_key}. "
                f"Found: {list(health_annotations.keys())}"
            )
        logger.test_step("Validate annotation JSON content")

        for key, value in health_annotations.items():
            parsed = json.loads(value)
            logger.info(f"Annotation {key}: {parsed}")
            logger.assertion(f"state is 'healthy', got '{parsed.get('state')}'")
            assert (
                parsed.get("state") == "healthy"
            ), f"Expected 'healthy', got '{parsed.get('state')}'"
            last_checked = parsed.get("lastChecked", "")
            logger.assertion(f"lastChecked is valid RFC3339: {last_checked}")
            dt_last = datetime.fromisoformat(last_checked.replace("Z", "+00:00"))
            assert (
                dt_last.tzinfo is not None
            ), f"lastChecked missing timezone: {last_checked}"
            since = parsed.get("since", "")
            logger.assertion(f"since is valid RFC3339: {since}")
            dt_since = datetime.fromisoformat(since.replace("Z", "+00:00"))
            assert dt_since.tzinfo is not None, f"since missing timezone: {since}"
            ann_node = parsed.get("node", "")
            logger.assertion(f"node field present: {ann_node}")
            assert ann_node, "node field is missing or empty"
            key_suffix = key.replace(
                constants.VOLUME_HEALTH_ANNOTATION_PREFIX,
                "",
            )
            logger.assertion(f"Key suffix {key_suffix} matches a pod's node UID")
            assert key_suffix in expected_node_uids, (
                f"Key suffix {key_suffix} not in "
                f"expected UIDs: {expected_node_uids}"
            )
        logger.test_step(f"Check VolumeConditionHealthy events for PVC {pvc_obj.name}")
        health_events = assert_pvc_volume_health_event(
            pvc_obj,
            reason="VolumeConditionHealthy",
            event_type="Normal",
            message_substr="volume is in a healthy condition",
        )
        for evt in health_events:
            event_host = evt.get("source", {}).get("host", "")
            logger.assertion(f"source.host '{event_host}' matches a pod node")
            assert (
                event_host in pod_node_names
            ), f"source.host '{event_host}' not in pod nodes: {pod_node_names}"
        logger.test_step("Verify event in 'oc describe pvc'")
        describe_output = pvc_obj.describe()
        logger.assertion("VolumeConditionHealthy in describe output")
        assert (
            "VolumeConditionHealthy" in describe_output
        ), "VolumeConditionHealthy not in 'oc describe pvc' output"
        logger.test_step("Check csi-addons sidecar logs")
        pvc_obj.reload()
        pv_name = pvc_obj.backed_pv
        logger.assertion(f"PVC {pvc_obj.name} has backed PV: {pv_name}")
        assert pv_name, f"PVC {pvc_obj.name} has no backed PV"
        for node_name in pod_node_names:
            addon_pod = get_csi_addon_pod_on_node(node_name, driver)
            log_output = pod.get_pod_logs(
                pod_name=addon_pod,
                container="csi-addons",
                namespace=ns,
                tail=200,
            )
            logger.assertion(f"PV {pv_name} in csi-addons logs on node {node_name}")
            assert (
                pv_name in log_output
            ), f"PV '{pv_name}' not in logs of {addon_pod} on {node_name}"
        logger.info(f"{driver.upper()} PVC volume health annotation test passed")


@green_squad
@skipif_ocs_version("<4.23")
@skipif_mcg_only
class TestPVCVolumeHealthUnhealthy(ManageTest):
    """
    Test PVC volume health annotation transitions to unhealthy state
    when Ceph connectivity is disrupted, and recovers when restored.
    """

    def _create_pvc_and_pod_with_io(self, pvc_factory, pod_factory):
        """
        Create a CephFS RWO PVC, mount it in a pod, and run FIO I/O.

        Returns:
            tuple: (pvc_obj, pod_obj)
        """
        logger.info("Create CephFS RWO PVC (5Gi)")
        pvc_obj = pvc_factory(
            interface=constants.CEPHFILESYSTEM,
            size=5,
            access_mode=constants.ACCESS_MODE_RWO,
        )
        logger.info(f"PVC {pvc_obj.name} created and Bound")

        logger.info("Create pod mounting the PVC")
        pod_obj = pod_factory(
            pvc=pvc_obj,
            interface=constants.CEPHFILESYSTEM,
        )
        pod_node = pod_obj.get()["spec"]["nodeName"]
        logger.info(f"Pod {pod_obj.name} on {pod_node}")

        logger.info("Run FIO I/O")
        pod_obj.run_io(
            storage_type="fs",
            size="512M",
            fio_filename=pod_obj.name,
        )
        pod.get_fio_rw_iops(pod_obj)
        logger.info("FIO I/O completed")
        return pvc_obj, pod_obj

    def _restart_cephfs_nodeplugin_on_nodes(self, node_names):
        """
        Restart the CephFS RPC nodeplugin pods on the given nodes for unhealthy
        transition to happen quickly.

        Args:
            node_names (list): Node names whose CephFS nodeplugin pods to
                restart.
        """
        plugin_pods = pod.get_plugin_pods(constants.CEPHFILESYSTEM)
        targets = set(node_names)
        restarted = []
        for plugin_pod in plugin_pods:
            plugin_node = plugin_pod.get()["spec"]["nodeName"]
            if plugin_node in targets:
                logger.info(
                    f"Restarting CephFS nodeplugin pod {plugin_pod.name} "
                    f"on node {plugin_node} to force the health-check probe"
                )
                plugin_pod.delete(wait=True)
                restarted.append(plugin_node)
        logger.assertion(
            f"CephFS nodeplugin pods restarted on nodes: "
            f"expected={sorted(targets)}, actual={sorted(restarted)}"
        )
        assert set(restarted) == targets, (
            f"No CephFS nodeplugin pod found on nodes: "
            f"{sorted(targets - set(restarted))}"
        )

    @tier1
    @pytest.mark.polarion_id("OCS-8228")
    def test_pvc_health_unhealthy_via_ceph_blocklist(
        self, pvc_factory, pod_factory, request
    ):
        """
        Verify PVC health transitions to unhealthy when the CephFS
        client is blocklisted, and recovers after removal + pod restart.

        Steps:
            1. Create CephFS RWO PVC + pod, run I/O, wait for healthy.
            2. Identify the worker node running the pod.
            3. Find CephFS client, blocklist + evict via toolbox.
            4. Wait ~1-2 min for reporter tick.
            5. Assert annotation state == 'unhealthy'.
            6. Assert VolumeConditionAbnormal Warning event.
            7. (Manual) Check ODF dashboard PVC health widget.
            8. Remove blocklist, restart pod.
            9. Wait ~1-2 min, assert state == 'healthy'.
        """
        logger.test_step("Verify Ceph health")
        ceph_health_check(tries=3, delay=10)

        pvc_obj, pod_obj = self._create_pvc_and_pod_with_io(pvc_factory, pod_factory)

        logger.info("Wait for healthy annotation")
        time.sleep(REPORTER_TICK_WAIT)
        pvc_obj.wait_for_volume_health_state("healthy")

        logger.test_step("Identify the worker node running the pod")
        pod_node = pod_obj.get()["spec"]["nodeName"]
        logger.info(f"Pod {pod_obj.name} running on {pod_node}")

        logger.test_step("Find CephFS client and blocklist via toolbox")
        client_id, client_addr = blocklist_cephfs_client(pvc_obj)

        def finalizer():
            remove_cephfs_client_blocklist(client_addr)

        request.addfinalizer(finalizer)

        logger.test_step("Wait ~2 min for reporter tick")
        time.sleep(REPORTER_TICK_WAIT * 2)

        logger.test_step("Assert annotation state == 'unhealthy'")
        pvc_obj.wait_for_volume_health_state(
            "unhealthy", timeout=UNHEALTHY_POLL_TIMEOUT
        )

        logger.test_step("Assert VolumeConditionAbnormal Warning event")
        assert_pvc_volume_health_event(
            pvc_obj,
            reason="VolumeConditionAbnormal",
            event_type="Warning",
            message_substr="health-check has not responded",
        )
        logger.test_step("Remove blocklist and restart pod")
        remove_cephfs_client_blocklist(client_addr)

        pod_obj.delete()
        pod_obj.ocp.wait_for_delete(pod_obj.name)
        logger.info(f"Deleted pod {pod_obj.name}")

        logger.info("Recreating pod for fresh CephFS mount")
        new_pod = pod_factory(
            pvc=pvc_obj,
            interface=constants.CEPHFILESYSTEM,
        )
        new_pod_node = new_pod.get()["spec"]["nodeName"]
        logger.info(f"New pod {new_pod.name} on {new_pod_node}")
        logger.test_step("Wait ~2 min, assert state == 'healthy'")
        time.sleep(REPORTER_TICK_WAIT * 2)
        pvc_obj.wait_for_volume_health_state("healthy", timeout=RECOVERY_POLL_TIMEOUT)
        assert_pvc_volume_health_event(
            pvc_obj,
            reason="VolumeConditionHealthy",
            event_type="Normal",
            message_substr="volume is in a healthy condition",
        )
        logger.info("PVC health unhealthy via ceph blocklist test passed")

    @tier2
    @skipif_managed_service
    @skipif_rosa_hcp
    @skipif_external_mode
    @pytest.mark.polarion_id("OCS-8261")
    def test_pvc_health_unhealthy_via_mds_scaledown(
        self, pvc_factory, pod_factory, request
    ):
        """
        Verify RWX PVC per-node health transitions to unhealthy when both
        CephFS MDS daemons are scaled down, and recovers when restored.

        Steps:
            1. Create CephFS RWX PVC, 2 pods on different nodes, run I/O.
            2. Wait for reporter tick; assert 2 per-node annotation keys are
               healthy and snapshot 'since' per key
            3. Scale down both MDS deployments (a & b) to 0, then restart the
               CephFS nodeplugin pods on the pod nodes to trigger the probe.
            4. Assert both per-node keys report state == 'unhealthy'.
            5. Assert 'since' timestamp advanced vs the healthy snapshot.
            6. Assert VolumeConditionAbnormal Warning event fired.
            7. Restore both MDS deployments to replicas=1.
            8. Assert both keys return to state == 'healthy'.
            9. Assert new VolumeConditionHealthy Normal event on recovery.
        """
        logger.test_step("Verify Ceph health is HEALTH_OK")
        ceph_health_check(tries=3, delay=10)

        logger.test_step("Create CephFS RWX PVC (5Gi)")
        pvc_obj = pvc_factory(
            interface=constants.CEPHFILESYSTEM,
            size=5,
            access_mode=constants.ACCESS_MODE_RWX,
        )
        logger.info(f"PVC {pvc_obj.name} created and Bound")

        logger.test_step("Create 2 pods on different nodes")
        worker_nodes = node.get_worker_nodes()
        logger.assertion(
            f"Worker node count: expected >= 2, actual={len(worker_nodes)}"
        )
        assert (
            len(worker_nodes) >= 2
        ), f"Need >= 2 worker nodes, found {len(worker_nodes)}"
        pod_objs = []
        for i in range(2):
            p = pod_factory(
                pvc=pvc_obj,
                interface=constants.CEPHFILESYSTEM,
                node_name=worker_nodes[i],
            )
            pod_objs.append(p)
        pod_node_names = [p.get()["spec"]["nodeName"] for p in pod_objs]
        logger.info(
            f"Pod {pod_objs[0].name} on {pod_node_names[0]}, "
            f"Pod {pod_objs[1].name} on {pod_node_names[1]}"
        )
        logger.assertion(
            f"Pods on different nodes: {pod_node_names[0]} != {pod_node_names[1]}"
        )
        assert (
            pod_node_names[0] != pod_node_names[1]
        ), f"Both pods on same node: {pod_node_names[0]}"

        logger.test_step("Run FIO I/O on both pods")
        for p in pod_objs:
            p.run_io(
                storage_type="fs",
                size="512M",
                fio_filename=p.name,
            )
        for p in pod_objs:
            pod.get_fio_rw_iops(p)
        logger.info("FIO I/O completed")

        logger.test_step("Poll for 2 healthy per-node annotation keys")
        health_annotations = pvc_obj.wait_for_volume_health_state(
            expected_state="healthy",
            timeout=ANNOTATION_POLL_TIMEOUT,
            interval=ANNOTATION_POLL_INTERVAL,
            expected_count=2,
        )
        healthy_since = {}
        for node_name in pod_node_names:
            uid = ocp.OCP(kind="node", resource_name=node_name).get()["metadata"]["uid"]
            key = f"{constants.VOLUME_HEALTH_ANNOTATION_PREFIX}{uid}"
            logger.assertion(
                f"Healthy annotation key present for node {node_name} (uid={uid})"
            )
            assert key in health_annotations, (
                f"No volume health annotation for node {node_name} (key {key}). "
                f"Present keys: {list(health_annotations.keys())}"
            )
            parsed = json.loads(health_annotations[key])
            since = parsed.get("since")
            logger.assertion(
                f"'since' field present for annotation key {key}: actual={since}"
            )
            assert since, f"Annotation {key} has no 'since' field: {parsed}"
            healthy_since[key] = since
        logger.info(f"Healthy 'since' snapshot: {healthy_since}")

        logger.test_step("Scale down both MDS deployments (a & b) to 0 replicas")
        mds_deployments = [
            constants.MDS_DAEMON_DEPLOYMENT_ONE,
            constants.MDS_DAEMON_DEPLOYMENT_TWO,
        ]

        def finalizer():
            logger.info("Finalizer: restore both MDS deployments to 1")
            failed = []
            for dep in mds_deployments:
                if not modify_deployment_replica_count(dep, 1):
                    logger.error(f"Failed to restore deployment {dep} to 1 replica")
                    failed.append(dep)
                else:
                    logger.info(f"Restored deployment {dep} to 1 replica")
            ceph_health_check(tries=20, delay=30)
            assert (
                not failed
            ), f"Failed to restore MDS deployments to 1 replica: {failed}"

        request.addfinalizer(finalizer)

        for dep in mds_deployments:
            logger.assertion(f"Deployment {dep} scaled to 0 replicas")
            assert modify_deployment_replica_count(
                dep, 0
            ), f"Failed to scale deployment {dep} to 0 replicas"
            logger.info(f"Scaled deployment {dep} to 0")

        logger.test_step(
            "Restart CephFS nodeplugin pods on the pod nodes to trigger the "
            "unhealthy probe"
        )
        self._restart_cephfs_nodeplugin_on_nodes(pod_node_names)

        logger.test_step("Assert both per-node keys report 'unhealthy'")
        unhealthy_annotations = pvc_obj.wait_for_volume_health_state(
            expected_state="unhealthy",
            timeout=UNHEALTHY_POLL_TIMEOUT,
            interval=ANNOTATION_POLL_INTERVAL,
            expected_count=2,
        )

        logger.test_step("Assert 'since' timestamp advanced for both keys")
        for key, healthy_ts in healthy_since.items():
            logger.assertion(f"Annotation key {key} present while unhealthy")
            assert key in unhealthy_annotations, (
                f"Key {key} missing from unhealthy annotations: "
                f"{list(unhealthy_annotations.keys())}"
            )
            parsed = json.loads(unhealthy_annotations[key])
            logger.assertion(
                f"Annotation state for {key}: expected='unhealthy', "
                f"actual='{parsed.get('state')}'"
            )
            assert (
                parsed.get("state") == "unhealthy"
            ), f"Expected 'unhealthy' for {key}, got '{parsed.get('state')}'"
            new_ts = parsed.get("since", "")
            dt_healthy = datetime.fromisoformat(healthy_ts.replace("Z", "+00:00"))
            dt_unhealthy = datetime.fromisoformat(new_ts.replace("Z", "+00:00"))
            logger.assertion(f"'since' advanced for {key}: {new_ts} > {healthy_ts}")
            assert dt_unhealthy > dt_healthy, (
                f"'since' did not advance for {key}: "
                f"healthy={healthy_ts}, unhealthy={new_ts}"
            )

        logger.test_step("Assert VolumeConditionAbnormal Warning event fired")
        try:
            for _ in TimeoutSampler(
                timeout=ANNOTATION_POLL_TIMEOUT,
                sleep=ANNOTATION_POLL_INTERVAL,
                func=assert_pvc_volume_health_event,
                pvc_obj=pvc_obj,
                reason="VolumeConditionAbnormal",
                event_type="Warning",
                message_substr="health-check has not responded",
            ):
                break
        except TimeoutExpiredError:
            pytest.fail(
                "VolumeConditionAbnormal event not found for "
                f"PVC {pvc_obj.name} within {ANNOTATION_POLL_TIMEOUT}s"
            )

        logger.test_step("Capture pre-recovery healthy event count")
        pre_recovery_healthy_count = count_pvc_volume_health_events(
            pvc_obj,
            reason="VolumeConditionHealthy",
            event_type="Normal",
            message_substr="volume is in a healthy condition",
        )
        logger.info(
            f"Pre-recovery VolumeConditionHealthy event count: "
            f"{pre_recovery_healthy_count}"
        )

        logger.test_step("Restore both MDS deployments to 1 replica")
        for dep in mds_deployments:
            modify_deployment_replica_count(dep, 1)
            logger.info(f"Scaled deployment {dep} to 1")

        logger.test_step("Assert both per-node keys return to 'healthy'")
        pvc_obj.wait_for_volume_health_state(
            expected_state="healthy",
            timeout=RECOVERY_POLL_TIMEOUT,
            interval=ANNOTATION_POLL_INTERVAL,
            expected_count=2,
        )

        logger.test_step(
            "Poll for a new VolumeConditionHealthy event after recovery "
            "(event count must increase)"
        )

        def new_healthy_event():
            return (
                count_pvc_volume_health_events(
                    pvc_obj,
                    reason="VolumeConditionHealthy",
                    event_type="Normal",
                    message_substr="volume is in a healthy condition",
                )
                > pre_recovery_healthy_count
            )

        try:
            for is_new in TimeoutSampler(
                timeout=RECOVERY_POLL_TIMEOUT,
                sleep=ANNOTATION_POLL_INTERVAL,
                func=new_healthy_event,
            ):
                if is_new:
                    break
        except TimeoutExpiredError:
            pytest.fail(
                "No new VolumeConditionHealthy event for "
                f"PVC {pvc_obj.name} within {RECOVERY_POLL_TIMEOUT}s"
            )
        logger.info("PVC health unhealthy via MDS scale-down test passed")
