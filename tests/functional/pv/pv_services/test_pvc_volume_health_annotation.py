"""
Tests for PVC volume health annotation feature (RHSTOR-7596).
"""

import json
import logging
import time
from datetime import datetime, timezone

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    green_squad,
    skipif_ocs_version,
    skipif_mcg_only,
    skipif_managed_service,
    skipif_rosa_hcp,
    skipif_external_mode,
    ui,
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
    remove_csi_addons_config_key,
    restart_csi_addons_controller,
    update_csi_addons_config,
)
from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator
from ocs_ci.ocs.exceptions import CommandFailed, TimeoutExpiredError
from ocs_ci.utility.utils import ceph_health_check, TimeoutSampler

logger = logging.getLogger(__name__)

ANNOTATION_POLL_TIMEOUT = 180
ANNOTATION_POLL_INTERVAL = 15
REPORTER_TICK_WAIT = 60
UNHEALTHY_POLL_TIMEOUT = 300
RECOVERY_POLL_TIMEOUT = 300
STALE_CLEANUP_WAIT = 210
MAX_LAST_CHECKED_AGE_SECONDS = 120


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
    @ui
    @pytest.mark.polarion_id("OCS-8228")
    def test_pvc_health_unhealthy_via_ceph_blocklist(
        self, pvc_factory, pod_factory, request, setup_ui_class_factory
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
            7. UI: Verify PVC appears in Volume Health Card table.
            8. Remove blocklist, restart pod.
            9. Wait ~1-2 min, assert state == 'healthy'.
            10. UI: Verify PVC disappears from Volume Health Card.
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

        logger.test_step("UI: Verify unhealthy PVC appears in Volume Health Card")

        setup_ui_class_factory()
        page_nav = PageNavigator().nav_storage_cluster_default_page()
        sc_page = page_nav.nav_block_and_file_tab()
        card = sc_page.get_volume_health_card()

        pvc_in_table = card.wait_for_pvc_in_table(pvc_obj.name, timeout=60)
        logger.assertion(f"PVC in table: expected=True, actual={pvc_in_table}")
        assert pvc_in_table, f"PVC {pvc_obj.name} not found in health table"

        attention_text = card.get_attention_text()
        logger.assertion(f"'need attention' in text: {repr(attention_text)}")
        assert (
            "need attention" in attention_text.lower()
        ), f"Unexpected text: {attention_text}"

        all_row_data = card.get_all_row_data()

        test_pvc_rows = [row for row in all_row_data if row.pvc_name == pvc_obj.name]
        logger.assertion(
            f"Row count for {pvc_obj.name}: expected=1 (RWO), actual={len(test_pvc_rows)}"
        )
        assert (
            len(test_pvc_rows) == 1
        ), f"Expected 1 row for RWO PVC {pvc_obj.name}, got {len(test_pvc_rows)}"

        row_data = test_pvc_rows[0]
        logger.assertion(
            f"PVC name: expected={pvc_obj.name}, actual={row_data.pvc_name}"
        )
        assert row_data.pvc_name == pvc_obj.name

        logger.assertion(f"Node name: expected={pod_node}, actual={row_data.node_name}")
        assert row_data.node_name == pod_node

        logger.assertion(
            f"Events href contains {pvc_obj.name}/events: {row_data.events_href}"
        )
        assert pvc_obj.name in row_data.events_href
        assert "/events" in row_data.events_href

        card.take_screenshot("unhealthy_state_confirmed")
        events_page = card.click_view_events(pvc_obj.name)

        current_url = events_page.driver.current_url
        logger.assertion(
            f"Events URL: expected={row_data.events_href}, actual={current_url}"
        )
        assert current_url == row_data.events_href

        logger.info("Unhealthy state UI verification passed")

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
        logger.test_step("UI: Verify card returns to healthy state after recovery")
        card.nav_storage_cluster_default_page()
        logger.info("Check unhealthy PVC removed from table")
        pvc_cleared = card.wait_for_pvc_not_in_table(pvc_obj.name, timeout=60)
        assert pvc_cleared, f"PVC {pvc_obj.name} still in table after recovery"
        card_healthy = card.wait_for_healthy(timeout=300)
        logger.assertion(f"Card healthy: expected=True, actual={card_healthy}")
        assert card_healthy, "Card did not return to healthy state"

        no_issues_text = card.get_no_issues_text()
        logger.assertion(
            f"No issues text: expected='No issues found.', actual='{no_issues_text}'"
        )
        assert no_issues_text == "No issues found."

        pvc_cleared = card.wait_for_pvc_not_in_table(pvc_obj.name, timeout=60)
        logger.assertion(f"PVC cleared from table: expected=True, actual={pvc_cleared}")
        assert pvc_cleared, f"PVC {pvc_obj.name} still in table after recovery"

        card.take_screenshot("recovery_confirmed")
        logger.info("Recovery UI verification passed")
        logger.info("PVC health unhealthy via ceph blocklist test passed")

    @tier2
    @ui
    @skipif_managed_service
    @skipif_rosa_hcp
    @skipif_external_mode
    @pytest.mark.polarion_id("OCS-8261")
    def test_pvc_health_unhealthy_via_mds_scaledown(
        self, pvc_factory, pod_factory, request, setup_ui_class_factory
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
            7. UI: Verify PVC appears in table with 2 rows (one per node).
            8. Restore both MDS deployments to replicas=1.
            9. Assert both keys return to state == 'healthy'.
            10. Assert new VolumeConditionHealthy Normal event on recovery.
            11. UI: Verify card returns to healthy state.
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
        initial_cluster_index = config.cur_index
        is_hci = (
            config.ENV_DATA["platform"].lower()
            in constants.HCI_PROVIDER_CLIENT_PLATFORMS
        )

        def finalizer():
            logger.info("Finalizer: restore both MDS deployments to 1")
            failed = []
            try:
                if is_hci:
                    config.switch_to_provider()
                for dep in mds_deployments:
                    if not modify_deployment_replica_count(dep, 1):
                        logger.error(f"Failed to restore deployment {dep} to 1 replica")
                        failed.append(dep)
                    else:
                        logger.info(f"Restored deployment {dep} to 1 replica")
                ceph_health_check(tries=20, delay=30)
            finally:
                if is_hci:
                    config.switch_ctx(initial_cluster_index)
            assert (
                not failed
            ), f"Failed to restore MDS deployments to 1 replica: {failed}"

        request.addfinalizer(finalizer)

        try:
            if is_hci:
                logger.info("HCI platform: switching to provider for MDS operations")
                config.switch_to_provider()
            for dep in mds_deployments:
                logger.assertion(f"Deployment {dep} scaled to 0 replicas")
                assert modify_deployment_replica_count(
                    dep, 0
                ), f"Failed to scale deployment {dep} to 0 replicas"
                logger.info(f"Scaled deployment {dep} to 0")

            logger.test_step(
                "Restart CephFS nodeplugin pods on the pod nodes to trigger "
                "the unhealthy probe"
            )
            self._restart_cephfs_nodeplugin_on_nodes(pod_node_names)
        finally:
            if is_hci:
                config.switch_ctx(initial_cluster_index)

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
        logger.test_step(
            "UI: Verify unhealthy RWX PVC appears in Volume Health Card (2 nodes)"
        )
        setup_ui_class_factory()
        page_nav = PageNavigator().nav_storage_cluster_default_page()
        sc_page = page_nav.nav_block_and_file_tab()
        card = sc_page.get_volume_health_card()

        pvc_in_table = card.wait_for_pvc_in_table(pvc_obj.name, timeout=60)
        logger.assertion(f"PVC in table: expected=True, actual={pvc_in_table}")
        assert pvc_in_table

        all_row_data = card.get_all_row_data()
        test_pvc_rows = [row for row in all_row_data if row.pvc_name == pvc_obj.name]
        logger.assertion(
            f"Row count for {pvc_obj.name}: expected=2 (RWX, 2 nodes), "
            f"actual={len(test_pvc_rows)}"
        )
        assert (
            len(test_pvc_rows) == 2
        ), f"Expected 2 rows for RWX PVC {pvc_obj.name}, got {len(test_pvc_rows)}"

        node_names = {row.node_name for row in test_pvc_rows}
        expected_nodes = set(pod_node_names)
        logger.assertion(f"Node names: expected={expected_nodes}, actual={node_names}")
        assert node_names == expected_nodes

        for row in test_pvc_rows:
            logger.assertion(f"Events href for node {row.node_name}: {row.events_href}")
            assert pvc_obj.name in row.events_href
            assert "/events" in row.events_href

        card.take_screenshot("mds_unhealthy_state_confirmed")
        card.click_view_events(pvc_obj.name)

        logger.info("MDS scaledown unhealthy UI verification passed")

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

        logger.test_step("UI: Verify card returns to healthy after MDS recovery")
        card.nav_storage_cluster_default_page()
        logger.info("Check unhealthy PVC removed from table")
        pvc_cleared = card.wait_for_pvc_not_in_table(pvc_obj.name, timeout=60)
        assert pvc_cleared, f"PVC {pvc_obj.name} still in table after recovery"
        card_healthy = card.wait_for_healthy(timeout=300)
        logger.assertion(f"Card healthy: expected=True, actual={card_healthy}")
        assert card_healthy

        no_issues_text = card.get_no_issues_text()
        logger.assertion(f"No issues text: {repr(no_issues_text)}")
        assert no_issues_text == "No issues found."

        pvc_cleared = card.wait_for_pvc_not_in_table(pvc_obj.name, timeout=60)
        logger.assertion(f"PVC cleared: expected=True, actual={pvc_cleared}")
        assert pvc_cleared

        card.take_screenshot("mds_recovery_confirmed")
        logger.info("PVC health unhealthy via MDS scale-down test passed")


@tier2
@green_squad
@skipif_ocs_version("<4.23")
@skipif_managed_service
@skipif_rosa_hcp
@skipif_external_mode
@skipif_mcg_only
class TestPVCVolumeHealthStaleAnnotation(TestPVCVolumeHealthUnhealthy):
    """
    Verify that stale volumehealth annotations are cleaned up by the
    CSI Addons controller once the stale threshold elapses after the
    mounting pod is deleted, while annotations from an actively-mounted
    PVC are preserved.
    """

    @pytest.fixture(autouse=True)
    def restore_csi_addons_config(self, request):
        """
        Snapshot the CSI Addons ConfigMap state before the test and
        restore it afterwards via a finalizer.

        If the ConfigMap did not exist before the test, the finalizer
        deletes it and restarts the controller.  If it did exist, the
        finalizer restores the original values for the two keys used by
        this test, removing them when they were absent before.

        Args:
            request: pytest ``FixtureRequest`` object used to register
                the teardown finalizer.
        """
        ns = config.ENV_DATA["cluster_namespace"]
        cm_ocp = ocp.OCP(
            kind=constants.CONFIGMAP,
            namespace=ns,
            resource_name=constants.CSI_ADDONS_CONFIGMAP_NAME,
        )
        cm_existed = cm_ocp.is_exist(resource_name=constants.CSI_ADDONS_CONFIGMAP_NAME)

        threshold_key = "volume-health-stale-threshold"
        interval_key = "volume-health-cleanup-interval"

        threshold_existed = False
        interval_existed = False
        original_threshold = ""
        original_interval = ""

        if cm_existed:
            cm_data = cm_ocp.get().get("data", {})
            if threshold_key in cm_data:
                threshold_existed = True
                original_threshold = cm_data[threshold_key]
            if interval_key in cm_data:
                interval_existed = True
                original_interval = cm_data[interval_key]

        logger.info(
            f"Snapshotted CSI Addons config — cm_existed={cm_existed}, "
            f"{threshold_key}_existed={threshold_existed} value={repr(original_threshold)}, "
            f"{interval_key}_existed={interval_existed} value={repr(original_interval)}"
        )

        def finalizer():
            logger.info("restore_csi_addons_config finalizer: restoring state")
            if not cm_existed:
                logger.info(
                    "ConfigMap did not exist before test; deleting it "
                    "and restarting controller"
                )
                try:
                    cm_ocp.delete(resource_name=constants.CSI_ADDONS_CONFIGMAP_NAME)
                    logger.info(
                        f"Deleted ConfigMap " f"{constants.CSI_ADDONS_CONFIGMAP_NAME}"
                    )
                except CommandFailed as e:
                    if "not found" not in str(e).lower():
                        logger.error(
                            f"Failed to delete ConfigMap "
                            f"{constants.CSI_ADDONS_CONFIGMAP_NAME}: {e}"
                        )
                        raise
                    logger.info(
                        f"ConfigMap {constants.CSI_ADDONS_CONFIGMAP_NAME} "
                        f"already deleted"
                    )
                restart_csi_addons_controller()
                return
            for key, existed, original in (
                (threshold_key, threshold_existed, original_threshold),
                (interval_key, interval_existed, original_interval),
            ):
                if existed:
                    update_csi_addons_config(key, original, restart=False)
                    logger.info(f"Restored {key}={original!r}")
                else:
                    remove_csi_addons_config_key(key)
                    logger.info(f"Removed {key} (was absent before test)")
            restart_csi_addons_controller()

        request.addfinalizer(finalizer)

    def _validate_stale_cleanup(
        self,
        pvc_stale,
        stale_key,
        active_key,
        pvc_active=None,
    ):
        """
        Validate stale annotation cleanup behavior.

        Polls for stale annotation cleanup, asserts the stale key is removed
        and the active key persists with a fresh lastChecked timestamp.

        Args:
            pvc_stale: PVC object containing the stale annotation key
            stale_key: Annotation key expected to be cleaned up
            active_key: Annotation key expected to remain active
            pvc_active: Optional separate PVC for active annotation.
                If None, validates active key on pvc_stale (same PVC).
        """
        logger.test_step(
            f"Poll up to {STALE_CLEANUP_WAIT}s for stale annotation cleanup"
        )
        try:
            for cleaned in TimeoutSampler(
                timeout=STALE_CLEANUP_WAIT,
                sleep=10,
                func=lambda: (
                    stale_key not in pvc_stale.get_volume_health_annotations()
                ),
            ):
                if cleaned:
                    logger.info(f"Stale annotation {stale_key} cleaned up")
                    break
        except TimeoutExpiredError:
            logger.warning(f"Stale annotation not cleaned within {STALE_CLEANUP_WAIT}s")

        remaining_stale = pvc_stale.get_volume_health_annotations()
        logger.assertion(
            f"stale key not in annotations; "
            f"remaining={list(remaining_stale.keys())}"
        )
        assert stale_key not in remaining_stale, (
            f"Stale key {stale_key} still present "
            f"on PVC {pvc_stale.name} after {STALE_CLEANUP_WAIT}s"
        )

        logger.test_step("Assert active annotation key is still present")
        # Use separate PVC for active validation if provided, else same PVC
        pvc_for_active = pvc_active if pvc_active else pvc_stale
        remaining_active = pvc_for_active.get_volume_health_annotations()
        logger.assertion(
            f"active key in annotations; " f"present={list(remaining_active.keys())}"
        )
        assert active_key in remaining_active, (
            f"Active key {active_key} missing from "
            f"PVC {pvc_for_active.name} — should not have been cleaned up"
        )

        logger.test_step(
            f"Parse lastChecked from active annotation; "
            f"assert age <= {MAX_LAST_CHECKED_AGE_SECONDS}s"
        )
        active_parsed = json.loads(remaining_active[active_key])
        last_checked_raw = active_parsed.get("lastChecked", "")
        logger.info(f"Active annotation lastChecked: {last_checked_raw}")
        dt_last = datetime.fromisoformat(last_checked_raw.replace("Z", "+00:00"))
        now = datetime.now(tz=timezone.utc)
        age_seconds = (now - dt_last).total_seconds()
        logger.assertion(
            f"lastChecked age in range [0, {MAX_LAST_CHECKED_AGE_SECONDS}]s; "
            f"actual={age_seconds:.1f}s"
        )
        assert 0 <= age_seconds <= MAX_LAST_CHECKED_AGE_SECONDS, (
            f"Active annotation lastChecked age out of range: "
            f"{age_seconds:.1f}s "
            f"(expected 0 to {MAX_LAST_CHECKED_AGE_SECONDS}s). "
            f"lastChecked={last_checked_raw}"
        )
        logger.info(f"Active annotation lastChecked age={age_seconds:.1f}s — OK")

    @tier2
    @pytest.mark.polarion_id("OCS-8292")
    def test_stale_volume_health_annotation_cleanup(self, pvc_factory, pod_factory):
        """
        Verify stale volumehealth annotations are cleaned up after the
        mounting pod is deleted and the stale threshold elapses, while
        annotations from an actively-mounted PVC survive cleanup.

        Note:
            The ``restore_csi_addons_config`` autouse fixture
            automatically snapshots and restores the CSI Addons
            ConfigMap state after the test completes.

        Steps:
            1. Ceph health OK; create PVC+pod for stale scenario; wait
               for healthy annotation.
            2. Create PVC+pod for active scenario; wait for healthy
               annotation.
            3. Record annotation keys for both PVCs.
            4. Configure stale threshold (2 m) and cleanup interval
               (1 m) on the CSI Addons controller.
            5. Delete the stale pod and wait for it to disappear.
            6. Poll until stale annotation disappears (up to
               STALE_CLEANUP_WAIT seconds).
            7. Assert stale annotation key is gone from stale PVC.
            8. Assert active annotation key is still present on active
               PVC.
            9. Parse lastChecked from active annotation; assert age
               is within MAX_LAST_CHECKED_AGE_SECONDS.
        """
        logger.test_step(
            "Verify Ceph health OK; create stale PVC+pod; "
            "poll for healthy annotation"
        )
        ceph_health_check(tries=3, delay=10)
        pvc_stale, pod_stale = self._create_pvc_and_pod_with_io(
            pvc_factory, pod_factory
        )
        pvc_stale.wait_for_volume_health_state(
            "healthy",
            timeout=ANNOTATION_POLL_TIMEOUT,
            interval=ANNOTATION_POLL_INTERVAL,
        )

        logger.test_step("Create active PVC+pod; " "poll for healthy annotation")
        pvc_active, pod_active = self._create_pvc_and_pod_with_io(
            pvc_factory, pod_factory
        )
        pvc_active.wait_for_volume_health_state(
            "healthy",
            timeout=ANNOTATION_POLL_TIMEOUT,
            interval=ANNOTATION_POLL_INTERVAL,
        )

        logger.test_step("Record annotation keys for stale PVC and active PVC")
        stale_annotations = pvc_stale.get_volume_health_annotations()
        logger.assertion(
            f"stale PVC has exactly 1 annotation key; "
            f"actual={len(stale_annotations)}"
        )
        assert len(stale_annotations) == 1, (
            f"Expected 1 annotation on stale PVC, "
            f"got {len(stale_annotations)}: {list(stale_annotations.keys())}"
        )
        stale_key = next(iter(stale_annotations))

        active_annotations = pvc_active.get_volume_health_annotations()
        logger.assertion(
            f"active PVC has exactly 1 annotation key; "
            f"actual={len(active_annotations)}"
        )
        assert len(active_annotations) == 1, (
            f"Expected 1 annotation on active PVC, "
            f"got {len(active_annotations)}: "
            f"{list(active_annotations.keys())}"
        )
        active_key = next(iter(active_annotations))
        logger.info(f"stale_key={stale_key}, active_key={active_key}")

        logger.test_step(
            "Configure stale threshold=2m, cleanup interval=1m on "
            "CSI Addons controller"
        )
        update_csi_addons_config("volume-health-stale-threshold", "2m", restart=False)
        update_csi_addons_config("volume-health-cleanup-interval", "1m", restart=True)
        logger.info("CSI Addons config updated; controller restarted")

        logger.test_step(f"Delete pod {pod_stale.name} and wait for it to disappear")
        pod_stale.delete(wait=True)
        pod_stale.ocp.wait_for_delete(pod_stale.name)
        logger.info(f"Pod {pod_stale.name} deleted")

        self._validate_stale_cleanup(
            pvc_stale=pvc_stale,
            stale_key=stale_key,
            active_key=active_key,
            pvc_active=pvc_active,
        )

        logger.info("PVC volume health stale annotation cleanup test passed")

    @tier2
    @pytest.mark.polarion_id("OCS-8293")
    def test_stale_volume_health_annotation_cleanup_rwx_multi_node(
        self, pvc_factory, pod_factory, request
    ):
        """
        Verify stale volumehealth annotation cleanup on a CephFS RWX PVC
        mounted across two worker nodes: deleting pod-A on node-1 results
        in its per-node annotation key being removed after the stale
        threshold expires, while pod-B on node-2 remains running and its
        annotation key stays active and fresh.

        Steps:
            1. Ceph health OK; create CephFS RWX PVC (5Gi).
            2. Create pod-A on node-1 and pod-B on node-2; run FIO I/O
               on both.
            3. Wait for reporter tick and assert 2 per-node healthy
               annotation keys exist.
            4. Resolve per-node annotation keys for node-1 (stale candidate)
               and node-2 (active candidate).
            5. Configure stale threshold (2 m) and cleanup interval (1 m)
               on the CSI Addons controller.
            6. Delete pod-A on node-1 and wait for it to disappear.
            7. Poll until node-1 annotation key disappears (up to
               STALE_CLEANUP_WAIT seconds).
            8. Assert node-1 annotation key is gone from the RWX PVC.
            9. Assert node-2 annotation key is still present on the RWX PVC.
            10. Parse lastChecked from node-2 annotation; assert age
                is within MAX_LAST_CHECKED_AGE_SECONDS.
        """
        logger.test_step("Verify Ceph health OK; create CephFS RWX PVC (5Gi)")
        ceph_health_check(tries=3, delay=10)
        pvc_obj = pvc_factory(
            interface=constants.CEPHFILESYSTEM,
            size=5,
            access_mode=constants.ACCESS_MODE_RWX,
        )
        logger.info(f"RWX PVC {pvc_obj.name} created and Bound")

        logger.test_step("Create pod-A on node-1 and pod-B on node-2")
        worker_nodes = node.get_worker_nodes()
        logger.assertion(
            f"Worker node count: expected >= 2, actual={len(worker_nodes)}"
        )
        assert (
            len(worker_nodes) >= 2
        ), f"Need >= 2 worker nodes, found {len(worker_nodes)}"

        pod_a = pod_factory(
            pvc=pvc_obj,
            interface=constants.CEPHFILESYSTEM,
            node_name=worker_nodes[0],
        )
        pod_b = pod_factory(
            pvc=pvc_obj,
            interface=constants.CEPHFILESYSTEM,
            node_name=worker_nodes[1],
        )
        node_a = pod_a.get()["spec"]["nodeName"]
        node_b = pod_b.get()["spec"]["nodeName"]
        logger.info(
            f"Pod-A {pod_a.name} on {node_a}, " f"Pod-B {pod_b.name} on {node_b}"
        )
        logger.assertion(f"Pods on different nodes: {node_a} != {node_b}")
        assert node_a != node_b, f"Both pods on same node: {node_a}"

        def finalizer_cleanup_pods():
            """Clean up any remaining pods if test fails"""
            for p, name in [(pod_a, "pod_a"), (pod_b, "pod_b")]:
                try:
                    if p.ocp.is_exist(resource_name=p.name):
                        logger.info(f"Finalizer cleaning up {name}: {p.name}")
                        p.delete(wait=False)
                except Exception as e:
                    logger.warning(f"Failed to cleanup {name}: {e}")

        request.addfinalizer(finalizer_cleanup_pods)

        logger.test_step("Run FIO I/O on both pods")
        for p in (pod_a, pod_b):
            p.run_io(
                storage_type="fs",
                size="512M",
                fio_filename=p.name,
            )
        for p in (pod_a, pod_b):
            pod.get_fio_rw_iops(p)
        logger.info("FIO I/O completed on both pods")

        logger.test_step("Poll for 2 healthy per-node annotation keys")
        health_annotations = pvc_obj.wait_for_volume_health_state(
            expected_state="healthy",
            timeout=ANNOTATION_POLL_TIMEOUT,
            interval=ANNOTATION_POLL_INTERVAL,
            expected_count=2,
        )

        logger.test_step(
            "Resolve per-node annotation keys for node-1 (stale) and " "node-2 (active)"
        )
        node_a_ocp = ocp.OCP(kind="node", resource_name=node_a)
        uid_a = node_a_ocp.get()["metadata"]["uid"]
        key_a = f"{constants.VOLUME_HEALTH_ANNOTATION_PREFIX}{uid_a}"

        node_b_ocp = ocp.OCP(kind="node", resource_name=node_b)
        uid_b = node_b_ocp.get()["metadata"]["uid"]
        key_b = f"{constants.VOLUME_HEALTH_ANNOTATION_PREFIX}{uid_b}"

        logger.assertion(f"key_a ({key_a}) in health annotations")
        assert key_a in health_annotations, (
            f"Missing key_a {key_a} in annotations: "
            f"{list(health_annotations.keys())}"
        )
        logger.assertion(f"key_b ({key_b}) in health annotations")
        assert key_b in health_annotations, (
            f"Missing key_b {key_b} in annotations: "
            f"{list(health_annotations.keys())}"
        )
        logger.info(f"Resolved key_a={key_a} (node-1), key_b={key_b} (node-2)")

        logger.test_step(
            "Configure stale threshold=2m, cleanup interval=1m on "
            "CSI Addons controller"
        )
        update_csi_addons_config("volume-health-stale-threshold", "2m", restart=False)
        update_csi_addons_config("volume-health-cleanup-interval", "1m", restart=True)
        logger.info("CSI Addons config updated; controller restarted")

        logger.test_step(f"Delete pod-A ({pod_a.name}) on node-1 and wait for deletion")
        pod_a.delete(wait=True)
        pod_a.ocp.wait_for_delete(pod_a.name)
        logger.info(f"Pod-A {pod_a.name} deleted; pod-B {pod_b.name} still running")

        self._validate_stale_cleanup(
            pvc_stale=pvc_obj,
            stale_key=key_a,
            active_key=key_b,
        )

        logger.info("RWX multi-node stale volume health annotation cleanup test passed")
