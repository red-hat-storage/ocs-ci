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
    jira,
)
from ocs_ci.framework.testlib import ManageTest, tier1
from ocs_ci.framework import config
from ocs_ci.ocs import constants, ocp, node
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.resources.csi_addons import (
    get_csi_addon_pod_on_node,
)
from ocs_ci.utility.utils import ceph_health_check, TimeoutSampler

logger = logging.getLogger(__name__)

ANNOTATION_POLL_TIMEOUT = 180
ANNOTATION_POLL_INTERVAL = 15
REPORTER_TICK_WAIT = 60


@tier1
@green_squad
@skipif_ocs_version("<4.22")
@jira("DFBUGS-9421", run=False)
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
        logger.info("Step 1: Verify Ceph health is HEALTH_OK")
        ceph_health_check(tries=3, delay=10)
        logger.info(f"Step 2: Create {driver.upper()} PVC " f"({access_mode}, 5Gi)")
        pvc_obj = pvc_factory(
            interface=interface,
            size=5,
            access_mode=access_mode,
        )
        logger.info(f"PVC {pvc_obj.name} created and Bound")
        pod_objs = []
        if interface == constants.CEPHFILESYSTEM:
            logger.info("Step 3: Create 2 pods on different nodes")
            worker_nodes = node.get_worker_nodes()
            assert len(worker_nodes) >= 2, (
                f"Need >= 2 worker nodes, " f"found {len(worker_nodes)}"
            )
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
            assert node1 != node2, f"Both pods on same node: {node1}"
        else:
            logger.info("Step 3: Create 1 pod mounting the PVC")
            p = pod_factory(
                pvc=pvc_obj,
                interface=interface,
            )
            pod_objs.append(p)
            pod_node = p.get()["spec"]["nodeName"]
            logger.info(f"Pod {p.name} on {pod_node}")
        logger.info("Step 4: Run FIO I/O on all pods")
        for p in pod_objs:
            p.run_io(
                storage_type="fs",
                size="512M",
                fio_filename=p.name,
            )
        for p in pod_objs:
            pod.get_fio_rw_iops(p)
        logger.info("FIO I/O completed")
        logger.info(f"Step 5: Wait {REPORTER_TICK_WAIT}s " f"for reporter tick")
        time.sleep(REPORTER_TICK_WAIT)
        logger.info(
            f"Step 6: Poll for {expected_count} "
            f"volumehealth annotation(s) "
            f"(timeout={ANNOTATION_POLL_TIMEOUT}s)"
        )
        health_annotations = {}
        try:
            for sample in TimeoutSampler(
                timeout=ANNOTATION_POLL_TIMEOUT,
                sleep=ANNOTATION_POLL_INTERVAL,
                func=pvc_obj.get_volume_health_annotations,
            ):
                health_annotations = sample
                if len(health_annotations) >= expected_count:
                    logger.info(
                        f"Found {len(health_annotations)} "
                        f"volumehealth annotation(s)"
                    )
                    break
                logger.debug(
                    f"Found {len(health_annotations)}/" f"{expected_count}, waiting"
                )
        except TimeoutExpiredError:
            all_annots = pvc_obj.get().get("metadata", {}).get("annotations", {})
            logger.error(f"PVC annotations at timeout: " f"{all_annots}")
            pytest.fail(
                f"Expected {expected_count} volumehealth "
                f"annotation(s) on PVC {pvc_obj.name}, "
                f"found {len(health_annotations)} "
                f"within {ANNOTATION_POLL_TIMEOUT}s"
            )
        logger.assertion(
            f"Exactly {expected_count} volumehealth " f"annotation(s) present"
        )
        assert len(health_annotations) == expected_count, (
            f"Expected {expected_count}, " f"found {len(health_annotations)}"
        )
        pod_node_names = [p.get()["spec"]["nodeName"] for p in pod_objs]
        expected_node_uids = set()
        for node_name in pod_node_names:
            node_ocp = ocp.OCP(kind="node", resource_name=node_name)
            uid = node_ocp.get()["metadata"]["uid"]
            expected_node_uids.add(uid)
            expected_key = f"{constants.VOLUME_HEALTH_ANNOTATION_PREFIX}" f"{uid}"
            logger.assertion(
                f"Annotation key for node {node_name} " f"(uid={uid}) exists"
            )
            assert expected_key in health_annotations, (
                f"Missing key {expected_key}. "
                f"Found: "
                f"{list(health_annotations.keys())}"
            )
        logger.info("Step 7: Validate annotation JSON content")
        for key, value in health_annotations.items():
            parsed = json.loads(value)
            logger.info(f"Annotation {key}: {parsed}")
            logger.assertion(f"state is 'healthy', " f"got '{parsed.get('state')}'")
            assert parsed.get("state") == "healthy", (
                f"Expected 'healthy', " f"got '{parsed.get('state')}'"
            )
            last_checked = parsed.get("lastChecked", "")
            logger.assertion(f"lastChecked is valid RFC3339: " f"{last_checked}")
            dt_last = datetime.fromisoformat(last_checked.replace("Z", "+00:00"))
            assert dt_last.tzinfo is not None, (
                f"lastChecked missing timezone: " f"{last_checked}"
            )
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
            logger.assertion(f"Key suffix {key_suffix} matches " f"a pod's node UID")
            assert key_suffix in expected_node_uids, (
                f"Key suffix {key_suffix} not in "
                f"expected UIDs: {expected_node_uids}"
            )
        logger.info(
            f"Step 8: Check VolumeConditionHealthy " f"events for PVC {pvc_obj.name}"
        )
        pvc_ns = pvc_obj.namespace
        event_ocp = ocp.OCP(kind="Event", namespace=pvc_ns)
        events = event_ocp.get(
            field_selector=(f"involvedObject.name={pvc_obj.name}"),
        )["items"]
        health_events = [
            e
            for e in events
            if e.get("reason") == "VolumeConditionHealthy"
            and "volume is in a healthy condition" in e.get("message", "")
        ]
        logger.assertion(
            "At least 1 VolumeConditionHealthy event " "with healthy message"
        )
        assert len(health_events) >= 1, (
            f"No VolumeConditionHealthy events with "
            f"healthy message for PVC {pvc_obj.name}. "
            f"Event messages: "
            f"{[(e.get('reason'), e.get('message')) for e in events]}"
        )
        for evt in health_events:
            logger.info(
                f"Event: reason={evt.get('reason')}, "
                f"type={evt.get('type')}, "
                f"message={evt.get('message')}"
            )
            logger.assertion("Event type is 'Normal'")
            assert evt["type"] == "Normal", (
                f"Expected 'Normal', " f"got '{evt['type']}'"
            )
            source = evt.get("source", {})
            logger.assertion("source.component is 'CSI-Addons'")
            assert source.get("component") == "CSI-Addons", (
                f"Expected 'CSI-Addons', " f"got '{source.get('component')}'"
            )
            event_host = source.get("host", "")
            logger.assertion(f"source.host '{event_host}' " f"matches a pod node")
            assert event_host in pod_node_names, (
                f"source.host '{event_host}' not in " f"pod nodes: {pod_node_names}"
            )
        logger.info("Step 8b: Verify event in 'oc describe pvc'")
        describe_output = pvc_obj.describe()
        logger.assertion("VolumeConditionHealthy in describe output")
        assert "VolumeConditionHealthy" in describe_output, (
            "VolumeConditionHealthy not in " "'oc describe pvc' output"
        )
        logger.info("Step 9: Check csi-addons sidecar logs")
        pvc_obj.reload()
        pv_name = pvc_obj.backed_pv
        assert pv_name, f"PVC {pvc_obj.name} has no backed PV"
        for node_name in pod_node_names:
            addon_pod = get_csi_addon_pod_on_node(node_name, driver)
            log_output = pod.get_pod_logs(
                pod_name=addon_pod,
                container="csi-addons",
                namespace=ns,
                tail=200,
            )
            logger.assertion(f"PV {pv_name} in csi-addons logs " f"on node {node_name}")
            assert pv_name in log_output, (
                f"PV '{pv_name}' not in logs of " f"{addon_pod} on {node_name}"
            )
        logger.info(f"{driver.upper()} PVC volume health " f"annotation test passed")
