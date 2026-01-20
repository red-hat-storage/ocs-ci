import logging
import time
import json
import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    tier3,
    polarion_id,
    brown_squad,
)
from ocs_ci.framework.testlib import ManageTest, ignore_leftovers
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import (
    get_pods_having_label,
    wait_for_pods_to_be_running,
)
from ocs_ci.ocs.node import get_worker_nodes
from ocs_ci.helpers.helpers import (
    label_worker_node,
    remove_label_from_worker_node,
)
from ocs_ci.ocs.resources.storage_cluster import verify_storage_cluster
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)


@brown_squad
@ignore_leftovers
class TestStorageClusterLabelSelector(ManageTest):

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        def finalizer():
            logger.info("Starting teardown")

            worker_nodes = get_worker_nodes()

            try:
                remove_label_from_worker_node(
                    worker_nodes, label_key="node-role.kubernetes.io/infra-logging"
                )
                remove_label_from_worker_node(worker_nodes, label_key="portworx")
            except Exception as e:
                logger.warning(f"Failed removing labels: {e}")

            try:
                storagecluster_obj = OCP(
                    resource_name=constants.DEFAULT_CLUSTERNAME,
                    namespace=config.ENV_DATA["cluster_namespace"],
                    kind=constants.STORAGECLUSTER,
                )

                sc_data = storagecluster_obj.get()
                if "labelSelector" in sc_data.get("spec", {}):
                    params = '[{"op": "remove", "path": "/spec/labelSelector"}]'
                    storagecluster_obj.patch(params=params, format_type="json")
                    time.sleep(60)
                    verify_storage_cluster()
            except Exception as e:
                logger.warning(f"Failed restoring storagecluster: {e}")

            try:
                wait_for_pods_to_be_running(
                    namespace=config.ENV_DATA["cluster_namespace"], timeout=600
                )
            except Exception as e:
                logger.warning(f"Pods not fully recovered: {e}")

        request.addfinalizer(finalizer)

    @tier3
    @polarion_id("xxx")
    def test_storage_cluster_multiple_label_selectors(self):

        logger.info("Step 1: Patching storage cluster")

        storagecluster_obj = OCP(
            resource_name=constants.DEFAULT_CLUSTERNAME,
            namespace=config.ENV_DATA["cluster_namespace"],
            kind=constants.STORAGECLUSTER,
        )

        label_selector_patch = {
            "spec": {
                "labelSelector": {
                    "matchLabels": {
                        "node-role.kubernetes.io/infra-logging": "",
                        "portworx": "true",
                    }
                }
            }
        }

        storagecluster_obj.patch(
            params=json.dumps(label_selector_patch),
            format_type="merge",
        )

        time.sleep(30)

        logger.info("Step 2: Label worker nodes")

        worker_nodes = get_worker_nodes()

        label_worker_node(
            worker_nodes,
            label_key="node-role.kubernetes.io/infra-logging",
            label_value="",
        )

        label_worker_node(
            worker_nodes,
            label_key="portworx",
            label_value="true",
        )

        logger.info("Step 3: Waiting for stabilization")
        time.sleep(60)

        pod_obj = OCP(
            kind=constants.POD,
            namespace=config.ENV_DATA["cluster_namespace"],
        )

        logger.info("Step 4: Fetching metrics exporter pods")

        def get_metrics_pods():
            return get_pods_having_label(
                label="app.kubernetes.io/name=ocs-metrics-exporter",
                namespace=config.ENV_DATA["cluster_namespace"],
            )

        metrics_pods = get_metrics_pods()
        assert metrics_pods, "No ocs-metrics-exporter pods found!"

        logger.info("Step 5: Deleting metrics exporter pod(s)")
        for pod in metrics_pods:
            pod_name = pod["metadata"]["name"]
            logger.info(f"Deleting pod {pod_name}")
            pod_obj.delete(resource_name=pod_name, wait=True)

        time.sleep(30)

        logger.info("Step 6: Monitoring pod behavior")

        monitoring_duration = 300
        check_interval = 30

        uid_history = []
        terminating_events = []
        clbo_detected = False

        def is_terminating(pod):
            return pod["metadata"].get("deletionTimestamp") is not None

        def is_in_clbo(pod):
            statuses = pod["status"].get("containerStatuses", [])
            for s in statuses:
                waiting = s.get("state", {}).get("waiting", {})
                if waiting.get("reason") == "CrashLoopBackOff":
                    return True
            return False

        try:
            for _ in TimeoutSampler(
                timeout=monitoring_duration,
                sleep=check_interval,
                func=lambda: True,
            ):
                current_pods = pod_obj.get()["items"]

                for pod in current_pods:
                    if is_terminating(pod):
                        terminating_events.append(pod["metadata"]["name"])

                    if "rook-ceph-operator" in pod["metadata"]["name"]:
                        if is_in_clbo(pod):
                            clbo_detected = True

                metrics_pods = get_metrics_pods()
                for pod in metrics_pods:
                    uid_history.append(pod["metadata"]["uid"])

                logger.info(
                    f"UIDs seen so far: {set(uid_history)}, terminating: {len(terminating_events)}"
                )

        except Exception:
            logger.info("Monitoring window completed")

        logger.info("Step 7: Analyzing behavior")

        unique_uids = set(uid_history)

        logger.info(f"Observed metrics exporter pod UIDs: {unique_uids}")

        assert len(unique_uids) <= 2, (
            f"ocs-metrics-exporter pod is continuously recreated: {unique_uids}"
        )

        assert not clbo_detected, "rook-ceph-operator entered CrashLoopBackOff"

        verify_storage_cluster()

        wait_for_pods_to_be_running(
            namespace=config.ENV_DATA["cluster_namespace"], timeout=300
        )

        logger.info("Test passed: Pods stable with multiple label selectors")