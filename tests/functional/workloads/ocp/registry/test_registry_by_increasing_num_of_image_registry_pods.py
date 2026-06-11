import pytest
import logging

from ocs_ci.ocs import ocp, constants
from ocs_ci.ocs.registry import (
    validate_registry_pod_status,
    image_pull_and_push,
    validate_image_exists,
    modify_registry_pod_count,
    validate_pvc_mount_on_registry_pod,
    get_registry_pod_obj,
)
from ocs_ci.framework.pytest_customization.marks import magenta_squad
from ocs_ci.framework.testlib import E2ETest, workloads
from ocs_ci.utility import prometheus
from ocs_ci.ocs.resources.pod import get_pod_logs

logger = logging.getLogger(__name__)


@magenta_squad
@workloads
class TestRegistryByIncreasingNumPods(E2ETest):
    """
    Test to increase number of registry pods
    and validate the registry pod increased
    """

    @pytest.fixture(autouse=True)
    def setup(self, request):
        """
        Setup and clean up the namespace
        """
        logger.info("Setting up test environment")
        self.project_name = "test"
        ocp_obj = ocp.OCP(kind=constants.NAMESPACES)
        ocp_obj.new_project(project_name=self.project_name)
        logger.info(f"Created test project: {self.project_name}")

        def finalizer():
            logger.info("Clean up and remove namespace")
            ocp_obj.exec_oc_cmd(command=f"delete project {self.project_name}")

            ocp.switch_to_default_rook_cluster_project()
            ocp_obj.wait_for_delete(resource_name=self.project_name)
            logger.info(f"Deleted project: {self.project_name}")

            config_obj = ocp.OCP(
                kind=constants.IMAGE_REGISTRY_CONFIG,
                namespace=constants.OPENSHIFT_IMAGE_REGISTRY_NAMESPACE,
            )
            replica_count = config_obj.get().get("spec").get("replicas")
            logger.info(f"Current registry replica count: {replica_count}")
            if replica_count != 2:
                logger.info("Resetting registry replica count to 2")
                modify_registry_pod_count(count=2)
                validate_registry_pod_status()
                logger.info("Registry replica count reset to 2")

        request.addfinalizer(finalizer)

    @pytest.mark.polarion_id("OCS-1900")
    def test_registry_by_increasing_num_of_registry_pods(self, threading_lock, count=3):
        """
        Test registry by increasing number of registry pods and
        validate all the image-registry pod should have the same PVC backend.

        """
        logger.test_step(f"Increase registry replica count to {count}")
        api = prometheus.PrometheusAPI(threading_lock=threading_lock)

        assert modify_registry_pod_count(
            count
        ), "Number of registry pod doesn't match the count"
        logger.info(f"Registry replica count increased to {count}")

        logger.test_step("Validate registry pod status and PVC mounting")
        validate_registry_pod_status()
        logger.info("All registry pods are in Running state")

        validate_pvc_mount_on_registry_pod()
        logger.info("Validated PVC is mounted on all registry pods")

        logger.test_step("Pull and push images to registry")
        logger.info(f"Pulling and pushing images to project: {self.project_name}")
        image_pull_and_push(project_name=self.project_name)

        logger.test_step("Validate images exist in registry")
        validate_image_exists()
        logger.info("Images validated successfully in registry")

        logger.test_step("Reduce registry replica count to 2")
        assert modify_registry_pod_count(count=2)
        logger.info("Registry replica count reduced to 2")

        validate_registry_pod_status()
        logger.info("All registry pods are in Running state after scale down")

        logger.test_step("Verify no inode filling alert is triggered (BZ 2128263)")
        logger.info(
            f"Checking for alert: {constants.ALERT_KUBEPERSISTENTVOLUMEINODESFILLINGUP}"
        )
        alerts = api.wait_for_alert(
            name=constants.ALERT_KUBEPERSISTENTVOLUMEINODESFILLINGUP,
            timeout=100,
            sleep=1,
        )
        logger.assertion(
            f"Alert check: alert={constants.ALERT_KUBEPERSISTENTVOLUMEINODESFILLINGUP}, "
            f"triggered={len(alerts) > 0}, expected=False"
        )
        assert (
            len(alerts) == 0
        ), f"Failed: There should be no {constants.ALERT_KUBEPERSISTENTVOLUMEINODESFILLINGUP} alert"
        logger.info("Verified no inode filling alert triggered")

        logger.test_step(
            "Validate registry pod logs do not contain kubelet volume stats"
        )
        registry_pod_objs = get_registry_pod_obj()
        kubelet_volume_stats = "kubelet_volume_stats_inodes"
        logger.info(f"Checking {len(registry_pod_objs)} registry pod logs")

        for pod in registry_pod_objs:
            logger.debug(f"Checking logs for pod: {pod.name}")
            pod_logs = get_pod_logs(
                pod_name=pod.name,
                namespace=constants.OPENSHIFT_IMAGE_REGISTRY_NAMESPACE,
            )
            logger.assertion(
                f"Kubelet volume stats check: pod={pod.name}, "
                f"message='{kubelet_volume_stats}', present={kubelet_volume_stats in pod_logs}"
            )
            assert not (
                kubelet_volume_stats in pod_logs
            ), f"Logs should not contain '{kubelet_volume_stats}'"
        logger.info(f"Verified logs do not contain '{kubelet_volume_stats}'")
