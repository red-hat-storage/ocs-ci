import pytest
import logging

from ocs_ci.ocs import ocp, constants
from ocs_ci.ocs.registry import (
    validate_registry_pod_status,
    image_pull_and_push,
    validate_image_exists,
)
from ocs_ci.framework.pytest_customization.marks import magenta_squad
from ocs_ci.framework.testlib import E2ETest, workloads
from ocs_ci.helpers import disruption_helpers
from ocs_ci.helpers.sanity_helpers import Sanity

logger = logging.getLogger(__name__)
IMAGE_URL = "docker.io/library/busybox"


@magenta_squad
@workloads
class TestRegistryPodRespin(E2ETest):
    """
    Test to run svt workload for pushing
    images to registry and with Ceph pods respin
    """

    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance

        """
        self.sanity_helpers = Sanity()

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

        request.addfinalizer(finalizer)

    @pytest.mark.parametrize(
        argnames=["pod_name"],
        argvalues=[
            pytest.param(*["mon"], marks=pytest.mark.polarion_id("OCS-1797")),
            pytest.param(*["osd"], marks=pytest.mark.polarion_id("OCS-1798")),
            pytest.param(*["mgr"], marks=pytest.mark.polarion_id("OCS-1799")),
            pytest.param(*["mds"], marks=pytest.mark.polarion_id("OCS-1790")),
        ],
    )
    def test_registry_respin_pod(self, pod_name):
        """
        Test registry workload when backed by OCS respin of ceph pods
        """
        logger.test_step(f"Respin Ceph {pod_name} pod")
        logger.info(f"Respinning Ceph {pod_name} pod")
        disruption = disruption_helpers.Disruptions()
        disruption.set_resource(resource=f"{pod_name}")
        disruption.delete_resource()
        logger.info(f"Ceph {pod_name} pod respun successfully")

        logger.test_step("Pull and push images to registry")
        logger.info(f"Pulling and pushing images to project: {self.project_name}")
        image_pull_and_push(project_name=self.project_name)
        logger.info("Images pulled and pushed successfully")

        logger.test_step("Validate images exist in registry")
        validate_image_exists()
        logger.info("Images validated successfully in registry")

        logger.test_step("Validate registry pod status")
        validate_registry_pod_status()
        logger.info("All registry pods are in Running state")

        logger.test_step("Verify cluster and Ceph health")
        self.sanity_helpers.health_check(tries=40)
        logger.info("Cluster and Ceph health checks passed")
