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

logger = logging.getLogger(__name__)


@magenta_squad
@workloads
class TestRegistryPullAndPushImagestoRegistry(E2ETest):
    """
    Test to pull and push images
    to registry backed by OCS
    """

    @pytest.fixture(autouse=True)
    def setup(self, request):
        """
        Setup and clean up the namespace
        """

        self.project_name = "test"
        ocp_obj = ocp.OCP(kind=constants.NAMESPACES)
        logger.info(f"Creating test project: {self.project_name}")
        ocp_obj.new_project(project_name=self.project_name)

        def finalizer():
            logger.info(f"Cleaning up and removing namespace: {self.project_name}")
            ocp_obj.exec_oc_cmd(command=f"delete project {self.project_name}")

            ocp.switch_to_default_rook_cluster_project()
            ocp_obj.wait_for_delete(resource_name=self.project_name)
            logger.info("Namespace cleanup completed")

        request.addfinalizer(finalizer)

    @pytest.mark.polarion_id("OCS-1801")
    def test_registry_pull_and_push_images(self):
        """
        Test case to pull and push images
        to registry backed by OCS

        """
        logger.test_step("Pull and push images to registry")
        logger.info(f"Pulling and pushing images to project: {self.project_name}")
        image_pull_and_push(project_name=self.project_name)
        logger.info("Images pulled and pushed successfully")

        logger.test_step("Validate image exists in registry path")
        validate_image_exists()
        logger.info("Images validated successfully in registry")

        logger.test_step("Validate image registry pods are running")
        validate_registry_pod_status()
        logger.info("All registry pods are in Running state")
