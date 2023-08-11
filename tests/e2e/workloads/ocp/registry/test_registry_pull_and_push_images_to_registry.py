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

log = logging.getLogger(__name__)


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
        ocp_obj.new_project(project_name=self.project_name)

        def finalizer():
            log.info("Clean up and remove namespace")
            ocp_obj.exec_oc_cmd(command=f"delete project {self.project_name}")

            # Reset namespace to default
            ocp.switch_to_default_rook_cluster_project()
            ocp_obj.wait_for_delete(resource_name=self.project_name)

        request.addfinalizer(finalizer)

    @pytest.mark.polarion_id("OCS-1801")
    def test_registry_pull_and_push_images(self):
        """
        Test case to pull and push images
        to registry backed by OCS

        """

        # Pull and push images to registries
        log.info("Pull and push images to registries")
        image_pull_and_push(project_name=self.project_name)

        # Validate image exists in registries path
        validate_image_exists()

        # Validate image registry pods
        validate_registry_pod_status()
