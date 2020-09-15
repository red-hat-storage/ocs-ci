import pytest
import logging

from ocs_ci.ocs import ocp, constants
from ocs_ci.ocs.registry import (
    validate_registry_pod_status, image_pull_and_push
)
from ocs_ci.framework.testlib import E2ETest, workloads

log = logging.getLogger(__name__)


@workloads
class TestRegistryPullAndPushImagestoRegistry(E2ETest):
    """
    Test to pull and push images
    to registry backed by OCS
    """

    @pytest.fixture(autouse=True)
    def setup(self, request):
        """
        Clean up svt
        """
        self.project_name = 'test'

        def finalizer():
            ocp_obj = ocp.OCP(kind=constants.NAMESPACES)
            log.info("Clean up and remove namespace")
            ocp_obj.exec_oc_cmd(command=f'delete project {self.project_name}')

            # Reset namespace to default
            ocp.switch_to_default_rook_cluster_project()
            ocp_obj.wait_for_delete(resource_name='test')

        request.addfinalizer(finalizer)

    @pytest.mark.polarion_id("OCS-1801")
    def test_registry_pull_and_push_images(self):
        """
        Test case to pull and push images
        to registry backed by OCS

        """

        # Pull and push images to registries
        log.info("Pull and push images to registries")
        image_pull_and_push(
            project_name=self.project_name, template='eap-cd-basic-s2i',
            image='registry.redhat.io/jboss-eap-7-tech-preview/eap-cd-openshift-rhel8:latest',
            pattern='eap-app'
        )

        # Validate image registry pods
        validate_registry_pod_status()
