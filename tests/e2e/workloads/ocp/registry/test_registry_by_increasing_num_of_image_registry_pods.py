import pytest
import logging

from ocs_ci.ocs import ocp, constants
from ocs_ci.ocs.registry import (
    validate_registry_pod_status,
    image_pull_and_push,
    validate_image_exists,
    modify_registry_pod_count,
    validate_pvc_mount_on_registry_pod,
)
from ocs_ci.framework.testlib import E2ETest, workloads, bugzilla

log = logging.getLogger(__name__)


@workloads
@bugzilla("1981639")
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

        self.project_name = "test"
        ocp_obj = ocp.OCP(kind=constants.NAMESPACES)
        ocp_obj.new_project(project_name=self.project_name)

        def finalizer():
            log.info("Clean up and remove namespace")
            ocp_obj.exec_oc_cmd(command=f"delete project {self.project_name}")

            # Reset namespace to default
            ocp.switch_to_default_rook_cluster_project()
            ocp_obj.wait_for_delete(resource_name=self.project_name)

            # Validate replica count is set to 2
            config_obj = ocp.OCP(
                kind=constants.IMAGE_REGISTRY_CONFIG,
                namespace=constants.OPENSHIFT_IMAGE_REGISTRY_NAMESPACE,
            )
            replica_count = config_obj.get().get("spec").get("replicas")
            if replica_count != 2:
                modify_registry_pod_count(count=2)

                # Validate image registry pods
                validate_registry_pod_status()

        request.addfinalizer(finalizer)

    @pytest.mark.polarion_id("OCS-1900")
    def test_registry_by_increasing_num_of_registry_pods(self, count=3):
        """
        Test registry by increasing number of registry pods and
        validate all the image-registry pod should have the same PVC backend.

        """
        # Increase the replica count to 3
        assert modify_registry_pod_count(
            count
        ), "Number of registry pod doesn't match the count"

        # Validate image registry pods
        validate_registry_pod_status()

        # Validate pvc mounted on image registry pod
        validate_pvc_mount_on_registry_pod()

        # Pull and push images to registries
        log.info("Pull and push images to registries")
        image_pull_and_push(
            project_name=self.project_name,
            template="eap-cd-basic-s2i",
            image="registry.redhat.io/jboss-eap-7-tech-preview/eap-cd-openshift-rhel8:latest",
            pattern="eap-app",
        )

        # Validate image exists in registries path
        validate_image_exists(namespace=self.project_name)

        # Reduce number to 2
        assert modify_registry_pod_count(count=2)

        # Validate image registry pods
        validate_registry_pod_status()
