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

log = logging.getLogger(__name__)
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

        # Respin relevant pod
        log.info(f"Respin Ceph pod {pod_name}")
        disruption = disruption_helpers.Disruptions()
        disruption.set_resource(resource=f"{pod_name}")
        disruption.delete_resource()

        # Pull and push images to registries
        log.info("Pull and push images to registries")
        image_pull_and_push(project_name=self.project_name)

        # Validate image exists in registries path
        validate_image_exists()

        # Validate image registry pods
        validate_registry_pod_status()

        # Validate cluster health ok and all pods are running
        self.sanity_helpers.health_check(tries=40)
