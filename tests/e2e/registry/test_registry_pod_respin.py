import pytest
import logging


from ocs_ci.ocs.constants import OPENSHIFT_IMAGE_REGISTRY_NAMESPACE
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.ocs.registry import (
    validate_registry_pod_status,
    image_pull, image_push, image_list_all, image_rm,
    check_image_exists_in_registry
)
from ocs_ci.framework.testlib import E2ETest, workloads
from ocs_ci.utility.svt import svt_setup, svt_cleanup
from tests import disruption_helpers
from tests.sanity_helpers import Sanity

log = logging.getLogger(__name__)
IMAGE_URL = 'docker.io/library/busybox'


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
    def teardown(self, request):
        """
        Clean up svt

        """
        def finalizer():
            log.info("Remove image from registry")
            image_rm(registry_path=self.image_path, image_url=IMAGE_URL)
            log.info("Calling svt cleanup")
            assert svt_cleanup(), "Failed to cleanup svt"
        request.addfinalizer(finalizer)

    @pytest.mark.parametrize(
        argnames=[
            "pod_name", "iterations"
        ],
        argvalues=[
            pytest.param(
                *['mon', 5], marks=pytest.mark.polarion_id("OCS-1797")
            ),
            pytest.param(
                *['osd', 5], marks=pytest.mark.polarion_id("OCS-1798")
            ),
            pytest.param(
                *['mgr', 5], marks=pytest.mark.polarion_id("OCS-1799")
            ),
            pytest.param(
                *['mds', 5], marks=pytest.mark.polarion_id("OCS-1790")
            )
        ]
    )
    def test_registry_respin_pod(self, pod_name, iterations):
        """
        Test registry workload when backed by OCS respin of ceph pods
        """

        # Respin relevant pod
        log.info(f"Respin Ceph pod {pod_name}")
        disruption = disruption_helpers.Disruptions()
        disruption.set_resource(resource=f'{pod_name}')
        disruption.delete_resource()

        # Start SVT workload for pushing images to registry
        svt_setup(iterations=iterations)

        # Image pull and push to registry
        image_pull(image_url=IMAGE_URL)
        self.image_path = image_push(
            image_url=IMAGE_URL, namespace=OPENSHIFT_IMAGE_REGISTRY_NAMESPACE
        )

        # List the images in registry
        img_list = image_list_all()
        log.info(f"Image list {img_list}")

        # Check either image present in registry or not
        validate = check_image_exists_in_registry(image_url=IMAGE_URL)
        if not validate:
            raise UnexpectedBehaviour("Image URL not present in registry")

        # Validate image registry pods
        validate_registry_pod_status()

        # Validate cluster health ok and all pods are running
        self.sanity_helpers.health_check()
