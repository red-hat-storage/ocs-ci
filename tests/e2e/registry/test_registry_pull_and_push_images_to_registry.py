import pytest
import logging

from ocs_ci.ocs.constants import OPENSHIFT_IMAGE_REGISTRY_NAMESPACE
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.ocs.registry import (
    validate_registry_pod_status,
    image_pull, image_push, image_list_all, image_rm,
    check_image_exists_in_registry
)
from ocs_ci.framework.testlib import E2ETest, workloads, tier1

log = logging.getLogger(__name__)
IMAGE_URL = 'docker.io/library/busybox'


@workloads
@tier1
class TestRegistryPullAndPushImagestoRegistry(E2ETest):
    """
    Test to pull and push images
    to registry backed by OCS
    """

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        """
        Clean up svt
        """
        self.image_path = None

        def finalizer():
            log.info("Remove image from registry")
            image_rm(registry_path=self.image_path, image_url=IMAGE_URL)

        request.addfinalizer(finalizer)

    @pytest.mark.polarion_id("OCS-1801")
    def test_registry_pull_and_push_images(self):
        """
        Test case to pull and push images
        to registry backed by OCS

        """

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
