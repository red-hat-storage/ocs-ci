import pytest
import logging

from ocs_ci.ocs.constants import OPENSHIFT_IMAGE_REGISTRY_NAMESPACE
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.ocs.registry import (
    validate_registry_pod_status,
    image_pull, image_push, image_list_all, image_rm,
    check_image_exists_in_registry, validate_pvc_mount_on_registry_pod,
    modify_registry_pod_count
)
from ocs_ci.framework.testlib import E2ETest, workloads

log = logging.getLogger(__name__)
IMAGE_URL = 'docker.io/library/busybox'


@workloads
class TestRegistryByIncreasingNumPods(E2ETest):
    """
    Test to increase number of registry pods
    and validate the registry pod increased
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

    @pytest.mark.parametrize(
        argnames=[
            "count"
        ],
        argvalues=[
            pytest.param(
                *[3], marks=pytest.mark.polarion_id("OCS-1900")
            )
        ]
    )
    def test_registry_respin_pod(self, count):
        """
        Test registry by increasing number of registry pods and
        validate all the image-registry pod should have the same PVC backend.

        """
        # Increase the replica count to 3
        modify_registry_pod_count(count)

        # Validate image registry pods
        validate_registry_pod_status()

        # Validate pvc mounted on image registry pod
        validate_pvc_mount_on_registry_pod()

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

        # Reduce number to 2
        modify_registry_pod_count(count)
