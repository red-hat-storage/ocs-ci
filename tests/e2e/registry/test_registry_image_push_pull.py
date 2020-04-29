import logging
import pytest
from ocs_ci.framework.testlib import workloads, E2ETest, ignore_leftovers
from ocs_ci.ocs import ocp, registry, constants
from ocs_ci.framework import config
from ocs_ci.ocs.exceptions import UnexpectedBehaviour

logger = logging.getLogger(__name__)


class TestRegistryImagePullPush(E2ETest):
    """
    Test to check Image push and pull worked with registry backed by OCS
    """

    @workloads
    @ignore_leftovers
    @pytest.mark.polarion_id("OCS-1080")
    @pytest.mark.skip("Skip this test due to https://github.com/red-hat-storage/ocs-ci/issues/1547")
    def test_registry_image_pull_push(self):
        """
        Test case to validate registry image pull and push with OCS backend
        """
        image_url = 'docker.io/library/busybox'

        # Get openshift registry route and certificate access
        registry.enable_route_and_create_ca_for_registry_access()

        # Add roles to user so that user can perform image pull and push to registry
        role_type = ['registry-viewer', 'registry-editor',
                     'system:registry', 'admin', 'system:image-builder']
        for role in role_type:
            registry.add_role_to_user(
                role_type=role, user=config.RUN['username'],
                namespace=constants.OPENSHIFT_IMAGE_REGISTRY_NAMESPACE
            )

        # Provide write access to registry
        ocp_obj = ocp.OCP()
        read_only_cmd = (
            f"set env deployment.apps/image-registry"
            f" REGISTRY_STORAGE_MAINTENANCE_READONLY- -n "
            f"{constants.OPENSHIFT_IMAGE_REGISTRY_NAMESPACE}"
        )
        ocp_obj.exec_oc_cmd(read_only_cmd)

        # Pull image using podman
        registry.image_pull(image_url=image_url)

        # Push image to registry using podman
        registry.image_push(
            image_url=image_url, namespace=constants.OPENSHIFT_IMAGE_REGISTRY_NAMESPACE
        )

        # List the images in registry
        img_list = registry.image_list_all()
        logger.info(f"Image list {img_list}")

        # Check either image present in registry or not
        validate = registry.check_image_exists_in_registry(image_url=image_url)
        if not validate:
            raise UnexpectedBehaviour("Image URL not present in registry")

        # Remove user roles from User
        for role in role_type:
            registry.remove_role_from_user(
                role_type=role, user=config.RUN['username'],
                namespace=constants.OPENSHIFT_IMAGE_REGISTRY_NAMESPACE
            )
