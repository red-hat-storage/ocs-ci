import pytest
import logging
from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import magenta_squad
from ocs_ci.framework.testlib import E2ETest, tier1
from ocs_ci.helpers import helpers
from ocs_ci.utility import templating

logger = logging.getLogger(__name__)


@magenta_squad
@tier1
@pytest.mark.polarion_id("OCS-2085")
class TestRegistryImage(E2ETest):
    """
    Spin up a pod using custom image from openshift registry
    """

    pvc_size = 5

    def test_run_pod_local_image(self, pvc_factory, pod_factory):
        """
        Run a pod with image backed by local registry
        """
        logger.test_step(f"Create PVC with size {self.pvc_size}Gi")
        pvc_obj = pvc_factory(size=self.pvc_size)
        logger.info(f"Created PVC: {pvc_obj.name} in namespace {pvc_obj.namespace}")

        logger.test_step("Create custom fio image from Docker in local registry")
        image_obj = helpers.create_build_from_docker_image(
            namespace=pvc_obj.namespace,
            source_image_label="fio",
            image_name="fio",
            install_package="fio",
        )
        image_id = (
            image_obj.get()
            .get("status")
            .get("tags")[0]
            .get("items")[0]
            .get("dockerImageReference")
        )
        logger.info(f"Built image with ID: {image_id}")

        logger.test_step("Create pod using local registry image")
        pod_dict = templating.load_yaml(constants.CSI_CEPHFS_POD_YAML)
        pod_dict["spec"]["containers"][0]["image"] = image_obj.resource_name
        pod_dict["spec"]["volumes"][0]["persistentVolumeClaim"][
            "claimName"
        ] = pvc_obj.name
        pod_obj = pod_factory(pvc=pvc_obj, custom_data=pod_dict)
        logger.info(f"Created pod: {pod_obj.name}")

        logger.test_step("Verify pod is using the correct registry image")
        pod_image = pod_obj.get().get("status").get("containerStatuses")[0].get("image")
        logger.assertion(
            f"Image verification: expected='{image_id}', actual='{pod_image}', "
            f"match={image_id == pod_image}"
        )
        assert image_id == pod_image, f"Pod uses different image: {pod_image}"
