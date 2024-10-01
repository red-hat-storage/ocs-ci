import logging
import pytest

from ocs_ci.framework.testlib import (
    acceptance,
    provider_client_platform_required,
    tier1,
    ManageTest,
)
from ocs_ci.framework import config
from ocs_ci.ocs.resources.pod import get_ceph_csi_ctrl_pods, get_container_images
from ocs_ci.framework.pytest_customization.marks import yellow_squad
from ocs_ci.ocs.resources.storage_cluster import get_csi_images_for_client_ocp_version


log = logging.getLogger(__name__)


@yellow_squad
@tier1
@acceptance
@provider_client_platform_required
class TestCephCSIImageVersions(ManageTest):
    @pytest.fixture(autouse=True)
    def setup(self):
        """
        Save the original index
        """
        self.orig_index = config.cur_index

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        """
        Switch to the original cluster index
        """

        def finalizer():
            log.info("Switch to the original cluster index")
            config.switch_ctx(self.orig_index)

        request.addfinalizer(finalizer)

    @pytest.mark.polarion_id("OCS-6248")
    def test_client_clusters_csi_image_versions(self):
        """
        The test will perform the following:
        1. Iterate over the clients
        2. Get the ceph csi ctrl pods container images of the client
        3. Get the csi images for the client OCP version
        4. Check that the ceph csi ctr pod container images exist in the csi images

        """
        client_indices = config.get_consumer_indexes_list()
        for client_i in client_indices:
            config.switch_ctx(client_i)
            ceph_csi_ctrl_pods = get_ceph_csi_ctrl_pods()
            # Get the ceph csi ctrl pods container images of the client
            pods_container_images = set()
            for p in ceph_csi_ctrl_pods:
                pods_container_images.update(get_container_images(p))

            csi_images_for_client_ocp_version = set(
                get_csi_images_for_client_ocp_version()
            )
            log.info(f"Ceph csi ctrl pods container images: {pods_container_images}")
            log.info(
                f"csi images of client ocp version: {csi_images_for_client_ocp_version}"
            )
            assert pods_container_images.issubset(csi_images_for_client_ocp_version), (
                f"The ceph csi ctrl pod container images {pods_container_images} are not exist "
                f"in the csi images {csi_images_for_client_ocp_version}"
            )
