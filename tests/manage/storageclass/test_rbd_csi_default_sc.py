"""
Basic test for creating PVC with default StorageClass - RBD-CSI
"""

import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import tier1, ManageTest, skipif_external_mode
from ocs_ci.helpers import helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import ResourceLeftoversException
from tests.fixtures import (
    create_ceph_block_pool,
    create_rbd_secret,
)

log = logging.getLogger(__name__)


@pytest.fixture()
def resources(request):
    """
    Delete the resources created during the test
    Returns:
        tuple: empty lists of resources
    """
    pod, pvc, storageclass = ([] for _ in range(3))

    def finalizer():
        """
        Delete the resources created during the test
        """
        failed_to_delete = []
        for resource_type in pod, pvc, storageclass:
            for resource in resource_type:
                resource.delete()
                try:
                    resource.ocp.wait_for_delete(resource.name)
                except TimeoutError:
                    failed_to_delete.append(resource)
                if resource.kind == constants.PVC:
                    log.info("Checking whether PV is deleted")
                    assert helpers.validate_pv_delete(resource.backed_pv)
        if failed_to_delete:
            raise ResourceLeftoversException(
                f"Failed to delete resources: {failed_to_delete}"
            )

    request.addfinalizer(finalizer)

    return pod, pvc, storageclass


@green_squad
@skipif_external_mode
@tier1
@pytest.mark.usefixtures(
    create_ceph_block_pool.__name__,
    create_rbd_secret.__name__,
)
@pytest.mark.polarion_id("OCS-347")
class TestBasicPVCOperations(ManageTest):
    """
    Testing default storage class creation and pvc creation
    with rbd pool
    """

    def test_ocs_347(self, resources):
        pod, pvc, storageclass = resources

        log.info("Creating RBD StorageClass")
        storageclass.append(
            helpers.create_storage_class(
                interface_type=constants.CEPHBLOCKPOOL,
                interface_name=self.cbp_obj.name,
                secret_name=self.rbd_secret_obj.name,
            )
        )
        log.info("Creating a PVC")
        pvc.append(helpers.create_pvc(sc_name=storageclass[0].name))
        for pvc_obj in pvc:
            helpers.wait_for_resource_state(pvc_obj, constants.STATUS_BOUND)
            pvc_obj.reload()
        log.info(f"Creating a pod on with pvc {pvc[0].name}")
        pod_obj = helpers.create_pod(
            interface_type=constants.CEPHBLOCKPOOL,
            pvc_name=pvc[0].name,
            pod_dict_path=constants.NGINX_POD_YAML,
        )
        pod.append(pod_obj)
        helpers.wait_for_resource_state(pod_obj, constants.STATUS_RUNNING)
        pod_obj.reload()
