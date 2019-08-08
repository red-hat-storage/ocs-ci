"""
Basic test for creating PVC with default StorageClass - RBD-CSI
"""

import logging
import pytest

from ocs_ci.framework.testlib import tier1, ManageTest
from tests import helpers
from ocs_ci.ocs import constants
from tests.fixtures import (
    create_ceph_block_pool, create_rbd_secret,
)

log = logging.getLogger(__name__)


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

    def test_ocs_347(self):
        log.info("Creating RBD StorageClass")
        sc_obj = helpers.create_storage_class(
            interface_type=constants.CEPHBLOCKPOOL,
            interface_name=self.cbp_obj.name,
            secret_name=self.rbd_secret_obj.name,
        )

        log.info("Creating a PVC")
        pvc_obj = helpers.create_pvc(
            sc_name=sc_obj.name, wait=True,
        )

        log.info(
            f"Creating a pod on with pvc {pvc_obj.name}"
        )
        pod_obj = helpers.create_pod(
            interface_type=constants.CEPHBLOCKPOOL, pvc_name=pvc_obj.name,
            desired_status=constants.STATUS_RUNNING, wait=True,
            pod_dict_path=constants.NGINX_POD_YAML
        )

        log.info("Deleting Pod")
        pod_obj.delete()

        log.info("Deleting PVC")
        pvc_obj.delete()

        log.info("Checking whether PV is deleted")
        assert helpers.validate_pv_delete(pvc_obj.backed_pv)

        log.info(f"Deleting RBD StorageClass {sc_obj.name}")
        sc_obj.delete()
