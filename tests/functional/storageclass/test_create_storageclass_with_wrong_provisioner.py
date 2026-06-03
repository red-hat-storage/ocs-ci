import pytest
import logging
from ocs_ci.helpers import helpers
from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import ManageTest, tier3, skipif_external_mode
from tests.fixtures import (
    create_ceph_block_pool,
    create_rbd_secret,
    create_cephfs_secret,
)

logger = logging.getLogger(__name__)


@green_squad
@skipif_external_mode
@tier3
@pytest.mark.usefixtures(
    create_rbd_secret.__name__,
    create_cephfs_secret.__name__,
    create_ceph_block_pool.__name__,
)
class TestCreateStorageClassWithWrongProvisioner(ManageTest):
    """
    Create Storage Class with wrong provisioner
    """

    @pytest.mark.parametrize(
        argnames="interface",
        argvalues=[
            pytest.param(*["RBD"], marks=pytest.mark.polarion_id("OCS-620")),
            pytest.param(*["CEPHFS"], marks=pytest.mark.polarion_id("OCS-621")),
        ],
    )
    def test_create_storage_class_with_wrong_provisioner(self, interface):
        """
        Test function which creates Storage Class with
        wrong provisioner and verifies PVC status
        """
        logger.test_step(
            f"Create a {interface} StorageClass with wrong provisioner "
            f"({constants.AWS_EFS_PROVISIONER})"
        )
        if interface == "RBD":
            interface_type = constants.CEPHBLOCKPOOL
            secret = self.rbd_secret_obj.name
            interface_name = self.cbp_obj.name
        else:
            interface_type = constants.CEPHFILESYSTEM
            secret = self.cephfs_secret_obj.name
            interface_name = helpers.get_cephfs_data_pool_name()
        sc_obj = helpers.create_storage_class(
            interface_type=interface_type,
            interface_name=interface_name,
            secret_name=secret,
            provisioner=constants.AWS_EFS_PROVISIONER,
        )
        logger.info(f"{interface} StorageClass: {sc_obj.name} created successfully")

        logger.test_step(f"Create PVC using StorageClass {sc_obj.name}")
        pvc_obj = helpers.create_pvc(sc_name=sc_obj.name, do_reload=False)

        logger.test_step("Verify PVC remains in Pending state for 20 seconds")
        pvc_output = pvc_obj.get()
        pvc_status = pvc_output["status"]["phase"]
        logger.info(f"Status of PVC {pvc_obj.name} after creation: {pvc_status}")
        logger.info(
            f"Waiting for status '{constants.STATUS_PENDING}' "
            f"for 20 seconds (it shouldn't change)"
        )

        pvc_obj.ocp.wait_for_resource(
            resource_name=pvc_obj.name,
            condition=constants.STATUS_PENDING,
            timeout=20,
            sleep=5,
        )
        pvc_output = pvc_obj.get()
        pvc_status = pvc_output["status"]["phase"]
        logger.assertion(
            f"PVC {pvc_obj.name} status: expected='{constants.STATUS_PENDING}', "
            f"actual='{pvc_status}'"
        )
        assert (
            pvc_status == constants.STATUS_PENDING
        ), f"PVC {pvc_obj.name} is not in {constants.STATUS_PENDING} status"
        logger.info(f"Status of {pvc_obj.name} after 20 seconds: {pvc_status}")

        logger.test_step("Delete PVC and StorageClass")
        logger.info(f"Deleting PVC: {pvc_obj.name}")
        assert pvc_obj.delete()
        logger.info(f"PVC {pvc_obj.name} deleted successfully")

        logger.info(f"Deleting StorageClass: {sc_obj.name}")
        assert sc_obj.delete()
        logger.info(f"StorageClass: {sc_obj.name} deleted successfully")
