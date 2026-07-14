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

log = logging.getLogger(__name__)


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
        log.info(f"Creating a {interface} storage class")
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
        log.info(f"{interface}Storage class: {sc_obj.name} created successfully")

        # Create PVC
        pvc_obj = helpers.create_pvc(sc_name=sc_obj.name, do_reload=False)

        # Check PVC status
        pvc_output = pvc_obj.get()
        pvc_status = pvc_output["status"]["phase"]
        log.info(f"Status of PVC {pvc_obj.name} after creation: {pvc_status}")
        log.info(
            f"Waiting for status '{constants.STATUS_PENDING}' "
            f"for 20 seconds (it shouldn't change)"
        )

        pvc_obj.ocp.wait_for_resource(
            resource_name=pvc_obj.name,
            condition=constants.STATUS_PENDING,
            timeout=20,
            sleep=5,
        )
        # Check PVC status again after 20 seconds
        pvc_output = pvc_obj.get()
        pvc_status = pvc_output["status"]["phase"]
        assert_msg = (
            f"PVC {pvc_obj.name} is not in {constants.STATUS_PENDING} " f"status"
        )
        assert pvc_status == constants.STATUS_PENDING, assert_msg
        log.info(f"Status of {pvc_obj.name} after 20 seconds: {pvc_status}")

        # Delete PVC
        log.info(f"Deleting PVC: {pvc_obj.name}")
        assert pvc_obj.delete()
        log.info(f"PVC {pvc_obj.name} delete successfully")

        # Delete Storage Class
        log.info(f"Deleting Storageclass: {sc_obj.name}")
        assert sc_obj.delete()
        log.info(f"Storage Class: {sc_obj.name} deleted successfully")
