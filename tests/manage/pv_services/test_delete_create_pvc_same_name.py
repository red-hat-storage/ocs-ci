"""
A test for deleting an existing PVC and create a new PVC with the same name
"""
import logging
import pytest

from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import ManageTest, tier2
from ocs_ci.helpers import helpers

logger = logging.getLogger(__name__)


@green_squad
@tier2
class TestDeleteCreatePVCSameName(ManageTest):
    """
    Automates the following test cases:
    OCS-324 - RBD: FT-OCP-PVCDeleteAndCreate-SameName: Delete PVC and create a
        new PVC with same name
    OCS-1137 - CEPHFS: FT-OCP-PVCDeleteAndCreate-SameName: Delete PVC and
        create a new PVC with same name
    """

    @pytest.mark.parametrize(
        argnames="interface",
        argvalues=[
            pytest.param(
                *[constants.CEPHBLOCKPOOL], marks=pytest.mark.polarion_id("OCS-324")
            ),
            pytest.param(
                *[constants.CEPHFILESYSTEM], marks=pytest.mark.polarion_id("OCS-1137")
            ),
        ],
    )
    def test_delete_create_pvc_same_name(
        self, interface, pvc_factory, teardown_factory
    ):
        """
        Delete PVC and create a new PVC with same name
        """
        # Create a PVC
        pvc_obj1 = pvc_factory(
            interface=interface,
            access_mode=constants.ACCESS_MODE_RWO,
            status=constants.STATUS_BOUND,
        )

        # Delete the PVC
        logger.info(f"Deleting PVC {pvc_obj1.name}")
        pvc_obj1.delete()
        pvc_obj1.ocp.wait_for_delete(pvc_obj1.name)
        logger.info(f"Deleted PVC {pvc_obj1.name}")

        # Create a new PVC with same name
        logger.info(f"Creating new PVC with same name {pvc_obj1.name}")
        pvc_obj2 = helpers.create_pvc(
            sc_name=pvc_obj1.storageclass.name,
            pvc_name=pvc_obj1.name,
            namespace=pvc_obj1.project.namespace,
            do_reload=False,
        )

        teardown_factory(pvc_obj2)

        # Check the new PVC and PV are Bound
        helpers.wait_for_resource_state(resource=pvc_obj2, state=constants.STATUS_BOUND)
        pv_obj2 = pvc_obj2.backed_pv_obj
        helpers.wait_for_resource_state(resource=pv_obj2, state=constants.STATUS_BOUND)
