import pytest
import logging

from tests import helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.utility import templating
from ocs_ci.framework.testlib import ManageTest, tier1
from tests.fixtures import (
    create_ceph_block_pool,
    create_rbd_secret, create_cephfs_secret
)

log = logging.getLogger(__name__)

SC_OBJ = None


@tier1
@pytest.mark.usefixtures(
    create_rbd_secret.__name__,
    create_cephfs_secret.__name__,
    create_ceph_block_pool.__name__,
)
class TestVerifyAllFieldsInScYamlWithOcDescribe(ManageTest):
    """
    This class checks whether all the fields in the Storage Class
    yaml matches oc describe sc output or not
    """
    @pytest.mark.parametrize(
        argnames="interface",
        argvalues=[
            pytest.param(
                *["RBD"], marks=pytest.mark.polarion_id("OCS-521")
            ),
            pytest.param(
                *["CEPHFS"], marks=pytest.mark.polarion_id("OCS-522")
            )
        ]
    )
    def test_verify_all_fields_in_sc_yaml_with_oc_describe(self, interface):
        """
        Test function for RBD
        """
        if interface == "RBD":
            log.info("Creating a RBD Storage Class")
            self.sc_data = templating.load_yaml_to_dict(
                constants.CSI_RBD_STORAGECLASS_YAML
            )
            self.sc_data['metadata']['name'] = (
                helpers.create_unique_resource_name(
                'test', 'csi-rbd'
                )
            )
        elif interface == "CEPHFS":
            log.info("Creating a CephFS Storage Class")
            self.sc_data = templating.load_yaml_to_dict(
                constants.CSI_CEPHFS_STORAGECLASS_YAML
                )
            self.sc_data['metadata']['name'] = (
                helpers.create_unique_resource_name(
                    'test', 'csi-cephfs'
                )
            )

        global SC_OBJ
        SC_OBJ = OCS(**self.sc_data)
        assert SC_OBJ.create()
        log.info(
            f"{interface}Storage class: {SC_OBJ.name} created successfully"
        )
        log.info(self.sc_data)

        # Get oc describe sc output
        describe_out = SC_OBJ.get("sc")
        log.info(describe_out)

        # Confirm that sc yaml details matches oc describe sc output
        value = {
            k: describe_out[k] for k in set(describe_out) - set(self.sc_data)
        }
        if len(value) == 1 and value['volumeBindingMode'] == 'Immediate':
            log.info("OC describe sc output matches storage class yaml")
        else:
            assert ("OC describe sc output didn't match storage class yaml")

        # Delete Storage Class
        log.info(f"Deleting Storageclass: {SC_OBJ.name}")
        assert SC_OBJ.delete()
        log.info(f"Storage Class: {SC_OBJ.name} deleted successfully")
        del SC_OBJ
