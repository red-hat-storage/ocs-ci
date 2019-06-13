import logging
import os.path
import pytest

from resources.ocs import OCS
from resources.pvc import PVC
from tests import helpers
from ocs import constants, defaults
from ocs.exceptions import TimeoutExpiredError
from ocsci.testlib import tier1, ManageTest


logger = logging.getLogger(__name__)

@tier1
class TestCaseOCS331(ManageTest):
    def test_storageclass_cephfs_invalid(self, invalid_cephfs_storageclass):
        """
        Test that Persistent Volume Claim can not be created from misconfigured
        CephFS Storage Class.
        """
        pvc_data = defaults.CSI_PVC_DICT.copy()
        pvc_name = helpers.create_unique_resource_name('test', 'pvc')
        pvc_data['metadata']['name'] = pvc_name
        pvc_data['metadata']['namespace'] = defaults.ROOK_CLUSTER_NAMESPACE
        pvc_data['spec']['storageClassName'] = invalid_cephfs_storageclass[
            'metadata']['name']
        logger.info(
            f"Create PVC {pvc_name} "
            f"with storageClassName "
            f"{invalid_cephfs_storageclass['metadata']['name']}"
        )
        pvc = PVC(**pvc_data)
        pvc.create()

        pvc_status = pvc.status
        logger.debug(f"Status of PVC {pvc_name} after creation: {pvc_status}")
        assert pvc_status == constants.STATUS_PENDING

        try:
            logger.info(
                f"Waiting for status '{constants.STATUS_BOUND}' "
                f"for 60 seconds (it shouldn't change)"
            )
            pvc_status_changed = pvc.ocp.wait_for_resource(
                resource_name=pvc_name,
                condition=constants.STATUS_BOUND,
                timeout=60,
                sleep=20
            )
            logger.debug('Check that PVC status did not changed')
            assert not pvc_status_changed
        except TimeoutExpiredError:
            # raising TimeoutExpiredError is expected behavior
            pass

        pvc_status = pvc.get()['status']['phase']
        logger.info(f"Status of PVC {pvc_name} after 60 seconds: {pvc_status}")
        assert_msg = (
            f"PVC {pvc_name} hasn't reached status "
            f"{constants.STATUS_PENDING}"
        )
        assert pvc_status == constants.STATUS_PENDING, assert_msg

        logger.info(f"Deleting PVC {pvc_name}")
        pvc.delete()
