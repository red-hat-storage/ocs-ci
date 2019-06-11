import logging
import os.path

from resources.ocs import OCS
from tests import helpers
from ocs import defaults
from ocs.exceptions import TimeoutExpiredError


logger = logging.getLogger(__name__)

TEMPLATE_DIR = os.path.join('templates', 'ocs-deployment')
PVC_TEMPLATE = os.path.join(TEMPLATE_DIR, 'PersistentVolumeClaim.yaml')


def test_storageclass_cephfs_invalid(invalid_cephfs_storageclass, tmpdir):
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
    pvc = OCS(**pvc_data)
    pvc.create()

    pvc_status = pvc.get()['status']['phase']
    logger.debug(f"Status of PVC {pvc_name} after creation: {pvc_status}")
    assert pvc_status == 'Pending'

    try:
        logger.info(
            f"Wait 60 seconds for status of PVC {pvc_name} "
            f"to change to Bound (it shouldn't change)"
        )
        pvc_status_changed = pvc.ocp.wait_for_resource(
            resource_name=pvc_name,
            condition="Bound",
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
    assert pvc_status == 'Pending', f"PVC {pvc_name} hasn't reached status Pending"

    logger.info(f"Deleting PVC {pvc_name}")
    pvc.delete()
