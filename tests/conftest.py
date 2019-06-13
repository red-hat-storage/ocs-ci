import logging
import os.path
import pytest
import yaml

from ocs import constants
from resources.ocs import OCS


logger = logging.getLogger(__name__)


@pytest.fixture(
    params=[{
        'storageclass_name': 'invalid-storageclass',
        'provisioner': "invalid_provisioner",
        'monitors': 'invalid_monitors',
        'provision_volume': "invalid_provisioner_volume",
        'ceph_pool': 'invalid_pool',
        'root_path': 'invalid_root_path',
        'provisioner_secret_name': 'invalid_provisioner_secret_name',
        'provisioner_secret_namespace': 'invalid_provisioner_secret_namespace',
        'node_stage_secret_name': 'invalid_node_stage_secret_name',
        'node_stage_secret_namespace': 'invalid_node_stage_secret_namespace',
        'mounter': 'invalid_mounter',
        'reclaim_policy': 'Delete'
    }]  # TODO: add more test case parameters
)
def invalid_cephfs_storageclass(request):
    """
    Creates StorageClass with CephFS filesystem that have invalid parameters.
    Storageclass is removed at the end of test.

    Returns:
        str: Name of created StorageClass
    """
    logger.info(
        f"SETUP - creating storageclass "
        f"{request.param['storageclass_name']}"
    )
    yaml_path = os.path.join(
        constants.TEMPLATE_CSI_FS_DIR, "storageclass.yaml"
    )
    yaml_data = yaml.safe_load(open(yaml_path, 'r'))
    yaml_data.update(request.param)
    storageclass = OCS(**yaml_data)
    sc_data = storageclass.create()

    logger.debug('Check that storageclass has assigned creationTimestamp')
    assert sc_data['metadata']['creationTimestamp']

    yield sc_data

    logger.info(
        f"TEARDOWN - removing storageclass "
        f"{request.param['storageclass_name']}"
    )
    storageclass.delete()
