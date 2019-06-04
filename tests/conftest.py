import logging
import os.path
import pytest
import yaml

from ocs import ocp
from utility import templating


logger = logging.getLogger(__name__)

TEMPLATE_DIR = os.path.join('templates', 'ocs-deployment')
SC_CEPHFS_TEMPLATE = os.path.join(TEMPLATE_DIR, 'storageclass.cephfs.yaml')


@pytest.fixture(
    params=[{
        'storageclass_name': 'invalid-storageclass',
        'provisioner': "invalid_provisioner",
        'monitors': 'invalid_monitors',
        'provision_volume': "invalid_provisioner_volume",
        'ceph_pool': 'invalid_pool',
        'provisioner_secret_name': 'invalid_provisioner_secret_name',
        'provisioner_secret_namespace': 'invalid_provisioner_secret_namespace',
        'node_stage_secret_name': 'invalid_node_stage_secret_name',
        'node_stage_secret_namespace': 'invalid_node_stage_secret_namespace',
        'mounter': 'invalid_mounter',
        'reclaim_policy': 'Delete'
    }]  # TODO: add more test case parameters
)
def invalid_cephfs_storageclass(tmpdir, request):
    """
    Creates StorageClass with CephFS filesystem that have invalid parameters.
    Storageclass is removed at the end of test.

    Returns:
        str: Name of created StorageClass
    """
    storageclass = ocp.OCP(
        kind='storageclass'
    )
    logger.info(
        f"SETUP - creating storageclass "
        f"{request.param['storageclass_name']}"
    )
    sc_yaml_content = templating.generate_yaml_from_jinja2_template_with_data(
        SC_CEPHFS_TEMPLATE,
        **request.param  # storageclass template parameters
    )
    temp_sc_file = tmpdir.join('storageclass.cephfs.yaml')
    temp_sc_file.write(yaml.dump(sc_yaml_content))
    sc_data = storageclass.create(yaml_file=temp_sc_file)

    logger.debug('Check that storageclass has assigned creationTimestamp')
    assert sc_data['metadata']['creationTimestamp']

    yield sc_data

    logger.info(
        f"TEARDOWN - removing storageclass "
        f"{request.param['storageclass_name']}"
    )
    storageclass.delete(yaml_file=temp_sc_file)
