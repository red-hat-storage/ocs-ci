import logging
import os.path
import pytest
import yaml

from ocs import ocp
from ocs import defaults
from ocsci.config import ENV_DATA
from ocsci import run_this, tier1
from utility import templating


logger = logging.getLogger(__name__)

TEMPLATE_DIR = 'templates/ocs-deployment'
PVC_TEMPLATE = os.path.join(TEMPLATE_DIR, 'PersistentVolumeClaim.yaml')


@tier1
def test_storageclass_cephfs_invalid(invalid_cephfs_storageclass, tmpdir):
    """
    Test that Persistent Volume Claim can not be created from misconfigured
    CephFS Storage Class.
    """
    pvc = ocp.OCP(
        kind='pvc',
        namespace=defaults.ROOK_CLUSTER_NAMESPACE
    )
    pvc_name = 'test-pvc'
    pvc_yaml_content = templating.generate_yaml_from_jinja2_template_with_data(
        PVC_TEMPLATE,
        pvc_name=pvc_name,
        storageclass_name=invalid_cephfs_storageclass
    )
    temp_pvc_file = tmpdir.join('pvc.yaml')
    temp_pvc_file.write(
        yaml.dump(pvc_yaml_content))
    pvc.create(yaml_file=temp_pvc_file)
    status = pvc.get(resource_name=pvc_name)['status']['phase']
    logger.info(status)
    pvc.delete(yaml_file=temp_pvc_file)
