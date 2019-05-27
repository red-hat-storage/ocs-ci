import logging
import os.path
import time
import yaml

from ocs import ocp
from ocs import defaults
from ocsci import tier1
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
    logger.info(
        f"Create PVC {pvc_name} "
        f"with storageClassName "
        f"{invalid_cephfs_storageclass['metadata']['name']}"
    )
    pvc_yaml_content = templating.generate_yaml_from_jinja2_template_with_data(
        PVC_TEMPLATE,
        pvc_name=pvc_name,
        storageclass_name=invalid_cephfs_storageclass['metadata']['name']
    )
    temp_pvc_file = tmpdir.join('pvc.yaml')
    temp_pvc_file.write(
        yaml.dump(pvc_yaml_content)
    )
    pvc.create(yaml_file=temp_pvc_file)

    pvc_status = pvc.get(resource_name=pvc_name)['status']['phase']
    logger.debug(f"Status of PVC {pvc_name} after creation: {pvc_status}")
    assert pvc_status == 'Pending'

    logger.info('Wait for 60 seconds')
    time.sleep(60)

    pvc_status = pvc.get(resource_name=pvc_name)['status']['phase']
    logger.info(f"Status of PVC {pvc_name} after 60 seconds: {pvc_status}")
    assert pvc_status == 'Pending'

    logger.info(f"Deleting PVC {pvc_name}")
    pvc.delete(yaml_file=temp_pvc_file)
