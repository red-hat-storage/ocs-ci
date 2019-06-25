from ocs import constants
from utility import templating


def test_yaml_to_dict():
    assert templating.load_yaml_to_dict(
        constants.CEPHFILESYSTEM_YAML
    )['apiVersion'] == 'ceph.rook.io/v1'
    assert templating.load_yaml_to_dict(
        constants.CEPHFILESYSTEM_YAML
    )['spec']['metadataPool']['replicated']['size'] == 3
    assert templating.load_yaml_to_dict(
        constants.CSI_PVC_YAML
    )['spec']['accessModes'] == ['ReadWriteOnce']
