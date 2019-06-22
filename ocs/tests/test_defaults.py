from ocs import defaults
from tests import helpers


def test_yaml_to_dict():
    assert helpers.get_crd_dict(
        defaults.CEPHFILESYSTEM_YAML
    )['apiVersion'] == 'ceph.rook.io/v1'
    assert helpers.get_crd_dict(
        defaults.CEPHFILESYSTEM_YAML
    )['spec']['metadataPool']['replicated']['size'] == 3
    assert helpers.get_crd_dict(
        defaults.CSI_PVC_DICT
    )['spec']['accessModes'] == ['ReadWriteOnce']
