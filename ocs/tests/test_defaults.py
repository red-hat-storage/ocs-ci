from ocs import defaults
from utility import templating


def test_yaml_to_dict():
    assert templating.get_crd_dict(
        defaults.CEPHFILESYSTEM_YAML
    )['apiVersion'] == 'ceph.rook.io/v1'
    assert templating.get_crd_dict(
        defaults.CEPHFILESYSTEM_YAML
    )['spec']['metadataPool']['replicated']['size'] == 3
    assert templating.get_crd_dict(
        defaults.CSI_PVC_DICT
    )['spec']['accessModes'] == ['ReadWriteOnce']
