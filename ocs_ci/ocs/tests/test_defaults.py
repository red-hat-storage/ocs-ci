from ocs_ci.ocs import constants
from ocs_ci.utility import templating


def test_yaml_to_dict():
    assert templating.load_yaml(
        constants.CEPHFILESYSTEM_YAML
    )['apiVersion'] == 'ceph.rook.io/v1'
    assert templating.load_yaml(
        constants.CEPHFILESYSTEM_YAML
    )['spec']['metadataPool']['replicated']['size'] == 3
    assert templating.load_yaml(
        constants.CSI_PVC_YAML
    )['spec']['accessModes'] == ['ReadWriteOnce']
