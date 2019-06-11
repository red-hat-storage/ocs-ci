from ocs import defaults


def test_yaml_to_dict():
    assert defaults.CEPHFILESYSTEM_DICT['apiVersion'] == 'ceph.rook.io/v1'
    assert defaults.CEPHFILESYSTEM_DICT['spec']['metadataPool']['replicated']['size'] == 3
    assert defaults.CSI_PVC_DICT['spec']['accessModes'] == ['ReadWriteOnce']
