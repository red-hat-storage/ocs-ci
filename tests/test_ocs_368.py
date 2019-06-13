"""
OCS-368
"""
import logging
import pytest

from ocs import ocp, defaults
from ocsci.config import ENV_DATA
from ocsci.testlib import tier1, ManageTest
from resources.pvc import PVC
from resources.pod import Pod

log = logging.getLogger(__name__)

OCS_BUG_ID = 'test3-ocs-368'

NAMESPACE = ocp.OCP(kind='namespace', namespace=ENV_DATA['cluster_name'])
OUR_PVC = None
POD = None


@pytest.fixture(scope='class')
def test_fixture(request):
    """
    Finalize teardown and call setup
    """
    self = request.node.cls

    def finalizer():
        teardown()
    request.addfinalizer(finalizer)
    setup(self)


def setup(self):
    """
    Setting up the environment for the test

    Create namespace
    Create pvc
    Create pod
    """
    NAMESPACE.create(resource_name=OCS_BUG_ID)

    pvc_data = defaults.CSI_PVC_DICT.copy()
    pvc_data['metadata']['namespace'] = OCS_BUG_ID
    pvc_data['spec']['storageClassName'] = 'rook-ceph-block'
    pvc_data['spec']['resources']['requests']['storage'] = '100Gi'
    pvc_name = pvc_data['metadata']['name']
    global OUR_PVC
    OUR_PVC = PVC(**pvc_data)
    OUR_PVC.create()

    pod_data = defaults.CSI_RBD_POD_DICT.copy()
    pod_data['metadata']['namespace'] = OCS_BUG_ID
    first_claim = pod_data['spec']['volumes'][0]
    first_claim['persistentVolumeClaim']['claimName'] = pvc_name
    global POD
    POD = Pod(**pod_data)
    POD.create()


def teardown():
    """
    Cleanup

    Delete pod
    Delete pvc
    Delete namespace
    """
    POD.delete()
    POD.delete_temp_yaml_file()
    OUR_PVC.delete()
    OUR_PVC.delete_temp_yaml_file()
    NAMESPACE.delete(resource_name=OCS_BUG_ID)


@tier1
@pytest.mark.usefixtures(
    test_fixture.__name__,
)
class TestOcs368(ManageTest):
    """
    Actual OCS-368 test.
    """
    def test_ocs_368(self):
        pass
