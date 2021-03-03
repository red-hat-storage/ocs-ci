import pytest
import logging

from ocs_ci.framework.testlib import ManageTest, workloads, acceptance
from ocs_ci.ocs import constants
from ocs_ci.utility import templating

logger = logging.getLogger(__name__)


@pytest.fixture()
def pod(request, pvc_factory, pod_factory, interface_iterate):
    pvc_obj = pvc_factory(interface=interface_iterate, status=constants.STATUS_BOUND)
    pod_dict = templating.load_yaml(constants.CSI_CEPHFS_POD_YAML)
    pod_dict["spec"]["containers"][0]["image"] = "quay.io/ocsci/git-ubuntu-image"
    pod_dict["spec"]["volumes"][0]["persistentVolumeClaim"]["claimName"] = pvc_obj.name
    pod = pod_factory(custom_data=pod_dict, interface=interface_iterate, pvc=pvc_obj)
    return pod


class TestJenkinsSimulation(ManageTest):
    """
    Run simulation for "Jenkins" - git clone
    """

    @acceptance
    @workloads
    def test_git_clone(self, pod):
        """
        git clones a large repository
        """
        pod.run_git_clone()
