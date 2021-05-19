import pytest
import logging

from ocs_ci.framework.testlib import ManageTest, workloads
from ocs_ci.ocs import constants
from ocs_ci.utility import templating
from ocs_ci.ocs.resources.pod import Pod

logger = logging.getLogger(__name__)


@pytest.fixture()
def pod(request, pvc_factory, pod_factory, interface_iterate):
    """
    Creates a pod with git pre-installed in it and attach PVC to it.
    """
    pvc_obj = pvc_factory(interface=interface_iterate, status=constants.STATUS_BOUND)
    pod_dict = templating.load_yaml(constants.CSI_CEPHFS_POD_YAML)
    # The image below is a mirror of hub.docker.com/library/alpine mirrored by Google
    pod_dict["spec"]["containers"][0]["image"] = "mirror.gcr.io/library/alpine"
    pod_dict["spec"]["containers"][0]["command"] = [
        "sh",
        "-c",
        "mkdir -p /var/www/html && tail -f /dev/null",
    ]
    pod_dict["spec"]["volumes"][0]["persistentVolumeClaim"]["claimName"] = pvc_obj.name
    ocs_obj = pod_factory(
        custom_data=pod_dict, interface=interface_iterate, pvc=pvc_obj
    )
    pod_yaml = ocs_obj.get()
    pod = Pod(**pod_yaml)
    return pod


class TestJenkinsSimulation(ManageTest):
    """
    Run simulation for "Jenkins" - git clone
    """

    @workloads
    def test_git_clone(self, pod):
        """
        git clones a large repository
        """
        pod.run_git_clone()
