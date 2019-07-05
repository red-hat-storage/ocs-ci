import pytest
import logging

from ocs_ci.framework.testlib import ManageTest, workloads, acceptance

logger = logging.getLogger(__name__)


@pytest.fixture()
def pod(request, pod_factory, interface_iterate):
    pod = pod_factory(interface=interface_iterate)
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
