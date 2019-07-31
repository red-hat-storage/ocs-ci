import pytest
import logging

from ocs_ci.framework.testlib import ManageTest, tier1

logger = logging.getLogger(__name__)


class TestJenkinsSimulation(ManageTest):
    """
    Run simulation for "Jenkins" - git clone
    """

    @tier1
    def test_git_clone(self, rbd_pod_factory):
        """
        git clones a large repository
        """
        self.pod_obj = rbd_pod_factory()
        self.pod_obj.run_git_clone()
