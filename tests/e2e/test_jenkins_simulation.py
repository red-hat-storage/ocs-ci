import pytest
import logging

from ocs_ci.framework.testlib import ManageTest, tier1
from tests.fixtures import (
    create_rbd_storageclass, create_rbd_pod, create_pvc, create_ceph_block_pool,
    create_rbd_secret, delete_pvc, delete_pod, create_project
)

logger = logging.getLogger(__name__)


@pytest.mark.usefixtures(
    create_rbd_secret.__name__,
    create_ceph_block_pool.__name__,
    create_rbd_storageclass.__name__,
    create_project.__name__,
    create_pvc.__name__,
    create_rbd_pod.__name__,
    delete_pvc.__name__,
    delete_pod.__name__,
)
class TestJenkinsSimulation(ManageTest):
    """
    Run simulation for "Jenkins" - git clone
    """

    @tier1
    def test_git_clone(self):
        """
        git clones a large repository
        """
        self.pod_obj.run_git_clone()
