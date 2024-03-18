import pytest
from logging import getLogger

from ocs_ci.ocs.resources.pod import Pod
from ocs_ci.ocs.ocp import OCP

log = getLogger(__name__)


# WIP - move functions to right modoule #
def get_replica_1_osd(osd: Pod) -> str:
    pass


def delete_replica_1_osd(osd: Pod):
    pass


def get_failure_domain_name(cephblockpool: OCP) -> str:
    pass


def count_osd_pods(osd: Pod) -> int:
    pass


@pytest.fixture(scope="class")
def setup_replica_1(request):
    def finalizer():
        pass


# test
class TestReplicaOne:
    def test_configure_replica1(self):
        pass

    def test_topology_validation(self):
        pass

    def test_test_expend_replica1_cluster(self):
        pass
