import pytest
from logging import getLogger

from ocs_ci.framework import config
from ocs_ci.ocs.resources.pod import Pod
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.storage_cluster import (
    set_non_resilient_pool,
    validate_non_resilient_pool,
    StorageCluster,
)
from ocs_ci.ocs.constants import DEFAULT_CEPHBLOCKPOOL

log = getLogger(__name__)


# WIP - move functions to right modoule #
def get_failures_domain_name(cephblockpool: OCP) -> list[str]:
    failure_domains = list()
    prefix = DEFAULT_CEPHBLOCKPOOL
    cephblockpools_names = cephblockpool["items"]["metadata"]["name"]

    for name in cephblockpools_names:
        if name.startswith(prefix):
            corrected_name = name[len(prefix) :].lstrip("-")
            log.info(corrected_name)
            if corrected_name:
                failure_domains.append(corrected_name)

    return failure_domains


def get_replica_1_osd(osd: Pod) -> str:
    pass


def delete_replica_1_osd(osd: Pod):
    pass


def count_osd_pods(osd: Pod) -> int:
    pass


def delete_replica_1_sc() -> None:
    pass


def delete_replica_1_storagecluster() -> None:
    pass


# test
class TestReplicaOne:
    @pytest.fixture(scope="class")
    def setup_replica_1(self, project_factory, pvc_factory, pod_factory):
        storage_cluster = StorageCluster(
            resource_name=config.ENV_DATA["storage_cluster_name"],
            namespace=config.ENV_DATA["cluster_namespace"],
        )
        set_non_resilient_pool(storage_cluster)
        # try:
        validate_non_resilient_pool(storage_cluster)
        # except:
        #     Exception  # TODO: add proper exception
        # create_new_project()
        # create_pvc_on_zone1 https://hackmd.io/0PE0vG1RToCk8jKUgJJVkg#Create-a-pvc
        # create pod on zone1 https://hackmd.io/0PE0vG1RToCk8jKUgJJVkg#Create-a-pod-to-consume-the-pvc

        def finalizer() -> None:
            set_non_resilient_pool(storage_cluster, enable=False)
            delete_replica_1_sc()
            delete_replica_1_storagecluster()
            delete_replica_1_osd()
            # delete_pod_and_pvc

    def test_configure_replica1(self):
        pass
        # check ceph health
        # list cephblock pools
        # list new created osd

    def test_topology_validation(self):
        pass

    def test_test_expend_replica1_cluster(self):
        pass
