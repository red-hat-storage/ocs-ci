import pytest
from logging import getLogger

from ocs_ci.framework import config
from ocs_ci.ocs.resources.pod import get_pods_having_label, delete_pods
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.storage_cluster import (
    set_non_resilient_pool,
    validate_non_resilient_pool,
    StorageCluster,
)
from ocs_ci.ocs.constants import (
    DEFAULT_CEPHBLOCKPOOL,
    OSD_APP_LABEL,
    CEPHBLOCKPOOL,
    ACCESS_MODE_RWO,
)
from ocs_ci.helpers.helpers import delete_storageclasses, delete_cephblockpools

log = getLogger(__name__)

REPLICA1_STORAGECLASS = "ocs-storagecluster-ceph-non-resilient-rbd"


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


def get_replica_1_osds() -> list[OCP]:
    replica1_osds = list()
    all_osds = get_pods_having_label(label=OSD_APP_LABEL)
    for domain in get_failures_domain_name():
        for osd in all_osds:
            if osd.get(selector=f"ceph.rook.io/DeviceSet={domain}"):
                replica1_osds.append(osd)

    return replica1_osds


def count_osd_pods() -> int:
    return len(get_pods_having_label(label=OSD_APP_LABEL))


def delete_replica_1_sc() -> None:
    delete_storageclasses([REPLICA1_STORAGECLASS])

    delete_cephblockpools


def create_replica1_pvc(pvc_factory, project_factory):
    proj_obj = project_factory()
    proj = proj_obj.namespace
    pvc_factory(
        project=proj,
        interface=None,
        storageclass=REPLICA1_STORAGECLASS,
        size="1G",
        access_mode=ACCESS_MODE_RWO,
    )


def create_pod_on_failure_domain(pod_factory, failure_domain: str):
    pvc = create_replica1_pvc()
    pod_factory(pvc=pvc, nodeSelector="topology.kubernetes.io/zone: f{failure_domain}")


@pytest.fixture(scope="function")
def setup_rellica1(request, pod_factory):
    storage_cluster = StorageCluster(
        resource_name=config.ENV_DATA["storage_cluster_name"],
        namespace=config.ENV_DATA["cluster_namespace"],
    )

    set_non_resilient_pool(storage_cluster)
    validate_non_resilient_pool(storage_cluster)
    cephblockpools = OCP(kind=CEPHBLOCKPOOL)
    testing_pod = pod_factory
    log.info(testing_pod)

    def teardown() -> None:
        # delete workload pod
        # delete pvc
        set_non_resilient_pool(storage_cluster, enable=False)
        delete_replica_1_sc()
        delete_cephblockpools(cephblockpools)
        delete_pods(get_replica_1_osds())

        request.addfinalizer(teardown)


class TestReplicaOne:
    def test_configure_replica1(self):
        pass
        # check ceph health
        # list cephblock pools
        # list new created osd

    def test_topology_validation(self):
        pass

    def test_test_expend_replica1_cluster(self):
        pass
