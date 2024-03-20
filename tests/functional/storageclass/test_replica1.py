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
    STORAGECLASS,
)
from ocs_ci.helpers.helpers import create_pvc


log = getLogger(__name__)

REPLICA1_STORAGECLASS = "ocs-storagecluster-ceph-non-resilient-rbd"
config.ENV_DATA["worker_availability_zones"] = ["us-south-1", "us-south-2"]


# WIP - move functions to right modoule #
def get_failures_domain_name(cpb_object: OCP) -> list[str]:
    failure_domains = list()
    cephblockpools_names = list()
    prefix = DEFAULT_CEPHBLOCKPOOL
    for i in range(0, len((cpb_object.data["items"]))):
        cephblockpools_names.append(cpb_object.data["items"][i]["metadata"]["name"])

    log.info(f"Cephblockpool names:{cephblockpools_names}")

    for name in cephblockpools_names:
        if name.startswith(prefix):
            corrected_name = name[len(prefix) :].lstrip("-")
            log.info(corrected_name)
            if corrected_name:
                failure_domains.append(corrected_name)

    log.info(f"Failure domains:{failure_domains}")

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
    sc_obj = OCP(kind=STORAGECLASS, resource_name=REPLICA1_STORAGECLASS)
    sc_obj.delete(resource_name=REPLICA1_STORAGECLASS)


def delete_replica1_cephblockpools(cpb_object: OCP):
    for i in range(0, len((cpb_object.data["items"]))):
        resourcename = cpb_object.data["items"][i]["metadata"]["name"]
        if resourcename != DEFAULT_CEPHBLOCKPOOL:
            cpb_object.delete(resource_name=resourcename)
            log.info(f"deleting {resourcename}")


def create_replica1_pvc(project_factory):
    proj_obj = project_factory()
    proj = proj_obj.namespace
    create_pvc(
        namespace=proj,
        sc_name=REPLICA1_STORAGECLASS,
        size="1G",
        access_mode=ACCESS_MODE_RWO,
    )


def create_pod_on_failure_domain(project_factory, pod_factory, failure_domain: str):
    pvc = create_replica1_pvc(project_factory)
    pod_factory(pvc=pvc, node_selector={"topology.kubernetes.io/zone": failure_domain})


@pytest.fixture(scope="function")
def setup_rellica1(request, pod_factory, project_factory):
    log.info("setup fixture called")
    storage_cluster = StorageCluster(
        resource_name=config.ENV_DATA["storage_cluster_name"],
        namespace=config.ENV_DATA["cluster_namespace"],
    )

    set_non_resilient_pool(storage_cluster)
    validate_non_resilient_pool(storage_cluster)

    failure_domains = config.ENV_DATA["worker_availability_zones"]
    testing_pod = create_pod_on_failure_domain(
        project_factory,
        pod_factory,
        failure_domain=failure_domains[1],
    )
    log.info(testing_pod)

    def teardown() -> None:
        log.info("Teardown fixture called")
        cephblockpools = OCP(kind=CEPHBLOCKPOOL)
        set_non_resilient_pool(storage_cluster, enable=False)
        delete_replica_1_sc()
        delete_replica1_cephblockpools(cephblockpools)
        delete_pods(get_replica_1_osds())

    request.addfinalizer(teardown)


class TestReplicaOne:
    def test_configure_replica1(
        self,
    ):  # setup_rellica1):
        log.info("Starting Tier1 replica one test")
        cephblockpools = OCP(kind=CEPHBLOCKPOOL)
        get_failures_domain_name(cephblockpools)

    def test_topology_validation(self):
        pass

    def test_test_expend_replica1_cluster(self):
        pass
