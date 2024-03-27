import pytest
from logging import getLogger

from ocs_ci.framework import config
from ocs_ci.ocs.resources.pod import (
    get_pods_having_label,
    get_ceph_tools_pod,
    run_osd_removal_job,
)
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
    DEPLOYMENT,
)
from ocs_ci.helpers.helpers import create_pvc
from ocs_ci.ocs.exceptions import CommandFailed


log = getLogger(__name__)

REPLICA1_STORAGECLASS = "ocs-storagecluster-ceph-non-resilient-rbd"
config.ENV_DATA["worker_availability_zones"] = [
    "us-south-1",
    "us-south-2",
    "us-south-3",
]


# WIP - move functions to right modoule #
def get_failures_domain_name() -> list[str]:
    cpb_object = OCP(kind=CEPHBLOCKPOOL)
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


def get_replica_1_osds() -> tuple[list[str], list[str]]:
    replica1_osds_id = list()
    replica1_osds = list()
    all_osds = get_pods_having_label(label=OSD_APP_LABEL)
    for domain in config.ENV_DATA["worker_availability_zones"]:
        for osd in all_osds:
            if osd["metadata"]["labels"]["ceph.rook.io/DeviceSet"] == domain:
                replica1_osds.append(osd["metadata"]["name"])
                replica1_osds_id.append(osd["metadata"]["labels"]["ceph-osd-id"])
    log.info(replica1_osds)
    return (replica1_osds, replica1_osds_id)


def get_replica1_osd_deployment() -> list[str]:
    dep_obj = OCP(kind=DEPLOYMENT)
    deployments = dep_obj.get()["items"]
    replica1_osd_deployment = list()
    osd_deployment = list()
    for deployment in deployments:
        if (
            "metadata" in deployment
            and "labels" in deployment["metadata"]
            and "app.kubernetes.io/name" in deployment["metadata"]["labels"]
        ):
            if deployment["metadata"]["labels"]["app.kubernetes.io/name"] == "ceph-osd":
                osd_deployment.append(deployment)

    for deployment in osd_deployment:
        if (
            deployment["metadata"]["labels"]["ceph.rook.io/DeviceSet"]
            in config.ENV_DATA["worker_availability_zones"]
        ):
            log.info(deployment["metadata"]["name"])
            replica1_osd_deployment.append(deployment["metadata"]["name"])

    return replica1_osd_deployment


def scaledown_replica1_osd_deployment(deployments_name=list[str]) -> None:
    deployment_obj = OCP(kind=DEPLOYMENT)
    for deployment in deployments_name:
        deployment_obj.exec_oc_cmd(f"scale deployment {deployment} --replicas=0")
        log.info(f"scaling to 0: {deployment}")


def count_osd_pods() -> int:
    return len(get_pods_having_label(label=OSD_APP_LABEL))


def delete_replica_1_sc() -> None:
    sc_obj = OCP(kind=STORAGECLASS, resource_name=REPLICA1_STORAGECLASS)
    try:
        sc_obj.delete(resource_name=REPLICA1_STORAGECLASS)
    except CommandFailed as e:
        if "Error is Error from server (NotFound)" in str(e):
            log.info(
                f"{REPLICA1_STORAGECLASS} not found, assuming it was already deleted"
            )
        else:
            log.error("Failed to delete storage class")


def purge_replica1_osd():
    deployments_name = get_replica1_osd_deployment()
    scaledown_replica1_osd_deployment(deployments_name)
    osds_id = get_replica_1_osds(1)
    log.info(f"OSD IDs: {osds_id}")
    run_osd_removal_job(osd_ids=osds_id)


def delete_replica1_cephblockpools_cr():
    pass


def delete_replica1_cephblockpools(cpb_object: OCP):
    toolbox_pod = get_ceph_tools_pod()
    for i in range(0, len((cpb_object.data["items"]))):
        replica1_pool_name = cpb_object.data["items"][i]["metadata"]["name"]
        if replica1_pool_name != DEFAULT_CEPHBLOCKPOOL:
            command = f"ceph osd pool rm {replica1_pool_name} {replica1_pool_name} --yes-i-really-really-mean-it"
            toolbox_pod.exec_cmd_on_pod(command)

            log.info(f"deleting {replica1_pool_name}")


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
def setup_rellica1(
    request: pytest.FixtureRequest,
    pod_factory,
    project_factory,
):
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

    request.addfinalizer(teardown)


class TestReplicaOne:
    def test_configure_replica1(
        self,
    ):  # setup_rellica1):
        log.info("Starting Tier1 replica one test")
        storage_cluster = StorageCluster(
            resource_name=config.ENV_DATA["storage_cluster_name"],
            namespace=config.ENV_DATA["cluster_namespace"],
        )
        cephblockpools = OCP(kind=CEPHBLOCKPOOL)
        set_non_resilient_pool(storage_cluster, enable=False)
        delete_replica_1_sc()
        log.info("StorageClass Deleted")
        delete_replica1_cephblockpools(cephblockpools)
        deployments_name = get_replica1_osd_deployment()
        log.info(deployments_name)
        scaledown_replica1_osd_deployment(deployments_name)

    def test_topology_validation(self):
        pass

    def test_test_expend_replica1_cluster(self):
        pass
