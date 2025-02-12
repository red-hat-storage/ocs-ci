import pytest
from logging import getLogger

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    brown_squad,
    polarion_id,
    bugzilla,
    tier1,
    skipif_external_mode,
)
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.storage_cluster import (
    set_non_resilient_pool,
    validate_non_resilient_pool,
    StorageCluster,
)
from ocs_ci.ocs.constants import (
    CEPHBLOCKPOOL,
    ACCESS_MODE_RWO,
    POD,
    STATUS_READY,
    REPLICA1_STORAGECLASS,
    STATUS_RUNNING,
    VOLUME_MODE_BLOCK,
    CSI_RBD_RAW_BLOCK_POD_YAML,
    DEFALUT_DEVICE_CLASS,
)
from ocs_ci.helpers.helpers import create_pvc
from ocs_ci.utility.utils import validate_dict_values, compare_dictionaries
from ocs_ci.ocs.replica_one import (
    delete_replica_1_sc,
    get_osd_pgs_used,
    get_replica_1_osds,
    purge_replica1_osd,
    delete_replica1_cephblockpools_cr,
    count_osd_pods,
    get_osd_kb_used_data,
    get_device_class_from_ceph,
    get_all_osd_names_by_device_class,
    get_failure_domains,
)


log = getLogger(__name__)


def create_pod_on_failure_domain(project_factory, pod_factory, failure_domain: str):
    """
    Creates a pod on the specified failure domain.

    Args:
        failure_domain (str): Failure domain to create the pod on.

    Returns:
        Pod: Pod object
    """
    proj_obj = project_factory()
    proj = proj_obj.namespace
    pvc = create_pvc(
        namespace=proj,
        sc_name=REPLICA1_STORAGECLASS,
        size="80G",
        access_mode=ACCESS_MODE_RWO,
    )

    node = {"topology.kubernetes.io/zone": failure_domain}
    return pod_factory(pvc=pvc, node_selector=node)


@polarion_id("OCS-5720")
@brown_squad
@bugzilla("2274175")
@tier1
@skipif_external_mode
class TestReplicaOne:
    @pytest.fixture(scope="class")
    def replica1_setup(self, request):
        """
        Setup function to enable replica-1 feature.
        Skip for test_cluster_before_configuration.
        """
        storage_cluster = StorageCluster(
            resource_name=config.ENV_DATA["storage_cluster_name"],
            namespace=config.ENV_DATA["cluster_namespace"],
        )

        if not request.node.get_closest_marker("replica1_validation"):
            log.info("Setup function called")
            set_non_resilient_pool(storage_cluster)
            validate_non_resilient_pool(storage_cluster)
            storage_cluster.wait_for_resource(
                condition=STATUS_READY, column="PHASE", timeout=180, sleep=15
            )
            osd_names_n_id = get_replica_1_osds()
            osd_names = list(osd_names_n_id.keys())

            for osd in osd_names:
                pod = OCP(
                    kind=POD,
                    namespace=config.ENV_DATA["cluster_namespace"],
                    resource_name=osd,
                )
                pod.wait_for_resource(condition=STATUS_RUNNING, column="STATUS")

        yield storage_cluster

    @pytest.fixture(scope="class")
    def replica1_teardown(self, request, replica1_setup):
        yield
        log.info("Teardown function called")
        storage_cluster = replica1_setup
        cephblockpools = OCP(
            kind=CEPHBLOCKPOOL, namespace=config.ENV_DATA["cluster_namespace"]
        )
        set_non_resilient_pool(storage_cluster, enable=False)
        delete_replica_1_sc()
        log.info("StorageClass Deleted")
        delete_replica1_cephblockpools_cr(cephblockpools)
        log.info("CephBlockPool CR Deleted")
        purge_replica1_osd()
        storage_cluster.wait_for_resource(
            condition=STATUS_READY, column="PHASE", timeout=1800, sleep=60
        )

    @pytest.mark.replica1_validation
    def test_cluster_before_configuration(
        self, pod_factory, pvc_factory, project_factory
    ):
        """
        Test cluster configuration before enabling replica-1 feature.
        Skip if replica-1 is already enabled.

        Args:
            pod_factory: Factory for creating pods
            pvc_factory: Factory for creating PVCs
            project_factory: Factory for creating projects
        """
        storage_cluster = StorageCluster(
            resource_name=config.ENV_DATA["storage_cluster_name"],
            namespace=config.ENV_DATA["cluster_namespace"],
        )

        # Check if replica-1 is enabled
        if (
            storage_cluster.data.get("spec", {})
            .get("managedResources", {})
            .get("cephNonResilientPools", {})
            .get("enable")
        ):
            pytest.skip("Test requires replica-1 to be disabled initially")

        self.osd_before_test = count_osd_pods()
        log.info(f"{self.osd_before_test} OSD pods detected Before test")
        self.kb_before_workload = get_osd_kb_used_data()
        log.info(f"{self.kb_before_workload} KB used before test")
        self.device_class_before_test = get_device_class_from_ceph()
        log.info(f"{self.device_class_before_test} device class detected before test")
        self.project = project_factory()
        self.pvc = pvc_factory(
            interface=CEPHBLOCKPOOL,
            project=self.project,
            size="1",
            volume_mode=VOLUME_MODE_BLOCK,
        )
        self.pod = pod_factory(pvc=self.pvc, pod_dict_path=CSI_RBD_RAW_BLOCK_POD_YAML)

        self.pod.run_io(storage_type="fs", size="100M")
        self.kb_after_workload = get_osd_kb_used_data()
        self.pgs_used = get_osd_pgs_used()
        pgs_value = list(self.pgs_used.values())
        log.warning(
            validate_dict_values(self.kb_after_workload)
        ), f"KB amount in used OSD is not equal {self.kb_after_workload}"
        if not all(value == pgs_value[0] for value in pgs_value):
            log.warning("PGS amount in used OSD is not equal")
        assert all(
            value == DEFALUT_DEVICE_CLASS
            for value in self.device_class_before_test.values()
        ), f"Device class is not as expected. expected 'ssd', actual: {self.device_class_before_test}"

    def test_configure_replica1(
        self, replica1_setup, project_factory, pod_factory, replica1_teardown
    ):
        log.info("Starting Tier1 replica one test")
        failure_domains = get_failure_domains()
        testing_pod = create_pod_on_failure_domain(
            project_factory,
            pod_factory,
            failure_domain=failure_domains[0],
        )
        log.info(testing_pod.data)
        try:
            log.info(
                f"Pod created on {testing_pod.data['spec']['nodeSelector']['topology.kubernetes.io/zone']}"
            )
        except Exception:
            log.error(f"Pod data is {testing_pod.data}")

        pgs_before_workload = get_osd_pgs_used()
        kb_before_workload = get_osd_kb_used_data()
        testing_pod.run_io(storage_type="fs", size="50g")
        testing_pod.get_fio_results(timeout=1200)
        pgs_after_workload = get_osd_pgs_used()
        log.info(
            f"{pgs_before_workload} PGS before test\n{pgs_after_workload} PGS after test"
        )
        kb_after_workload = get_osd_kb_used_data()
        log.info(f"KB before workload: {kb_before_workload}")
        log.info(f"KB after workload: {kb_after_workload}")
        osds = get_device_class_from_ceph()
        osd_number = get_all_osd_names_by_device_class(osds, failure_domains[0])
        diff = compare_dictionaries(kb_before_workload, kb_after_workload, osd_number)
        assert not diff, "KB amount in used OSD is not equal"
