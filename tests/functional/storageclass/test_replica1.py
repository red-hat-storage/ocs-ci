import pytest
from logging import getLogger

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    brown_squad,
    polarion_id,
    bugzilla,
    tier1,
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
    STATUS_READY,
    REPLICA1_STORAGECLASS,
    VOLUME_MODE_BLOCK,
    CSI_RBD_RAW_BLOCK_POD_YAML,
    DEFALUT_DEVICE_CLASS,
    OPENSHIFT_STORAGE_NAMESPACE,
)
from ocs_ci.helpers.helpers import create_pvc
from ocs_ci.ocs.replica_one import (
    delete_replica_1_sc,
    get_osd_pgs_used,
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


def validate_dict_values(input_dict: dict) -> bool:
    """
    Validate that all values in the dictionary are the same when ignoring the last two digits.

    Args:
        input_dict (dict: {str:int}): The dictionary to validate.

    Returns:
        bool: True if all values pass the validation, False otherwise.

    """
    values = list(input_dict.values())
    first_value = values[0] // 100
    for value in values[1:]:
        if value // 100 != first_value:
            return False
    return True


def compare_dictionaries(
    dict1: dict, dict2: dict, known_different_keys: list, tolerance: int = 10
) -> dict:
    """
    Compares two dictionaries and returns a dictionary with the keys that have different values,
    but allow a tolerance between this values.

    Args:
        dict1 (dict): dictionary to compare.
        dict2 (dict): dictionary to compare.
        known_different_keys (list): keys to ignore from the comparison.
        tolerance (int): level of tolerance by precentage. Defaults to 10.

    Returns:
        dict: difrerences between the two dictionaries.
    """
    differences = dict()

    for key in dict1.keys():
        if key not in known_different_keys:
            value1 = dict1[key]
            value2 = dict2[key]

            if isinstance(value1, (int)) and isinstance(value2, (int)):
                # Calculate percentage difference
                max_value = max(abs(value1), abs(value2))
                if max_value != 0:
                    diff_percentage = abs(value1 - value2) / max_value * 100

                    if diff_percentage > tolerance:
                        differences[key] = (value1, value2)
                    elif 1 <= diff_percentage <= tolerance:
                        log.warning(
                            f"Key '{key}' has a {diff_percentage:.2f}% difference (values: {value1}, {value2})"
                        )
            elif value1 != value2:
                differences[key] = (value1, value2)
    log.info(f"Differences: {differences}")
    return differences


@polarion_id("OCS-5720")
@brown_squad
@bugzilla("2274175")
@tier1
class TestReplicaOne:
    @pytest.fixture(scope="class")
    def replica1_setup(self):
        log.info("Setup function called")
        storage_cluster = StorageCluster(
            resource_name=config.ENV_DATA["storage_cluster_name"],
            namespace=config.ENV_DATA["cluster_namespace"],
        )
        set_non_resilient_pool(storage_cluster)
        validate_non_resilient_pool(storage_cluster)
        storage_cluster.wait_for_resource(
            condition=STATUS_READY, column="PHASE", timeout=180, sleep=15
        )
        return storage_cluster

    @pytest.fixture(scope="class")
    def replica1_teardown(self, request, replica1_setup):
        yield
        log.info("Teardown function called")
        storage_cluster = replica1_setup
        cephblockpools = OCP(kind=CEPHBLOCKPOOL, namespace=OPENSHIFT_STORAGE_NAMESPACE)
        set_non_resilient_pool(storage_cluster, enable=False)
        delete_replica_1_sc()
        log.info("StorageClass Deleted")
        delete_replica1_cephblockpools_cr(cephblockpools)
        log.info("CephBlockPool CR Deleted")
        purge_replica1_osd()
        storage_cluster.wait_for_resource(
            condition=STATUS_READY, column="PHASE", timeout=1800, sleep=60
        )

    def test_cluster_before_configuration(
        self, pod_factory, pvc_factory, project_factory
    ):
        self.osd_before_test = count_osd_pods()
        self.kb_before_workload = get_osd_kb_used_data()
        log.info(f"{self.kb_before_workload} KB used before test")
        self.device_class_before_test = get_device_class_from_ceph()
        log.info(f"{self.device_class_before_test} device class detected")
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
        log.info(testing_pod)
        pgs_before_workload = get_osd_pgs_used()
        kb_before_workload = get_osd_kb_used_data()
        testing_pod.run_io(storage_type="fs", size="50g")
        testing_pod.get_fio_results()
        pgs_after_workload = get_osd_pgs_used()
        log.info(
            f"{pgs_before_workload} PGS before test\n{pgs_after_workload} PGS after test"
        )
        kb_after_workload = get_osd_kb_used_data()
        osds = get_device_class_from_ceph()
        osd_number = get_all_osd_names_by_device_class(osds, failure_domains[0])
        diff = compare_dictionaries(kb_before_workload, kb_after_workload, osd_number)
        assert not diff, "KB amount in used OSD is not equal"
