import pytest
from logging import getLogger
from typing import Optional

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    brown_squad,
    polarion_id,
    tier1,
    skipif_external_mode,
)
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.exceptions import CommandFailed
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
    DEFALUT_DEVICE_CLASS,
    VSPHERE_PLATFORM,
    RACK_LABEL,
    ZONE_LABEL,
)
from ocs_ci.helpers.helpers import create_pvc, create_pod, wait_for_resource_state
from ocs_ci.utility.utils import (
    validate_dict_values,
    compare_dictionaries,
    TimeoutSampler,
)
from ocs_ci.ocs.replica_one import (
    delete_replica_1_sc,
    get_osd_pgs_used,
    wait_for_replica1_osds,
    sequential_remove_replica1_osds,
    delete_replica1_cephblockpools_cr,
    count_osd_pods,
    get_osd_kb_used_data,
    get_device_class_from_ceph,
    get_all_osd_names_by_device_class,
    get_failure_domains,
    get_failures_domain_name,
)
from ocs_ci.ocs.resources.pvc import get_pvcs_using_storageclass
from ocs_ci.ocs.node import get_worker_nodes, get_node_objs


log = getLogger(__name__)


def _get_node_selector_for_failure_domain(
    failure_domain: str,
) -> Optional[dict[str, str]]:
    """
    Return a hard node selector if workers exist with the requested failure domain label.

    Args:
        failure_domain (str): The failure domain value to match.

    Returns:
        dict[str, str] | None: Hard node selector if workers found, None otherwise.
    """
    if config.ENV_DATA["platform"].lower() == VSPHERE_PLATFORM:
        label_key = RACK_LABEL
    else:
        label_key = ZONE_LABEL

    worker_names = get_worker_nodes()
    workers = get_node_objs(worker_names)

    workers_with_label = [
        n for n in workers if label_key in n.data["metadata"]["labels"]
    ]
    workers_in_domain = [
        n
        for n in workers_with_label
        if n.data["metadata"]["labels"][label_key] == failure_domain
    ]

    if workers_in_domain:
        log.info(
            f"Found worker(s) with {label_key}={failure_domain}, using hard node selector."
        )
        return {label_key: failure_domain}

    log.warning(
        f"No workers with {label_key}={failure_domain}. Pod will be created without node selector."
    )
    return None


def create_pod_on_failure_domain(project_factory, failure_domain: str):
    ns = project_factory().namespace
    pvc = create_pvc(
        namespace=ns,
        sc_name=REPLICA1_STORAGECLASS,
        size="80G",
        access_mode=ACCESS_MODE_RWO,
    )

    node_selector = _get_node_selector_for_failure_domain(failure_domain)

    if node_selector:
        log.info(f"Creating pod with node selector: {node_selector}")
    else:
        log.info(
            f"Creating pod without node selector for failure domain: {failure_domain}"
        )

    pod_obj = create_pod(
        interface_type=CEPHBLOCKPOOL,
        pvc_name=pvc.name,
        namespace=ns,
        node_selector=node_selector,
    )
    wait_for_resource_state(pod_obj, STATUS_RUNNING, timeout=300)
    pod_obj.reload()
    return pod_obj


@polarion_id("OCS-5720")
@brown_squad
@tier1
@skipif_external_mode
class TestReplicaOne:
    @pytest.fixture(autouse=True)
    def ensure_cleanup(self, request):
        """
        Fixture to ensure replica-1 cleanup runs even if test fails.
        """
        self.storage_cluster = None
        self.replica1_enabled = False

        def finalizer():
            if not self.replica1_enabled:
                log.info("Replica-1 was not enabled, skipping teardown")
                return
            if self.storage_cluster is not None:
                try:
                    self.replica1_teardown(self.storage_cluster)
                except (CommandFailed, TimeoutError) as e:
                    log.warning(f"Teardown error (continuing): {e}")

        request.addfinalizer(finalizer)

    def replica1_setup(self):
        # Initialize workload tracking for this test run
        self.created_projects = []
        self.created_pvcs = []
        self.created_pods = []

        log.info("Setup function called")
        storage_cluster = StorageCluster(
            resource_name=config.ENV_DATA["storage_cluster_name"],
            namespace=config.ENV_DATA["cluster_namespace"],
        )
        set_non_resilient_pool(storage_cluster)
        self.replica1_enabled = True
        validate_non_resilient_pool(storage_cluster)
        failure_domains = get_failure_domains()
        for domains in TimeoutSampler(
            timeout=180, sleep=10, func=get_failures_domain_name
        ):
            if set(failure_domains).issubset(set(domains)):
                log.info(f"All replica-1 CephBlockPools created: {domains}")
                break
        osd_names_n_id = wait_for_replica1_osds(sleep=15)
        osd_names = list(osd_names_n_id.keys())

        for osd in osd_names:
            pod = OCP(
                kind=POD,
                namespace=config.ENV_DATA["cluster_namespace"],
            )
            pod.wait_for_resource(
                condition=STATUS_RUNNING, column="STATUS", resource_name=osd
            )

        return storage_cluster

    def delete_replica1_pvcs(self):
        """
        Delete all PVCs created with replica-1 StorageClass.

        Returns:
            int: Number of PVCs deleted
        """
        log.info("Deleting PVCs created with replica-1 StorageClass")
        try:
            replica1_pvcs = get_pvcs_using_storageclass(REPLICA1_STORAGECLASS)
            if replica1_pvcs:
                log.info(
                    f"Found {len(replica1_pvcs)} PVCs using {REPLICA1_STORAGECLASS}"
                )
                for pvc_info in replica1_pvcs:
                    pvc_name = pvc_info["name"]
                    pvc_namespace = pvc_info["namespace"]
                    log.info(f"Deleting PVC {pvc_name} in namespace {pvc_namespace}")
                    pvc_obj = OCP(kind="PersistentVolumeClaim", namespace=pvc_namespace)
                    pvc_obj.delete(resource_name=pvc_name)
                    log.info(f"PVC {pvc_name} deletion initiated")
                return len(replica1_pvcs)
            else:
                log.info(f"No PVCs found using StorageClass {REPLICA1_STORAGECLASS}")
                return 0
        except (CommandFailed, ValueError) as e:
            log.error(f"Failed to delete replica-1 PVCs: {e}")
            raise

    def delete_tracked_workloads(self):
        """
        Delete all workloads tracked during test execution.

        Returns:
            dict: Summary of deleted resources
        """
        log.info("Deleting tracked workloads created during test")
        summary = {"pods": 0, "pvcs": 0, "projects": 0}

        # Delete pods first
        for pod in getattr(self, "created_pods", []):
            try:
                log.info(f"Deleting pod {pod.name} in namespace {pod.namespace}")
                pod.delete()
                summary["pods"] += 1
            except CommandFailed as e:
                log.warning(f"Failed to delete pod {pod.name}: {e}")

        # Delete PVCs
        for pvc in getattr(self, "created_pvcs", []):
            try:
                log.info(f"Deleting PVC {pvc.name} in namespace {pvc.namespace}")
                pvc.delete()
                summary["pvcs"] += 1
            except CommandFailed as e:
                log.warning(f"Failed to delete PVC {pvc.name}: {e}")

        # Delete projects last
        for project in getattr(self, "created_projects", []):
            try:
                log.info(f"Deleting project {project.namespace}")
                project.delete()
                summary["projects"] += 1
            except CommandFailed as e:
                log.warning(f"Failed to delete project {project.namespace}: {e}")

        log.info(f"Workload deletion summary: {summary}")
        return summary

    def replica1_teardown(self, storage_cluster):
        """
        Comprehensive replica-1 teardown orchestrator following 7-step documentation.

        Steps:
        1. Delete workloads using replica-1 storage
        2. Delete PVCs created with replica-1 StorageClass
        3. Mark non-resilient pool for disabling
        4. Delete replica-1 StorageClass
        5. Delete replica-1 CephBlockPools
        6. Remove replica-1 OSDs sequentially
        """
        log.info("Starting comprehensive replica-1 teardown orchestrator")

        # Capture failure domains early before deleting CephBlockPools
        log.info("Capturing failure domains before teardown")
        failure_domains = get_failure_domains()
        log.info(f"Captured failure domains: {failure_domains}")

        # Step 1: Delete workloads using replica-1 storage
        log.info("Step 1: Deleting workloads using replica-1 storage")
        workload_summary = self.delete_tracked_workloads()
        log.info(f"Step 1 completed: {workload_summary}")

        # Step 2: Delete PVCs created with replica-1 StorageClass
        log.info("Step 2: Deleting PVCs created with replica-1 StorageClass")
        deleted_pvcs = self.delete_replica1_pvcs()
        log.info(f"Step 2 completed: {deleted_pvcs} PVCs deleted")

        # Step 3: Mark non-resilient pool for disabling
        log.info("Step 3: Marking non-resilient pool for disabling")
        set_non_resilient_pool(storage_cluster, enable=False)

        # Step 4: Delete replica-1 StorageClass
        log.info("Step 4: Deleting replica-1 StorageClass")
        delete_replica_1_sc()
        log.info("StorageClass Deleted")

        # Step 5: Delete replica-1 CephBlockPools
        log.info("Step 5: Deleting replica-1 CephBlockPools")
        cephblockpools = OCP(
            kind=CEPHBLOCKPOOL, namespace=config.ENV_DATA["cluster_namespace"]
        )
        delete_replica1_cephblockpools_cr(cephblockpools)
        log.info("CephBlockPool CR Deleted")

        # Step 6: Remove replica-1 OSDs sequentially
        log.info("Step 6: Removing replica-1 OSDs sequentially")
        removal_summary = sequential_remove_replica1_osds(failure_domains)
        log.info(f"Step 6 completed: {removal_summary}")

        # Final verification
        log.info("Waiting for storage cluster to return to ready state")
        storage_cluster.wait_for_resource(
            condition=STATUS_READY, column="PHASE", timeout=1800, sleep=60
        )
        log.info("Replica-1 teardown orchestrator completed successfully")

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
            access_mode=ACCESS_MODE_RWO,
            volume_mode=VOLUME_MODE_BLOCK,
        )
        self.pod = pod_factory(
            interface=CEPHBLOCKPOOL,
            pvc=self.pvc,
            raw_block_pv=True,
        )

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

    def test_configure_replica1(self, project_factory, pod_factory):
        log.info("Starting Tier1 replica one test")

        # Call setup function directly
        self.storage_cluster = self.replica1_setup()
        failure_domains = get_failure_domains()
        testing_pod = create_pod_on_failure_domain(
            project_factory,
            failure_domain=failure_domains[0],
        )
        # Track the testing pod (note: create_pod_on_failure_domain also creates PVC but doesn't return it)
        self.created_pods.append(testing_pod)
        log.info(testing_pod)
        pgs_before_workload = get_osd_pgs_used()
        kb_before_workload = get_osd_kb_used_data()
        testing_pod.run_io(
            storage_type="fs", size="1G", bs="128k", depth=16, rate="32m", direct=1
        )
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
