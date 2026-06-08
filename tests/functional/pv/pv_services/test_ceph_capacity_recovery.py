import logging
import time
import pytest

from ocs_ci.ocs.perftests import PASTest
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import (
    tier2,
    skipif_ocs_version,
    skipif_disconnected_cluster,
    skipif_external_mode,
    polarion_id,
)

from ocs_ci.ocs import node, constants
from ocs_ci.helpers import helpers
from ocs_ci.ocs.exceptions import (
    PodNotCreated,
    CephHealthException,
    TimeoutExpiredError,
)
from ocs_ci.helpers.managed_services import (
    get_used_capacity,
    verify_osd_used_capacity_greater_than_expected,
)

from ocs_ci.ocs.resources import pvc, ocs
from ocs_ci.utility.utils import TimeoutSampler, ceph_health_check_base

from ocs_ci.ocs.benchmark_operator_fio import get_file_size, BenchmarkOperatorFIO

logger = logging.getLogger(__name__)


def check_health_status():
    """
    Exec `ceph health` cmd on tools pod to determine health of cluster and logs the results

    Returns:
        boolean: True if HEALTH_OK

    """
    try:
        status = ceph_health_check_base()
        if status:
            logger.info("Health check passed")
        else:
            logger.info("Health check failed")
        return status
    except CephHealthException as e:
        # skip because ceph is not in good health
        logger.info(f"Ceph health exception received: {e}")
        return False


@pytest.mark.skip(reason="Skipping this test temporarily due to ocs-ci 12263")
@green_squad
@tier2
@polarion_id("OCS-5399")
@skipif_external_mode
@skipif_disconnected_cluster
@skipif_ocs_version("<4.12")
class TestCephCapacityRecovery(PASTest):
    def setup(self):
        """
        Setting up test parameters
        """
        logger.info("Starting the test setup")
        # Run the test in its own project (namespace)
        self.create_test_project()

        self.interface = "CephFileSystem"

        super(TestCephCapacityRecovery, self).setup()

        # Getting the total Storage capacity
        try:
            self.ceph_capacity = int(self.ceph_cluster.get_ceph_capacity())
        except Exception as err:
            err_msg = f"Failed to get Storage capacity : {err}"
            logger.exception(err_msg)
            raise Exception(err_msg)

        logger.info(
            f"Working on cluster {self.ceph_cluster.cluster_name} with capacity {self.ceph_capacity}"
        )

    def teardown(self):
        """
        Cleanup the test environment
        """
        logger.info("Starting the test cleanup")

        if self.benchmark_obj is not None:
            self.benchmark_obj.cleanup()

        # Deleting the namespace used by the test
        self.delete_test_project()

        super(TestCephCapacityRecovery, self).teardown()

    def test_capacity_recovery(
        self,
    ):
        logger.test_step("Pull performance image and prepare cluster for capacity test")
        get_used_capacity("Before pulling perf image")

        helpers.pull_images(constants.PERF_IMAGE)

        worker_nodes_list = node.get_worker_nodes()
        assert len(worker_nodes_list) > 1
        node_one = worker_nodes_list[0]

        self.sc_obj = ocs.OCS(
            kind="StorageCluster",
            metadata={
                "namespace": self.namespace,
                "name": constants.CEPHFILESYSTEM_SC,
            },
        )

        used_now = get_used_capacity("After pulling perf image")

        self.num_of_pvcs = 10
        self.pvc_size = (
            self.ceph_capacity * (1 - used_now / 100 - 0.15) / self.num_of_pvcs
        )
        self.pvc_size_str = str(self.pvc_size) + "Gi"
        logger.info(f"Creating pvs of {self.pvc_size_str} size")

        logger.test_step(
            f"Create {int(self.num_of_pvcs / 2)} PVCs with pods and clones, filling each to 95%"
        )
        pvc_list = []
        pod_list = []
        for i in range(
            int(self.num_of_pvcs / 2)
        ):  # on each loop cycle 1 pvc and 1 clone
            index = i + 1

            logger.debug(f"Start creating PVC {index}/{int(self.num_of_pvcs / 2)}")
            pvc_obj = helpers.create_pvc(
                sc_name=self.sc_obj.name,
                size=self.pvc_size_str,
                namespace=self.namespace,
                access_mode=constants.ACCESS_MODE_RWX,
            )
            helpers.wait_for_resource_state(pvc_obj, constants.STATUS_BOUND)

            logger.debug(
                f"PVC {pvc_obj.name} was successfully created in namespace {self.namespace}."
            )
            # Create a pod on one node
            logger.debug(f"Creating Pod with pvc {pvc_obj.name} on node {node_one}")

            pvc_obj.reload()

            try:
                pod_obj = helpers.create_pod(
                    interface_type=self.interface,
                    pvc_name=pvc_obj.name,
                    namespace=pvc_obj.namespace,
                    node_name=node_one,
                    pod_dict_path=constants.PERF_POD_YAML,
                )
            except Exception as e:
                logger.exception(
                    f"Pod on PVC {pvc_obj.name} was not created, exception {str(e)}"
                )
                raise PodNotCreated("Pod on PVC was not created.")

            # Confirm that pod is running on the selected_nodes
            helpers.wait_for_resource_state(
                resource=pod_obj, state=constants.STATUS_RUNNING, timeout=600
            )
            pvc_list.append(pvc_obj)
            pod_list.append(pod_obj)

            file_name = f"{pod_obj.name}-ceph_capacity_recovery"
            logger.debug(f"Starting IO on the POD {pod_obj.name}")

            filesize = int(float(self.pvc_size_str[:-2]) * 0.95)
            # Change the file size to MB for the FIO function
            file_size = f"{filesize * constants.GB2MB}M"

            logger.debug(f"Going to write file of size {file_size}")
            pod_obj.fillup_fs(
                size=file_size, fio_filename=file_name, performance_pod=True
            )
            # Wait for fio to finish
            pod_obj.get_fio_results(timeout=3600)

            get_used_capacity(f"After creation of pvc {index}")

            logger.debug(f"Start creation of clone for pvc number {index}.")
            cloned_pvc_obj = pvc.create_pvc_clone(
                sc_name=pvc_obj.backed_sc,
                parent_pvc=pvc_obj.name,
                pvc_name=f"clone-pas-test-{index}",
                clone_yaml=constants.CSI_CEPHFS_PVC_CLONE_YAML,
                namespace=pvc_obj.namespace,
                storage_size=self.pvc_size_str,
            )
            helpers.wait_for_resource_state(
                cloned_pvc_obj, constants.STATUS_BOUND, 3600
            )
            logger.debug(
                f"Finished successfully creation of clone for pvc number {index}."
            )
            get_used_capacity(f"After creation of clone {index}")

        logger.test_step("Run FIO benchmark to fill cluster capacity above 85%")
        size = get_file_size(100)
        self.benchmark_obj = BenchmarkOperatorFIO()
        self.benchmark_obj.setup_benchmark_fio(total_size=size)
        self.benchmark_obj.run_fio_benchmark_operator(is_completed=False)

        logger.info("Verify used capacity bigger than 85%")
        sample = TimeoutSampler(
            timeout=2500,
            sleep=40,
            func=verify_osd_used_capacity_greater_than_expected,
            expected_used_capacity=85.0,
        )
        if not sample.wait_for_func_status(result=True):
            logger.error("The after 2500 seconds the used capacity smaller than 85%")
            raise TimeoutExpiredError

        logger.test_step(f"Delete {len(pod_list)} pods and PVCs to recover capacity")
        get_used_capacity("Before PVCs deletion")
        check_health_status()

        for pod_obj, pvc_obj in zip(pod_list, pvc_list):
            logger.debug(f"Deleting the test POD : {pod_obj.name}")
            try:
                pod_obj.delete()
                logger.debug("Wait until the pod is deleted.")
                pod_obj.ocp.wait_for_delete(resource_name=pod_obj.name)
            except Exception as ex:
                logger.warning(f"Cannot delete the test pod : {ex}")

            # Deleting the PVC which used in the test.
            logger.debug(f"Delete the PVC : {pvc_obj.name}")
            try:
                pvc_obj.delete()
                logger.debug("Wait until the pvc is deleted.")
                pvc_obj.ocp.wait_for_delete(resource_name=pvc_obj.name)
            except Exception as ex:
                logger.warning(f"Cannot delete the test pvc : {ex}")

            get_used_capacity(f"After deletion of pvc  {pvc_obj.name}")
            check_health_status()
            time.sleep(600)

        logger.test_step("Wait for Ceph cluster health to recover after PVC deletion")
        get_used_capacity("After PVCs deletion")

        sample = TimeoutSampler(timeout=1800, sleep=30, func=check_health_status)
        if not sample.wait_for_func_status(result=True):
            logger.error("The after 1800 seconds the cluster health is still not OK")
            raise TimeoutExpiredError
        else:
            get_used_capacity("After cluster health returned to be OK")
