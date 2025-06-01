import logging
import pytest
from uuid import uuid4

from ocs_ci.framework.pytest_customization.marks import grey_squad
from ocs_ci.framework.testlib import skipif_ocs_version
from ocs_ci.ocs.perftests import PASTest
from ocs_ci.helpers.performance_lib import run_oc_command
from ocs_ci.helpers import helpers, performance_lib
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import StorageNotSufficientException, BenchmarkTestFailed
from ocs_ci.ocs.resources import ocs, pvc

logger = logging.getLogger(__name__)
ERRMSG = "Error in command"


# @performance
# Test case is disabled
# Creating reclaim space cron job for namespace with prefix openshift-* has been deprecated since 4.16
# This will be implemented at storageclass level rather than at the namespace level in 4.17
@grey_squad
@skipif_ocs_version("<4.14")
class TestReclaimSpaceCronJobMultiClones(PASTest):
    """
    This test verifies automatic creation of Reclaim Space Cron Jobs for multiple clones of RBD PVCs in
    openshift-* namespace. It also verifies that no Reclaim Space Cron Job is created automatically any of the
    multiple clones of CephFS PVC in this namespace
    """

    def setup(self):
        logger.info("Starting the test setup")
        super(TestReclaimSpaceCronJobMultiClones, self).setup()

        self.num_of_clones = 512

        # Getting the total Storage capacity
        ceph_capacity = int(self.ceph_cluster.get_ceph_capacity())
        # Getting the free Storage capacity
        ceph_free_capacity = int(self.ceph_cluster.get_ceph_free_capacity())
        # Use 70% of the free storage capacity in the test
        capacity_to_use = int(ceph_free_capacity * 0.7)

        # since we do not want to use more then 65%, we add 35% to the needed
        # capacity, and minimum PVC size is 1 GiB
        need_capacity = int((self.num_of_clones + 2) * 1.35)
        # Test will run only on system with enough capacity
        if capacity_to_use < need_capacity:
            err_msg = (
                f"The system has only {ceph_capacity} GiB, "
                f"Of which {ceph_free_capacity} GiB is free, "
                f"we want to use  {capacity_to_use} GiB, "
                f"and we need {need_capacity} GiB to run the test"
            )
            logger.error(err_msg)
            raise StorageNotSufficientException(err_msg)

        # Calculating the PVC size in GiB
        self.pvc_size = int(capacity_to_use / (self.num_of_clones + 2))

        logger.info(
            f"Total capacity size is : {ceph_capacity} GiB, "
            f"Free capacity size is : {ceph_free_capacity} GiB, "
            f"Going to use {need_capacity} GiB, "
            f"With {self.num_of_clones} clones to {self.pvc_size} GiB PVC. "
        )

        namespace = f"openshift-{uuid4().hex}"
        self.namespace = namespace
        result = run_oc_command(cmd=f"create namespace {self.namespace}")
        assert ERRMSG not in result[0], (
            f"Failed to create namespace with name {namespace}" f"got result: {result}"
        )
        logger.info(f"Namespace {namespace} created")

        self.cloned_obj_list = []

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        """
        Cleanup the test environment
        """

        def finalizer():
            logger.info("Starting the test cleanup")

            # Delete the created clones
            try:
                for clone in self.cloned_obj_list:
                    performance_lib.run_oc_command(
                        cmd=f"delete pvc {clone}", namespace=self.namespace
                    )
            except Exception:
                logger.warning("Clones were not deleted")

            # Delete the test PVC
            self.cleanup_testing_pvc()

            # Delete the test StorageClass
            try:
                logger.info(f"Deleting the test StorageClass : {self.sc_obj.name}")
                self.sc_obj.delete()
                logger.info("Wait until the SC is deleted.")
                self.sc_obj.ocp.wait_for_delete(resource_name=self.sc_obj.name)
            except Exception as ex:
                logger.error(f"Can not delete the test sc : {ex}")

            # Delete the test storage pool

            logger.info(f"Try to delete the Storage pool {self.pool_name}")
            try:
                self.delete_ceph_pool(self.pool_name)
            except Exception:
                pass
            finally:
                if self.interface == constants.CEPHBLOCKPOOL:
                    # Verify deletion by checking the backend CEPH pools using the toolbox
                    results = self.ceph_cluster.toolbox.exec_cmd_on_pod(
                        "ceph osd pool ls"
                    )
                    logger.debug(f"Existing pools are : {results}")
                    if self.pool_name in results.split():
                        logger.warning(
                            "The pool did not deleted by CSI, forcing delete it manually"
                        )
                        self.ceph_cluster.toolbox.exec_cmd_on_pod(
                            f"ceph osd pool delete {self.pool_name} {self.pool_name} "
                            "--yes-i-really-really-mean-it"
                        )
                    else:
                        logger.info(
                            f"The pool {self.pool_name} was deleted successfully"
                        )

            run_oc_command(cmd=f"delete namespace {self.namespace}")

            super(TestReclaimSpaceCronJobMultiClones, self).teardown()

        request.addfinalizer(finalizer)

    def __create_clones(self, clone_yaml):
        """
        Creates self.num_of_clones clones of self.pvc

        Args:
            clone_yaml(str): yaml file path to be used for clone creation

        """
        self.cloned_obj_list = []
        for clone_num in range(1, self.num_of_clones + 1):
            logger.info(f"Starting cloning number {clone_num}")
            try:
                cloned_pvc_obj = pvc.create_pvc_clone(
                    sc_name=self.pvc_obj.backed_sc,
                    parent_pvc=self.pvc_obj.name,
                    pvc_name=f"pvc-clone-pas-test-{clone_num}",
                    clone_yaml=clone_yaml,
                    namespace=self.namespace,
                    storage_size=f"{self.pvc_obj.size}Gi",
                )
                helpers.wait_for_resource_state(
                    cloned_pvc_obj, constants.STATUS_BOUND, 600
                )
            except Exception as e:
                logger.error(f"Failed to create clone number {clone_num} : [{e}]")
                break
            self.cloned_obj_list.append(cloned_pvc_obj.name)

        if len(self.cloned_obj_list) != self.num_of_clones:
            logger.error("Not all clones created.")
            raise BenchmarkTestFailed("Not all clones created.")

        logger.info(f"All {self.num_of_clones} created and reached bound state.")

    def test_rbd_pvc_multiple_clone_cronjobs(
        self,
        secret_factory,
    ):
        """
        This test case does the following:
            1. Create new RBD PVC in the new openshift-** workspace created in setup
            2. Create self.num_of_clones clones of this PVC
            3. Make sure that there are num_of_clones+1 cronjobs in the workspace -- 1 for PVC and 1 for each clone
        """

        self.interface = constants.CEPHBLOCKPOOL

        self.pool_name = "pas-test-pool-rbd"
        secret = secret_factory(interface=constants.CEPHBLOCKPOOL)
        self.create_new_pool(self.pool_name)

        # Creating new StorageClass (pool) for the test.
        self.sc_obj = helpers.create_storage_class(
            interface_type=constants.CEPHBLOCKPOOL,
            interface_name=self.pool_name,
            secret_name=secret.name,
            sc_name=self.pool_name,
            fs_name=self.pool_name,
        )
        logger.info(f"The new SC is : {self.sc_obj.name}")

        self.create_testing_pvc_and_wait_for_bound()

        self.__create_clones(constants.CSI_RBD_PVC_CLONE_YAML)

        performance_lib.wait_for_cronjobs(
            self.namespace,
            self.num_of_clones + 1,  # 1 for PVC and 1 for each clone
            f"Expected number of cronjobs {self.num_of_clones+1} not found.",
        )

        logger.info(f"{self.num_of_clones+1} cronjobs found")

    def test_cephfs_pvc_multiple_clone_cronjobs(
        self,
    ):
        """
        This test case does the following:
            1. Create new CephFS PVC in the new openshift-** workspace created in setup
            2. Create self.num_of_clones clones of this PVC
            3. Make sure that there are no cronjobs in the workspace
        """

        self.interface = constants.CEPHFILESYSTEM

        self.sc_obj = ocs.OCS(
            kind="StorageCluster",
            metadata={
                "namespace": self.namespace,
                "name": constants.CEPHFILESYSTEM_SC,
            },
        )
        self.pool_name = "ocs-storagecluster-cephfilesystem"

        self.create_testing_pvc_and_wait_for_bound()

        self.__create_clones(constants.CSI_CEPHFS_PVC_CLONE_YAML)

        performance_lib.wait_for_cronjobs(
            self.namespace,
            0,
            "No cronjobs should be created for CephFS ",
        )

        logger.info("No cronjobs found")
