import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import tier1
from ocs_ci.framework.testlib import skipif_ocs_version, ManageTest, ui
from ocs_ci.ocs.ui.block_pool import BlockPoolUI
from ocs_ci.ocs.ui.storageclass import StorageClassUI
from ocs_ci.ocs.exceptions import (
    PoolDidNotReachReadyState,
    PoolNotDeleted,
    StorageclassIsNotDeleted,
    StorageclassNotCreated,
    PoolNotCompressedAsExpected,
    PoolNotReplicatedAsNeeded,
    PoolNotFound,
    ResourceNotDeleted
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import get_fio_rw_iops
from ocs_ci.ocs.cluster import (
    validate_compression,
    validate_replica_data,
)
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.helpers.helpers import delete_all_resource_of_kind_containing_string
from ocs_ci.ocs.ocp import OCP

logger = logging.getLogger(__name__)


class TestPoolUserInterface(ManageTest):
    """
    Test Pool User Interface

    """

    @pytest.fixture(scope="function", autouse=True)
    def teardown(self, request):
        def finalizer():
            if not \
                delete_all_resource_of_kind_containing_string(search_string="storageclass-test",
                                                              kind=constants.STORAGECLASS) \
                or not delete_all_resource_of_kind_containing_string(search_string="rbd-pool-test",
                                                                     kind=constants.CEPHBLOCKPOOL):
                raise ResourceNotDeleted("Not all UI resource where deleted - check the above output")
        request.addfinalizer(finalizer)

    pvc_size = 30

    @pytest.mark.parametrize(
        argnames=["replica", "compression"],
        argvalues=[
            pytest.param(
                *[3, True], marks=pytest.mark.polarion_id("OCS-2589")
            ),
            pytest.param(
                *[3, False], marks=pytest.mark.polarion_id("OCS-2588")
            ),
            pytest.param(
                *[2, True], marks=pytest.mark.polarion_id("OCS-2587")
            ),
            pytest.param(
                *[3, False], marks=pytest.mark.polarion_id("OCS-2586")
            ),
        ],
    )
    @ui
    @tier1
    @skipif_ocs_version("<4.8")
    def test_create_delete_pool(self, setup_ui, replica, compression, pvc_factory, pod_factory, project_factory):
        """
        test create delete pool have the following workflow
        .* Create new RBD pool
        .* Associate the pool with storageclass
        .* Create PVC based on the storageclass
        .* Create POD based on the PVC
        .* Run IO on the POD
        .* Check replication and compression

        """

        block_pool_ui_object = BlockPoolUI(setup_ui)

        # Creating new pool
        pool_name, pool_status = block_pool_ui_object.create_pool(replica, compression)
        if pool_status:
            logger.info(f"Pool {pool_name} with replica {replica} and compression {compression} was created and "
                        f"is in ready state")
        else:
            block_pool_ui_object.take_screenshot()
            raise PoolDidNotReachReadyState(f"Pool {pool_name} with replica {replica} and compression {compression}"
                                            f" did not reach ready state")

        # Checking pool existence
        if block_pool_ui_object.check_pool_existence(pool_name):
            logger.info(f"Pool {pool_name} was found in pool list page")
        else:
            block_pool_ui_object.take_screenshot()
            raise PoolNotFound(f"Pool {pool_name} not found in pool page list")

        # Creating storageclass
        storageclass_object = StorageClassUI(setup_ui)
        sc_name = storageclass_object.create_rbd_storage_class(pool_name)
        if sc_name is None:
            logger.error("Storageclass was not created")
            storageclass_object.take_screenshot()
            raise StorageclassNotCreated(f"Storageclass is not found in storageclass list page")
        else:
            logger.info(f"Storageclass created with name {sc_name}")

        proj_obj = project_factory()

        # Converting the storageclass name to python object
        ocp = OCP(kind=constants.STORAGECLASS)
        sc_ocp_obj = ocp.get(resource_name=sc_name)
        sc_ocs_obj = OCS(**sc_ocp_obj)

        # Creating PVC
        pvc_obj = pvc_factory(
            project=proj_obj,
            interface=constants.CEPHBLOCKPOOL,
            storageclass=sc_ocs_obj,
            size=self.pvc_size,
        )

        # Creating POD
        pod_obj = pod_factory(
            pvc=pvc_obj
        )

        # Running IO on POD
        pod_obj.run_io("fs", size="1024m", rate="500m", runtime=0, buffer_compress_percentage=60,
                       buffer_pattern="0xdeadface", bs="8K", numjobs=10, readwrite="readwrite")
        get_fio_rw_iops(pod_obj)

        # Checking Results for compression and replication
        if compression:
            compression_result = validate_compression(pool_name)
            if compression_result is False:
                raise PoolNotCompressedAsExpected(
                    f"Pool {pool_name} compression did not reach expected value"
                )
        replica_result = validate_replica_data(pool_name, replica)
        if replica_result is False:
            raise PoolNotReplicatedAsNeeded(
                f"Pool {pool_name} not replicated to size {replica}"
            )
        from pdb import set_trace
        set_trace()
        # Deleting POD and PVC
        pod_obj.delete()
        pvc_obj.delete()

        # Delete storageclass
        if storageclass_object.delete_rbd_storage_class(sc_name):
            logger.info(f"Storageclass {sc_name} deleted")
        else:
            raise StorageclassIsNotDeleted(f"Storageclass {sc_name} is not deleted")

        # Delete pool
        if block_pool_ui_object.delete_pool(pool_name):
            logger.info(f"Pool {pool_name} was deleted successfully.")
        else:
            raise PoolNotDeleted(
                f"Pool {pool_name} was not deleted successfully")
