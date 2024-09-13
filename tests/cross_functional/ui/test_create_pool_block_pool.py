import logging
import pytest
from ocs_ci.framework.pytest_customization.marks import (
    tier1,
    skipif_ui_not_support,
    skipif_hci_provider_or_client,
    green_squad,
)
from ocs_ci.framework.testlib import skipif_ocs_version, ManageTest, ui
from ocs_ci.ocs.exceptions import (
    PoolNotCompressedAsExpected,
    PoolNotReplicatedAsNeeded,
    PoolCephValueNotMatch,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import get_fio_rw_iops
from ocs_ci.ocs.cluster import (
    validate_compression,
    validate_replica_data,
    check_pool_compression_replica_ceph_level,
)
from ocs_ci.ocs.ui.block_pool import StoragePoolUI

logger = logging.getLogger(__name__)

need_to_delete = []


@skipif_ui_not_support("block_pool")
@pytest.mark.parametrize(
    argnames=["replica", "compression", "pool_type"],
    argvalues=[
        pytest.param(*[3, True], "rbd", marks=pytest.mark.polarion_id("OCS-2589")),
        pytest.param(*[3, False], "rbd", marks=pytest.mark.polarion_id("OCS-2588")),
        pytest.param(*[2, True], "rbd", marks=pytest.mark.polarion_id("OCS-2587")),
        pytest.param(*[2, False], "rbd", marks=pytest.mark.polarion_id("OCS-2586")),
        pytest.param(*[2, False], "cephfs", marks=pytest.mark.polarion_id("OCS-6215")),
    ],
)
@skipif_hci_provider_or_client
class TestPoolUserInterface(ManageTest):
    """
    Test Pool User Interface

    """

    pvc_size = 40

    @pytest.fixture()
    def namespace(self, project_factory):
        self.proj_obj = project_factory()
        self.proj = self.proj_obj.namespace

    @pytest.fixture()
    def storage(self, storageclass_factory_ui, replica, compression, pool_type="rbd"):
        if pool_type == "rbd":
            provisioner = constants.OCS_PROVISIONERS[0]
        else:
            logger.info(f"Choosing provisioner: {constants.OCS_PROVISIONERS[1]}")
            provisioner = constants.OCS_PROVISIONERS[1]
        self.sc_obj = storageclass_factory_ui(
            create_new_pool=True,
            replica=replica,
            compression=compression,
            vol_binding_mode="Immediate",
            provisioner=provisioner,
        )
        self.pool_name = self.sc_obj.get()["parameters"]["pool"]

    @pytest.fixture()
    def pvc(self, pvc_factory, pool_type="rbd"):
        if pool_type == "rbd":
            interface = constants.CEPHBLOCKPOOL
        else:
            interface = constants.CEPHFILESYSTEM
        self.pvc_obj = pvc_factory(
            project=self.proj_obj,
            interface=interface,
            storageclass=self.sc_obj,
            size=self.pvc_size,
        )

    @pytest.fixture()
    def pod(self, pod_factory):
        self.pod_obj = pod_factory(pvc=self.pvc_obj)

    @ui
    @tier1
    @skipif_ocs_version("<4.8")
    @green_squad
    def test_create_delete_pool(
        self,
        replica,
        compression,
        namespace,
        storage,
        pvc,
        pod,
        setup_ui,
        pool_type,
    ):
        """
        test create delete pool has the following workflow
        .* Create new RBD pool
        .* Associate the pool with storageclass
        .* Create PVC based on the storageclass
        .* Create POD based on the PVC
        .* Run IO on the POD
        .* Check replication and compression

        """

        if not check_pool_compression_replica_ceph_level(
            self.pool_name, compression, replica
        ):
            raise PoolCephValueNotMatch(
                f"Pool {self.pool_name} values do not match configuration"
            )
        # Running IO on POD
        self.pod_obj.run_io(
            "fs",
            size="100m",
            rate="1500m",
            runtime=0,
            buffer_compress_percentage=60,
            buffer_pattern="0xdeadface",
            bs="8K",
            jobs=5,
            readwrite="readwrite",
        )

        # Getting IO results
        get_fio_rw_iops(self.pod_obj)

        # Checking the raw capcity is loaded on the UI or not.
        storage_pool_ui_object = StoragePoolUI()
        assert storage_pool_ui_object.pool_raw_capacity_loaded(
            self.pool_name
        ), "Block pool raw capacity is not visible on UI"

        # Cross checking the raw capacity of the blockpool between CLI and UI
        assert storage_pool_ui_object.cross_check_raw_capacity(
            self.pool_name
        ), "Block pool raw capacity did not match with UI"

        # Checking Results for compression and replication
        if compression:
            compression_result = validate_compression(self.pool_name)
            if compression_result is False:
                raise PoolNotCompressedAsExpected(
                    f"Pool {self.pool_name} compression did not reach expected value"
                )
        replica_result = validate_replica_data(self.pool_name, replica)
        if replica_result is False:
            raise PoolNotReplicatedAsNeeded(
                f"Pool {self.pool_name} not replicated to size {replica}"
            )
