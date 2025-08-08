import logging
import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    tier2,
    skipif_ui_not_support,
    skipif_hci_provider_or_client,
    skipif_external_mode,
    skipif_disconnected_cluster,
    green_squad,
    jira,
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
    validate_num_of_pgs,
)
from ocs_ci.ocs.ui.block_pool import BlockPoolUI
from ocs_ci.ocs.resources.pod import get_ceph_tools_pod
from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility.utils import run_cmd


logger = logging.getLogger(__name__)

need_to_delete = []


@skipif_ui_not_support("block_pool")
@skipif_external_mode
@skipif_disconnected_cluster
@pytest.mark.parametrize(
    argnames=["replica", "compression"],
    argvalues=[
        pytest.param(*[3, True], marks=pytest.mark.polarion_id("OCS-2589")),
        pytest.param(*[3, False], marks=pytest.mark.polarion_id("OCS-2588")),
        pytest.param(*[2, True], marks=pytest.mark.polarion_id("OCS-2587")),
        pytest.param(*[2, False], marks=pytest.mark.polarion_id("OCS-2586")),
        pytest.param(*[2, False], marks=pytest.mark.polarion_id("OCS-6255")),
    ],
)
@skipif_hci_provider_or_client
@jira("DFBUGS-2139")
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
    def storage(self, storageclass_factory_ui, replica, compression):
        self.sc_obj = storageclass_factory_ui(
            create_new_pool=True,
            replica=replica,
            compression=compression,
            vol_binding_mode="Immediate",
        )
        self.pool_name = self.sc_obj.get()["parameters"]["pool"]

    @pytest.fixture()
    def pvc(self, pvc_factory):
        self.pvc_obj = pvc_factory(
            project=self.proj_obj,
            interface=constants.CEPHBLOCKPOOL,
            storageclass=self.sc_obj,
            size=self.pvc_size,
        )

    @pytest.fixture()
    def pod(self, pod_factory):
        self.pod_obj = pod_factory(pvc=self.pvc_obj)

    @ui
    @tier2
    @skipif_ocs_version("<4.16")
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
    ):
        """
        test create delete pool has the following workflow
        .* Create new RBD pool
        .* Associate the pool with storageclass
        .* Create PVC based on the storageclass
        .* Create POD based on the PVC
        .* Run IO on the POD
        .* Check replication and compression
        .* Check the values of pg_num , it should be equal to osd_pool_default_pg_num
        .* Check PG autoscale is ON
        .* New pool is having non-blank deviceclass

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
        blockpool_ui_object = BlockPoolUI()
        assert blockpool_ui_object.pool_raw_capacity_loaded(
            self.pool_name
        ), "Block pool raw capacity is not visible on UI"

        # Cross checking the raw capacity of the blockpool between CLI and UI
        assert blockpool_ui_object.cross_check_raw_capacity(
            self.pool_name
        ), "Block pool raw capacity did not matched with UI"

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

        # Check pg_num and osd_pool_default_pg_num matches
        ct_pod = get_ceph_tools_pod()
        osd_pool_default_pg_num = ct_pod.exec_ceph_cmd(
            ceph_cmd="ceph config get mon osd_pool_default_pg_num"
        )
        logger.info(f"The osd pool default pg num value is {osd_pool_default_pg_num}")
        expected_pgs = {
            self.pool_name: osd_pool_default_pg_num,
        }
        assert validate_num_of_pgs(
            expected_pgs
        ), "pg_num is not equal to the osd pool default pg num"
        logger.info(
            f"pg_num of the new pool {self.pool_name} "
            f"is equal to the osd pool default pg num {osd_pool_default_pg_num}"
        )

        # Check if the pg-autoscale is ON
        pool_autoscale_status = ct_pod.exec_ceph_cmd(
            ceph_cmd="ceph osd pool autoscale-status"
        )
        for pool in pool_autoscale_status:
            if pool["pool_name"] == self.pool_name:
                assert pool["pg_autoscale_mode"] == "on", "PG autoscale mode is off"
        logger.info(f"{self.pool_name} autoscale mode is on")

        # Check the pool is not none
        oc_obj = OCP(kind=constants.CEPHBLOCKPOOL)
        cbp_output = run_cmd(
            cmd=f"oc get cephblockpool/{self.pool_name} -n {config.ENV_DATA['cluster_namespace']} -o yaml"
        )
        cbp_output = oc_obj.exec_oc_cmd(
            command=f"get cephblockpool/{self.pool_name} -n {config.ENV_DATA['cluster_namespace']} -o yaml"
        )
        assert cbp_output["spec"]["deviceClass"] is not None, "The Deviceclass is none"
        logger.info(
            f"The deviceClass of the pool {self.pool_name} is {cbp_output['spec']['deviceClass']}"
        )
