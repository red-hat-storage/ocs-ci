import logging
import pytest
from ocs_ci.framework.pytest_customization.marks import tier1, skipif_ui_not_support
from ocs_ci.framework.testlib import skipif_ocs_version, ManageTest, ui
from ocs_ci.ocs.exceptions import (
    PoolNotCompressedAsExpected,
    PoolNotReplicatedAsNeeded,
    PoolCephValueNotMatch,
    PoolUiEfficiencyParametersNotEqualToPrometheus,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.ui.block_pool import BlockPoolUI
from ocs_ci.ocs.cluster import (
    validate_compression,
    validate_replica_data,
    check_pool_compression_replica_ceph_level,
)
from semantic_version.base import Version
from ocs_ci.utility.utils import get_ocp_version

logger = logging.getLogger(__name__)

need_to_delete = []


@skipif_ui_not_support("block_pool")
@pytest.mark.parametrize(
    argnames=["replica", "compression", "compression_saving"],
    argvalues=[
        pytest.param(*[3, True, "1.51 GiB"], marks=pytest.mark.polarion_id("OCS-2589")),
        pytest.param(*[3, False, None], marks=pytest.mark.polarion_id("OCS-2588")),
        pytest.param(*[2, True, "1 GiB"], marks=pytest.mark.polarion_id("OCS-2587")),
        pytest.param(*[2, False, None], marks=pytest.mark.polarion_id("OCS-2586")),
    ],
)
class TestPoolUserInterface(ManageTest):
    """
    Test Pool User Interface

    """

    ocp_version = get_ocp_version()
    pvc_size = 40

    @pytest.fixture()
    def namespace(self, project_factory):
        self.proj_obj = project_factory()
        self.proj = self.proj_obj.namespace

    @pytest.fixture()
    def storage(self, storageclass_factory_ui, replica, compression):
        self.sc_obj = storageclass_factory_ui(
            create_new_pool=True, replica=replica, compression=compression
        )
        self.pool_name = self.sc_obj.get()["parameters"]["pool"]

    @pytest.fixture()
    def pvc(self, pvc_factory):
        status = None
        if Version.coerce(self.ocp_version) > Version.coerce("4.8"):
            status = constants.STATUS_PENDING
        else:
            status = constants.STATUS_BOUND
        self.pvc_obj = pvc_factory(
            project=self.proj_obj,
            interface=constants.CEPHBLOCKPOOL,
            storageclass=self.sc_obj,
            size=self.pvc_size,
            status=status,
        )

    @pytest.fixture()
    def pod(self, pod_factory):
        self.pod_obj = pod_factory(pvc=self.pvc_obj)

    @ui
    @tier1
    @skipif_ocs_version("<4.8")
    def test_create_delete_pool(
        self,
        replica,
        compression,
        setup_ui,
        compression_saving,
        namespace,
        storage,
        pvc,
        pod,
    ):
        """
        test create delete pool have the following workflow
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
            size="1g",
            rate="1500m",
            runtime=60,
            buffer_compress_percentage=60,
            buffer_pattern="0xdeadface",
            bs="8K",
            jobs=5,
            readwrite="readwrite",
        )

        # Getting IO results
        self.pod_obj.get_fio_results()

        # If above 4.8 check efficiency compression parameters against prometheus
        if Version.coerce(self.ocp_version) > Version.coerce("4.8") and compression:
            blockpool_ui_object = BlockPoolUI(setup_ui)
            if not blockpool_ui_object.check_ui_pool_efficiency_parameters_against_prometheus(
                self.pool_name, compression_saving
            ):
                raise PoolUiEfficiencyParametersNotEqualToPrometheus(
                    f"Pool {self.pool_name} "
                    f"compression efficiency parameters are "
                    f"not equal to Prometheus parameters"
                )
        # Check compression with ceph df detail.
        if compression:
            compression_result = validate_compression(self.pool_name)
            if compression_result is False:
                raise PoolNotCompressedAsExpected(
                    f"Pool {self.pool_name} compression did not reach expected value"
                )

        # Check replica size with ceph df detail.
        replica_result = validate_replica_data(self.pool_name, replica)
        if replica_result is False:
            raise PoolNotReplicatedAsNeeded(
                f"Pool {self.pool_name} not replicated to size {replica}"
            )
