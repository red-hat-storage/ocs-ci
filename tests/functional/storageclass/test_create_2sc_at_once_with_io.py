import logging
import pytest

from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import ManageTest, tier1
from ocs_ci.framework.pytest_customization.marks import (
    skipif_external_mode,
    skipif_ocs_version,
    green_squad,
)
from ocs_ci.ocs.cluster import (
    validate_compression,
    validate_replica_data,
)
from ocs_ci.ocs.exceptions import (
    PoolNotCompressedAsExpected,
    PoolNotReplicatedAsNeeded,
)

log = logging.getLogger(__name__)


@green_squad
@tier1
@skipif_external_mode
@skipif_ocs_version("<4.6")
@pytest.mark.polarion_id("OCS-2394")
class TestCreate2ScAtOnceWithIo(ManageTest):
    """
    Create a new 2 Storage Class on a new rbd pool with
    different replica and compression options
    """

    def test_new_sc_rep2_rep3_at_once(
        self, storageclass_factory, pvc_factory, pod_factory
    ):
        """

        This test function does below,
        *. Creates 2 Storage Class with creating new rbd pool replica 2 and 3 with compression
        *. Creates PVCs using new Storage Classes
        *. Mount PVC to an app pod
        *. Run IO on an app pod
        *. Validate compression and replication

        """

        log.info("Creating storageclasses")
        interface_type = constants.CEPHBLOCKPOOL
        sc_obj1 = storageclass_factory(
            interface=interface_type,
            new_rbd_pool=True,
            replica=2,
            compression="aggressive",
        )

        sc_obj2 = storageclass_factory(
            interface=interface_type,
            new_rbd_pool=True,
            replica=3,
            compression="aggressive",
        )

        replicas = dict()
        replicas[sc_obj1.name] = 2
        replicas[sc_obj2.name] = 3
        sc_obj_list = [sc_obj1, sc_obj2]

        log.info("Creating pvc and pods")
        pod_obj_list = []
        for sc_obj in sc_obj_list:
            for pod_num in range(1, 5):
                pvc_obj = pvc_factory(
                    interface=interface_type, storageclass=sc_obj, size=10
                )
                pod_obj_list.append(pod_factory(interface=interface_type, pvc=pvc_obj))

        log.info("Running io on pods")

        for pod_obj in pod_obj_list:
            pod_obj.run_io(
                "fs",
                size="2G",
                rate="1500m",
                runtime=60,
                buffer_compress_percentage=60,
                buffer_pattern="0xdeadface",
                bs="8K",
                jobs=5,
                readwrite="readwrite",
            )

        for sc_obj in sc_obj_list:
            cbp_name = sc_obj.get()["parameters"]["pool"]
            cbp_size = replicas[sc_obj.name]
            compression_result = validate_compression(cbp_name)
            replica_result = validate_replica_data(cbp_name, cbp_size)
            if compression_result is False:
                raise PoolNotCompressedAsExpected(
                    f"Pool {cbp_name} compression did not reach expected value"
                )
            if replica_result is False:
                raise PoolNotReplicatedAsNeeded(
                    f"Pool {cbp_name} not replicated to size {cbp_size}"
                )
