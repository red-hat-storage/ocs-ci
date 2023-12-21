import logging
import pytest
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
from ocs_ci.ocs.constants import CEPHBLOCKPOOL
from ocs_ci.ocs.exceptions import PoolNotReplicatedAsNeeded, PoolNotCompressedAsExpected

log = logging.getLogger(__name__)


@green_squad
@tier1
@skipif_external_mode
@skipif_ocs_version("<4.6")
@pytest.mark.polarion_id("OCS-2391")
class TestMultipleScOnePoolRep2Comp(ManageTest):
    """
    Create new rbd pool with replica 2 and compression.
    Attach it to 2 new storageclasses.
    Create PVCs and PODs for each storageclass.
    Run IO.
    Delete PODs, PVSs, Storageclass and pool.

    """

    replica = 2

    def test_multiple_sc_one_pool_rep2_comp(
        self,
        ceph_pool_factory,
        storageclass_factory,
        pvc_factory,
        pod_factory,
    ):
        """
        This test function does below,
        *. Creates 2 Storage Class with creating one rbd pool for both
        *. Creates PVCs using new Storage Classes
        *. Mount PVC to an app pod
        *. Run IO on an app pod
        *. Verify compression and replication

        """

        log.info("Creating new pool with replica2 and compression")
        pool_obj = ceph_pool_factory(
            interface=CEPHBLOCKPOOL,
            replica=self.replica,
            compression="aggressive",
        )

        log.info(f"Creating first storageclass with pool {pool_obj.name}")
        sc_obj1 = storageclass_factory(
            interface=CEPHBLOCKPOOL,
            new_rbd_pool=False,
            pool_name=pool_obj.name,
        )

        log.info(f"Creating second storageclass with pool {pool_obj.name}")
        sc_obj2 = storageclass_factory(
            interface=CEPHBLOCKPOOL,
            new_rbd_pool=False,
            pool_name=pool_obj.name,
        )

        sc_obj_list = [sc_obj1, sc_obj2]
        pod_obj_list = []

        log.info("Creating PVCs and PODs")
        for sc_obj in sc_obj_list:
            pvc_obj = pvc_factory(interface=CEPHBLOCKPOOL, storageclass=sc_obj, size=10)
            pod_obj_list.append(pod_factory(interface=CEPHBLOCKPOOL, pvc=pvc_obj))

        log.info("Running IO on pods")
        for pod_obj in pod_obj_list:
            pod_obj.run_io(
                "fs",
                size="1G",
                rate="1500m",
                runtime=60,
                buffer_compress_percentage=60,
                buffer_pattern="0xdeadface",
                bs="8K",
                jobs=5,
                readwrite="readwrite",
            )

        log.info(f"validating info on pool {pool_obj.name}")
        validate_rep_result = validate_replica_data(pool_obj.name, self.replica)
        if validate_rep_result is False:
            raise PoolNotReplicatedAsNeeded(
                f"pool {pool_obj.name} not replicated as expected"
            )
        validate_comp_result = validate_compression(pool_obj.name)
        if validate_comp_result is False:
            raise PoolNotCompressedAsExpected(
                f"pool {pool_obj.name} not compressed as expected"
            )
