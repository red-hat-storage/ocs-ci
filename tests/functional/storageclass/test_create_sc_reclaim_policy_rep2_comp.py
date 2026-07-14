import logging
from ocs_ci.framework.testlib import ManageTest, tier2
from ocs_ci.framework.pytest_customization.marks import (
    polarion_id,
    skipif_external_mode,
    skipif_ocs_version,
    green_squad,
)
from ocs_ci.ocs.resources.pod import delete_pods
from ocs_ci.ocs.cluster import (
    validate_compression,
    validate_replica_data,
)

from ocs_ci.ocs.constants import (
    CEPHBLOCKPOOL,
    RECLAIM_POLICY_RETAIN,
)
from ocs_ci.ocs.exceptions import (
    PoolNotReplicatedAsNeeded,
    PoolNotCompressedAsExpected,
    ImageIsNotDeletedOrNotFound,
)
from ocs_ci.helpers.helpers import (
    delete_volume_in_backend,
)

log = logging.getLogger(__name__)


@green_squad
@tier2
@skipif_external_mode
@skipif_ocs_version("<4.6")
@polarion_id("OCS-2398")
class TestScReclaimPolicyRetainRep2Comp(ManageTest):
    """
    Create storageclass with reclaim policy retain
    attached to pool with replica 2 and compression
    Create pvc and pod and run IO
    Verify compression and replication
    Delete Pod, pvc, pv, rbd image

    """

    compression = "aggressive"
    replica = 2
    reclaim_policy = RECLAIM_POLICY_RETAIN

    def test_sc_reclaim_policy_retain_rep2_comp(
        self,
        storageclass_factory,
        pvc_factory,
        pod_factory,
    ):
        """
        This test function does below,
        *. Create storageclass with reclaim policy retain
            and pool with rep2 and compression
        *. Create pvc and pod
        *. Run IO on pod
        *. Verify compression and replication
        *. Delete Pod, Pvc, Pv, Rbd image
        """

        log.info(
            f"Creating storageclass with replica {self.replica}"
            f", compression {self.compression} and"
            f"reclaim policy {self.reclaim_policy}"
        )
        sc_obj = storageclass_factory(
            interface=CEPHBLOCKPOOL,
            new_rbd_pool=True,
            replica=self.replica,
            compression=self.compression,
            reclaim_policy=self.reclaim_policy,
        )
        pool = sc_obj.get()["parameters"]["pool"]

        log.info("Creating PVCs and PODs")
        pvc_obj = pvc_factory(interface=CEPHBLOCKPOOL, storageclass=sc_obj, size=10)
        pod_obj = pod_factory(interface=CEPHBLOCKPOOL, pvc=pvc_obj)

        log.info("Running IO on pod")
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

        log.info(f"validating info on pool {pool}")
        validate_rep_result = validate_replica_data(pool, self.replica)
        if validate_rep_result is False:
            raise PoolNotReplicatedAsNeeded(f"pool {pool} not replicated as expected")
        validate_comp_result = validate_compression(pool)
        if validate_comp_result is False:
            raise PoolNotCompressedAsExpected(f"pool {pool} not compressed as expected")

        log.info("Deleting pod")
        pod_obj_list = [pod_obj]
        delete_pods(pod_obj_list, wait=True)

        log.info("Deleting pvc, pv and rbd image")
        pvc_obj.reload()
        pvc_uuid_map = pvc_obj.image_uuid
        pv_obj = pvc_obj.backed_pv_obj
        pvc_obj.delete()
        pv_obj.delete()
        delete_results = delete_volume_in_backend(img_uuid=pvc_uuid_map, pool_name=pool)
        if not delete_results:
            raise ImageIsNotDeletedOrNotFound(
                f"Could not delete or find image csi-vol-{pvc_uuid_map}"
            )
