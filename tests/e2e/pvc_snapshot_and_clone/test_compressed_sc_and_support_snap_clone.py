import logging
import pytest

from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    skipif_ocp_version,
    E2ETest,
    tier2,
)
from ocs_ci.ocs.constants import (
    VOLUME_MODE_FILESYSTEM,
    CEPHBLOCKPOOL,
    STATUS_READYTOUSE,
)
from ocs_ci.ocs.ocp import OCP

log = logging.getLogger(__name__)


@tier2
@skipif_ocs_version("<4.6")
@skipif_ocp_version("<4.6")
@pytest.mark.parametrize(
    argnames=["replica", "compression"],
    argvalues=[
        pytest.param(*[3, "aggressive"], marks=pytest.mark.polarion_id("OCS-2536")),
        pytest.param(*[2, "aggressive"], marks=pytest.mark.polarion_id("OCS-2305")),
    ],
)
class TestCompressedSCAndSupportSnapClone(E2ETest):
    """
    Tests to create new compressed sc and their support for
    snapshot/cloned/restored pgsql pvcs
    """

    @pytest.fixture(autouse=True)
    def pgsql_teardown(
        self,
        request,
        storageclass_factory,
        pgsql_factory_fixture,
        multi_snapshot_factory,
        multi_snapshot_restore_factory,
        multi_pvc_clone_factory,
    ):
        def teardown():

            # Delete created postgres and pgbench pods
            log.info("Deleting postgres pods which are attached to restored PVCs")
            for pgsql_obj in self.pgsql_obj_list:
                pgsql_obj.delete()

        request.addfinalizer(teardown)

    def test_compressed_sc_and_support_snap_clone(
        self,
        storageclass_factory,
        pgsql_factory_fixture,
        multi_snapshot_factory,
        multi_snapshot_restore_factory,
        multi_pvc_clone_factory,
        replica,
        compression,
    ):
        """
        1. Create new sc with compression
        2. Deploy PGSQL workload using those new sc created
        3. Take a snapshot of the pgsql PVC.
        4. Create a new PVC out of that snapshot or restore snapshot
        5. Attach a new pgsql pod to it.
        6. Resize the new PVC
        7. Clone pgsql PVC and attach a new pgsql pod to it
        8. Resize cloned PVC

        """
        pvc_size_new = 25
        self.pgsql_obj_list = []

        interface_type = CEPHBLOCKPOOL
        sc_obj = storageclass_factory(
            interface=interface_type,
            new_rbd_pool=True,
            replica=replica,
            compression=compression,
        )

        # Deploy PGSQL workload
        log.info("Deploying pgsql workloads")
        pgsql = pgsql_factory_fixture(
            replicas=3, clients=3, transactions=600, sc_name=sc_obj.name
        )

        # Get postgres pvc list obj
        postgres_pvcs_obj = pgsql.get_postgres_pvc()

        snapshots = multi_snapshot_factory(
            pvc_obj=postgres_pvcs_obj, snapshot_name_suffix="snap", wait=False
        )
        for snap_obj in snapshots:
            ocs_obj = OCP(kind=snap_obj.kind, namespace=snap_obj.namespace)
            # Increase time because of the bz1969427, should be removed later
            ocs_obj.wait_for_resource(
                condition="true",
                resource_name=snap_obj.name,
                column=STATUS_READYTOUSE,
                timeout=600,
            )
        log.info("Created snapshots from all the PVCs and snapshots are in Ready state")

        restored_pvc_objs = multi_snapshot_restore_factory(
            snapshot_obj=snapshots,
            restore_pvc_suffix="restore",
            storageclass=sc_obj.name,
        )
        log.info("Created new PVCs from all the snapshots")

        sset_list = pgsql.attach_pgsql_pod_to_claim_pvc(
            pvc_objs=restored_pvc_objs,
            postgres_name="postgres-restore",
            run_benchmark=False,
        )
        self.pgsql_obj_list.extend(sset_list)

        # Resize new PVCs created from snapshots
        for pvc_obj in restored_pvc_objs:
            log.info(f"Expanding size of PVC {pvc_obj.name} to {pvc_size_new}G")
            pvc_obj.resize_pvc(pvc_size_new, True)

        cloned_pvcs = multi_pvc_clone_factory(
            pvc_obj=postgres_pvcs_obj,
            volume_mode=VOLUME_MODE_FILESYSTEM,
            storageclass=sc_obj.name,
        )
        log.info("Created new PVCs from all postrges volumes")

        # Attach a new pgsql pod to cloned pvcs
        sset_list = pgsql.attach_pgsql_pod_to_claim_pvc(
            pvc_objs=cloned_pvcs, postgres_name="postgres-clone", run_benchmark=False
        )
        self.pgsql_obj_list.extend(sset_list)

        # Resize cloned PVCs
        for pvc_obj in cloned_pvcs:
            log.info(f"Expanding size of PVC {pvc_obj.name} to {pvc_size_new}G")
            pvc_obj.resize_pvc(pvc_size_new, True)
