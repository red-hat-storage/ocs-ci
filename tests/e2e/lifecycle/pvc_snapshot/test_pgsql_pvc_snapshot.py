import logging
import pytest

from ocs_ci.framework.testlib import skipif_ocs_version, E2ETest, tier2

log = logging.getLogger(__name__)


@tier2
@skipif_ocs_version("<4.6")
@pytest.mark.polarion_id("OCS-2302")
class TestPvcSnapshotOfWorkloads(E2ETest):
    """
    Tests to verify PVC snapshot feature for pgsql workloads
    """

    @pytest.fixture(autouse=True)
    def pgsql_teardown(
        self, request, pgsql_factory_fixture, snapshot_factory, snapshot_restore_factory
    ):
        def teardown():

            # Delete created postgres and pgbench pods
            log.info("Deleting postgres pods which are attached to restored PVCs")
            for pgsql_obj in self.pgsql_obj_list:
                pgsql_obj.delete()

        request.addfinalizer(teardown)

    def test_pvc_snapshot(
        self, pgsql_factory_fixture, snapshot_factory, snapshot_restore_factory
    ):
        """
        1. Deploy PGSQL workload
        2. Take a snapshot of the pgsql PVC.
        3. Create a new PVC out of that snapshot or restore snapshot
        4. Attach a new pgsql pod to it.
        5. Create pgbench benchmark to new pgsql pod

        """
        self.pgsql_obj_list = []

        # Deploy PGSQL workload
        log.info("Deploying pgsql workloads")
        pgsql = pgsql_factory_fixture(replicas=3)

        # Get postgres pvc list obj
        postgres_pvcs_obj = pgsql.get_postgres_pvc()

        # Take a snapshot
        log.info("Creating snapshot of all postgres PVCs")
        snapshots = []
        for pvc_obj in postgres_pvcs_obj:
            log.info(f"Creating snapshot of PVC {pvc_obj.name}")
            snap_obj = snapshot_factory(
                pvc_obj=pvc_obj, snapshot_name=f"{pvc_obj.name}-snap"
            )
            snapshots.append(snap_obj)
        log.info("Snapshots creation completed and in Ready state")

        # Create PVCs out of the snapshots
        log.info("Creating new PVCs from snapshots")
        restore_pvc_objs = []
        for snapshot in snapshots:
            log.info(f"Creating a PVC from snapshot {snapshot.name}")
            restore_pvc_obj = snapshot_restore_factory(
                snapshot_obj=snapshot,
                restore_pvc_name=f"{snapshot.name}-restored",
                volume_mode=snapshot.parent_volume_mode,
                access_mode=snapshot.parent_access_mode,
            )

            log.info(
                f"Created PVC {restore_pvc_obj.name} from snapshot " f"{snapshot.name}"
            )
            restore_pvc_objs.append(restore_pvc_obj)
        log.info("Created new PVCs from all the snapshots and in Bound state")

        self.pgsql_obj_list = pgsql.attach_pgsql_pod_to_claim_pvc(
            pvc_objs=restore_pvc_objs,
            postgres_name="postgres-snap",
            pgbench_name="pgbench-snap",
        )
