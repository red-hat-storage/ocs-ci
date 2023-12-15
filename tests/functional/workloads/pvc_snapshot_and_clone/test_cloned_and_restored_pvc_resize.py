import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import magenta_squad
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    skipif_ocp_version,
    E2ETest,
    tier2,
)
from ocs_ci.ocs.constants import VOLUME_MODE_FILESYSTEM

log = logging.getLogger(__name__)


@magenta_squad
@tier2
@skipif_ocs_version("<4.6")
@skipif_ocp_version("<4.6")
@pytest.mark.polarion_id("OCS-2309")
class TestPvcResizeOfClonedAndRestoredPVC(E2ETest):
    """
    Tests to verify PVC resize feature for
    cloned/restored pgsql pvcs
    """

    @pytest.fixture(autouse=True)
    def pgsql_teardown(
        self,
        request,
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

    def test_pvc_resize(
        self,
        pgsql_factory_fixture,
        multi_snapshot_factory,
        multi_snapshot_restore_factory,
        multi_pvc_clone_factory,
    ):
        """
        1. Deploy PGSQL workload
        2. Take a snapshot of the pgsql PVC.
        3. Create a new PVC out of that snapshot or restore snapshot
        4. Attach a new pgsql pod to it.
        5. Resize the new PVC
        6. Clone pgsql PVC and attach a new pgsql pod to it
        7. Resize cloned PVC

        """
        pvc_size_new = 25
        self.pgsql_obj_list = []

        # Deploy PGSQL workload
        log.info("Deploying pgsql workloads")
        pgsql = pgsql_factory_fixture(replicas=1)

        # Get postgres pvc list obj
        postgres_pvcs_obj = pgsql.get_postgres_pvc()

        snapshots = multi_snapshot_factory(
            pvc_obj=postgres_pvcs_obj, snapshot_name_suffix="snap"
        )
        log.info("Created snapshots from all the PVCs and snapshots are in Ready state")

        restored_pvc_objs = multi_snapshot_restore_factory(
            snapshot_obj=snapshots, restore_pvc_suffix="restore"
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
            pvc_obj=postgres_pvcs_obj, volume_mode=VOLUME_MODE_FILESYSTEM
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
