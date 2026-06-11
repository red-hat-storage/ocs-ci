import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import magenta_squad
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    skipif_ocp_version,
    E2ETest,
    tier2,
    skipif_disconnected_cluster,
    skipif_proxy_cluster,
)
from ocs_ci.ocs.constants import VOLUME_MODE_FILESYSTEM

logger = logging.getLogger(__name__)


@magenta_squad
@tier2
@skipif_ocs_version("<4.6")
@skipif_ocp_version("<4.6")
@pytest.mark.polarion_id("OCS-2309")
@skipif_disconnected_cluster
@skipif_proxy_cluster
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
            logger.info("Deleting postgres pods which are attached to restored PVCs")
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

        logger.test_step("Deploy PostgreSQL workload")
        logger.info("Deploying pgsql workload with 1 replica")
        pgsql = pgsql_factory_fixture(replicas=1)
        logger.info("PostgreSQL workload deployed successfully")

        postgres_pvcs_obj = pgsql.get_postgres_pvc()
        logger.info(f"Retrieved {len(postgres_pvcs_obj)} PostgreSQL PVC(s)")

        logger.test_step("Create snapshots from PostgreSQL PVCs")
        snapshots = multi_snapshot_factory(
            pvc_obj=postgres_pvcs_obj, snapshot_name_suffix="snap"
        )
        logger.info(
            f"Created {len(snapshots)} snapshot(s) from all PVCs in Ready state"
        )

        logger.test_step("Restore PVCs from snapshots")
        restored_pvc_objs = multi_snapshot_restore_factory(
            snapshot_obj=snapshots, restore_pvc_suffix="restore"
        )
        logger.info(f"Created {len(restored_pvc_objs)} new PVC(s) from snapshots")

        logger.test_step("Attach PostgreSQL pods to restored PVCs")
        sset_list = pgsql.attach_pgsql_pod_to_claim_pvc(
            pvc_objs=restored_pvc_objs,
            postgres_name="postgres-restore",
            run_benchmark=False,
        )
        self.pgsql_obj_list.extend(sset_list)
        logger.info(f"Attached {len(sset_list)} PostgreSQL pod(s) to restored PVCs")

        logger.test_step(f"Resize restored PVCs to {pvc_size_new}Gi")
        for idx, pvc_obj in enumerate(restored_pvc_objs, 1):
            logger.info(
                f"Resizing restored PVC {idx}/{len(restored_pvc_objs)}: {pvc_obj.name} to {pvc_size_new}Gi"
            )
            pvc_obj.resize_pvc(pvc_size_new, True)
        logger.info(
            f"All {len(restored_pvc_objs)} restored PVC(s) resized successfully"
        )

        logger.test_step("Clone PostgreSQL PVCs")
        cloned_pvcs = multi_pvc_clone_factory(
            pvc_obj=postgres_pvcs_obj, volume_mode=VOLUME_MODE_FILESYSTEM
        )
        logger.info(f"Created {len(cloned_pvcs)} cloned PVC(s) from PostgreSQL volumes")

        logger.test_step("Attach PostgreSQL pods to cloned PVCs")
        sset_list = pgsql.attach_pgsql_pod_to_claim_pvc(
            pvc_objs=cloned_pvcs, postgres_name="postgres-clone", run_benchmark=False
        )
        self.pgsql_obj_list.extend(sset_list)
        logger.info(f"Attached {len(sset_list)} PostgreSQL pod(s) to cloned PVCs")

        logger.test_step(f"Resize cloned PVCs to {pvc_size_new}Gi")
        for idx, pvc_obj in enumerate(cloned_pvcs, 1):
            logger.info(
                f"Resizing cloned PVC {idx}/{len(cloned_pvcs)}: {pvc_obj.name} to {pvc_size_new}Gi"
            )
            pvc_obj.resize_pvc(pvc_size_new, True)
        logger.info(f"All {len(cloned_pvcs)} cloned PVC(s) resized successfully")
