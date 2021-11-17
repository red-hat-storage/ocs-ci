import logging
import pytest

from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    skipif_ocp_version,
    E2ETest,
    flowtests,
)


log = logging.getLogger(__name__)


@flowtests
@skipif_ocs_version("<4.6")
@skipif_ocp_version("<4.6")
@pytest.mark.polarion_id("OCS-2310")
class TestPvcSnapshotAndCloneWithBaseOperation(E2ETest):
    """
    Tests Story/Flow based test scenario for pgsql snapshot and clone
    """

    def test_pvc_snapshot_and_clone(
        self, pgsql_factory_fixture, multiple_snapshot_and_clone_of_postgres_pvc_factory
    ):
        """
        1. Deploy PGSQL workload
        2. Take a snapshot of the pgsql PVC.
        3. Create a new PVC out of that snapshot or restore snapshot
        4. Create a clone of restored snapshot
        5. Attach a new pgsql pod to it.
        6. Resize cloned pvc
        7. Create snapshots of cloned pvc and restore those snapshots
        8. Attach a new pgsql pod to it and Resize the new restored pvc

        """

        # Deploy PGSQL workload
        log.info("Deploying pgsql workloads")
        pgsql = pgsql_factory_fixture(replicas=1, clients=3, transactions=600)

        log.info("Starting multiple creation & clone of postgres PVC")
        multiple_snapshot_and_clone_of_postgres_pvc_factory(
            pvc_size_new=25, pgsql=pgsql
        )
