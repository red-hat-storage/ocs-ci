import logging
import pytest
import time

from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    skipif_ocp_version,
    E2ETest,
    tier2,
)
from ocs_ci.ocs.constants import (
    STATUS_COMPLETED,
    VOLUME_MODE_FILESYSTEM,
)
from ocs_ci.ocs.resources.pod import get_pod_obj
from ocs_ci.ocs.utils import get_pod_name_by_pattern

log = logging.getLogger(__name__)

BMO_NAME = "benchmark-operator"


@tier2
@skipif_ocs_version("<4.6")
@skipif_ocp_version("<4.6")
@pytest.mark.polarion_id("OCS-2342")
class TestPvcCloneOfWorkloads(E2ETest):
    """
    Tests to create multiple clones of same pgsql PVC at different utilization
    """

    @pytest.fixture(autouse=True)
    def pgsql_teardown(self, request, pgsql_factory_fixture, pvc_clone_factory):
        def teardown():

            # Delete created postgres and pgbench pods
            log.info("Deleting postgres pods which are attached to restored PVCs")
            for pgsql_obj in self.sset_list:
                pgsql_obj.delete()

        request.addfinalizer(teardown)

    def test_pvc_clone(self, pgsql_factory_fixture, pvc_clone_factory):
        """
        1. Deploy PGSQL workload
        2. Create multiple clone of same PVC when the PVC usage is different
        3. Attach a new pgsql pod to it.
        4. Create pgbench benchmark to new pgsql pod
        """

        self.sset_list = []

        # Deploy PGSQL workload
        log.info("Deploying pgsql workloads")
        pgsql = pgsql_factory_fixture(replicas=1)

        # Get postgres pvcs obj list
        postgres_pvcs_obj = pgsql.get_postgres_pvc()

        # Get postgres pods obj list
        postgres_pods_obj = pgsql.get_postgres_pods()

        self.sset_list = []
        for i in range(3):

            # Create clone of pgsql pvc
            log.info("Creating clone of the Postgres PVCs")
            cloned_pvcs = [
                pvc_clone_factory(pvc_obj, volume_mode=VOLUME_MODE_FILESYSTEM)
                for pvc_obj in postgres_pvcs_obj
            ]
            log.info("Created clone of the PVCs and all cloned PVCs are in Bound state")

            # Attach to new postgres pod
            self.pgsql_obj_list = pgsql.attach_pgsql_pod_to_claim_pvc(
                pvc_objs=cloned_pvcs,
                postgres_name=f"postgres-cloned-{i}",
                run_benchmark=False,
            )
            self.sset_list.extend(self.pgsql_obj_list)

            # Get usage of pgsql pvc
            parent_pods_obj = pgsql.get_postgres_used_file_space(postgres_pods_obj)

            # Wait time to copy the data to cloned PVC
            time.sleep(600)
            # Validate cloned pvcs file space matches with parent
            cloned_pods_list = get_pod_name_by_pattern(
                pattern=f"postgres-cloned-{i}", namespace=BMO_NAME
            )
            cloned_pods_obj = [
                get_pod_obj(name=pods, namespace=BMO_NAME) for pods in cloned_pods_list
            ]
            cloned_obj = pgsql.get_postgres_used_file_space(cloned_pods_obj)
            for pod_obj in parent_pods_obj:
                if (
                    pod_obj.filespace
                    != cloned_obj[parent_pods_obj.index(pod_obj)].filespace
                ):
                    # ToDo: Before clone need to check data is synced
                    if (
                        not abs(
                            int(pod_obj.filespace.strip("M"))
                            - int(
                                cloned_obj[
                                    parent_pods_obj.index(pod_obj)
                                ].filespace.strip("M")
                            )
                        )
                        < 3
                    ):
                        raise Exception(
                            f"Parent pvc {pod_obj.name} used file space is {pod_obj.filespace}. "
                            f"And for cloned pvc {cloned_obj[parent_pods_obj.index(pod_obj)].name} "
                            f"used file space is {cloned_obj[parent_pods_obj.index(pod_obj)].filespace}"
                        )
                    log.warn(
                        f"Parent pvc {pod_obj.name} used file space is {pod_obj.filespace}. "
                        f"And for cloned pvc {cloned_obj[parent_pods_obj.index(pod_obj)].name} "
                        f"used file space is {cloned_obj[parent_pods_obj.index(pod_obj)].filespace}"
                    )
            log.info("All cloned PVC matches the parent PVC data")

            # Run benchmark on parent PVC
            pgsql.create_pgbench_benchmark(
                replicas=1, pgbench_name=f"pgbench-{i}", wait=False
            )

            # Wait till pgbench client pods up
            wait_time = 180
            log.info(f"Waiting {wait_time} seconds for pgbench client pods to be up")
            time.sleep(180)

            # Wait for pg_bench pod to initialized and complete
            log.info("Checking all pgbench benchmark reached Completed state")
            pgsql.wait_for_pgbench_status(status=STATUS_COMPLETED, timeout=1800)
