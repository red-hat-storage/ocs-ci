import logging

import time
import pytest

from subprocess import CalledProcessError

from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import (
    skipif_ocs_version, E2ETest, tier1
)
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.utility import templating
from ocs_ci.ocs.exceptions import CommandFailed

log = logging.getLogger(__name__)


@tier1
@skipif_ocs_version('<4.6')
@pytest.mark.polarion_id("OCS-2302")
class TestPvcSnapshotOfWorkloads(E2ETest):
    """
    Tests to verify PVC snapshot feature for pgsql workloads
    """

    def test_pvc_snapshot(self, pgsql_factory_fixture, snapshot_factory, snapshot_restore_factory):
        """
        1. Deploy PGSQL workload
        2. Take a snapshot of the pgsql PVC.
        3. Create a new PVC out of that snapshot or restore snapshot
        4. Attach a new pgsql pod to it.
        5. Create pgbench benchmark to new pgsql pod

        """

        # Deploy PGSQL workload
        log.info("Deploying pgsql workloads")
        pgsql = pgsql_factory_fixture(replicas=3, clients=3, transactions=600)

        # Get postgres pvc list obj
        postgres_pvcs_obj = pgsql.get_postgres_pvc()

        # Take a snapshot
        log.info(f"Creating snapshot of all postgres PVCs")
        snapshots = []
        for pvc_obj in postgres_pvcs_obj:
            log.info(
                f"Creating snapshot of PVC {pvc_obj.name}"
            )
            snap_obj = snapshot_factory(pvc_obj=pvc_obj, snapshot_name=f"{pvc_obj.name}-snap")
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
                size='10Gi',
                volume_mode=snapshot.parent_volume_mode,
                access_mode=snapshot.parent_access_mode,
                status=constants.STATUS_BOUND
            )

            log.info(
                f"Created PVC {restore_pvc_obj.name} from snapshot "
                f"{snapshot.name}"
            )
            restore_pvc_objs.append(restore_pvc_obj)
        log.info("Created new PVCs from all the snapshots and in Bound state")

        # Attach a new pgsql pod to created pvc's and run pgbench benchmark on pod
        i = 0
        pgsql_obj_list = []
        for pvc_obj in restore_pvc_objs:
            try:
                pgsql_sset = templating.load_yaml(
                    constants.PGSQL_STATEFULSET_YAML
                )
                del pgsql_sset['spec']['volumeClaimTemplates']
                pgsql_sset['metadata']['name'] = 'postgres-snap' + f"{i}"
                pgsql_sset['spec']['template']['spec']['containers'][0]['volumeMounts'][0]['name'] = pvc_obj.name
                pgsql_sset['spec']['template']['spec']['volumes'] = [
                    {'name': f'{pvc_obj.name}', 'persistentVolumeClaim': {'claimName': f'{pvc_obj.name}'}}
                ]
                pgsql_sset = OCS(**pgsql_sset)
                pgsql_sset.create()
                pgsql_obj_list.append(pgsql_sset)

                pgsql.wait_for_postgres_status(status=constants.STATUS_RUNNING, timeout=300)

                pg_data = templating.load_yaml(constants.PGSQL_BENCHMARK_YAML)
                pg_data['metadata']['name'] = 'pgbench-snap' + f"{i}"
                pg_data['spec']['workload']['args']['databases'][0][
                    'host'
                ] = "postgres-snap" + f"{i}-0" + ".postgres"
                pg_obj = OCS(**pg_data)
                pg_obj.create()
                pgsql_obj_list.append(pg_obj)
                i += 1

                wait_time = 120
                log.info(f"Wait {wait_time} seconds before mounting pod")
                time.sleep(wait_time)

            except (CommandFailed, CalledProcessError) as cf:
                log.error('Failed during creation of postgres pod')
                raise cf

        pgsql.wait_for_pgbench_status(status=constants.STATUS_COMPLETED, timeout=1800)

        # Delete created postgres and pgbench pods
        log.info("Deleting created postgres pods and pgbench configuration")
        for pgsql_obj in pgsql_obj_list:
            pgsql_obj.delete()
