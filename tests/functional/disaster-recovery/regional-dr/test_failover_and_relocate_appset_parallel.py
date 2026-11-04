import logging
from time import sleep

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import rdr, turquoise_squad
from ocs_ci.framework.testlib import acceptance, tier1
from ocs_ci.helpers import dr_helpers
from ocs_ci.helpers.dr_parallel import (
    collect_dr_details,
    get_common_primary_and_wait_time,
    run_failover_stage,
    run_rbd_cephfs_test,
    verify_cephfs_replication,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.drpc import DRPC

logger = logging.getLogger(__name__)


@rdr
@tier1
@turquoise_squad
class TestFailoverAndRelocateAppSetParallel:
    """
    Failover and relocate of RBD and CephFS ApplicationSet workloads, executed
    in parallel but reported as separate tests.

    Every PVC interface is an independent unit: when one fails in any stage the
    other one carries on, and only its own test is reported as failed.
    Steps on shared resources (stopping/starting the primary cluster nodes, the
    waits for IOs) run once for both interfaces.

    """

    @pytest.mark.parametrize(
        argnames=["primary_cluster_down", "pvc_interface"],
        argvalues=[
            pytest.param(
                False, constants.CEPHBLOCKPOOL, marks=acceptance, id="primary_up-rbd"
            ),
            pytest.param(
                False,
                constants.CEPHFILESYSTEM,
                marks=acceptance,
                id="primary_up-cephfs",
            ),
            pytest.param(True, constants.CEPHBLOCKPOOL, id="primary_down-rbd"),
            pytest.param(True, constants.CEPHFILESYSTEM, id="primary_down-cephfs"),
        ],
    )
    def test_failover_and_relocate_appset_parallel(
        self, primary_cluster_down, pvc_interface, request
    ):
        """
        The first item of each ``primary_cluster_down`` value runs the flow for
        RBD and CephFS in parallel; every item reports only the outcome of its
        own ``pvc_interface`` (see ``run_rbd_cephfs_test``).

        Test steps:
            1. Deploy an RBD and a CephFS ApplicationSet workload.
            2. Wait for IOs, verify lastGroupSyncTime.
            3. Optionally stop the primary cluster nodes.
            4. Failover both interfaces in parallel, verify the resources on
               the secondary cluster, start the primary nodes again.
            5. Verify the resources are gone from the old primary cluster and
               lastGroupSyncTime.
            6. Relocate both interfaces in parallel and verify the resources
               and lastGroupSyncTime.

        """

        def execute(runner):
            # Fetched here so that only the item which executes the run sets
            # them up.
            self._execute(
                runner,
                request.getfixturevalue("dr_workload"),
                request.getfixturevalue("nodes_multicluster"),
                primary_cluster_down,
                request,
            )

        run_rbd_cephfs_test(request, execute)

    def _execute(self, runner, dr_workload, nodes_multicluster, primary_down, request):
        """
        Run all the stages. Returns early when no interface is left healthy.
        """
        request.getfixturevalue("node_restart_teardown")

        # The factory returns every workload created so far (including a
        # half-deployed one of an interface that failed), so pick the ones of
        # this interface. Deployment is serial: it clones a git repository into
        # a fixed directory.
        def deploy(group):
            result = dr_workload(
                num_of_subscription=0,
                num_of_appset=1,
                pvc_interface=group.pvc_interface,
            )
            group.workloads = [
                wl for wl in result if wl.pvc_interface == group.pvc_interface
            ]

        runner.run_stage("deploy workloads", deploy, serial=True)

        def collect_details(group):
            first = group.workloads[0]
            collect_dr_details(
                group,
                [
                    DRPC(
                        namespace=constants.GITOPS_CLUSTER_NAMESPACE,
                        resource_name=f"{wl.appset_placement_name}-drpc",
                    )
                    for wl in group.workloads
                ],
                namespace=first.workload_namespace,
                workload_type=constants.APPLICATION_SET,
                resource_name=f"{first.appset_placement_name}-drpc",
            )

        runner.run_stage("collect details", collect_details)
        if not runner.names:
            return

        primary_cluster, wait_time = get_common_primary_and_wait_time(
            runner, primary_down
        )

        logger.info(f"Waiting for {wait_time} minutes to run IOs")
        sleep(wait_time * 60)

        def sync_before_failover(group):
            group.last_sync_times = [
                dr_helpers.verify_last_group_sync_time(drpc, group.scheduling_interval)
                for drpc in group.drpc_objs
            ]

        runner.run_stage(
            "verify lastGroupSyncTime before failover", sync_before_failover
        )

        def failover(group):
            for wl in group.workloads:
                dr_helpers.failover(
                    group.secondary,
                    wl.workload_namespace,
                    wl.workload_type,
                    wl.appset_placement_name,
                    skip_odf_cli_validation=primary_down,
                )
            # Verified while the primary cluster is still down (if it is)
            config.switch_to_cluster_by_name(group.secondary)
            for wl in group.workloads:
                dr_helpers.wait_for_all_resources_creation(
                    wl.workload_pvc_count,
                    wl.workload_pod_count,
                    wl.workload_namespace,
                    performed_dr_action=True,
                )

        run_failover_stage(
            runner,
            failover,
            primary_down,
            nodes_multicluster,
            primary_cluster,
            wait_time,
        )
        if not runner.names:
            return

        def verify_after_failover(group):
            config.switch_to_cluster_by_name(group.primary)
            for wl in group.workloads:
                dr_helpers.wait_for_all_resources_deletion(wl.workload_namespace)
            if group.is_cephfs:
                if group.cg_cephfs_enabled:
                    verify_cephfs_replication(group, group.secondary, group.primary)
            else:
                self._verify_mirroring(group)

        runner.run_stage("verify resources after failover", verify_after_failover)

        logger.info(f"Waiting for {wait_time} minutes to run IOs")
        sleep(wait_time * 60)

        def sync_after_failover(group):
            group.last_sync_times = [
                dr_helpers.verify_last_group_sync_time(
                    drpc, group.scheduling_interval, initial
                )
                for drpc, initial in zip(group.drpc_objs, group.last_sync_times)
            ]

        runner.run_stage("verify lastGroupSyncTime after failover", sync_after_failover)

        def relocate(group):
            for wl in group.workloads:
                dr_helpers.relocate(
                    group.primary,
                    wl.workload_namespace,
                    wl.workload_type,
                    wl.appset_placement_name,
                )

        runner.run_stage("relocate", relocate)

        def verify_after_relocate(group):
            config.switch_to_cluster_by_name(group.secondary)
            for wl in group.workloads:
                dr_helpers.wait_for_all_resources_deletion(wl.workload_namespace)
            config.switch_to_cluster_by_name(group.primary)
            for wl in group.workloads:
                dr_helpers.wait_for_all_resources_creation(
                    wl.workload_pvc_count,
                    wl.workload_pod_count,
                    wl.workload_namespace,
                    timeout=1800,
                    performed_dr_action=True,
                )
            if group.is_cephfs:
                verify_cephfs_replication(group, group.primary, group.secondary)
            else:
                self._verify_mirroring(group)

        runner.run_stage("verify resources after relocate", verify_after_relocate)

        def sync_after_relocate(group):
            for drpc, initial in zip(group.drpc_objs, group.last_sync_times):
                dr_helpers.verify_last_group_sync_time(
                    drpc, group.scheduling_interval, initial
                )

        runner.run_stage("verify lastGroupSyncTime after relocate", sync_after_relocate)

    @staticmethod
    def _verify_mirroring(group):
        dr_helpers.wait_for_mirroring_status_ok(
            replaying_images=sum(wl.workload_pvc_count for wl in group.workloads)
        )
