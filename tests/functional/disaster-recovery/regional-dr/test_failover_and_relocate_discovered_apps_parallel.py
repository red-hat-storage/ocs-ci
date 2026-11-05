import logging
from time import sleep

import pytest
from semantic_version import Version

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import rdr, turquoise_squad
from ocs_ci.framework.testlib import acceptance, skipif_ocs_version, tier1
from ocs_ci.helpers import dr_helpers
from ocs_ci.helpers.dr_helpers_ui import (
    verify_pending_cleanup_alert_firing,
    verify_pending_cleanup_alert_resolved,
)
from ocs_ci.helpers.dr_parallel import (
    collect_dr_details,
    get_common_primary_and_wait_time,
    run_failover_stage,
    run_rbd_cephfs_test,
    verify_cephfs_replication,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.acm.acm import AcmAddClusters
from ocs_ci.ocs.resources.drpc import DRPC
from ocs_ci.utility.version import get_semantic_ocs_version_from_config

logger = logging.getLogger(__name__)


@rdr
@tier1
@turquoise_squad
@skipif_ocs_version("<4.19")
class TestFailoverAndRelocateWithDiscoveredAppsParallel:
    """
    Failover and relocate of RBD and CephFS Discovered Apps workloads, executed
    in parallel but reported as separate tests.

    Every PVC interface is an independent unit: when one fails (or skips) in any
    stage the other one carries on, and only its own test is reported as
    failed. Steps that touch shared resources (stopping/starting the primary
    cluster nodes, the single ACM UI session) run once for both interfaces.

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
    def test_failover_and_relocate_discovered_apps_parallel(
        self, primary_cluster_down, pvc_interface, request
    ):
        """
        The first item of each ``primary_cluster_down`` value runs the flow for
        RBD and CephFS in parallel; every item reports only the outcome of its
        own ``pvc_interface`` (see ``run_rbd_cephfs_test``).

        Test steps:
            1. Deploy RBD and CephFS discovered-apps workloads.
            2. Wait for the scheduling interval, optionally stop the primary
               cluster nodes.
            3. Failover both interfaces in parallel, start the nodes again.
            4. Verify ApplicationCleanupPending alert, cleanup, verify resources
               on the secondary cluster (OCS 4.22+ alert checks).
            5. Relocate both interfaces in parallel and run the same checks on
               the primary cluster.

        """

        def execute(runner):
            # Fetched here so that only the item which executes the run sets
            # them up (including the ACM UI login).
            self._execute(
                runner,
                request.getfixturevalue("discovered_apps_dr_workload"),
                request.getfixturevalue("nodes_multicluster"),
                primary_cluster_down,
                request,
            )

        run_rbd_cephfs_test(request, execute)

    def _execute(
        self,
        runner,
        discovered_apps_dr_workload,
        nodes_multicluster,
        primary_down,
        request,
    ):
        """
        Run all the stages. Returns early when no interface is left healthy.
        """
        request.getfixturevalue("node_restart_teardown")
        request.getfixturevalue("setup_acm_ui")
        alerts_enabled = get_semantic_ocs_version_from_config() >= Version(
            "4.22", partial=True
        )

        # The factory returns every workload created so far (including a
        # half-deployed one of an interface that failed), so pick the ones of
        # this interface by the interface token in the namespace name.
        # Deployment is serial: it clones a git repository into a fixed directory.
        def deploy(group):
            result = discovered_apps_dr_workload(
                pvc_interface=group.pvc_interface, kubeobject=1, recipe=1
            )
            group.workloads = [
                wl for wl in result if group.name in wl.workload_namespace.split("-")
            ]

        runner.run_stage("deploy workloads", deploy, serial=True)

        def collect_details(group):
            first = group.workloads[0]
            collect_dr_details(
                group,
                [
                    DRPC(
                        namespace=constants.DR_OPS_NAMESPACE,
                        resource_name=wl.discovered_apps_placement_name,
                    )
                    for wl in group.workloads
                ],
                namespace=first.workload_namespace,
                discovered_apps=True,
                resource_name=first.discovered_apps_placement_name,
            )

        runner.run_stage("collect details", collect_details)
        if not runner.names:
            return

        primary_cluster, wait_time = get_common_primary_and_wait_time(
            runner, primary_down
        )

        logger.info(f"Waiting for {wait_time} minutes to run IOs")
        sleep(wait_time * 60)
        runner.run_stage(
            "verify kubeobject protection before failover",
            self._verify_kubeobject_protection,
        )

        def failover(group):
            for wl in group.workloads:
                dr_helpers.failover(
                    failover_cluster=group.secondary,
                    namespace=wl.workload_namespace,
                    discovered_apps=True,
                    workload_placement_name=wl.discovered_apps_placement_name,
                    old_primary=group.primary,
                    skip_odf_cli_validation=primary_down,
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

        self._cleanup_with_alerts(
            runner, "Failover", lambda g: g.primary, alerts_enabled
        )

        runner.run_stage(
            "verify resources on secondary after failover",
            lambda g: self._verify_resources(g, g.secondary, g.primary),
        )

        logger.info(f"Waiting for {wait_time} minutes to run IOs")
        sleep(wait_time * 60)
        runner.run_stage(
            "verify kubeobject protection after failover",
            self._verify_kubeobject_protection,
        )

        def relocate(group):
            for wl in group.workloads:
                dr_helpers.relocate(
                    preferred_cluster=group.primary,
                    namespace=wl.workload_namespace,
                    workload_placement_name=wl.discovered_apps_placement_name,
                    discovered_apps=True,
                    old_primary=group.secondary,
                    workload_instance=wl,
                    vm_auto_cleanup=alerts_enabled,
                )

        runner.run_stage("relocate", relocate)

        if alerts_enabled:
            self._cleanup_with_alerts(
                runner, "Relocate", lambda g: g.secondary, alerts_enabled
            )

        runner.run_stage(
            "verify resources on primary after relocate",
            lambda g: self._verify_resources(g, g.primary, g.secondary),
        )
        runner.run_stage(
            "verify kubeobject protection after relocate",
            self._verify_kubeobject_protection,
        )

    @staticmethod
    def _verify_kubeobject_protection(group):
        for drpc_obj, wl in zip(group.drpc_objs, group.workloads):
            dr_helpers.verify_last_kubeobject_protection_time(
                drpc_obj, wl.kubeobject_capture_interval_int
            )

    @staticmethod
    def _cleanup_with_alerts(runner, action, old_primary_of, alerts_enabled):
        """
        Fire check -> cleanup -> resolve check for every healthy interface.

        The alert checks use the single ACM UI session so they run serially;
        the cleanup itself runs in parallel. All firing checks are done before
        any cleanup, because the cleanup is what resolves the alert.

        Args:
            runner (ParallelWorkloadRunner): The runner
            action (str): 'Failover' or 'Relocate'
            old_primary_of (callable): group -> cluster to clean up
            alerts_enabled (bool): Verify ApplicationCleanupPending alert

        """
        acm_obj = None
        if alerts_enabled:
            wait_time_for_alert = (
                constants.ALERT_APPLICATION_CLEANUP_PENDING_THRESHOLD + 180
            )
            logger.info(
                f"Waiting for {wait_time_for_alert} seconds for "
                f"ApplicationCleanupPending alert to fire after {action}"
            )
            sleep(wait_time_for_alert)
            # Fresh ACM UI session as the previous one may have expired
            acm_obj = AcmAddClusters()

            def firing(group):
                for wl in group.workloads:
                    verify_pending_cleanup_alert_firing(
                        acm_obj, action, drpc_name=wl.discovered_apps_placement_name
                    )

            runner.run_stage(f"alert firing after {action}", firing, serial=True)

        def cleanup(group):
            for wl in group.workloads:
                dr_helpers.do_discovered_apps_cleanup(
                    drpc_name=wl.discovered_apps_placement_name,
                    old_primary=old_primary_of(group),
                    workload_namespace=wl.workload_namespace,
                    workload_dir=wl.workload_dir,
                    vrg_name=wl.discovered_apps_placement_name,
                )

        runner.run_stage(f"cleanup after {action}", cleanup)

        if alerts_enabled:

            def resolved(group):
                for wl in group.workloads:
                    verify_pending_cleanup_alert_resolved(
                        acm_obj, action, drpc_name=wl.discovered_apps_placement_name
                    )

            runner.run_stage(f"alert resolved after {action}", resolved, serial=True)

    @staticmethod
    def _verify_resources(group, new_primary, new_secondary):
        """
        Verify the workload resources on the cluster where it now runs and, for
        CephFS, the replication destinations on both clusters.
        """
        config.switch_to_cluster_by_name(new_primary)
        for wl in group.workloads:
            dr_helpers.wait_for_all_resources_creation(
                wl.workload_pvc_count,
                wl.workload_pod_count,
                wl.workload_namespace,
                timeout=1200,
                discovered_apps=True,
                vrg_name=wl.discovered_apps_placement_name,
                performed_dr_action=True,
            )

        verify_cephfs_replication(group, new_primary, new_secondary)
