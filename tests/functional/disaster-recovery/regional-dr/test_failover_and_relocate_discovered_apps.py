import re
import logging
from time import sleep

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.testlib import acceptance, tier1, tier4, skipif_ocs_version
from ocs_ci.framework.pytest_customization.marks import rdr, turquoise_squad
from ocs_ci.helpers import dr_helpers
from ocs_ci.ocs.node import get_node_objs, wait_for_nodes_status
from ocs_ci.ocs.resources.drpc import DRPC
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import (
    wait_for_pods_to_be_running,
    get_pods_having_label,
    get_pod_logs,
)
from ocs_ci.utility.utils import ceph_health_check

logger = logging.getLogger(__name__)


@rdr
@turquoise_squad
@skipif_ocs_version("<4.16")
class TestFailoverAndRelocateWithDiscoveredApps:
    """
    Test Failover and Relocate with Discovered Apps

    """

    @pytest.mark.parametrize(
        argnames=[
            "primary_cluster_down",
            "pvc_interface",
            "kubeobject",
            "recipe",
            "iterations",
        ],
        argvalues=[
            pytest.param(
                False,
                constants.CEPHBLOCKPOOL,
                1,
                1,
                1,
                marks=[tier1, acceptance],
                id="primary_up-rbd",
            ),
            pytest.param(
                True,
                constants.CEPHBLOCKPOOL,
                1,
                1,
                1,
                marks=tier4,
                id="primary_down-rbd",
            ),
            pytest.param(
                False,
                constants.CEPHBLOCKPOOL,
                1,
                1,
                3,
                marks=tier4,
                id="primary_up-rbd-multiple-iterations",
            ),
            pytest.param(
                True,
                constants.CEPHBLOCKPOOL,
                1,
                1,
                3,
                marks=tier4,
                id="primary_down-rbd-multiple-iterations",
            ),
            pytest.param(
                False,
                constants.CEPHFILESYSTEM,
                1,
                1,
                1,
                marks=[skipif_ocs_version("<4.19"), tier1, acceptance],
                id="primary_up-cephfs",
            ),
            pytest.param(
                True,
                constants.CEPHFILESYSTEM,
                1,
                1,
                1,
                marks=[skipif_ocs_version("<4.19"), tier4],
                id="primary_down-cephfs",
            ),
            pytest.param(
                False,
                constants.CEPHFILESYSTEM,
                1,
                1,
                3,
                marks=[skipif_ocs_version("<4.19"), tier4],
                id="primary_up-cephfs-multiple-iterations",
            ),
            pytest.param(
                True,
                constants.CEPHFILESYSTEM,
                1,
                1,
                3,
                marks=[skipif_ocs_version("<4.19"), tier4],
                id="primary_down-cephfs-multiple-iterations",
            ),
        ],
    )
    def test_failover_and_relocate_discovered_apps(
        self,
        discovered_apps_dr_workload,
        primary_cluster_down,
        pvc_interface,
        nodes_multicluster,
        kubeobject,
        recipe,
        iterations,
    ):
        """
        Tests to verify application failover and Relocate with Discovered Apps
        There are two test cases:
            1) Failover to secondary cluster when primary cluster is UP
            2) Relocate back to primary

        """
        rdr_workload = discovered_apps_dr_workload(
            pvc_interface=pvc_interface, kubeobject=kubeobject, recipe=recipe
        )[0]

        def check_ramen_dr_cluster_logs(cluster_name):
            """
            Check for exec hook executions in logs for applications

            Args:
                cluster_name (str): The name of the cluster to which the app has been failovered/relocated

            """
            restore_index = config.cur_index
            config.switch_to_cluster_by_name(cluster_name=cluster_name)

            # Get pods with the Ramen DR operator label
            pods = get_pods_having_label(
                label=constants.RAMEN_DR_CLUSTER_OPERATOR_APP_LABEL,
                namespace=constants.OPENSHIFT_DR_SYSTEM_NAMESPACE,
            )

            if not pods:
                logger.warning(
                    "No pods found with label %s",
                    constants.RAMEN_DR_CLUSTER_OPERATOR_APP_LABEL,
                )
                config.switch_ctx(restore_index)
                return

            # Fetch logs of the first pod
            pod_name = pods[0]["metadata"]["name"]
            pod_logs = get_pod_logs(
                pod_name=pod_name, namespace=constants.OPENSHIFT_DR_SYSTEM_NAMESPACE
            )

            if "executed exec command successfully" in pod_logs:
                # Regex to extract name and namespace from the log line
                pattern = re.compile(
                    r'executed exec command successfully.*?"name":"([^"]+)","namespace":"([^"]+)"'
                )
                matches = pattern.findall(pod_logs)

                # Track unique applications to avoid repeated logs
                seen = set()
                for name, namespace in matches:
                    if (
                        name in rdr_workload.workload_namespace
                        and (name, namespace) not in seen
                    ):
                        logger.info(
                            f"Exechook running on {name} application in discovered namespace {namespace}"
                        )
                        seen.add((name, namespace))

            else:
                logger.info(
                    "No 'executed exec command successfully' entries found in pod logs."
                )

            # Restore original cluster context
            config.switch_ctx(restore_index)

        iteration = 0
        while iteration < iterations:
            primary_cluster_name_before_failover = (
                dr_helpers.get_current_primary_cluster_name(
                    rdr_workload.workload_namespace,
                    discovered_apps=True,
                    resource_name=rdr_workload.discovered_apps_placement_name,
                )
            )
            config.switch_to_cluster_by_name(primary_cluster_name_before_failover)
            primary_cluster_name_before_failover_index = config.cur_index
            primary_cluster_name_before_failover_nodes = get_node_objs()
            secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
                rdr_workload.workload_namespace,
                discovered_apps=True,
                resource_name=rdr_workload.discovered_apps_placement_name,
            )

            scheduling_interval = dr_helpers.get_scheduling_interval(
                rdr_workload.workload_namespace,
                discovered_apps=True,
                resource_name=rdr_workload.discovered_apps_placement_name,
            )
            drpc_obj = DRPC(
                namespace=constants.DR_OPS_NAMESAPCE,
                resource_name=rdr_workload.discovered_apps_placement_name,
            )

            wait_time = 2 * scheduling_interval  # Time in minutes
            logger.info(f"Waiting for {wait_time} minutes to run IOs")
            sleep(wait_time * 60)
            if pvc_interface == constants.CEPHFILESYSTEM:
                # Verify the creation of ReplicationDestination resources on secondary cluster
                config.switch_to_cluster_by_name(secondary_cluster_name)
                dr_helpers.wait_for_replication_destinations_creation(
                    rdr_workload.workload_pvc_count, rdr_workload.workload_namespace
                )

            logger.info("Checking for lastKubeObjectProtectionTime")
            dr_helpers.verify_last_kubeobject_protection_time(
                drpc_obj, rdr_workload.kubeobject_capture_interval_int
            )

            if primary_cluster_down:
                config.switch_to_cluster_by_name(primary_cluster_name_before_failover)
                logger.info(
                    f"Stopping nodes of primary cluster: {primary_cluster_name_before_failover}"
                )
                nodes_multicluster[
                    primary_cluster_name_before_failover_index
                ].stop_nodes(primary_cluster_name_before_failover_nodes)

            dr_helpers.failover(
                failover_cluster=secondary_cluster_name,
                namespace=rdr_workload.workload_namespace,
                discovered_apps=True,
                workload_placement_name=rdr_workload.discovered_apps_placement_name,
                old_primary=primary_cluster_name_before_failover,
            )
            check_ramen_dr_cluster_logs(cluster_name=secondary_cluster_name)

            if primary_cluster_down:
                logger.info(
                    f"Waiting for {wait_time} minutes before starting nodes "
                    f"of primary cluster: {primary_cluster_name_before_failover}"
                )
                sleep(wait_time * 60)
                nodes_multicluster[
                    primary_cluster_name_before_failover_index
                ].start_nodes(primary_cluster_name_before_failover_nodes)
                wait_for_nodes_status(
                    [node.name for node in primary_cluster_name_before_failover_nodes]
                )
                logger.info(
                    "Wait for all the pods in openshift-storage to be in running state"
                )
                assert wait_for_pods_to_be_running(
                    timeout=720
                ), "Not all the pods reached running state"
                logger.info("Checking for Ceph Health OK")
                ceph_health_check()

            logger.info("Doing Cleanup Operations")
            dr_helpers.do_discovered_apps_cleanup(
                drpc_name=rdr_workload.discovered_apps_placement_name,
                old_primary=primary_cluster_name_before_failover,
                workload_namespace=rdr_workload.workload_namespace,
                workload_dir=rdr_workload.workload_dir,
                vrg_name=rdr_workload.discovered_apps_placement_name,
            )

            # Verify resources creation on secondary cluster (failoverCluster)
            config.switch_to_cluster_by_name(secondary_cluster_name)
            dr_helpers.wait_for_all_resources_creation(
                rdr_workload.workload_pvc_count,
                rdr_workload.workload_pod_count,
                rdr_workload.workload_namespace,
                timeout=1200,
                discovered_apps=True,
                vrg_name=rdr_workload.discovered_apps_placement_name,
            )

            if pvc_interface == constants.CEPHFILESYSTEM:
                config.switch_to_cluster_by_name(secondary_cluster_name)
                dr_helpers.wait_for_replication_destinations_deletion(
                    rdr_workload.workload_namespace
                )
                # Verify the creation of ReplicationDestination resources on primary cluster
                config.switch_to_cluster_by_name(primary_cluster_name_before_failover)
                dr_helpers.wait_for_replication_destinations_creation(
                    rdr_workload.workload_pvc_count, rdr_workload.workload_namespace
                )
            # Doing Relocate
            primary_cluster_name_after_failover = (
                dr_helpers.get_current_primary_cluster_name(
                    rdr_workload.workload_namespace,
                    discovered_apps=True,
                    resource_name=rdr_workload.discovered_apps_placement_name,
                )
            )
            config.switch_to_cluster_by_name(primary_cluster_name_before_failover)
            secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
                rdr_workload.workload_namespace,
                discovered_apps=True,
                resource_name=rdr_workload.discovered_apps_placement_name,
            )

            logger.info("Running Relocate Steps")
            logger.info(f"Waiting for {wait_time} minutes to run IOs")
            sleep(wait_time * 60)

            logger.info("Checking for lastKubeObjectProtectionTime")
            dr_helpers.verify_last_kubeobject_protection_time(
                drpc_obj, rdr_workload.kubeobject_capture_interval_int
            )

            dr_helpers.relocate(
                preferred_cluster=secondary_cluster_name,
                namespace=rdr_workload.workload_namespace,
                workload_placement_name=rdr_workload.discovered_apps_placement_name,
                discovered_apps=True,
                old_primary=primary_cluster_name_after_failover,
                workload_instance=rdr_workload,
            )
            check_ramen_dr_cluster_logs(cluster_name=secondary_cluster_name)

            logger.info(
                "Checking for lastKubeObjectProtectionTime post Relocate Operation"
            )
            dr_helpers.verify_last_kubeobject_protection_time(
                drpc_obj, rdr_workload.kubeobject_capture_interval_int
            )

            # Verify resources creation on secondary cluster (failoverCluster)
            config.switch_to_cluster_by_name(primary_cluster_name_before_failover)
            dr_helpers.wait_for_all_resources_creation(
                rdr_workload.workload_pvc_count,
                rdr_workload.workload_pod_count,
                rdr_workload.workload_namespace,
                timeout=1200,
                discovered_apps=True,
                vrg_name=rdr_workload.discovered_apps_placement_name,
            )

            if pvc_interface == constants.CEPHFILESYSTEM:
                config.switch_to_cluster_by_name(primary_cluster_name_before_failover)
                dr_helpers.wait_for_replication_destinations_deletion(
                    rdr_workload.workload_namespace
                )
                # Verify the creation of ReplicationDestination resources on primary cluster
                config.switch_to_cluster_by_name(primary_cluster_name_after_failover)
                dr_helpers.wait_for_replication_destinations_creation(
                    rdr_workload.workload_pvc_count, rdr_workload.workload_namespace
                )
            logger.info(f"Iteration {iteration} completed !!!!!!!")
            iteration += 1
