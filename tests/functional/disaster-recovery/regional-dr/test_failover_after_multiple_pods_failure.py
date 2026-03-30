import logging
import time
from concurrent.futures import ThreadPoolExecutor

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    rdr,
    turquoise_squad,
    tier4,
    tier4c,
)
from ocs_ci.helpers import dr_helpers
from ocs_ci.helpers.dr_helpers import (
    get_current_primary_cluster_name,
    get_current_secondary_cluster_name,
    get_scheduling_interval,
    failover,
    wait_for_all_resources_creation,
    wait_for_all_resources_deletion,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.drpc import DRPC
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.utility.utils import ceph_health_check

logger = logging.getLogger(__name__)


@rdr
@tier4
@tier4c
@turquoise_squad
class TestFailoverAfterMultiplePodsFailure:
    """
    Test Failover action via CLI after multiple pods failure on the primary managed cluster which is done by
    scaling down various deployments in namespaces "openshift-storage" and "submariner-operator".

    """

    def test_failover_after_multiple_pods_failure(
        self,
        dr_workload,
        scale_deployments,
        node_restart_teardown,
    ):
        """
        Tests to verify application failover between managed clusters when multiple deployments are scaled down on the
        primary managed cluster.

        """

        # Deploy Subscription and Appset based application of both RBD and CephFS SC
        rdr_workload = dr_workload(
            num_of_subscription=1,
            num_of_appset=1,
            pvc_interface=constants.CEPHBLOCKPOOL,
        )
        dr_workload(
            num_of_subscription=1,
            num_of_appset=1,
            pvc_interface=constants.CEPHFILESYSTEM,
        )
        drpc_objs = []
        for wl in rdr_workload:
            if wl.workload_type == constants.SUBSCRIPTION:
                drpc_objs.append(DRPC(namespace=wl.workload_namespace))
            else:
                drpc_objs.append(
                    DRPC(
                        namespace=constants.GITOPS_CLUSTER_NAMESPACE,
                        resource_name=f"{wl.appset_placement_name}-drpc",
                    )
                )

        primary_cluster_name = get_current_primary_cluster_name(
            rdr_workload[0].workload_namespace
        )
        secondary_cluster_name = get_current_secondary_cluster_name(
            rdr_workload[0].workload_namespace
        )

        # Verify the creation of ReplicationDestination resources on secondary cluster in case of CephFS
        config.switch_to_cluster_by_name(secondary_cluster_name)
        for wl in rdr_workload:
            if wl.pvc_interface == constants.CEPHFILESYSTEM:
                dr_helpers.wait_for_replication_destinations_creation(
                    wl.workload_pvc_count, wl.workload_namespace
                )
                # Verifying the existence of replication group destination and volume snapshots
                # in case of CG enabled for CephFS
                if dr_helpers.is_cg_cephfs_enabled():
                    dr_helpers.wait_for_resource_existence(
                        kind=constants.REPLICATION_GROUP_DESTINATION,
                        namespace=wl.workload_namespace,
                        should_exist=True,
                    )
                    dr_helpers.wait_for_resource_count(
                        kind=constants.VOLUMESNAPSHOT,
                        namespace=wl.workload_namespace,
                        expected_count=wl.workload_pvc_count,
                    )

        scheduling_interval = get_scheduling_interval(
            rdr_workload[0].workload_namespace, rdr_workload[0].workload_type
        )

        two_times_scheduling_interval = 2 * scheduling_interval  # Time in minutes
        time.sleep(two_times_scheduling_interval * 60)

        logger.info("Calling fixture to scale down deployments")
        config.switch_to_cluster_by_name(primary_cluster_name)
        scale_deployments("down")
        time.sleep(120)

        # Failover action via CLI
        logger.info("Failover workloads after pods failure")
        failover_results = []
        with ThreadPoolExecutor() as executor:
            for wl in rdr_workload:
                failover_results.append(
                    executor.submit(
                        failover,
                        failover_cluster=secondary_cluster_name,
                        namespace=wl.workload_namespace,
                        workload_type=wl.workload_type,
                        workload_placement_name=(
                            wl.appset_placement_name
                            if wl.workload_type != constants.SUBSCRIPTION
                            else None
                        ),
                    )
                )
                time.sleep(5)

        # Wait for failover results
        for fl in failover_results:
            fl.result()

        # Verify resources creation on secondary cluster (failoverCluster)
        config.switch_to_cluster_by_name(secondary_cluster_name)
        for wl in rdr_workload:
            wait_for_all_resources_creation(
                wl.workload_pvc_count,
                wl.workload_pod_count,
                wl.workload_namespace,
            )

        logger.info("Calling fixture to scale up deployments")
        config.switch_to_cluster_by_name(primary_cluster_name)
        scale_deployments("up")
        wait_for_pods_to_be_running(
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE, timeout=420, sleep=30
        )
        wait_for_pods_to_be_running(
            namespace=constants.SUBMARINER_OPERATOR_NAMESPACE, timeout=420, sleep=30
        )
        ceph_health_check()

        # Verify application are deleted from old cluster
        for wl in rdr_workload:
            wait_for_all_resources_deletion(wl.workload_namespace, timeout=1800)

        for wl in rdr_workload:
            if wl.pvc_interface == constants.CEPHFILESYSTEM:
                # Verify the deletion of ReplicationDestination resources on secondary cluster
                config.switch_to_cluster_by_name(secondary_cluster_name)
                dr_helpers.wait_for_replication_destinations_deletion(
                    wl.workload_namespace
                )
                cg_enabled = dr_helpers.is_cg_cephfs_enabled()
                if cg_enabled:
                    dr_helpers.wait_for_resource_existence(
                        kind=constants.REPLICATION_GROUP_DESTINATION,
                        namespace=wl.workload_namespace,
                        should_exist=False,
                    )
                # Verify the creation of ReplicationDestination resources on primary cluster
                config.switch_to_cluster_by_name(primary_cluster_name)
                dr_helpers.wait_for_replication_destinations_creation(
                    wl.workload_pvc_count, wl.workload_namespace
                )

                if cg_enabled:
                    dr_helpers.wait_for_resource_existence(
                        kind=constants.REPLICATION_GROUP_DESTINATION,
                        namespace=wl.workload_namespace,
                        should_exist=True,
                    )

                    # Verify the creation of Volume Snapshot
                    dr_helpers.wait_for_resource_count(
                        kind=constants.VOLUMESNAPSHOT,
                        namespace=wl.workload_namespace,
                        expected_count=wl.workload_pvc_count,
                    )

        dr_helpers.wait_for_mirroring_status_ok(
            replaying_images=sum(
                [
                    wl.workload_pvc_count
                    for wl in rdr_workload
                    if wl.pvc_interface == constants.CEPHBLOCKPOOL
                ]
            )
        )
        logger.info("Failover successful")
