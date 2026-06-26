import logging
import pytest

from time import sleep

from ocs_ci.deployment.cnv import CNVInstaller
from ocs_ci.framework import config
from ocs_ci.framework.testlib import tier1
from ocs_ci.framework.pytest_customization.marks import turquoise_squad, rdr
from ocs_ci.helpers import dr_helpers
from ocs_ci.helpers.cnv_helpers import run_dd_io
from ocs_ci.helpers.dr_helpers import (
    check_mirroring_status_for_custom_pool,
    verify_custom_pool_image_isolation,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.dr.dr_workload import validate_data_integrity_vm
from ocs_ci.ocs.node import get_node_objs, wait_for_nodes_status
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.utility.utils import ceph_health_check

logger = logging.getLogger(__name__)


@rdr
@tier1
@turquoise_squad
class TestCNVFailoverAndRelocateWithDiscoveredApps:
    """
    Test CNV Failover and Relocate with Discovered Apps

    """

    @pytest.mark.parametrize(
        argnames=["custom_sc", "replica", "compression"],
        argvalues=[
            pytest.param(
                False,
                3,
                None,
                marks=pytest.mark.polarion_id("OCS-6266"),
                id="default_pool_replica3_without_compression",
            ),
            pytest.param(
                True,
                2,
                None,
                # marks=pytest.mark.polarion_id("OCS-XXXX"),
                id="custom_pool_replica2_without_compression",
            ),
            # TODO: ADD Polarion ID for Custom SC test
            pytest.param(
                True,
                3,
                "aggressive",
                # marks=pytest.mark.polarion_id("OCS-XXXX"),
                id="custom_pool_replica3_with_compression",
            ),
            # TODO: ADD Polarion ID for Custom SC test
            pytest.param(
                True,
                2,
                "aggressive",
                # marks=pytest.mark.polarion_id("OCS-XXXX"),
                id="custom_pool_replica2_with_compression",
            ),
            # TODO: ADD Polarion ID for Custom SC test
        ],
    )
    def test_cnv_failover_and_relocate_discovered_apps(
        self,
        custom_sc,
        replica,
        compression,
        cnv_custom_storage_class,
        discovered_apps_dr_workload_cnv,
        nodes_multicluster,
        node_restart_teardown,
    ):
        """
        Tests to verify cnv application failover and Relocate with Discovered Apps
        There are two test cases:
            1) Failover to secondary cluster when primary cluster is down. Primary managed cluster is failed
            before failover operation and recovered after successful failover.
            2) Relocate back to primary

        Test is parametrized to run with Custom RBD Storage Class and Pool of Replica-2.

        """

        md5sum_original = []
        md5sum_failover = []
        vm_filepaths = ["/dd_file1.txt", "/dd_file2.txt", "/dd_file3.txt"]

        custom_dr_policy_name = None
        if custom_sc:
            logger.test_step(
                "Create custom pool and StorageClass with "
                f"replica={replica}, compression={compression}"
            )
            custom_dr_policy_name = cnv_custom_storage_class(
                replica=replica, compression=compression
            )

        logger.test_step("Deploy CNV discovered app workloads")
        cnv_workloads = discovered_apps_dr_workload_cnv(
            pvc_vm=1,
            custom_sc=custom_sc,
            dr_policy_name=custom_dr_policy_name,
        )

        primary_cluster_name_before_failover = (
            dr_helpers.get_current_primary_cluster_name(
                cnv_workloads[0].workload_namespace,
                discovered_apps=True,
                resource_name=cnv_workloads[0].discovered_apps_placement_name,
            )
        )
        logger.info(
            "Primary cluster before failover: %s",
            primary_cluster_name_before_failover,
        )
        config.switch_to_cluster_by_name(primary_cluster_name_before_failover)
        CNVInstaller().download_and_extract_virtctl_binary()
        secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
            cnv_workloads[0].workload_namespace,
            discovered_apps=True,
            resource_name=cnv_workloads[0].discovered_apps_placement_name,
        )

        logger.test_step("Write initial data to VMs and record checksums")
        for cnv_wl in cnv_workloads:
            md5sum_original.append(
                run_dd_io(
                    vm_obj=cnv_wl.vm_obj,
                    file_path=vm_filepaths[0],
                    username=cnv_wl.vm_username,
                    verify=True,
                )
            )

        for cnv_wl, md5sum in zip(cnv_workloads, md5sum_original):
            logger.info(
                "Original checksum: file=%s, vm=%s, md5=%s",
                vm_filepaths[0],
                cnv_wl.workload_name,
                md5sum,
            )

        scheduling_interval = dr_helpers.get_scheduling_interval(
            cnv_workloads[0].workload_namespace,
            discovered_apps=True,
            resource_name=cnv_workloads[0].discovered_apps_placement_name,
        )
        wait_time = 2 * scheduling_interval
        logger.info("Waiting %s minutes for IOs to sync", wait_time)
        sleep(wait_time * 60)

        if custom_sc:
            logger.test_step(
                "Verify RBD images exist only in custom pool"
                " on both managed clusters"
            )
            verify_custom_pool_image_isolation(pool_name=constants.RDR_CUSTOM_RBD_POOL)

            logger.test_step("Verify mirroring status for custom pool")
            logger.assertion(
                "Mirroring status check for custom pool %s",
                constants.RDR_CUSTOM_RBD_POOL,
            )
            assert check_mirroring_status_for_custom_pool(
                pool_name=constants.RDR_CUSTOM_RBD_POOL
            ), "Mirroring status check for custom SC failed"

        logger.test_step("Shutdown primary managed cluster nodes")
        config.switch_to_cluster_by_name(primary_cluster_name_before_failover)
        active_primary_index = config.cur_index
        active_primary_cluster_node_objs = get_node_objs()

        nodes_multicluster[active_primary_index].stop_nodes(
            active_primary_cluster_node_objs
        )
        logger.info(
            "All nodes of primary cluster powered off, "
            "waiting 300s for cluster to become unreachable"
        )
        sleep(300)

        logger.test_step("Failover to secondary cluster %s", secondary_cluster_name)
        dr_helpers.failover(
            failover_cluster=secondary_cluster_name,
            namespace=cnv_workloads[0].workload_namespace,
            discovered_apps=True,
            workload_placement_name=cnv_workloads[0].discovered_apps_placement_name,
            old_primary=primary_cluster_name_before_failover,
        )

        logger.test_step("Verify resources on secondary cluster after failover")
        config.switch_to_cluster_by_name(secondary_cluster_name)
        dr_helpers.wait_for_all_resources_creation(
            cnv_workloads[0].workload_pvc_count,
            cnv_workloads[0].workload_pod_count,
            cnv_workloads[0].workload_namespace,
            discovered_apps=True,
            vrg_name=cnv_workloads[0].discovered_apps_placement_name,
        )
        dr_helpers.wait_for_cnv_workload(
            vm_name=cnv_workloads[0].vm_name,
            namespace=cnv_workloads[0].workload_namespace,
            phase=constants.STATUS_RUNNING,
        )

        logger.test_step("Validate data integrity after failover")
        validate_data_integrity_vm(
            cnv_workloads, vm_filepaths[0], md5sum_original, "Failover"
        )

        logger.test_step("Write new data to VMs post-failover")
        for cnv_wl in cnv_workloads:
            md5sum_failover.append(
                run_dd_io(
                    vm_obj=cnv_wl.vm_obj,
                    file_path=vm_filepaths[1],
                    username=cnv_wl.vm_username,
                    verify=True,
                )
            )

        for cnv_wl, md5sum in zip(cnv_workloads, md5sum_failover):
            logger.info(
                "Post-failover checksum: file=%s, vm=%s, md5=%s",
                vm_filepaths[1],
                cnv_wl.workload_name,
                md5sum,
            )

        logger.test_step("Recover the primary managed cluster")
        config.switch_to_cluster_by_name(primary_cluster_name_before_failover)
        nodes_multicluster[active_primary_index].start_nodes(
            active_primary_cluster_node_objs
        )
        wait_for_nodes_status([node.name for node in active_primary_cluster_node_objs])
        wait_for_pods_to_be_running(timeout=420, sleep=15)
        logger.assertion("Ceph health check after primary cluster recovery")
        assert ceph_health_check(tries=10, delay=30)

        logger.test_step("Cleanup discovered apps resources after failover")
        dr_helpers.do_discovered_apps_cleanup(
            drpc_name=cnv_workloads[0].discovered_apps_placement_name,
            old_primary=primary_cluster_name_before_failover,
            workload_namespace=cnv_workloads[0].workload_namespace,
            workload_dir=cnv_workloads[0].workload_dir,
            vrg_name=cnv_workloads[0].discovered_apps_placement_name,
        )

        if custom_sc:
            logger.assertion(
                "Mirroring status check for custom pool %s after failover cleanup",
                constants.RDR_CUSTOM_RBD_POOL,
            )
            assert check_mirroring_status_for_custom_pool(
                pool_name=constants.RDR_CUSTOM_RBD_POOL
            ), "Mirroring status check for custom SC failed"

        logger.test_step("Relocate workloads back to original primary cluster")
        primary_cluster_name_after_failover = (
            dr_helpers.get_current_primary_cluster_name(
                cnv_workloads[0].workload_namespace,
                discovered_apps=True,
                resource_name=cnv_workloads[0].discovered_apps_placement_name,
            )
        )
        config.switch_to_cluster_by_name(primary_cluster_name_before_failover)
        secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
            cnv_workloads[0].workload_namespace,
            discovered_apps=True,
            resource_name=cnv_workloads[0].discovered_apps_placement_name,
        )

        scheduling_interval = dr_helpers.get_scheduling_interval(
            cnv_workloads[0].workload_namespace,
            discovered_apps=True,
            resource_name=cnv_workloads[0].discovered_apps_placement_name,
        )
        wait_time = 2 * scheduling_interval
        logger.info("Waiting %s minutes for IOs to sync before relocate", wait_time)
        sleep(wait_time * 60)

        dr_helpers.relocate(
            preferred_cluster=secondary_cluster_name,
            namespace=cnv_workloads[0].workload_namespace,
            workload_placement_name=cnv_workloads[0].discovered_apps_placement_name,
            discovered_apps=True,
            old_primary=primary_cluster_name_after_failover,
            workload_instance=cnv_workloads[0],
        )

        logger.test_step("Verify resources on primary cluster after relocate")
        config.switch_to_cluster_by_name(primary_cluster_name_before_failover)
        dr_helpers.wait_for_all_resources_creation(
            cnv_workloads[0].workload_pvc_count,
            cnv_workloads[0].workload_pod_count,
            cnv_workloads[0].workload_namespace,
            discovered_apps=True,
            vrg_name=cnv_workloads[0].discovered_apps_placement_name,
        )
        dr_helpers.wait_for_cnv_workload(
            vm_name=cnv_workloads[0].vm_name,
            namespace=cnv_workloads[0].workload_namespace,
            phase=constants.STATUS_RUNNING,
        )

        logger.test_step("Validate data integrity after relocate")
        validate_data_integrity_vm(
            cnv_workloads, vm_filepaths[0], md5sum_original, "Relocate"
        )
        validate_data_integrity_vm(
            cnv_workloads, vm_filepaths[1], md5sum_failover, "Relocate"
        )

        logger.test_step("Write data to VMs post-relocate")
        for cnv_wl in cnv_workloads:
            run_dd_io(
                vm_obj=cnv_wl.vm_obj,
                file_path=vm_filepaths[2],
                username=cnv_wl.vm_username,
                verify=True,
            )

        if custom_sc:
            logger.assertion(
                "Mirroring status check for custom pool %s after relocate",
                constants.RDR_CUSTOM_RBD_POOL,
            )
            assert check_mirroring_status_for_custom_pool(
                pool_name=constants.RDR_CUSTOM_RBD_POOL
            ), "Mirroring status check for custom SC failed"
