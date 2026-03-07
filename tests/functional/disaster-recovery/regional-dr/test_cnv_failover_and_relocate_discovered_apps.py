import logging
import pytest

from time import sleep

from ocs_ci.deployment.cnv import CNVInstaller
from ocs_ci.framework import config
from ocs_ci.framework.testlib import tier1
from ocs_ci.framework.pytest_customization.marks import turquoise_squad, rdr
from ocs_ci.helpers import dr_helpers
from ocs_ci.helpers.cnv_helpers import run_dd_io
from ocs_ci.helpers.dr_helpers import check_mirroring_status_for_custom_pool
from ocs_ci.ocs import constants
from ocs_ci.ocs.dr.dr_workload import validate_data_integrity_vm
from ocs_ci.ocs.node import get_node_objs, wait_for_nodes_status
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.ocs.resources.pvc import get_all_pvcs
from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility.utils import ceph_health_check

logger = logging.getLogger(__name__)


def validate_vm_pvcs(vm_obj, expected_pvc_count, namespace):
    """
    Validate that VM has the expected number of PVCs attached and they are in Bound state

    Args:
        vm_obj: Virtual Machine object
        expected_pvc_count (int): Expected number of PVCs
        namespace (str): Namespace where VM and PVCs exist

    Returns:
        bool: True if validation passes, False otherwise

    """
    logger.info(f"Validating {expected_pvc_count} PVCs for VM {vm_obj.name}")

    # Get VM spec to find all PVCs
    vm_data = vm_obj.get()
    volumes = (
        vm_data.get("spec", {})
        .get("template", {})
        .get("spec", {})
        .get("volumes", [])
    )

    pvc_names = []
    for volume in volumes:
        if "persistentVolumeClaim" in volume:
            pvc_name = volume["persistentVolumeClaim"].get("claimName")
            if pvc_name:
                pvc_names.append(pvc_name)

    logger.info(f"Found {len(pvc_names)} PVCs attached to VM: {pvc_names}")

    if len(pvc_names) != expected_pvc_count:
        logger.error(
            f"Expected {expected_pvc_count} PVCs but found {len(pvc_names)}"
        )
        return False

    # Verify all PVCs are in Bound state
    pvc_obj = OCP(kind=constants.PVC, namespace=namespace)
    for pvc_name in pvc_names:
        pvc_data = pvc_obj.get(resource_name=pvc_name)
        pvc_status = pvc_data.get("status", {}).get("phase")
        logger.info(f"PVC {pvc_name} status: {pvc_status}")
        if pvc_status != constants.STATUS_BOUND:
            logger.error(f"PVC {pvc_name} is not in Bound state: {pvc_status}")
            return False

    logger.info(f"All {expected_pvc_count} PVCs are validated successfully")
    return True


def validate_vm_network_interfaces(vm_obj, expected_nic_count):
    """
    Validate that VM has the expected number of network interfaces

    Args:
        vm_obj: Virtual Machine object
        expected_nic_count (int): Expected number of network interfaces

    Returns:
        bool: True if validation passes, False otherwise

    """
    logger.info(f"Validating {expected_nic_count} NICs for VM {vm_obj.name}")

    # Get VM spec to find all network interfaces
    vm_data = vm_obj.get()
    networks = (
        vm_data.get("spec", {})
        .get("template", {})
        .get("spec", {})
        .get("networks", [])
    )

    logger.info(f"Found {len(networks)} network interfaces on VM")

    if len(networks) != expected_nic_count:
        logger.error(
            f"Expected {expected_nic_count} NICs but found {len(networks)}"
        )
        return False

    # Get VMI to check interface status
    try:
        vmi_data = vm_obj.vmi_obj.get()
        interfaces = (
            vmi_data.get("status", {})
            .get("interfaces", [])
        )

        logger.info(f"VMI has {len(interfaces)} active interfaces")

        for idx, interface in enumerate(interfaces):
            interface_name = interface.get("name", f"interface-{idx}")
            ip_address = interface.get("ipAddress", "N/A")
            logger.info(f"Interface {interface_name}: IP={ip_address}")

        if len(interfaces) < expected_nic_count:
            logger.warning(
                f"VMI shows {len(interfaces)} active interfaces, "
                f"expected {expected_nic_count}"
            )
    except Exception as e:
        logger.warning(f"Could not verify VMI interface status: {e}")

    logger.info(f"All {expected_nic_count} NICs are validated successfully")
    return True


@rdr
@tier1
@turquoise_squad
class TestCNVFailoverAndRelocateWithDiscoveredApps:
    """
    Test CNV Failover and Relocate with Discovered Apps

    """

    @pytest.mark.parametrize(
        argnames=["custom_sc", "replica", "compression", "num_pvcs", "num_nics"],
        argvalues=[
            pytest.param(
                False,
                3,
                None,
                1,
                1,
                marks=pytest.mark.polarion_id("OCS-6266"),
                id="default_pool_replica3_without_compression_1pvc_1nic",
            ),
            pytest.param(
                False,
                3,
                None,
                3,
                2,
                # marks=pytest.mark.polarion_id("OCS-XXXX"),
                id="default_pool_replica3_without_compression_3pvcs_2nics",
            ),
            pytest.param(
                True,
                2,
                None,
                1,
                1,
                # marks=pytest.mark.polarion_id("OCS-XXXX"),
                id="custom_pool_replica2_without_compression_1pvc_1nic",
            ),
            pytest.param(
                True,
                2,
                None,
                2,
                2,
                # marks=pytest.mark.polarion_id("OCS-XXXX"),
                id="custom_pool_replica2_without_compression_2pvcs_2nics",
            ),
            # TODO: ADD Polarion ID for Custom SC test
            pytest.param(
                True,
                3,
                "aggressive",
                1,
                1,
                # marks=pytest.mark.polarion_id("OCS-XXXX"),
                id="custom_pool_replica3_with_compression_1pvc_1nic",
            ),
            # TODO: ADD Polarion ID for Custom SC test
            pytest.param(
                True,
                2,
                "aggressive",
                3,
                2,
                # marks=pytest.mark.polarion_id("OCS-XXXX"),
                id="custom_pool_replica2_with_compression_3pvcs_2nics",
            ),
            # TODO: ADD Polarion ID for Custom SC test
        ],
    )
    def test_cnv_failover_and_relocate_discovered_apps(
        self,
        custom_sc,
        replica,
        compression,
        num_pvcs,
        num_nics,
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

        Test is parametrized to run with:
        - Custom RBD Storage Class and Pool of Replica-2/3
        - Multiple PVCs (1-3) attached to VM
        - Multiple NICs (1-2) attached to VM

        """

        md5sum_original = []
        md5sum_failover = []
        vm_filepaths = ["/dd_file1.txt", "/dd_file2.txt", "/dd_file3.txt"]

        if custom_sc:
            logger.info("Calling fixture to create Custom Pool/SC..")
            cnv_custom_storage_class(replica=replica, compression=compression)

        logger.info(f"Creating VM with {num_pvcs} PVCs and {num_nics} NICs")
        cnv_workloads = discovered_apps_dr_workload_cnv(
            pvc_vm=num_pvcs, custom_sc=custom_sc, num_nics=num_nics
        )

        primary_cluster_name_before_failover = (
            dr_helpers.get_current_primary_cluster_name(
                cnv_workloads[0].workload_namespace,
                discovered_apps=True,
                resource_name=cnv_workloads[0].discovered_apps_placement_name,
            )
        )
        logger.info(
            f"Primary cluster name before failover is {primary_cluster_name_before_failover}"
        )
        config.switch_to_cluster_by_name(primary_cluster_name_before_failover)
        # Download and extract the virtctl binary to bin_dir. Skips if already present.
        CNVInstaller().download_and_extract_virtctl_binary()
        secondary_cluster_name = dr_helpers.get_current_secondary_cluster_name(
            cnv_workloads[0].workload_namespace,
            discovered_apps=True,
            resource_name=cnv_workloads[0].discovered_apps_placement_name,
        )

        # Creating a file (file1) on VM and calculating its MD5sum
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
                f"Original checksum of file {vm_filepaths[0]} on VM {cnv_wl.workload_name}: {md5sum}"
            )

        scheduling_interval = dr_helpers.get_scheduling_interval(
            cnv_workloads[0].workload_namespace,
            discovered_apps=True,
            resource_name=cnv_workloads[0].discovered_apps_placement_name,
        )
        if custom_sc:
            assert check_mirroring_status_for_custom_pool(
                pool_name=constants.RDR_CUSTOM_RBD_POOL
            ), "Mirroring status check for custom SC failed"
            logger.info("Mirroring status check for custom SC passed")

        wait_time = 2 * scheduling_interval  # Time in minutes
        logger.info(f"Waiting for {wait_time} minutes to run IOs")
        sleep(wait_time * 60)

        config.switch_to_cluster_by_name(primary_cluster_name_before_failover)
        active_primary_index = config.cur_index
        active_primary_cluster_node_objs = get_node_objs()

        # Shutdown primary managed cluster nodes
        logger.info("Shutting down all the nodes of the primary managed cluster")
        nodes_multicluster[active_primary_index].stop_nodes(
            active_primary_cluster_node_objs
        )
        logger.info(
            "All nodes of the primary managed cluster are powered off, "
            "waiting for cluster to be unreachable.."
        )
        sleep(300)

        dr_helpers.failover(
            failover_cluster=secondary_cluster_name,
            namespace=cnv_workloads[0].workload_namespace,
            discovered_apps=True,
            workload_placement_name=cnv_workloads[0].discovered_apps_placement_name,
            old_primary=primary_cluster_name_before_failover,
        )

        # Verify resources creation on secondary cluster (failoverCluster)
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

        # Validate PVCs and NICs after failover
        logger.info("Validating PVCs and NICs after failover")
        for cnv_wl in cnv_workloads:
            assert validate_vm_pvcs(
                cnv_wl.vm_obj, num_pvcs, cnv_wl.workload_namespace
            ), f"PVC validation failed for VM {cnv_wl.workload_name} after failover"

            assert validate_vm_network_interfaces(
                cnv_wl.vm_obj, num_nics
            ), f"NIC validation failed for VM {cnv_wl.workload_name} after failover"

        # Validating data integrity (file1) after failing-over VMs to secondary managed cluster
        validate_data_integrity_vm(
            cnv_workloads, vm_filepaths[0], md5sum_original, "Failover"
        )

        # Creating a file (file2) post failover
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
                f"Checksum of files written after Failover: {vm_filepaths[1]} on VM {cnv_wl.workload_name}: {md5sum}"
            )

        logger.info("Recover the down managed cluster")
        config.switch_to_cluster_by_name(primary_cluster_name_before_failover)
        nodes_multicluster[active_primary_index].start_nodes(
            active_primary_cluster_node_objs
        )
        wait_for_nodes_status([node.name for node in active_primary_cluster_node_objs])
        wait_for_pods_to_be_running(timeout=420, sleep=15)
        assert ceph_health_check(tries=10, delay=30)

        logger.info("Doing Cleanup Operations")
        dr_helpers.do_discovered_apps_cleanup(
            drpc_name=cnv_workloads[0].discovered_apps_placement_name,
            old_primary=primary_cluster_name_before_failover,
            workload_namespace=cnv_workloads[0].workload_namespace,
            workload_dir=cnv_workloads[0].workload_dir,
            vrg_name=cnv_workloads[0].discovered_apps_placement_name,
        )

        if custom_sc:
            assert check_mirroring_status_for_custom_pool(
                pool_name=constants.RDR_CUSTOM_RBD_POOL
            ), "Mirroring status check for custom SC failed"
            logger.info("Mirroring status check for custom SC passed")

        # Doing Relocate in below code
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
        logger.info("Running Relocate Steps")
        wait_time = 2 * scheduling_interval  # Time in minutes
        logger.info(f"Waiting for {wait_time} minutes to run IOs")
        sleep(wait_time * 60)

        dr_helpers.relocate(
            preferred_cluster=secondary_cluster_name,
            namespace=cnv_workloads[0].workload_namespace,
            workload_placement_name=cnv_workloads[0].discovered_apps_placement_name,
            discovered_apps=True,
            old_primary=primary_cluster_name_after_failover,
            workload_instance=cnv_workloads[0],
        )
        # Verify resources creation on primary managed cluster
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

        # Validate PVCs and NICs after relocate
        logger.info("Validating PVCs and NICs after relocate")
        for cnv_wl in cnv_workloads:
            assert validate_vm_pvcs(
                cnv_wl.vm_obj, num_pvcs, cnv_wl.workload_namespace
            ), f"PVC validation failed for VM {cnv_wl.workload_name} after relocate"

            assert validate_vm_network_interfaces(
                cnv_wl.vm_obj, num_nics
            ), f"NIC validation failed for VM {cnv_wl.workload_name} after relocate"

        # Validating data integrity (file1) after relocating VMs back to primary managed cluster
        validate_data_integrity_vm(
            cnv_workloads, vm_filepaths[0], md5sum_original, "Relocate"
        )

        # Validating data integrity (file2) after relocating VMs back to primary managed cluster
        validate_data_integrity_vm(
            cnv_workloads, vm_filepaths[1], md5sum_failover, "Relocate"
        )

        # Creating a file (file3) post relocate
        for cnv_wl in cnv_workloads:
            run_dd_io(
                vm_obj=cnv_wl.vm_obj,
                file_path=vm_filepaths[2],
                username=cnv_wl.vm_username,
                verify=True,
            )

        if custom_sc:
            assert check_mirroring_status_for_custom_pool(
                pool_name=constants.RDR_CUSTOM_RBD_POOL
            ), "Mirroring status check for custom SC failed"
            logger.info("Mirroring status check for custom SC passed")
