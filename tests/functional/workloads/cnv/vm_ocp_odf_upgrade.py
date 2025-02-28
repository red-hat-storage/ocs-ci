import logging
import os
import random
import pytest
from concurrent.futures import ThreadPoolExecutor, as_completed

from ocs_ci.deployment.disconnected import mirror_ocp_release_images
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import magenta_squad, workloads
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.helpers.cnv_helpers import get_vm_status, run_dd_io, cal_md5sum_vm
from ocs_ci.helpers.keyrotation_helper import PVKeyrotation
from ocs_ci.ocs import constants
from ocs_ci.ocs.cluster import (
    CephCluster,
    CephClusterMultiCluster,
    MulticlusterCephHealthMonitor,
    CephHealthMonitor,
)
from ocs_ci.ocs.exceptions import CephHealthException
from ocs_ci.ocs.ocp import check_cluster_operator_versions
from ocs_ci.ocs.ocs_upgrade import run_ocs_upgrade
from ocs_ci.ocs.resources.pod import get_ceph_tools_pod, check_pods_in_running_state
from ocs_ci.ocs.utils import is_acm_cluster, get_non_acm_cluster_config
from ocs_ci.utility.multicluster import MDRClusterUpgradeParametrize
from ocs_ci.utility.ocp_upgrade import (
    pause_machinehealthcheck,
    resume_machinehealthcheck,
)
from ocs_ci.utility.rosa import upgrade_rosa_cluster
from ocs_ci.utility.utils import (
    get_latest_ocp_version,
    expose_ocp_version,
    TimeoutSampler,
    ceph_health_check,
    ceph_crash_info_display,
    archive_ceph_crashes,
    load_config_file,
)
from ocs_ci.utility.version import (
    ocp_version_available_on_rosa,
    drop_z_version,
    get_latest_rosa_ocp_version,
    get_semantic_ocp_running_version,
    VERSION_4_8,
)
from ocs_ci.ocs import ocp
from semantic_version import Version
from pkg_resources import parse_version
from ocs_ci.ocs.node import get_nodes

logger = logging.getLogger(__name__)


@magenta_squad
class TestOcvUpgrade(E2ETest):
    """
    Test OCV upgrade while VMs are in different states,
    snapshots and clone exists and while performing
    various operation during operator upgrade
    """

    @pytest.fixture(autouse=True)
    def setup(
        self,
        project_factory,
        pv_encryption_kms_setup_factory,
        storageclass_factory,
        cnv_workload,
        snapshot_factory,
        clone_vm_workload,
    ):
        """
        Setting up VMs for tests
        """
        self.file_paths = ["/file.txt"]
        num_vms = 3
        logger.info("Setting up csi-kms-connection-details configmap")
        kms = pv_encryption_kms_setup_factory(kv_version="v2")
        logger.info("csi-kms-connection-details setup successful")

        sc_obj_def = storageclass_factory(
            interface=constants.CEPHBLOCKPOOL,
            encrypted=True,
            encryption_kms_id=kms.kmsid,
            new_rbd_pool=True,
            mapOptions="krbd:rxbounce",
            mounter="rbd",
        )

        proj_obj = project_factory()
        kms.vault_path_token = kms.generate_vault_token()
        kms.create_vault_csi_kms_token(namespace=proj_obj.namespace)
        pvk_obj = PVKeyrotation(sc_obj_def)
        pvk_obj.annotate_storageclass_key_rotation(schedule="*/3 * * * *")

        self.vm_list_all = []

        logger.info(f"Creating {num_vms} VMs serially.")
        for _ in range(num_vms):
            try:
                vm = cnv_workload(
                    storageclass=sc_obj_def.name,
                    namespace=proj_obj.namespace,
                    volume_interface=constants.VM_VOLUME_PVC,
                )
                self.vm_list_all.append(vm)
            except Exception as e:
                logger.error(f"Error creating VM: {e}")
        logger.info(f"Successfully created {num_vms} VMs.")

        self.vm_list_running = self.vm_list_all[:]

        num_stop = 1
        logger.info(f"Stopping {num_stop} VMs.")
        self.vm_list_stop = random.sample(self.vm_list_running, num_stop)
        for vm_obj in self.vm_list_stop:
            vm_obj.stop()
            self.vm_list_running.remove(vm_obj)
        logger.info(f"Successfully stopped {num_stop} VMs.")

        num_pause = 1
        logger.info(f"Pausing {num_pause} VMs.")
        self.vm_list_pause = random.sample(self.vm_list_running, num_pause)
        for vm_obj in self.vm_list_pause:
            vm_obj.pause()
            self.vm_list_running.remove(vm_obj)
        logger.info(f"Successfully paused {num_pause} VMs.")

        logger.info("Creating snapshots of VMs in different states concurrently.")
        self.vm_snap_list = []

        with ThreadPoolExecutor() as executor:
            snap_futures = []

            num_running_snapshots = 2
            if self.vm_list_running:
                running_vms_to_snapshot = random.sample(
                    self.vm_list_running,
                    min(num_running_snapshots, len(self.vm_list_running)),
                )
                for running_vm in running_vms_to_snapshot:
                    snap_futures.append(
                        executor.submit(
                            snapshot_factory, pvc_obj=running_vm.get_vm_pvc_obj()
                        )
                    )
                logger.info(
                    f"Scheduled snapshot for {len(running_vms_to_snapshot)} running VMs."
                )
            else:
                logger.warning("No running VMs to snapshot.")

            if self.vm_list_stop:
                stopped_vm = random.sample(self.vm_list_stop, 1)[0]
                snap_futures.append(
                    executor.submit(snapshot_factory, stopped_vm.get_vm_pvc_obj())
                )
                logger.info("Scheduled snapshot for one stopped VM.")
            else:
                logger.warning("No stopped VMs to snapshot.")

            if self.vm_list_pause:
                paused_vm = random.sample(self.vm_list_pause, 1)[0]
                snap_futures.append(
                    executor.submit(snapshot_factory, paused_vm.get_vm_pvc_obj())
                )
                logger.info("Scheduled snapshot for one paused VM.")
            else:
                logger.warning("No paused VMs to snapshot.")

            for future in as_completed(snap_futures):
                self.vm_snap_list.append(future.result())
        self.before_upgrade_snap_len = len(self.vm_snap_list)
        logger.info(f"Successfully created {self.before_upgrade_snap_len} snapshots.")

        logger.info("Creating clones of VMs in different states concurrently.")
        self.vm_clone_list = []
        self.source_csum = {}
        with ThreadPoolExecutor() as executor:
            clone_futures = []

            if self.vm_list_running:
                running_vm = random.sample(self.vm_list_running, 1)[0]
                clone_futures.append(
                    executor.submit(
                        clone_vm_workload,
                        vm_obj=running_vm,
                        namespace=running_vm.namespace,
                    )
                )
                logger.info("Scheduled clone for one running VM.")
            else:
                logger.warning("No running VMs to clone.")

            if self.vm_list_stop:
                stopped_vm = random.sample(self.vm_list_stop, 1)[0]
                clone_futures.append(
                    executor.submit(
                        clone_vm_workload,
                        vm_obj=stopped_vm,
                        namespace=stopped_vm.namespace,
                    )
                )
                logger.info("Scheduled clone for one stopped VM.")
            else:
                logger.warning("No stopped VMs to clone.")

            if self.vm_list_pause:
                paused_vm = random.sample(self.vm_list_pause, 1)[0]
                clone_futures.append(
                    executor.submit(
                        clone_vm_workload,
                        vm_obj=paused_vm,
                        namespace=paused_vm.namespace,
                    )
                )
                logger.info("Scheduled clone for one paused VM.")
            else:
                logger.warning("No paused VMs to clone.")

            for future in as_completed(clone_futures):
                self.vm_clone_list.append(future.result())
        logger.info(f"Successfully created {len(self.vm_clone_list)} clones.")

        for vm_obj in self.vm_clone_list + self.vm_list_running:
            self.source_csum[vm_obj] = run_dd_io(
                vm_obj=vm_obj, file_path=self.file_paths[0], verify=True
            )

    @workloads
    @pytest.mark.polarion_id("OCS-")
    def test_ocv_upgrd(self, setup_cnv, upgrade_stats):

        initial_vm_states = {
            vm_obj.name: get_vm_status(vm_obj)
            for vm_obj in self.vm_list_pause
            + self.vm_list_stop
            + self.vm_list_running
            + self.vm_clone_list
        }
        logger.info(f"Initial VM states: {initial_vm_states}")

        # execution
        """Upgrade process ocp and odf"""
        cluster_ver = ocp.run_cmd("oc get clusterversions/version -o yaml")
        logger.debug(f"Cluster versions before upgrade:\n{cluster_ver}")
        if (
            config.multicluster
            and config.MULTICLUSTER["multicluster_mode"] == "metro-dr"
            and is_acm_cluster(config)
        ):
            # Find the ODF cluster in current zone
            mdr_upgrade = MDRClusterUpgradeParametrize()
            mdr_upgrade.config_init()
            local_zone_odf = None
            for cluster in get_non_acm_cluster_config():
                if config.ENV_DATA["zone"] == cluster.ENV_DATA["zone"]:
                    local_zone_odf = cluster
            ceph_cluster = CephClusterMultiCluster(local_zone_odf)
            health_monitor = MulticlusterCephHealthMonitor
        else:
            ceph_cluster = CephCluster()
            health_monitor = CephHealthMonitor

        with health_monitor(ceph_cluster):
            ocp_channel = config.UPGRADE.get(
                "ocp_channel", ocp.get_ocp_upgrade_channel()
            )
            logger.info(f"OCP Channel: {ocp_channel}")

            ocp_upgrade_version = config.UPGRADE.get("ocp_upgrade_version")
            logger.info(f"OCP upgrade version: {ocp_upgrade_version}")

            rosa_platform = (
                config.ENV_DATA["platform"].lower() in constants.ROSA_PLATFORMS
            )

            if rosa_platform:
                # Handle ROSA-specific upgrade logic
                # On ROSA environment, Nightly builds are not supported.
                # rosa cli uses only "X.Y.Z" format for the version (builds and images are not supported)
                # If not provided ocp_upgrade_version - get the latest released version of the channel.
                # If provided - check availability and use the provided version in format "X.Y.Z"
                if ocp_upgrade_version and ocp_version_available_on_rosa(
                    ocp_upgrade_version
                ):
                    target_image = ocp_upgrade_version
                else:
                    latest_ocp_ver = get_latest_ocp_version(channel=ocp_channel)
                    # check, if ver is not available on rosa then get the latest version available on ROSA
                    if not ocp_version_available_on_rosa(latest_ocp_ver):
                        version_major_minor = drop_z_version(latest_ocp_ver)
                        latest_ocp_ver = get_latest_rosa_ocp_version(
                            version_major_minor
                        )
                    target_image = latest_ocp_ver
            else:
                # Handle non-ROSA upgrade logic
                if ocp_upgrade_version:
                    target_image = (
                        expose_ocp_version(ocp_upgrade_version)
                        if ocp_upgrade_version.endswith(".nightly")
                        else ocp_upgrade_version
                    )
                else:
                    ocp_upgrade_version = get_latest_ocp_version(channel=ocp_channel)
                    ocp_arch = config.UPGRADE["ocp_arch"]
                    target_image = f"{ocp_upgrade_version}-{ocp_arch}"
            logger.info(f"Target image: {target_image}")

            image_path = config.UPGRADE["ocp_upgrade_path"]
            cluster_operators = ocp.get_all_cluster_operators()
            logger.info(f" oc version: {ocp.get_current_oc_version()}")
            # disconnected environment prerequisites
            if config.DEPLOYMENT.get("disconnected"):
                # mirror OCP release images to mirror registry
                image_path, target_image, _, _ = mirror_ocp_release_images(
                    image_path, target_image
                )

            # Verify Upgrade subscription channel:
            if not rosa_platform:
                ocp.patch_ocp_upgrade_channel(ocp_channel)
                for sampler in TimeoutSampler(
                    timeout=250,
                    sleep=15,
                    func=ocp.verify_ocp_upgrade_channel,
                    channel_variable=ocp_channel,
                ):
                    if sampler:
                        logger.info(f"OCP Channel:{ocp_channel}")
                        break

                # pause a MachineHealthCheck resource
                # no machinehealthcheck on ROSA
                if get_semantic_ocp_running_version() > VERSION_4_8:
                    pause_machinehealthcheck()

                logger.info(f"full upgrade path: {image_path}:{target_image}")
                ocp.upgrade_ocp(image=target_image, image_path=image_path)
            else:
                logger.info(f"upgrade rosa cluster to target version: '{target_image}'")
                upgrade_rosa_cluster(config.ENV_DATA["cluster_name"], target_image)

            # Wait for upgrade
            # ROSA Upgrades Are Controlled by the Hive Operator
            # HCP Clusters use a Control Plane Queue to manage the upgrade process
            # upgrades on ROSA clusters does not start immediately after the upgrade command but scheduled
            operator_upgrade_timeout = 4000 if not rosa_platform else 8000
            for ocp_operator in cluster_operators:
                logger.info(f"Checking upgrade status of {ocp_operator}:")
                # ############ Workaround for issue 2624 #######
                name_changed_between_versions = (
                    "service-catalog-apiserver",
                    "service-catalog-controller-manager",
                )
                if ocp_operator in name_changed_between_versions:
                    logger.info(f"{ocp_operator} upgrade will not be verified")
                    continue
                # ############ End of Workaround ###############
                if ocp_operator == "aro":
                    logger.debug(
                        f"{ocp_operator} do not match with OCP upgrade, check will be ignored!"
                    )
                    continue
                ver = ocp.get_cluster_operator_version(ocp_operator)
                logger.info(f"current {ocp_operator} version: {ver}")
                check_cluster_operator_versions(target_image, operator_upgrade_timeout)

            # resume a MachineHealthCheck resource
            if get_semantic_ocp_running_version() > VERSION_4_8 and not rosa_platform:
                resume_machinehealthcheck()

            # post upgrade validation: check cluster operator status
            operator_ready_timeout = 2700 if not rosa_platform else 5400
            cluster_operators = ocp.get_all_cluster_operators()
            for ocp_operator in cluster_operators:
                logger.info(f"Checking cluster status of {ocp_operator}")
                for sampler in TimeoutSampler(
                    timeout=operator_ready_timeout,
                    sleep=60,
                    func=ocp.verify_cluster_operator_status,
                    cluster_operator=ocp_operator,
                ):
                    if sampler:
                        break
                    else:
                        logger.info(f"{ocp_operator} status is not valid")
            # Post upgrade validation: check cluster version status
            logger.info("Checking clusterversion status")
            cluster_version_timeout = 900 if not rosa_platform else 1800
            for sampler in TimeoutSampler(
                timeout=cluster_version_timeout,
                sleep=15,
                func=ocp.validate_cluster_version_status,
            ):
                if sampler:
                    logger.info("Upgrade Completed Successfully!")
                    break

        cluster_ver = ocp.run_cmd("oc get clusterversions/version -o yaml")
        logger.debug(f"Cluster versions post upgrade:\n{cluster_ver}")

        version = Version.coerce(ocp_upgrade_version)
        short_ocp_upgrade_version = ".".join([str(version.major), str(version.minor)])
        version_before_upgrade = parse_version(
            config.DEPLOYMENT.get("installer_version")
        )
        version_post_upgrade = parse_version(ocp_upgrade_version)
        version_change = version_post_upgrade > version_before_upgrade
        if version_change:
            version_config_file = os.path.join(
                constants.OCP_VERSION_CONF_DIR,
                f"ocp-{short_ocp_upgrade_version}-config.yaml",
            )
            logger.debug(f"config file to be loaded: {version_config_file}")
            load_config_file(version_config_file)
        else:
            logger.info(
                f"Upgrade version {version_post_upgrade} is not higher than old version:"
                f" {version_before_upgrade}, new config file will not be loaded"
            )

        if not config.ENV_DATA["mcg_only_deployment"] and not config.multicluster:
            new_ceph_cluster = CephCluster()
            # Increased timeout because of this bug:
            # https://bugzilla.redhat.com/show_bug.cgi?id=2038690
            new_ceph_cluster.wait_for_rebalance(timeout=3000)
            ct_pod = get_ceph_tools_pod()
            try:
                ceph_health_check(tries=240, delay=30)
            except CephHealthException as err:
                if "daemons have recently crashed" in str(err):
                    logger.error(err)
                    ceph_crash_info_display(ct_pod)
                    archive_ceph_crashes(ct_pod)
                raise err

        # Simulate odf upgrade process
        run_ocs_upgrade(upgrade_stats=upgrade_stats)

        """Ensuring all the vms are in their expected state"""
        logger.info("Validating VM states after upgrade.")
        final_vm_states = {
            vm_obj.name: get_vm_status(vm_obj)
            for vm_obj in self.vm_list_pause
            + self.vm_list_stop
            + self.vm_list_running
            + self.vm_clone_list
        }
        logger.info(f"Final VM states: {final_vm_states}")
        for vm_name in initial_vm_states:
            assert initial_vm_states[vm_name] == final_vm_states[vm_name], (
                f"VM state mismatch for {vm_name}: "
                f"initial state was {initial_vm_states[vm_name]}, "
                f"but final state is {final_vm_states[vm_name]}"
            )
        logger.info("VM state validation successful.")

        """verify all OCV pods are running"""
        logger.info("Verifying all OCV pods are running")
        # Below label needs to be present

        assert check_pods_in_running_state(
            namespace=constants.CNV_NAMESPACE
        ), "Some of the OCV pods are not in Running state!"

        """verify all nodes are running"""
        logger.info("Verifying if all nodes are in Ready state")
        nodes = get_nodes(node_type=constants.MASTER_MACHINE) + get_nodes()
        logger.info(nodes)
        for node in nodes:
            assert (
                node.ocp.get_resource_status(resource_name=node.name) == "Ready"
            ), f"{node.name} is not in ready state"
        logger.info("All nodes are in Ready state")

        logger.info("Verifying data integrity after upgrade")
        self.final_csum = {}
        for vm_obj in self.vm_clone_list + self.vm_list_running:
            new_md5 = self.source_csum.get(vm_obj)
            calculated_md5 = cal_md5sum_vm(vm_obj=vm_obj, file_path=self.file_paths[0])
            assert (
                new_md5 == calculated_md5
            ), f"MD5 mismatch after upgrade for VM {vm_obj.name}"
        logger.info("Data integrity verification successful")

        # un pausing the paused vms
        for vm_obj in self.vm_list_pause:
            vm_obj.unpause()

        # Stopping all the vms
        for vm_obj in self.vm_list_pause + self.vm_list_running + self.vm_clone_list:
            vm_obj.stop()
