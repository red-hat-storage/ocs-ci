"""
Two-Node Fencing (TNF) cluster deployment module

This module handles deployment of OpenShift Data Foundation on
two-node clusters with DRBD for high availability.

OCP deployment uses the AWS hypervisor method (fencing-ipi):
provisions an EC2 bare-metal instance, runs dev-scripts to create
a 2-node OCP cluster, then deploys ODF with DRBD.

For pre-existing clusters, set skip_ocp_deployment: true to skip
OCP provisioning and deploy ODF only.
"""

import json
import logging
import os

from ocs_ci.deployment.deployment import Deployment
from ocs_ci.ocs import constants
from ocs_ci.deployment.helpers.tnf_helpers import (
    verify_tnf_cluster_topology,
    get_tnf_node_info,
    create_local_storage_class,
    create_persistent_volumes,
    configure_drbd,
    verify_drbd_configuration,
    verify_drbd_status,
    verify_port_connectivity,
    get_block_devices_on_node,
    discover_available_disks,
    resolve_disk_by_id_path,
)
from ocs_ci.framework import config
from ocs_ci.ocs.exceptions import UnexpectedDeploymentConfiguration
from ocs_ci.ocs.resources.pod import get_pods_having_label

logger = logging.getLogger(__name__)


class TNFBASE(Deployment):
    """
    Base class for Two-Node Fencing deployments
    """

    def __init__(self):
        super().__init__()
        self.tnf_config = config.ENV_DATA.get("tnf") or {}
        logger.info("Initializing TNF Base Deployment")


class TNF(TNFBASE):
    """
    Two-Node Fencing deployment class

    Handles complete OCP + ODF deployment on two-node clusters
    following Red Hat ODF 4.22 documentation.

    Supports Baremetal platform only.
    """

    def __init__(self):
        logger.info("TNF Deployment")
        super().__init__()

        platform = config.ENV_DATA.get("platform", "").lower()

        if platform in constants.BAREMETAL_PLATFORMS:
            logger.info("TNF deployment on Baremetal platform")
        else:
            raise UnexpectedDeploymentConfiguration(
                f"TNF deployment not supported on platform: {platform}. "
                f"Supported platforms: {constants.BAREMETAL_PLATFORMS}"
            )

        self.hypervisor = None
        hypervisor_config = self.tnf_config.get("hypervisor")
        if hypervisor_config:
            from ocs_ci.utility.tnf_hypervisor import TNFHypervisor

            self.hypervisor = TNFHypervisor(
                hypervisor_config=hypervisor_config,
                dev_scripts_config=self.tnf_config.get("dev_scripts", {}),
                proxy_config=self.tnf_config.get("proxy"),
            )
            logger.info("TNF hypervisor provisioning enabled (AWS EC2)")
            self._setup_proxy_env()
        elif not config.ENV_DATA.get("skip_ocp_deployment"):
            raise UnexpectedDeploymentConfiguration(
                "TNF OCP deployment requires hypervisor configuration "
                "(tnf.hypervisor in config). For pre-existing clusters, "
                "set skip_ocp_deployment: true."
            )

    def _setup_proxy_env(self):
        """
        Set proxy env vars from existing hypervisor metadata.
        Needed for skip_ocp_deployment (ODF-only) and teardown flows
        where _deploy_via_hypervisor() doesn't run.
        """
        proxy_config = self.tnf_config.get("proxy", {})
        if not proxy_config.get("enabled", True):
            return

        cluster_path = config.ENV_DATA.get("cluster_path", "")
        if cluster_path and self.hypervisor.load_instance_info(cluster_path):
            proxy_url = self.hypervisor.get_proxy_url()
            os.environ["HTTPS_PROXY"] = proxy_url
            os.environ["HTTP_PROXY"] = proxy_url
            os.environ["NO_PROXY"] = "localhost,127.0.0.1,.svc,.cluster.local"
            logger.info(f"Proxy configured from metadata: {proxy_url}")

    class OCPDeployment(object):
        """
        TNF OCP deployment via AWS EC2 hypervisor + dev-scripts.

        Only instantiated when skip_ocp_deployment is false (i.e.,
        hypervisor config must be present — validated in TNF.__init__).
        """

        def __init__(self):
            self.tnf_config = config.ENV_DATA.get("tnf") or {}
            self.cluster_path = config.ENV_DATA["cluster_path"]

        def deploy_prereq(self):
            logger.info("Preparing cluster path for dev-scripts output")
            os.makedirs(os.path.join(self.cluster_path, "auth"), exist_ok=True)

        def deploy(self, log_cli_level="DEBUG"):
            self._deploy_via_hypervisor()

        def _deploy_via_hypervisor(self):
            """
            Full hypervisor-based deployment:
            1. Launch EC2 bare-metal instance
            2. Configure host as KVM hypervisor
            3. Clone and run dev-scripts
            4. Set up proxy and retrieve kubeconfig
            """
            from ocs_ci.deployment.ocp import OCPDeployment as BaseOCPDeployment

            hypervisor = self._get_hypervisor()

            try:
                logger.info("Step 1: Launching EC2 bare-metal instance...")
                hypervisor.launch_instance()
                hypervisor.save_instance_info(self.cluster_path)

                logger.info("Step 2: Waiting for SSH access...")
                hypervisor.wait_for_ssh_ready()

                logger.info("Step 3: Configuring host as KVM hypervisor...")
                hypervisor.configure_host()

                logger.info("Step 4: Cloning dev-scripts...")
                hypervisor.clone_dev_scripts()

                logger.info("Step 5: Generating dev-scripts config...")
                pull_secret_path = os.path.join(constants.DATA_DIR, "pull-secret")
                with open(pull_secret_path, "r") as f:
                    import json as json_mod

                    pull_secret = json.dumps(json_mod.load(f))
                ocp_version = config.RUN.get("client_version", "").split("-")[0]
                hypervisor.generate_dev_scripts_config(
                    pull_secret, ocp_version=ocp_version
                )

                logger.info("Step 6: Running dev-scripts (45-90 minutes)...")
                hypervisor.run_dev_scripts()

                logger.info("Step 6b: Resizing OSD disks on VMs...")
                hypervisor.resize_vm_disks()

                proxy_config = self.tnf_config.get("proxy", {})
                if proxy_config.get("enabled", True):
                    logger.info("Step 7: Setting up proxy...")
                    hypervisor.setup_proxy()
                    proxy_url = hypervisor.get_proxy_url()
                    os.environ["HTTPS_PROXY"] = proxy_url
                    os.environ["HTTP_PROXY"] = proxy_url
                    os.environ["NO_PROXY"] = "localhost,127.0.0.1,.svc,.cluster.local"

                logger.info("Step 8: Retrieving kubeconfig...")
                auth_dir = os.path.join(self.cluster_path, "auth")
                hypervisor.retrieve_kubeconfig(auth_dir)

                logger.info("Step 9: Testing cluster connectivity...")
                base_ocp = BaseOCPDeployment()
                base_ocp.test_cluster()

                logger.info("OCP cluster deployed via dev-scripts on EC2 hypervisor")
            except Exception:
                logger.error(
                    "Hypervisor deployment failed. Instance will be "
                    "preserved for debugging. Run --teardown to clean up."
                )
                raise

        def _get_hypervisor(self):
            """Get the TNFHypervisor instance from the outer TNF class."""
            tnf_config = config.ENV_DATA.get("tnf") or {}
            hypervisor_config = tnf_config.get("hypervisor")
            if not hypervisor_config:
                raise UnexpectedDeploymentConfiguration(
                    "Hypervisor mode but no hypervisor config found"
                )
            from ocs_ci.utility.tnf_hypervisor import TNFHypervisor

            return TNFHypervisor(
                hypervisor_config=hypervisor_config,
                dev_scripts_config=tnf_config.get("dev_scripts", {}),
                proxy_config=tnf_config.get("proxy"),
            )

        def destroy(self, log_level=""):
            logger.info("EC2 hypervisor termination handles OCP destroy")

    def deploy_prereq(self):
        """
        Pre-deployment checks for TNF ODF deployment
        """
        super().deploy_prereq()
        logger.info("Verifying TNF ODF deployment prerequisites...")

        # Verify cluster topology
        if not verify_tnf_cluster_topology():
            raise UnexpectedDeploymentConfiguration(
                "Cluster must have DualReplica topology for TNF deployment"
            )

        # Get node information
        self.node_info = get_tnf_node_info()
        if len(self.node_info) != 2:
            raise UnexpectedDeploymentConfiguration(
                f"TNF deployment requires exactly 2 nodes, found {len(self.node_info)}"
            )

        # Verify DRBD port connectivity
        for i, node in enumerate(self.node_info):
            peer_node = self.node_info[1 - i]
            verify_port_connectivity(
                node["name"], peer_node["ip"], constants.TNF_DRBD_PORT
            )

        logger.info("TNF ODF prerequisites verified")

    def deploy_ocs_via_operator(self, image=None):
        """
        Deploy ODF on TNF cluster via operator.

        Follows ODF 4.22 documentation order:
        1. Create StorageClass + PVs (Chapter 1 - before operator)
        2. Install ODF operator (parent handles catalog, subscription, CSV)
        3. Configure DRBD for floating monitor (Section 2.2 - after operator)
        4. Create StorageCluster (Section 2.3 - parent handles)

        The parent's deploy_ocs_via_operator is monolithic. We:
        - Create SC + PVs first so the parent skips LSO auto-discovery
          (it checks if 'localblock' SC exists and skips LSO if so)
        - Monkey-patch setup_storage_cluster to inject DRBD config
          between operator install and StorageCluster creation
        """
        from ocs_ci.utility.storage_cluster_setup import StorageClusterSetup

        logger.info("Deploying ODF on TNF cluster...")

        # Initialize node info
        if not hasattr(self, "node_info") or not self.node_info:
            self.node_info = get_tnf_node_info()
            if len(self.node_info) != 2:
                raise UnexpectedDeploymentConfiguration(
                    f"TNF requires exactly 2 nodes, found {len(self.node_info)}"
                )

        # Discover disks if not specified in config
        self._resolve_disk_config()

        # Step 1: Create StorageClass and PVs BEFORE parent flow
        # This prevents LSO auto-discovery (parent skips LSO if SC exists)
        logger.info("Creating local storage class and PVs for TNF...")
        create_local_storage_class()

        device_mappings = self.tnf_config.get("osd_device_mappings", [])
        if device_mappings:
            for mapping in device_mappings:
                by_id = resolve_disk_by_id_path(
                    mapping["node_name"], mapping["device_path"]
                )
                mapping["device_path"] = by_id
            create_persistent_volumes(device_mappings)
        else:
            logger.warning(
                "No OSD device mappings in config. "
                "PVs must be created manually before StorageCluster creation."
            )

        # Step 2+3: Monkey-patch to inject DRBD config between
        # operator install and StorageCluster creation
        original_setup = StorageClusterSetup.setup_storage_cluster
        tnf_instance = self

        def setup_with_drbd(storage_setup_self):
            logger.info("TNF: Configuring DRBD before StorageCluster creation...")
            tnf_instance._configure_drbd()
            original_setup(storage_setup_self)

        StorageClusterSetup.setup_storage_cluster = setup_with_drbd
        try:
            super().deploy_ocs_via_operator(image)
        finally:
            StorageClusterSetup.setup_storage_cluster = original_setup

        logger.info("ODF deployment on TNF cluster completed")

    def _resolve_disk_config(self):
        """
        Resolve monitor and OSD disk paths, auto-discovering if not in config.

        When monitor_disk_node_0/1 or osd_device_mappings are not specified,
        discovers available disks on both nodes and assigns them:
        - Smallest unused disk -> DRBD monitor
        - Largest unused disk -> OSD
        If only one unused disk exists per node, it is used for monitor
        (OSD PVs must be created manually).

        Updates self.tnf_config in-place with the resolved values.
        """
        monitor_disk_0 = self.tnf_config.get("monitor_disk_node_0")
        monitor_disk_1 = self.tnf_config.get("monitor_disk_node_1")
        osd_mappings = self.tnf_config.get("osd_device_mappings", [])
        need_monitor = not monitor_disk_0 or not monitor_disk_1
        need_osd = not osd_mappings

        if not need_monitor and not need_osd:
            logger.info("All disk paths specified in config, skipping discovery")
            return

        logger.info("Discovering available disks on nodes...")
        disk_info = discover_available_disks(self.node_info)
        node_0_name = self.node_info[0]["name"]
        node_1_name = self.node_info[1]["name"]
        all_n0 = disk_info[node_0_name]["all"]
        all_n1 = disk_info[node_1_name]["all"]

        min_disks = 2 if (need_monitor and need_osd) else 1
        if len(all_n0) < min_disks or len(all_n1) < min_disks:
            raise UnexpectedDeploymentConfiguration(
                f"TNF requires at least {min_disks} unused disk(s) per node.\n"
                f"  {node_0_name}: "
                f"{[d['path'] + ' ' + d['size'] for d in all_n0] or 'none'}\n"
                f"  {node_1_name}: "
                f"{[d['path'] + ' ' + d['size'] for d in all_n1] or 'none'}\n"
                f"Add disks to the nodes or set disk paths in config "
                f"(tnf.monitor_disk_node_0/1, tnf.osd_device_mappings)."
            )

        from ocs_ci.deployment.helpers.tnf_helpers import _parse_size_gb

        # Sort by size: smallest first
        n0_sorted = sorted(all_n0, key=lambda d: _parse_size_gb(d["size"]))
        n1_sorted = sorted(all_n1, key=lambda d: _parse_size_gb(d["size"]))

        # Assign monitor disk (smallest available)
        if need_monitor:
            self.tnf_config["monitor_disk_node_0"] = n0_sorted[0]["path"]
            self.tnf_config["monitor_disk_node_1"] = n1_sorted[0]["path"]
            logger.info(
                f"Auto-discovered monitor disks: "
                f"{node_0_name}={n0_sorted[0]['path']} ({n0_sorted[0]['size']}), "
                f"{node_1_name}={n1_sorted[0]['path']} ({n1_sorted[0]['size']})"
            )

        # Assign OSD disk (largest available, excluding monitor disk)
        if need_osd:
            monitor_path_0 = self.tnf_config.get("monitor_disk_node_0")
            monitor_path_1 = self.tnf_config.get("monitor_disk_node_1")
            n0_osd = [d for d in n0_sorted if d["path"] != monitor_path_0]
            n1_osd = [d for d in n1_sorted if d["path"] != monitor_path_1]
            if not n0_osd or not n1_osd:
                logger.warning(
                    "No unused disks remaining for OSD after monitor assignment. "
                    "PVs must be created manually."
                )
            else:
                osd_0 = n0_osd[-1]
                osd_1 = n1_osd[-1]
                self.tnf_config["osd_device_mappings"] = [
                    {
                        "node_name": node_0_name,
                        "device_path": osd_0["path"],
                        "size": osd_0["size"] + "i",
                        "pv_name": f"local-pv-{node_0_name}",
                    },
                    {
                        "node_name": node_1_name,
                        "device_path": osd_1["path"],
                        "size": osd_1["size"] + "i",
                        "pv_name": f"local-pv-{node_1_name}",
                    },
                ]
                logger.info(
                    f"Auto-discovered OSD disks: "
                    f"{node_0_name}={osd_0['path']} ({osd_0['size']}), "
                    f"{node_1_name}={osd_1['path']} ({osd_1['size']})"
                )

    def _configure_drbd(self):
        """
        Configure DRBD for the floating monitor.

        Fetches and runs the drbd-setup script from the ODF operator's
        rook-ceph-drbd-setup-script ConfigMap (ODF 4.22 section 2.2).
        """
        monitor_disk_node_0 = self.tnf_config.get("monitor_disk_node_0")
        monitor_disk_node_1 = self.tnf_config.get("monitor_disk_node_1")

        if not monitor_disk_node_0 or not monitor_disk_node_1:
            raise UnexpectedDeploymentConfiguration(
                "Monitor disk paths not resolved. "
                "Set 'tnf.monitor_disk_node_0' and 'tnf.monitor_disk_node_1' "
                "in config, or ensure nodes have suitable unused disks."
            )

        configure_drbd(
            self.node_info[0],
            self.node_info[1],
            monitor_disk_node_0,
            monitor_disk_node_1,
        )

        if not verify_drbd_configuration():
            raise UnexpectedDeploymentConfiguration(
                "DRBD configuration verification failed"
            )

        for node in self.node_info:
            verify_drbd_status(node["name"])

    def deploy_ocs(self):
        """
        Deploy ODF on TNF cluster

        Delegates to parent deploy_ocs which calls deploy_ocs_via_operator.
        TNF-specific DRBD configuration is injected via the overridden
        deploy_ocs_via_operator method.
        """
        super().deploy_ocs()

    def destroy_cluster(self, log_level="DEBUG"):
        """
        Destroy TNF cluster.

        For hypervisor-based deployments, terminates the EC2 instance
        which implicitly destroys the OCP cluster running inside it.
        For pre-existing clusters, delegates to parent.
        """
        if self.hypervisor:
            logger.info("Terminating TNF hypervisor EC2 instance...")
            if not self.hypervisor.instance_id:
                self.hypervisor.load_instance_info(self.cluster_path)
            try:
                self.hypervisor.terminate_instance()
                logger.info("TNF hypervisor EC2 instance terminated")
            except Exception as e:
                logger.error(f"Failed to terminate hypervisor: {e}")
                raise
        else:
            # Pre-existing cluster: uninstall OCS, leave OCP intact
            super().destroy_cluster(log_level)

    def verify_deployment(self):
        """
        Verify TNF ODF deployment

        Returns:
            bool: True if deployment is verified successfully
        """
        logger.info("Verifying TNF ODF deployment...")

        try:
            # Verify DRBD configuration
            if not verify_drbd_configuration():
                logger.error("DRBD configuration verification failed")
                return False

            # Verify DRBD status on both nodes
            for node in self.node_info:
                if not verify_drbd_status(node["name"]):
                    logger.error(f"DRBD status check failed on node {node['name']}")
                    return False

            # Verify floating monitor pod
            mon_pods = get_pods_having_label(
                label="app=rook-ceph-mon",
                namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
            )

            if not mon_pods:
                logger.error("No monitor pods found")
                return False

            logger.info(f"Found {len(mon_pods)} monitor pod(s)")
            logger.info("TNF ODF deployment verified successfully")
            return True

        except Exception as e:
            logger.error(f"TNF deployment verification failed: {e}")
            return False


def list_available_disks():
    """
    Helper function to list available disks on both nodes

    This can be used during deployment planning to identify
    suitable disks for OSD and monitor.

    Returns:
        dict: Dictionary with node names as keys and device lists as values
    """
    logger.info("Listing available disks on TNF nodes...")

    node_info = get_tnf_node_info()
    available_disks = {}

    for node in node_info:
        try:
            devices = get_block_devices_on_node(node["name"])
            available_disks[node["name"]] = devices

            logger.info(f"\nDisks on {node['name']}:")
            for device in devices:
                logger.info(
                    f"  {device['name']}: {device['path']} - "
                    f"{device['size']} (ROTA: {device['rota']}, "
                    f"TYPE: {device['type']})"
                )

        except Exception as e:
            logger.error(f"Failed to list disks on {node['name']}: {e}")

    return available_disks
