"""
This module provides installation of ODF and native storage-client creation in provider mode
"""

import logging
import time


from ocs_ci.framework import config
from ocs_ci.ocs import constants, ocp, defaults
from ocs_ci.ocs.rados_utils import (
    verify_cephblockpool_status,
    check_phase_of_rados_namespace,
)
from ocs_ci.deployment.helpers.lso_helpers import setup_local_storage
from ocs_ci.ocs.node import label_nodes, get_all_nodes, get_node_objs, get_nodes
from ocs_ci.ocs.utils import (
    setup_ceph_toolbox,
    enable_console_plugin,
    run_cmd,
)
from ocs_ci.utility.utils import (
    wait_for_machineconfigpool_status,
)
from ocs_ci.utility import templating, kms as KMS, version
from ocs_ci.deployment.deployment import Deployment, create_catalog_source
from ocs_ci.deployment.baremetal import clean_disk, disks_available_to_cleanup
from ocs_ci.ocs.resources.storage_cluster import verify_storage_cluster
from ocs_ci.ocs.resources.storage_client import StorageClient
from ocs_ci.ocs.bucket_utils import check_pv_backingstore_type
from ocs_ci.ocs.resources import pod
from ocs_ci.helpers.helpers import (
    get_all_storageclass_names,
    verify_block_pool_exists,
)
from ocs_ci.ocs.exceptions import CommandFailed


log = logging.getLogger(__name__)


class ODFAndNativeStorageClientDeploymentOnProvider(object):
    def __init__(self):
        # Call a function during initialization
        self.initial_function()

    def initial_function(self):
        log.info("initial_function called during initialization.")
        self.ns_obj = ocp.OCP(kind=constants.NAMESPACES)
        self.ocp_obj = ocp.OCP()
        self.storage_cluster_obj = ocp.OCP(
            kind="Storagecluster", namespace=config.ENV_DATA["cluster_namespace"]
        )
        self.storage_profile_obj = ocp.OCP(
            kind="Storageprofile", namespace=config.ENV_DATA["cluster_namespace"]
        )
        self.config_map_obj = ocp.OCP(
            kind="Configmap", namespace=config.ENV_DATA["cluster_namespace"]
        )
        self.pod_obj = ocp.OCP(
            kind="Pod", namespace=config.ENV_DATA["cluster_namespace"]
        )
        self.scheduler_obj = ocp.OCP(
            kind=constants.SCHEDULERS_CONFIG,
            namespace=config.ENV_DATA["cluster_namespace"],
        )
        self.sc_obj = ocp.OCP(kind=constants.STORAGECLASS)
        self.storageclass = "localblock"
        self.ocp_version = version.get_semantic_ocp_version_from_config()
        self.ocs_version = version.get_semantic_ocs_version_from_config()
        self.storage_class_claims = [
            constants.CEPHBLOCKPOOL_SC,
            constants.CEPHFILESYSTEM_SC,
        ]
        self.ocs_client_operator = defaults.OCS_CLIENT_OPERATOR_NAME
        self.storageclient_obj = ocp.OCP(
            kind=constants.STORAGECLIENT,
            namespace=config.ENV_DATA["cluster_namespace"],
        )

        self.platform = config.ENV_DATA.get("platform").lower()
        self.deployment = Deployment()
        self.storage_clients = StorageClient()

    def provider_and_native_client_installation(
        self,
    ):
        """
        This method installs odf on provider mode and creates native client

        1. allow ODF to be deployed on all nodes
        2. allow hosting cluster domain to be usable by hosted clusters
        3. Enable nested virtualization
        4. Install ODF
        5. Install LSO, create LocalVolumeDiscovery and LocalVolumeSet
        6. Disable ROOK_CSI_ENABLE_CEPHFS and ROOK_CSI_ENABLE_RBD
        7. Create storage profile
        """
        nodes = get_all_nodes()
        node_objs = get_node_objs(nodes)
        worker_node_objs = get_nodes(node_type=constants.WORKER_MACHINE)
        no_of_worker_nodes = len(worker_node_objs)

        # Allow hosting cluster domain to be usable by hosted clusters
        path = "/spec/routeAdmission"
        value = '{wildcardPolicy: "WildcardsAllowed"}'
        params = f"""[{{"op": "add", "path": "{path}", "value": {value}}}]"""
        patch_cmd = (
            f"patch {constants.INGRESSCONTROLLER} -n {constants.OPENSHIFT_INGRESS_OPERATOR_NAMESPACE} "
            + f"default --type json -p '{params}'"
        )
        self.ocp_obj.exec_oc_cmd(command=patch_cmd)

        # Enable nested virtualization on nodes
        machine_config_data = templating.load_yaml(
            constants.MACHINE_CONFIG_YAML, multi_document=True
        )
        templating.dump_data_to_temp_yaml(
            machine_config_data, constants.MACHINE_CONFIG_YAML
        )
        self.ocp_obj.exec_oc_cmd(f"apply -f {constants.MACHINE_CONFIG_YAML}")
        wait_for_machineconfigpool_status(node_type="all")
        log.info("All the nodes are upgraded")

        # Mark master nodes schedulable if mark_masters_schedulable: True
        if config.ENV_DATA.get("mark_masters_schedulable", False):
            path = "/spec/mastersSchedulable"
            params = f"""[{{"op": "replace", "path": "{path}", "value": true}}]"""
            assert self.scheduler_obj.patch(
                params=params, format_type="json"
            ), "Failed to run patch command to update control nodes as scheduleable"
            # Allow ODF to be deployed on all nodes
            log.info("labeling all nodes as storage nodes")
            label_nodes(nodes=node_objs, label=constants.OPERATOR_NODE_LABEL)
            worker_node_objs = get_nodes(node_type=constants.WORKER_MACHINE)
            no_of_worker_nodes = len(worker_node_objs)
        else:
            log.info("labeling worker nodes as storage nodes")
            label_nodes(nodes=worker_node_objs, label=constants.OPERATOR_NODE_LABEL)

        disks_available_on_worker_nodes_for_cleanup = disks_available_to_cleanup(
            worker_node_objs[0]
        )
        number_of_disks_available = len(disks_available_on_worker_nodes_for_cleanup)
        log.info(
            f"disks avilable for cleanup, {disks_available_on_worker_nodes_for_cleanup}"
            f"number of disks avilable for cleanup, {number_of_disks_available}"
        )

        # Install LSO, create LocalVolumeDiscovery and LocalVolumeSet
        is_local_storage_available = self.sc_obj.is_exist(
            resource_name=self.storageclass,
        )
        if not is_local_storage_available:
            for node in nodes:
                cmd = f"oc debug nodes/{node} -- chroot /host rm -rvf /var/lib/rook /mnt/local-storage"
                out = run_cmd(cmd)
                log.info(out)
                log.info(f"Mount data cleared from node, {node}")
            for node_obj in node_objs:
                clean_disk(node_obj)
            log.info("All nodes are wiped")
            setup_local_storage(storageclass=self.storageclass)
        else:
            log.info("local storage is already installed")

        # odf subscription for provider
        self.odf_subscription_on_provider()

        # Check for rook ceph pods
        assert self.pod_obj.wait_for_resource(
            condition="Running",
            selector="app=rook-ceph-operator",
            resource_count=1,
            timeout=600,
        )

        if (
            self.ocs_version < version.VERSION_4_16
            and self.ocs_version >= version.VERSION_4_14
        ):
            # Disable ROOK_CSI_ENABLE_CEPHFS and ROOK_CSI_ENABLE_RBD
            disable_CEPHFS_RBD_CSI = '{"data":{"ROOK_CSI_ENABLE_CEPHFS":"false", "ROOK_CSI_ENABLE_RBD":"false"}}'
            assert self.config_map_obj.patch(
                resource_name=constants.ROOK_OPERATOR_CONFIGMAP,
                params=disable_CEPHFS_RBD_CSI,
            ), "configmap/rook-ceph-operator-config not patched"

            # Storageprofiles are deprecated from ODF 4.16
            # Create storage profiles if not available
            is_storageprofile_available = self.storage_profile_obj.is_exist(
                resource_name="ssd-storageprofile"
            )
            if not is_storageprofile_available:
                storage_profile_data = templating.load_yaml(
                    constants.STORAGE_PROFILE_YAML
                )
                templating.dump_data_to_temp_yaml(
                    storage_profile_data, constants.STORAGE_PROFILE_YAML
                )
                self.ocp_obj.exec_oc_cmd(f"apply -f {constants.STORAGE_PROFILE_YAML}")

        # Create KMS resources if needed
        if config.DEPLOYMENT.get("kms_deployment"):
            kms = KMS.get_kms_deployment()
            kms.deploy()

        # Create storage cluster if not present already
        is_storagecluster = self.storage_cluster_obj.is_exist(
            resource_name=constants.DEFAULT_STORAGE_CLUSTER
        )
        if not is_storagecluster:
            if (
                self.ocs_version < version.VERSION_4_16
                and self.ocs_version >= version.VERSION_4_14
            ):
                storage_cluster_data = templating.load_yaml(
                    constants.OCS_STORAGE_CLUSTER_YAML
                )
                storage_cluster_data = self.add_encryption_details_to_cluster_data(
                    storage_cluster_data
                )
                storage_cluster_data["spec"]["storageDeviceSets"][0][
                    "replica"
                ] = no_of_worker_nodes

                if self.platform in constants.HCI_PROVIDER_CLIENT_PLATFORMS:
                    storage_cluster_data["spec"]["storageDeviceSets"][0][
                        "count"
                    ] = number_of_disks_available

                templating.dump_data_to_temp_yaml(
                    storage_cluster_data, constants.OCS_STORAGE_CLUSTER_YAML
                )
                self.ocp_obj.exec_oc_cmd(
                    f"apply -f {constants.OCS_STORAGE_CLUSTER_YAML}"
                )
            else:
                storage_cluster_data = templating.load_yaml(
                    constants.OCS_STORAGE_CLUSTER_UPDATED_YAML
                )
                storage_cluster_data = self.add_encryption_details_to_cluster_data(
                    storage_cluster_data
                )
                storage_cluster_data["spec"]["storageDeviceSets"][0][
                    "replica"
                ] = no_of_worker_nodes

                if self.platform in constants.HCI_PROVIDER_CLIENT_PLATFORMS:
                    storage_cluster_data["spec"]["storageDeviceSets"][0][
                        "count"
                    ] = number_of_disks_available
                templating.dump_data_to_temp_yaml(
                    storage_cluster_data, constants.OCS_STORAGE_CLUSTER_UPDATED_YAML
                )
                self.ocp_obj.exec_oc_cmd(
                    f"apply -f {constants.OCS_STORAGE_CLUSTER_UPDATED_YAML}"
                )

        # Creating toolbox pod
        setup_ceph_toolbox()

        # Native storageclients are created as part of ODF 4.16 subscription and each of rbd and
        # cephfs storageclaims gets created automatically with the storageclient creation
        if self.ocs_version >= version.VERSION_4_16:
            # Validate native client is created in openshift-storage namespace
            self.deployment.wait_for_csv(
                self.ocs_client_operator, constants.OPENSHIFT_STORAGE_NAMESPACE
            )

            # Verify native storageclient is created successfully
            self.storage_clients.verify_native_storageclient()

            # Validate cephblockpool created
            assert verify_block_pool_exists(
                constants.DEFAULT_BLOCKPOOL
            ), f"{constants.DEFAULT_BLOCKPOOL} is not created"
            assert (
                verify_cephblockpool_status()
            ), "the cephblockpool is not in Ready phase"

            # Validate radosnamespace created and in 'Ready' status
            assert (
                check_phase_of_rados_namespace()
            ), "The radosnamespace is not in Ready phase"

            # Validate storageclassrequests created
            storage_class_classes = get_all_storageclass_names()
            for storage_class in self.storage_class_claims:
                assert (
                    storage_class in storage_class_classes
                ), "Storage classes ae not created as expected"

        else:
            # Create ODF subscription for storage-client and native client
            self.storage_clients.create_native_storage_client()

            # Verify native storageclient is created successfully
            self.storage_clients.verify_native_storageclient()

    def odf_subscription_on_provider(self):
        """
        This method creates odf subscription for the provider
        """
        # Check if odf is available already on the provider
        ceph_cluster = ocp.OCP(
            kind="CephCluster", namespace=config.ENV_DATA["cluster_namespace"]
        )
        try:
            ceph_cluster.get().get("items")[0]
            log.info("OCS cluster already exists")
            return
        except (IndexError, CommandFailed):
            log.info("Running ODF subscription for the provider")

        live_deployment = config.DEPLOYMENT.get("live_deployment")
        if not live_deployment:
            create_catalog_source()

        log.info("Creating namespace and operator group.")
        olm_data = templating.load_yaml(constants.OLM_YAML, multi_document=True)
        templating.dump_data_to_temp_yaml(olm_data, constants.OLM_YAML)
        run_cmd(f"oc create -f {constants.OLM_YAML}")
        self.deployment.subscribe_ocs()

        ocs_version = version.get_semantic_ocs_version_from_config()
        log.info(f"Installed odf version: {ocs_version}")

        # Enable odf-console:
        enable_console_plugin()
        time.sleep(30)

    def add_encryption_details_to_cluster_data(self, storage_cluster_data):
        """
        Update storage cluster YAML data with encryption information from
        configuration.

        Args:
            storage_cluster_data (dict): storage cluster YAML data

        Returns:
            dict: updated storage storage cluster yaml
        """
        if config.ENV_DATA.get("encryption_at_rest"):
            log.info("Enabling encryption at REST!")
            storage_cluster_data["spec"]["encryption"] = {
                "enable": True,
            }
            storage_cluster_data["spec"]["encryption"] = {
                "clusterWide": True,
            }
        if config.DEPLOYMENT.get("kms_deployment"):
            storage_cluster_data["spec"]["encryption"]["kms"] = {
                "enable": True,
            }
        return storage_cluster_data

    def verify_provider_mode_deployment(self):
        """
        This method verifies provider mode deployment

        """

        # Check ux server pod, ocs-provider server pod and rgw pods are up and running
        self.pod_obj.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            selector=constants.UX_BACKEND_SERVER_LABEL,
            resource_count=1,
            timeout=180,
        )
        # Check nooba db pod is up and running
        self.pod_obj.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            selector=constants.NOOBAA_APP_LABEL,
            resource_count=1,
            timeout=300,
        )
        self.pod_obj.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            selector=constants.RGW_APP_LABEL,
            resource_count=1,
            timeout=300,
        )
        self.pod_obj.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            selector=constants.PROVIDER_SERVER_LABEL,
            resource_count=1,
            timeout=300,
        )
        self.pod_obj.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            selector=constants.RGW_APP_LABEL,
            resource_count=1,
            timeout=300,
        )

        # Check ocs-storagecluster is in 'Ready' status
        log.info("Verify storagecluster on Ready state")
        verify_storage_cluster()

        # Check backing storage is s3-compatible
        backingstore_type = check_pv_backingstore_type()
        log.info(f"backingstore value: {backingstore_type}")
        assert backingstore_type == constants.BACKINGSTORE_TYPE_S3_COMP

        # Verify rgw pod restart count is 0
        rgw_restart_count = pod.fetch_rgw_pod_restart_count()
        assert (
            rgw_restart_count == 0
        ), f"Error rgw pod has restarted {rgw_restart_count} times"
