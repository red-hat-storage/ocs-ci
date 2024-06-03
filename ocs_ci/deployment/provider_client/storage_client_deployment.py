"""
This module provides installation of ODF and native storage-client creation in provider mode
"""
import atexit
import logging
import pytest
import tempfile
import time


from ocs_ci.framework import config
from ocs_ci.ocs import constants, ocp, defaults
from ocs_ci.ocs.rados_utils import (
    verify_cephblockpool_status,
    check_phase_of_rados_namespace,
)
from ocs_ci.deployment.helpers.lso_helpers import setup_local_storage
from ocs_ci.ocs.node import label_nodes, get_all_nodes, get_node_objs
from ocs_ci.ocs.ui.validation_ui import ValidationUI
from ocs_ci.ocs.ui.base_ui import login_ui, close_browser
from ocs_ci.ocs.utils import (
    setup_ceph_toolbox,
    enable_console_plugin,
    run_cmd,
)
from ocs_ci.utility.utils import (
    wait_for_machineconfigpool_status,
)
from ocs_ci.utility import templating, version
from ocs_ci.deployment.deployment import Deployment, create_catalog_source
from ocs_ci.deployment.baremetal import clean_disk
from ocs_ci.ocs.resources.storage_cluster import verify_storage_cluster
from ocs_ci.ocs.resources.storage_client import StorageClients
from ocs_ci.ocs.resources.catalog_source import CatalogSource
from ocs_ci.ocs.bucket_utils import check_pv_backingstore_type
from ocs_ci.ocs.resources import pod
from ocs_ci.helpers.helpers import (
    get_all_storageclass_names,
    verify_block_pool_exists,
)
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.helpers.managed_services import (
    verify_storageclient,
)

log = logging.getLogger(__name__)


class ODFAndNativeStorageClientDeploymentOnProvider(object):
    def __init__(self):
        log.info("Initializing webdriver and login to webconsole")
        # Call a function during initialization
        self.initial_function()

        # Register a function to be called upon the destruction of the instance
        atexit.register(self.cleanup_function)

    def initial_function(self):
        log.info("initial_function called during initialization.")
        login_ui()

    def cleanup_function(self):
        log.info("cleanup_function called at exit.")
        # Remove debug namespace
        self.ns_obj.delete_project(project_name=constants.BM_DEBUG_NODE_NS)
        # Close browser
        close_browser()

    @pytest.fixture(scope="class", autouse=True)
    def setup(self):
        """
        Setup method for the class

        """
        self.validation_ui_obj = ValidationUI()
        self.ns_obj = ocp.OCP(kind=constants.NAMESPACES)
        self.ns_obj.new_project(
            project_name=constants.BM_DEBUG_NODE_NS, policy=constants.PSA_PRIVILEGED
        )
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
        self.deployment = Deployment()
        self.storage_clients = StorageClients()

    def provider_and_native_client_installation(
        self,
    ):
        """
        1. set control nodes as scheduleable
        2. allow ODF to be deployed on all nodes
        3. allow hosting cluster domain to be usable by hosted clusters
        4. Enable nested virtualization on vSphere nodes
        5. Install ODF
        6. Install LSO, create LocalVolumeDiscovery and LocalVolumeSet
        7. Disable ROOK_CSI_ENABLE_CEPHFS and ROOK_CSI_ENABLE_RBD
        8. Create storage profile
        """

        if (
            self.ocs_version < version.VERSION_4_16
            and self.ocs_version >= version.VERSION_4_14
        ):
            # set control nodes as scheduleable
            path = "/spec/mastersSchedulable"
            params = f"""[{{"op": "replace", "path": "{path}", "value": true}}]"""
            self.scheduler_obj.patch(params=params, format_type="json"), (
                "Failed to run patch command to update control nodes as scheduleable"
            )

        # Allow ODF to be deployed on all nodes
        nodes = get_all_nodes()
        node_objs = get_node_objs(nodes)

        log.info("labeling storage nodes")
        label_nodes(nodes=node_objs, label=constants.OPERATOR_NODE_LABEL)

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
        machine_config_data = templating.load_yaml(constants.MACHINE_CONFIG_YAML)
        templating.dump_data_to_temp_yaml(
            machine_config_data, constants.MACHINE_CONFIG_YAML
        )
        self.ocp_obj.exec_oc_cmd(f"apply -f {constants.MACHINE_CONFIG_YAML}")
        wait_for_machineconfigpool_status(node_type="all")
        log.info("All the nodes are upgraded")

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

        # Disable ROOK_CSI_ENABLE_CEPHFS and ROOK_CSI_ENABLE_RBD
        disable_CEPHFS_RBD_CSI = (
            '{"data":{"ROOK_CSI_ENABLE_CEPHFS":"false", "ROOK_CSI_ENABLE_RBD":"false"}}'
        )
        assert self.config_map_obj.patch(
            resource_name=constants.ROOK_OPERATOR_CONFIGMAP,
            params=disable_CEPHFS_RBD_CSI,
        ), "configmap/rook-ceph-operator-config not patched"

        # Storageprofiles are deprecated from ODF 4.16
        if (
            self.ocs_version < version.VERSION_4_16
            and self.ocs_version >= version.VERSION_4_14
        ):
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
                templating.dump_data_to_temp_yaml(
                    storage_cluster_data, constants.OCS_STORAGE_CLUSTER_UPDATED_YAML
                )
                self.ocp_obj.exec_oc_cmd(
                    f"apply -f {constants.OCS_STORAGE_CLUSTER_UPDATED_YAML}"
                )

        # Creating toolbox pod
        setup_ceph_toolbox()

        # Check ux server pod, ocs-provider server pod and rgw pods are up and running
        self.pod_obj.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            selector=constants.UX_BACKEND_SERVER_LABEL,
            resource_count=1,
            timeout=180,
        )
        # Native storageclients are created as part of ODF 4.16 subscription and each of rbd and
        # cephfs storageclaims gets created automatically with the storageclient creation
        if self.ocs_version >= version.VERSION_4_16:
            # Validate native client is created in openshift-storage namespace
            self.deployment.wait_for_csv(
                self.ocs_client_operator, constants.OPENSHIFT_STORAGE_NAMESPACE
            )

            # Validate storageclaims are Ready and associated storageclasses are created
            verify_storageclient()

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
            # Create ODF subscription for storage-client
            self.odf_installation_on_client()

            # Fetch storage provider endpoint details
            storage_provider_endpoint = self.storage_clients.fetch_provider_endpoint()

            # Create Network Policy
            self.create_network_policy(
                namespace_to_create_storage_client=constants.OPENSHIFT_STORAGE_CLIENT_NAMESPACE
            )
            onboarding_token = self.onboarding_token_generation_from_ui()

            # Create native storage client
            self.storage_clients.create_storage_client(
                storage_provider_endpoint=storage_provider_endpoint,
                onboarding_token=onboarding_token,
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
        list_of_rgw_pods = pod.get_rgw_pods(
            namespace=config.ENV_DATA["cluster_namespace"]
        )
        rgw_pod_obj = list_of_rgw_pods[0]
        restart_count_for_rgw_pod = pod.get_pod_restarts_count(
            list_of_pods=list_of_rgw_pods,
            namespace=config.ENV_DATA["cluster_namespace"],
        )
        rgw_pod_restart_count = restart_count_for_rgw_pod[rgw_pod_obj.name]
        log.info(f"restart count for rgw pod is: {rgw_pod_restart_count}")
        assert (
            restart_count_for_rgw_pod[rgw_pod_obj.name] == 0
        ), f"Error rgw pod has restarted {rgw_pod_restart_count} times"

        # Check ocs-storagecluster is in 'Ready' status
        log.info("Verify storagecluster on Ready state")
        verify_storage_cluster()

        # Check backing storage is s3-compatible
        backingstore_type = check_pv_backingstore_type()
        log.info(f"backingstore value: {backingstore_type}")
        assert backingstore_type == constants.BACKINGSTORE_TYPE_S3_COMP

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
        olm_data = templating.load_yaml(constants.OLM_YAML)
        templating.dump_data_to_temp_yaml(olm_data, constants.OLM_YAML)
        run_cmd(f"oc create -f {constants.OLM_YAML}")
        self.deployment.subscribe_ocs()

        ocs_version = version.get_semantic_ocs_version_from_config()
        log.info(f"Installed odf version: {ocs_version}")
        self.validation_ui_obj.refresh_web_console()

        # Enable odf-console:
        enable_console_plugin()
        time.sleep(30)
        self.validation_ui_obj.refresh_web_console()

    def odf_installation_on_client(
        self,
        catalog_yaml=False,
        enable_console=False,
        subscription_yaml=constants.STORAGE_CLIENT_SUBSCRIPTION_YAML,
        channel_to_client_subscription=config.ENV_DATA.get(
            "channel_to_client_subscription"
        ),
        client_subcription_image=config.DEPLOYMENT.get("ocs_registry_image", ""),
    ):
        """
        This method creates odf subscription on clients

        Inputs:
        catalog_yaml (bool): If enabled then constants.OCS_CATALOGSOURCE_YAML
        will be created.

        enable_console (bool): If enabled then odf-client-console will be enabled

        subscription_yaml: subscription yaml which needs to be created.
        default value, constants.STORAGE_CLIENT_SUBSCRIPTION_YAML

        channel(str): ENV_DATA:
            channel_to_client_subscription: "4.16"

        client_subcription_image(str): image details for client subscription

        """
        # Check namespace for storage-client is available or not
        is_available = self.ns_obj.is_exist(
            resource_name=constants.OPENSHIFT_STORAGE_CLIENT_NAMESPACE,
        )
        if not is_available:
            if catalog_yaml:
                # Note: Need to parameterize the image in future
                catalog_data = templating.load_yaml(constants.OCS_CATALOGSOURCE_YAML)
                log.info(
                    f"Updating image details for client subscription: {client_subcription_image}"
                )
                catalog_data["spec"]["image"] = client_subcription_image
                catalog_data_yaml = tempfile.NamedTemporaryFile(
                    mode="w+", prefix="catalog_data", delete=False
                )
                templating.dump_data_to_temp_yaml(catalog_data, catalog_data_yaml.name)
                self.ocp_obj.exec_oc_cmd(f"apply -f {catalog_data_yaml.name}")

                catalog_source = CatalogSource(
                    resource_name=constants.OCS_CATALOG_SOURCE_NAME,
                    namespace=constants.MARKETPLACE_NAMESPACE,
                )
                # Wait for catalog source is ready
                catalog_source.wait_for_state("READY")

            # Create ODF subscription for storage-client
            client_subscription_data = templating.load_yaml(subscription_yaml)

            log.info(f"Updating channel details: {channel_to_client_subscription}")
            client_subscription_data["spec"]["channel"] = channel_to_client_subscription
            client_subscription_data_yaml = tempfile.NamedTemporaryFile(
                mode="w+", prefix="client_subscription", delete=False
            )
            templating.dump_data_to_temp_yaml(
                client_subscription_data, client_subscription_data_yaml.name
            )
            self.ocp_obj.exec_oc_cmd(f"apply -f {client_subscription_data_yaml.name}")
            self.deployment.wait_for_subscription(
                self.ocs_client_operator, constants.OPENSHIFT_STORAGE_CLIENT_NAMESPACE
            )
            self.deployment.wait_for_csv(
                self.ocs_client_operator, constants.OPENSHIFT_STORAGE_CLIENT_NAMESPACE
            )
            log.info(
                f"Sleeping for 30 seconds after {self.ocs_client_operator} created"
            )
            time.sleep(30)

            if enable_console:
                enable_console_plugin(value="[odf-client-console]")
                self.validation_ui_obj.refresh_web_console()

    def create_network_policy(
        self, namespace_to_create_storage_client=None, resource_name=None
    ):
        """
        This method creates network policy for the namespace where storage-client will be created

        Inputs:
        namespace_to_create_storage_client (str): Namespace where the storage client will be created

        """
        # Pull network-policy yaml data
        log.info("Pulling NetworkPolicy CR data from yaml")
        network_policy_data = templating.load_yaml(constants.NETWORK_POLICY_YAML)

        resource_name = network_policy_data["metadata"]["name"]

        # Check network policy for the namespace_to_create_storage_client is available or not
        network_policy_obj = ocp.OCP(
            kind="NetworkPolicy", namespace=namespace_to_create_storage_client
        )

        is_available = network_policy_obj.is_exist(
            resource_name=resource_name,
        )

        if not is_available:
            # Set namespace value to the namespace where storageclient will be created
            log.info(
                "Updating namespace where to create storage client: %s",
                namespace_to_create_storage_client,
            )
            network_policy_data["metadata"][
                "namespace"
            ] = namespace_to_create_storage_client

            # Create network policy
            network_policy_data_yaml = tempfile.NamedTemporaryFile(
                mode="w+", prefix="network_policy", delete=False
            )
            templating.dump_data_to_temp_yaml(
                network_policy_data, network_policy_data_yaml.name
            )
            log.info("Creating NetworkPolicy CR")
            out = self.ocp_obj.exec_oc_cmd(f"apply -f {network_policy_data_yaml.name}")
            log.info(f"output: {out}")
            log.info(
                f"Sleeping for 30 seconds after {network_policy_data_yaml.name} created"
            )

            assert network_policy_obj.check_resource_existence(
                should_exist=True, timeout=300, resource_name=resource_name
            ), log.error(
                f"Networkpolicy does not exist for {namespace_to_create_storage_client} namespace"
            )

        else:
            log.info(
                f"Networkpolicy already exists for {namespace_to_create_storage_client} namespace"
            )
