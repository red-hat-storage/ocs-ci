"""
This module provides installation of ODF in provider mode and storage-client creation
on the hosting cluster.
"""
import pytest
import logging
import tempfile
import time


from ocs_ci.framework import config
from ocs_ci.ocs import constants, ocp, defaults
from ocs_ci.deployment.helpers.lso_helpers import setup_local_storage
from ocs_ci.ocs.node import label_nodes, get_all_nodes, get_node_objs

# from ocs_ci.ocs.ui.validation_ui import ValidationUI
from ocs_ci.ocs.ui.base_ui import login_ui, close_browser
from ocs_ci.ocs.utils import (
    setup_ceph_toolbox,
    enable_console_plugin,
    run_cmd,
)
from ocs_ci.utility.utils import (
    wait_for_machineconfigpool_status,
    get_ocp_version,
)
from ocs_ci.utility import templating, version
from ocs_ci.deployment.deployment import Deployment
from ocs_ci.deployment.baremetal import clean_disk
from ocs_ci.ocs.resources.storage_cluster import (
    verify_storage_cluster,
    check_storage_client_status,
)
from ocs_ci.ocs.resources.catalog_source import CatalogSource
from ocs_ci.ocs.bucket_utils import check_pv_backingstore_type
from ocs_ci.ocs.resources import pod


@pytest.fixture(scope="class")
def setup_ui_class(request):
    driver = login_ui()

    def finalizer():
        close_browser()

    request.addfinalizer(finalizer)
    return driver


log = logging.getLogger(__name__)
# Error message to look in a command output
ERRMSG = "Error in command"


@pytest.mark.usefixtures("setup_ui_class")
class TestStorageClientDeployment(object):
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
        from ocs_ci.ocs.ui.validation_ui import ValidationUI

        self.validation_ui_obj = ValidationUI()
        self.ingress_operator_namespace = "openshift-ingress-operator"
        self.ocp_obj_ns = ocp.OCP(kind=constants.NAMESPACES)
        self.ocp_obj_ns.new_project(
            project_name=constants.BM_DEBUG_NODE_NS, policy=constants.PSA_PRIVILEGED
        )
        self.ocp_obj = ocp.OCP()
        self.storage_cluster_obj = ocp.OCP(
            kind="Storagecluster", namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        self.config_map_obj = ocp.OCP(
            kind="Configmap", namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        self.pod_obj = ocp.OCP(
            kind="Pod", namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        self.service_obj = ocp.OCP(
            kind="Service", namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        ocp_version = get_ocp_version()
        log.info(f"ocp version is: {ocp_version}")
        self.pvc_obj = ocp.OCP(
            kind=constants.PVC, namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        self.ns_obj = ocp.OCP(
            kind=constants.SCHEDULERS_CONFIG,
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        )

        # set control nodes as scheduleable
        path = "/spec/mastersSchedulable"
        params = f"""[{{"op": "replace", "path": "{path}", "value": true}}]"""
        self.ns_obj.patch(params=params, format_type="json"), (
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
        self.ocp_obj.exec_oc_cmd(f"apply -f {constants.MACHINE_CONFIG_YAML}")
        wait_for_machineconfigpool_status(node_type="all")
        log.info("All the nodes are upgraded")

        # Install LSO, create LocalVolumeDiscovery and LocalVolumeSet
        for node in nodes:
            cmd = f"oc debug nodes/{node} -- chroot /host rm -rvf /var/lib/rook /mnt/local-storage"
            out = run_cmd(cmd)
            log.info(out)
            log.info(f"Mount data cleared from node, {node}")
        for node_obj in node_objs:
            clean_disk(node_obj)
        log.info("All nodes are wiped")
        setup_local_storage(storageclass="localblock")

        # Create ODF subscription for provider
        self.ocp_obj.exec_oc_cmd(f"apply -f {constants.PROVIDER_SUBSCRIPTION_YAML}")

        # Wait until odf is installed
        odf_operator = defaults.ODF_OPERATOR_NAME
        Deployment().wait_for_subscription(
            odf_operator, constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        Deployment().wait_for_csv(odf_operator, constants.OPENSHIFT_STORAGE_NAMESPACE)
        log.info(f"Sleeping for 30 seconds after {odf_operator} created")
        time.sleep(30)
        ocs_version = version.get_semantic_ocs_version_from_config()
        log.info(f"Installed odf version: {ocs_version}")

        # Check for rook ceph pods
        assert self.pod_obj.wait_for_resource(
            condition="Running",
            selector="app=rook-ceph-operator",
            resource_count=1,
            timeout=600,
        )

        # Enable odf-console:
        enable_console_plugin()
        self.validation_ui_obj.refresh_web_console()

        # Disable ROOK_CSI_ENABLE_CEPHFS and ROOK_CSI_ENABLE_RBD
        disable_CEPHFS_RBD_CSI = (
            '{"data":{"ROOK_CSI_ENABLE_CEPHFS":"false", "ROOK_CSI_ENABLE_RBD":"false"}}'
        )
        assert self.config_map_obj.patch(
            resource_name=constants.ROOK_OPERATOR_CONFIGMAP,
            params=disable_CEPHFS_RBD_CSI,
        ), "configmap/rook-ceph-operator-config not patched"

        # Create storage profiles
        self.ocp_obj.exec_oc_cmd(f"apply -f {constants.STORAGE_PROFILE_YAML}")

        # Create storage cluster
        self.ocp_obj.exec_oc_cmd(f"apply -f {constants.OCS_STORAGE_CLUSTER_YAML}")

        # Creating toolbox pod
        setup_ceph_toolbox()

        # Check ux server pod, ocs-provider server pod and rgw pods are up and running
        self.pod_obj.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            selector=constants.UX_BACKEND_SERVER_LABEL,
            resource_count=1,
            timeout=180,
        )

        # Create ODF subscription for storage-client
        self.odf_installation_on_client()

        # Fetch storage provider endpoint details
        storage_provider_endpoint = self.ocp_obj.exec_oc_cmd(
            (
                f"get storageclusters.ocs.openshift.io -n {constants.OPENSHIFT_STORAGE_NAMESPACE}"
                + " -o jsonpath={'.items[*].status.storageProviderEndpoint'}"
            ),
            out_yaml_format=False,
        )
        log.info(f"storage provider endpoint is: {storage_provider_endpoint}")

        self.create_network_policy(
            namespace_to_create_storage_client=constants.OPENSHIFT_STORAGE_CLIENT_NAMESPACE
        )
        onboarding_token = self.onboarding_token_generation_from_ui()
        self.create_storage_client(
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
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        rgw_pod_obj = list_of_rgw_pods[0]
        restart_count_for_rgw_pod = pod.get_pod_restarts_count(
            list_of_pods=list_of_rgw_pods,
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
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

    def odf_installation_on_client(
        self,
        catalog_yaml=False,
        enable_console=False,
        subscription_yaml=constants.STORAGE_CLIENT_SUBSCRIPTION_YAML,
    ):
        """
        This method creates odf subscription on clients

        Inputs:
        catalog_yaml (bool): If enabled then constants.OCS_CATALOGSOURCE_YAML
        will be created.

        enable_console (bool): If enabled then odf-client-console will be enabled

        subscription_yaml: subscription yaml which needs to be created.
        default value, constants.STORAGE_CLIENT_SUBSCRIPTION_YAML

        """
        if catalog_yaml:
            self.ocp_obj.exec_oc_cmd(f"apply -f {constants.OCS_CATALOGSOURCE_YAML}")

            catalog_source = CatalogSource(
                resource_name=constants.OCS_CATALOG_SOURCE_NAME,
                namespace=constants.MARKETPLACE_NAMESPACE,
            )
            # Wait for catalog source is ready
            catalog_source.wait_for_state("READY")

        # Create ODF subscription for storage-client
        self.ocp_obj.exec_oc_cmd(f"apply -f {subscription_yaml}")
        ocs_client_operator = defaults.OCS_CLIENT_OPERATOR_NAME
        Deployment().wait_for_subscription(
            ocs_client_operator, constants.OPENSHIFT_STORAGE_CLIENT_NAMESPACE
        )
        Deployment().wait_for_csv(
            ocs_client_operator, constants.OPENSHIFT_STORAGE_CLIENT_NAMESPACE
        )
        log.info(f"Sleeping for 30 seconds after {ocs_client_operator} created")
        time.sleep(30)

        if enable_console:
            enable_console_plugin(value="[odf-client-console]")
            self.validation_ui_obj.refresh_web_console()

    def create_network_policy(
        self,
        namespace_to_create_storage_client=None,
    ):
        """
        This method creates network policy for the namespace where storage-client will be created

        Inputs:
        namespace_to_create_storage_client (str): Namespace where the storage client will be created

        """
        # Pull network-policy yaml data
        log.info("Pulling NetworkPolicy CR data from yaml")
        network_policy_data = templating.load_yaml(constants.NETWORK_POLICY_YAML)

        # Set storage provider endpoint
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
        self.ocp_obj.exec_oc_cmd(f"apply -f {network_policy_data_yaml.name}")

    def onboarding_token_generation_from_ui(
        self,
    ):
        """
        This method generates onboarding token from UI

        Steps:
        1:- Check private and public keys are available
        2:- Check Storage-Clients pages available

        Returns:
        onboarding_token(str): client onboarding token

        """
        secret_ocp_obj = ocp.OCP(
            kind=constants.SECRET, namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        for secret_name in {
            constants.ONBOARDING_PRIVATE_KEY,
            constants.MANAGED_ONBOARDING_SECRET,
        }:
            assert secret_ocp_obj.is_exist(
                resource_name=secret_name
            ), f"{secret_name} does not exist in {config.ENV_DATA['cluster_namespace']} namespace"

        # verify storage-client page is available
        onboarding_token = (
            self.validation_ui_obj.verify_onboarding_token_generation_from_ui()
        )
        return onboarding_token

    def create_storage_client(
        self,
        storage_provider_endpoint=None,
        onboarding_token=None,
        expected_storageclient_status="Connected",
    ):
        """
        This method creates storage clients

        Inputs:
        storage_provider_endpoint (str): storage provider endpoint details.
        onboarding_token (str): onboarding token
        expected_storageclient_status (str): expected storaeclient phase default value is 'Connected'

        """
        # Pull storage-client yaml data
        log.info("Pulling storageclient CR data from yaml")
        storage_client_data = templating.load_yaml(constants.STORAGE_CLIENT_YAML)

        # Set storage provider endpoint
        log.info(
            "Updating storage provider endpoint details: %s", storage_provider_endpoint
        )
        storage_client_data["spec"][
            "storageProviderEndpoint"
        ] = storage_provider_endpoint

        # Set onboarding token
        log.info("Updating storage provider endpoint details: %s", onboarding_token)
        storage_client_data["spec"]["onboardingTicket"] = onboarding_token
        storage_client_data_yaml = tempfile.NamedTemporaryFile(
            mode="w+", prefix="storage_client", delete=False
        )
        templating.dump_data_to_temp_yaml(
            storage_client_data, storage_client_data_yaml.name
        )

        # Create storageclient CR
        log.info("Creating storageclient CR")
        self.ocp_obj.exec_oc_cmd(f"apply -f {storage_client_data_yaml.name}")

        # Check storage client is in 'Connected' status
        storage_client_status = check_storage_client_status()
        assert (
            storage_client_status == expected_storageclient_status
        ), "storage client phase is not as expected"

        # Create storage classclaim
        if storage_client_status == "Connected":
            self.ocp_obj.exec_oc_cmd(f"apply -f {constants.STORAGE_CLASS_CLAIM_YAML}")

    def teardown(self):
        """
        Remove debug namespace
        """
        self.ns_obj.delete_project(project_name=constants.BM_DEBUG_NODE_NS)


def provider_client_deployment():
    """
    test deployment code
    """
    storage_client_deployment_obj = TestStorageClientDeployment()
    storage_client_deployment_obj.provider_and_native_client_installation()
