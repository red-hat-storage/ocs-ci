"""
This module provides installation of ODF in provider mode and storage-client creation
on the hosting cluster.
"""
import pytest
import logging
import tempfile


from ocs_ci.framework import config
from ocs_ci.ocs import constants, ocp, defaults
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
    get_ocp_version,
)
from ocs_ci.utility import templating, version
from ocs_ci.deployment.deployment import Deployment


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
    def provider_and_native_client_installation(self):
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
        self.ingress_operator_namespace = "openshift-ingress-operator"
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

        # set control nodes as scheduleable
        path = "/spec/mastersSchedulable"
        params = f"""[{{"op": "replace", "path": "{path}", "value": true}}]"""
        ocp_obj = ocp.OCP(
            kind=constants.SCHEDULERS_CONFIG,
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        )
        ocp_obj.patch(params=params, format_type="json"), (
            "Failed to run patch command to update control nodes as scheduleable"
        )

        # allow ODF to be deployed on all nodes
        nodes = get_all_nodes()
        node_objs = get_node_objs(nodes)

        log.info("labeling storage nodes")
        label_nodes(nodes=node_objs, label=constants.OPERATOR_NODE_LABEL)

        # allow hosting cluster domain to be usable by hosted clusters
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
        number_of_storage_disks = config.ENV_DATA.get("number_of_storage_disks", 1)
        log.info(f"number of storage disks : {number_of_storage_disks}")
        for node in nodes:
            cmd = f"oc debug nodes/{node} -- chroot /host rm -rvf /var/lib/rook /mnt/local-storage"
            out = run_cmd(cmd)
            log.info(out)
        setup_local_storage(storageclass="localblock")

        # Create ODF subscription for provider
        catalog_source_data = templating.load_yaml(constants.CATALOG_SOURCE_YAML)
        catalog_source_data["spec"][
            "image"
        ] = "quay.io/rhceph-dev/ocs-registry:4.14.5-8"
        catalog_source_data_yaml = tempfile.NamedTemporaryFile(
            mode="w+", prefix="catalog_source_manifest", delete=False
        )
        templating.dump_data_to_temp_yaml(
            catalog_source_data, catalog_source_data_yaml.name
        )
        log.info("Creating storageclient CR")
        self.ocp_obj.exec_oc_cmd(f"apply -f {catalog_source_data_yaml.name}")
        self.ocp_obj.exec_oc_cmd(f"apply -f {constants.PROVIDER_SUBSCRIPTION_YAML}")

        # Wait until odf is installed
        Deployment().wait_for_subscription(defaults.ODF_OPERATOR_NAME)
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
            selector=constants.PROVIDER_SERVER_LABEL,
            resource_count=1,
        )
        self.pod_obj.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            selector=constants.UX_BACKEND_SERVER_LABEL,
            resource_count=1,
        )
        self.pod_obj.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            selector=constants.RGW_APP_LABEL,
            resource_count=1,
        )

        # Create ODF subscription for storage-client
        self.ocp_obj.exec_oc_cmd(
            f"apply -f {constants.STORAGE_CLIENT_SUBSCRIPTION_YAML}"
        )
        Deployment().wait_for_subscription(defaults.HCI_CLIENT_ODF_OPERATOR_NAME)
        Deployment().wait_for_csv(
            namespace=constants.OPENSHIFT_STORAGE_CLIENT_NAMESPACE
        )

        # Enable odf-console for storage-client
        enable_console_plugin(value="[odf-client-console]")

        # Fetch storage provider endpoint details
        storage_provider_endpoint = self.ocp_obj.exec_oc_cmd(
            (
                f"get storageclusters.ocs.openshift.io -n {constants.OPENSHIFT_STORAGE_NAMESPACE}"
                + " -o jsonpath={'.items[*].status.storageProviderEndpoint'}"
            ),
            out_yaml_format=False,
        )
        log.info(f"storage provider endpoint is: {storage_provider_endpoint}")

        self.create_network_policy(constants.OPENSHIFT_STORAGE_CLIENT_NAMESPACE)
        onboarding_tocken = self.onboarding_token_generation_from_ui()
        self.create_client(storage_provider_endpoint, onboarding_tocken)

        # Create storage classclaim
        self.ocp_obj.exec_oc_cmd(f"apply -f {constants.STORAGE_CLASS_CLAIM_YAML}")

        # Check nooba db pod is up and running
        self.pod_obj.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            selector=constants.NOOBAA_APP_LABEL,
            resource_count=1,
            timeout=180,
        )

        # Check ocs-storagecluster is in 'Ready' status
        storage_cluster_status = self.ocp_obj.exec_oc_cmd(
            (
                f"get storageclusters.ocs.openshift.io -n {constants.OPENSHIFT_STORAGE_NAMESPACE}"
                + " -o jsonpath={'.items[*].status.phase'}"
            ),
            out_yaml_format=False,
        )
        log.info(f"storage cluster's status is: {storage_cluster_status}")
        assert storage_cluster_status == "Ready"

        # Check backing storage is s3-compatible
        backing_store = self.ocp_obj.exec_oc_cmd(
            f"get backingstore {constants.OPENSHIFT_STORAGE_NAMESPACE}"
        )
        log.info(f"backingstore value: {backing_store}")

    def create_network_policy(
        self,
        namespace_to_create_storage_client,
    ):
        """ """
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
        validation_ui_obj = ValidationUI()
        onboarding_token = validation_ui_obj.verify_storage_clients_page()
        return onboarding_token

    def create_client(
        self,
        storage_provider_endpoint,
        onboarding_token,
    ):
        """ """
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
        log.info("Creating storageclient CR")
        self.ocp_obj.exec_oc_cmd(f"apply -f {storage_client_data_yaml.name}")
        # replace_content_in_file(constants.STORAGE_CLIENT_YAML, "PLACEHOLDER1", storage_provider_endpoint)
        # replace_content_in_file(constants.STORAGE_CLIENT_YAML, "PLACEHOLDER2", onboarding_token)
        # log.info(f"cluster yaml file: {storage_client_yaml}")

    def test_deployment(self):
        """
        test deployment code
        """
        self.provider_and_native_client_installation()
        # self.onboarding_token_generation_from_ui()
