"""
Storage client related functions
"""
import logging
import tempfile
import time


from ocs_ci.framework import config
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.resources.catalog_source import CatalogSource
from ocs_ci.ocs.utils import enable_console_plugin
from ocs_ci.utility import templating, version
from ocs_ci.utility.retry import retry
from ocs_ci.ocs.ui.validation_ui import ValidationUI
from ocs_ci.helpers.managed_services import (
    get_all_storageclassclaims,
)

log = logging.getLogger(__name__)


class StorageClient:
    """
    This class contains the functions for Storage Client page

    """

    def __init__(self):
        self.ocp_obj = ocp.OCP()
        self.storage_cluster_obj = ocp.OCP(
            kind="Storagecluster", namespace=config.ENV_DATA["cluster_namespace"]
        )
        self.storage_client_obj = ocp.OCP(kind="storageclient")
        self.ocp_version = version.get_semantic_ocp_version_from_config()
        self.ocs_version = version.get_semantic_ocs_version_from_config()

    def odf_installation_on_client(
        self,
        catalog_yaml=False,
        enable_console=False,
        subscription_yaml=constants.NATIVE_STORAGE_CLIENT_YAML,
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
        default value, constants.NATIVE_STORAGE_CLIENT_YAML

        channel(str): ENV_DATA:
            channel_to_client_subscription: "4.16"

        client_subcription_image(str): image details for client subscription

        """
        validation_ui_obj = ValidationUI()
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
            client_subscription_data = templating.load_yaml(
                subscription_yaml, multi_document=True
            )

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
                validation_ui_obj.refresh_web_console()

    def create_storage_client(
        self,
        storage_provider_endpoint=None,
        onboarding_token=None,
    ):
        """
        This method creates storage clients

        Inputs:
        storage_provider_endpoint (str): storage provider endpoint details.
        onboarding_token (str): onboarding token

        """

        # Pull storage-client yaml data
        log.info("Pulling storageclient CR data from yaml")
        storage_client_data = templating.load_yaml(constants.STORAGE_CLIENT_YAML)
        resource_name = storage_client_data["metadata"]["name"]
        log.info(f"the resource name: {resource_name}")

        # Check storageclient is available or not
        is_available = self.storage_client_obj.is_exist(
            resource_name=resource_name,
        )

        if not is_available:
            # Set storage provider endpoint
            log.info(
                "Updating storage provider endpoint details: %s",
                storage_provider_endpoint,
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

    def fetch_storage_client_status(self, namespace=None, storageclient_name=None):
        """
        Fetch storageclient status

        Inputs:
            namespace(str): Namespace where the storage client is created
            storageclient_name(str): name of the storageclient

        Returns:
            storageclient_status(str): storageclient phase

        """
        if not namespace:
            namespace = config.ENV_DATA["cluster_namespace"]

        cmd = (
            f"get storageclient {storageclient_name} -n {namespace} "
            "-o=jsonpath='{.status.phase}'"
        )
        storageclient_status = self.ocp_obj.exec_oc_cmd(
            command=cmd, out_yaml_format=False
        )
        return storageclient_status

    def fetch_provider_endpoint(self):
        """
        This method fetches storage provider endpoint

        Returns:
        storage_provider_endpoint(str): storage provider endpoint details

        """
        storage_provider_endpoint = self.ocp_obj.exec_oc_cmd(
            (
                f"get storageclusters.ocs.openshift.io -n {config.ENV_DATA['cluster_namespace']}"
                + " -o jsonpath={'.items[*].status.storageProviderEndpoint'}"
            ),
            out_yaml_format=False,
        )
        log.info(f"storage provider endpoint is: {storage_provider_endpoint}")
        return storage_provider_endpoint

    def create_storageclaim(
        self,
        storageclaim_name,
        type,
        storage_client_name,
        namespace_of_storageclient=None,
        storageprofile=None,
    ):
        """
        This method creates storageclaims.

        Args:
            storageclaim_name(str): name of the storageclaim/storageclassclaim to create
            type: type of the storageclaim
                  for ODF 4.16 >= : type = block/sharedfile
                  for ODF 4.14 & 4.15 : type =blockpool/sharedfilesystem
            storage_client_name(str): name of the storageclient for which storageclaim is created
            namespace_of_storageclient(str): namespace where the storageclient is created
            storageprofile(str): blcokpool name, optional field

        """
        # Create storage classclaim
        if self.ocs_version >= version.VERSION_4_16:
            storage_claim_data = templating.load_yaml(
                constants.STORAGE_CLASS_CLAIM_UPDATED_YAML
            )

            log.info(f"Updating storageclaim name: {storageclaim_name}")
            storage_claim_data["metadata"]["name"] = storageclaim_name

            log.info(f"Updating storageclient name: {storage_client_name}")
            storage_claim_data["spec"]["storageClient"] = storage_client_name

            log.info(f"Updating storageclaim type: {type}")
            storage_claim_data["spec"]["type"] = type

            if storageprofile:
                log.info(f"Updating storageprofile: {storageprofile}")
                storage_claim_data["spec"] = storageprofile

            # Create storageclaim
            storage_claim_data_yaml = tempfile.NamedTemporaryFile(
                mode="w+", prefix="storage_claim", delete=False
            )
            templating.dump_data_to_temp_yaml(
                storage_claim_data, storage_claim_data_yaml.name
            )
            self.ocp_obj.exec_oc_cmd(f"apply -f {storage_claim_data_yaml.name}")
        else:
            storage_classclaim_data = templating.load_yaml(
                constants.STORAGE_CLASS_CLAIM_YAML
            )

            log.info(f"Updating storageclaim name: {storageclaim_name}")
            storage_classclaim_data["metadata"]["name"] = storageclaim_name

            log.info(f"Updating storageclient name: {storage_client_name}")
            storage_classclaim_data["spec"]["storageClient"][
                "name"
            ] = storage_client_name

            log.info(f"Updating namespace: {namespace_of_storageclient}")
            storage_classclaim_data["spec"]["storageClient"][
                "namespace"
            ] = namespace_of_storageclient

            log.info(f"Updating storageclaim type: {type}")
            storage_classclaim_data["spec"]["type"] = type

            # Create storageclassclaim
            storage_classclaim_data_yaml = tempfile.NamedTemporaryFile(
                mode="w+", prefix="storage_classclaim", delete=False
            )
            templating.dump_data_to_temp_yaml(
                storage_classclaim_data, storage_classclaim_data_yaml.name
            )
            self.ocp_obj.exec_oc_cmd(f"apply -f {storage_classclaim_data_yaml.name}")

    def verify_storage_claim_status(
        self,
        storageclient_name=None,
        namespace=None,
        expected_status=constants.STATUS_READY,
    ):
        """
        This method checks that the storageclaims are in expected status for a storageclient

        Args:
            storageclient_name (str): Name of the storageclient to be verified.
            namespace (str): Namespace where the storageclient is present.
            expected_status(str): Expected status of the storageclaim

        """
        if not namespace:
            namespace = config.ENV_DATA["cluster_namespace"]
        sc_claims = get_all_storageclassclaims()
        for sc_claim in sc_claims:
            if self.ocs_version >= version.VERSION_4_16:
                if sc_claim.data["spec"]["storageClient"] == storageclient_name:
                    assert (
                        sc_claim.data["status"]["phase"] == expected_status
                    ), "storageclaim is not in expected status"
            else:
                if sc_claim.data["spec"]["storageClient"]["name"] == storageclient_name:
                    assert (
                        sc_claim.data["status"]["phase"] == expected_status
                    ), "storageclaim is not in expected status"
        log.info(sc_claim)

    def verify_storagerequest_exists(
        self, storageclient_name=None, namespace=config.ENV_DATA["cluster_namespace"]
    ):
        """
        Fetch storagerequests for storageclient

        Args:
            storageclient_name (str): Name of the storageclient to be verified.
            namespace (str): Namespace where the storageclient is present.

        Returns:
            storagerequest_exists (bool): returns true if the storagerequest exists

        """
        cmd = f"get storagerequests -n {namespace} " "-o=jsonpath='{.items[*]}'"
        storage_requests = self.ocp_obj.exec_oc_cmd(command=cmd, out_yaml_format=False)

        log.info(f"The list of storagerequests: {storage_requests}")
        return (
            f"ocs.openshift.io/storagerequest-name: {storageclient_name}-cephfs"
            in storage_requests
            and f"ocs.openshift.io/storagerequest-name: {storageclient_name}-chep-rbd"
            in storage_requests
        )

    @retry(AssertionError, 12, 10, 1)
    def verify_storageclient_status(
        self,
        storageclient_name,
        namespace=None,
        expected_storageclient_status="Connected",
    ):
        """
        Args:
            storageclient_name (str): Name of the storageclient to be verified.
            namespace (str): Namespace where the storageclient is present.
                Default value will be taken from ENV_DATA["cluster_namespace"]
            expected_storageclient_status (str): expected storageclient phase; default value is 'Connected'

        Returns:
            storagerequest_phase (bool): returns true if the
                    storagerequest_phase == expected_storageclient_status

        """
        if not namespace:
            namespace = config.ENV_DATA["cluster_namespace"]

        # Check storage client is in 'Connected' status
        storage_client_status = self.fetch_storage_client_status(
            storageclient_name=storageclient_name, namespace=namespace
        )
        assert (
            storage_client_status == expected_storageclient_status
        ), "storage client phase is not as expected"

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
        network_policy_data = templating.load_yaml(
            constants.NETWORK_POLICY_PROVIDER_TO_CLIENT_TEMPLATE
        )

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

    def create_native_storage_client(
        self,
        namespace_to_create_storage_client=constants.OPENSHIFT_STORAGE_CLIENT_NAMESPACE,
    ):
        """
        This method creates native storage client

        Args:
            namespace_to_create_storage_client(str): namespace where the storageclient will be created

        """
        # Fetch storage provider endpoint details
        storage_provider_endpoint = self.fetch_provider_endpoint()

        # Create Network Policy
        self.create_network_policy(
            namespace_to_create_storage_client=namespace_to_create_storage_client
        )

        # Generate onboarding token from UI
        validation_ui_obj = ValidationUI()
        storage_client_obj = validation_ui_obj.verify_storage_clients_page()
        onboarding_token = storage_client_obj.generate_client_onboarding_ticket()

        # Create ODF subscription for storage-client
        self.odf_installation_on_client()
        self.create_storage_client(
            storage_provider_endpoint=storage_provider_endpoint,
            onboarding_token=onboarding_token,
        )

        if self.ocs_version < version.VERSION_4_16:
            self.create_storageclaim(
                storageclaim_name="ocs-storagecluster-ceph-rbd",
                type="blockpool",
                storage_client_name="ocs-storagecluster",
                namespace_of_storageclient=constants.OPENSHIFT_STORAGE_CLIENT_NAMESPACE,
            )
            self.create_storageclaim(
                storageclaim_name="ocs-storagecluster-cephfs",
                type="sharedfilesystem",
                storage_client_name="ocs-storagecluster",
                namespace_of_storageclient=constants.OPENSHIFT_STORAGE_CLIENT_NAMESPACE,
            )

    def verify_native_storageclient(self):
        """
        This method verifies that native client is created successfully,
        in 'Connected' status.
        storageclaims, associated storageclasses and storagerequests are created successfully.

        """
        if self.ocs_version >= version.VERSION_4_16:
            namespace = config.ENV_DATA["cluster_namespace"]
        else:
            namespace = constants.OPENSHIFT_STORAGE_CLIENT_NAMESPACE

        storageclient_obj = ocp.OCP(
            kind=constants.STORAGECLIENT,
            namespace=namespace,
        )
        storageclient_data = storageclient_obj.get()["items"]
        log.info(f"storageclient data, {storageclient_data[0]}")
        storageclient_name = storageclient_data[0]["metadata"]["name"]

        # Verify storageclient is in Connected status
        self.verify_storageclient_status(
            storageclient_name=storageclient_name, namespace=namespace
        )

        # Validate storageclaims are Ready and associated storageclasses are created
        self.verify_storage_claim_status(storageclient_name)

        # Validate storagerequests are created successfully
        self.verify_storagerequest_exists(
            storageclient_name=storageclient_name, namespace=namespace
        )
