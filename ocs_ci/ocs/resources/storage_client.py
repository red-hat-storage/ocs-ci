"""
Storage client related functions
"""
import logging
import tempfile


from ocs_ci.framework import config
from ocs_ci.ocs import constants, ocp
from ocs_ci.utility import templating, version
from ocs_ci.utility.retry import retry

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

    def check_storage_client_status(
        self, namespace=config.ENV_DATA["cluster_namespace"], storageclient_name=None
    ):
        """
        Check storageclient status

        Inputs:
            namespace(str): Namespace where the storage client is created
            storageclient_name(str): name of the storageclient

        Returns:
            storageclient_status(str): storageclient phase

        """
        cmd = (
            f"oc get storageclient {storageclient_name} -n {namespace} "
            "-o=jsonpath='{.items[*].status.phase}'"
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

    def verify_storagerequest_exists(
        self, storageclient_name=None, namespace=config.ENV_DATA["cluster_namespace"]
    ):
        """
        Fetch storagerequests for storageclient

        Args:
            storageclient_name (str): Name of the storageclient to be verified.
            namespace (str): Namespace where the storageclient is present.
                Default value will be taken from ENV_DATA["cluster_namespace"]

        Returns:
            storagerequest_exists (bool): returns true if the storagerequest exists

        """
        cmd = f"oc get storagerequests -n {namespace} " "-o=jsonpath='{.items[*]}'"
        storage_requests = self.ocp_obj.exec_oc_cmd(command=cmd, out_yaml_format=True)

        log.info(f"The list of storagerequests: {storage_requests}")
        return (
            f"ocs.openshift.io/storagerequest-name: {storageclient_name}-cephfs"
            in storage_requests
            and f"ocs.openshift.io/storagerequest-name: {storageclient_name}-chep-rbd"
            in storage_requests
        )

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

    @retry(AssertionError, 12, 10, 1)
    def verify_storageclient_status(
        self,
        storageclient_name,
        namespace=config.ENV_DATA["cluster_namespace"],
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

        # Check storage client is in 'Connected' status
        storage_client_status = self.check_storage_client_status(
            storageclient_name, namespace=namespace
        )
        assert (
            storage_client_status == expected_storageclient_status
        ), "storage client phase is not as expected"

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
