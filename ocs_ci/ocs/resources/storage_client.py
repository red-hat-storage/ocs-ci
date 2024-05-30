"""
StorageClient related functionalities
"""

import logging
import tempfile
import time


# from ocs_ci.framework import config
from ocs_ci.ocs import constants, ocp
from ocs_ci.utility.retry import retry
from ocs_ci.ocs.utils import (
    run_cmd,
)
from ocs_ci.utility import templating
from ocs_ci.helpers.managed_services import verify_storageclient_storageclass_claims


log = logging.getLogger(__name__)


def check_storage_client_status(namespace=constants.OPENSHIFT_STORAGE_CLIENT_NAMESPACE):
    """
    Check storageclient status

    Inputs:
        namespace(str): Namespace where the storage client is created

    Returns:
        storageclient_status(str): storageclient phase

    """
    cmd = (
        f"oc get storageclient -n {namespace} " "-o=jsonpath='{.items[*].status.phase}'"
    )
    storageclient_status = run_cmd(cmd=cmd)
    return storageclient_status


@retry(AssertionError, 12, 10, 1)
def create_storage_client(
    storage_provider_endpoint=None,
    onboarding_token=None,
    expected_storageclient_status="Connected",
):
    """
    This method creates storage clients

    Inputs:
    storage_provider_endpoint (str): storage provider endpoint details.
    onboarding_token (str): onboarding token
    expected_storageclient_status (str): expected storageclient phase; default value is 'Connected'

    """
    ocp_obj = ocp.OCP()
    # Pull storage-client yaml data
    log.info("Pulling storageclient CR data from yaml")
    storage_client_data = templating.load_yaml(constants.STORAGE_CLIENT_YAML)
    resource_name = storage_client_data["metadata"]["name"]
    log.info(f"the resource name: {resource_name}")

    # Check storageclient is available or not
    storage_client_obj = ocp.OCP(kind="storageclient")
    is_available = storage_client_obj.is_exist(
        resource_name=resource_name,
    )

    # Check storageclaims available or not
    cmd = "oc get storageclassclaim"
    storage_claims = run_cmd(cmd=cmd)

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
        ocp_obj.exec_oc_cmd(f"apply -f {storage_client_data_yaml.name}")

        # Check storage client is in 'Connected' status
        storage_client_status = check_storage_client_status()
        assert (
            storage_client_status == expected_storageclient_status
        ), "storage client phase is not as expected"

        # Create storage classclaim
        if storage_client_status == "Connected" and not storage_claims:
            storage_classclaim_data = templating.load_yaml(
                constants.STORAGE_CLASS_CLAIM_YAML
            )
            templating.dump_data_to_temp_yaml(
                storage_classclaim_data, constants.STORAGE_CLASS_CLAIM_YAML
            )
            ocp_obj.exec_oc_cmd(f"apply -f {constants.STORAGE_CLASS_CLAIM_YAML}")
            time.sleep(30)
            verify_storageclient_storageclass_claims(resource_name)
