import logging
import pytest

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.managedservice import get_consumer_names
from ocs_ci.ocs.resources.storageconsumer import add_storageclasses_to_storageconsumer
from ocs_ci.helpers.odf_cli import odf_cli_setup_helper
from ocs_ci.utility.ssl_certs import (
    get_root_ca_cert,
    setup_object_browser_ca_cert_on_client,
)

log = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def remote_obc_setup_session(request):
    """
    Session-scoped fixture to enable remote OBC on all client clusters.

    This fixture:
    1. Adds noobaa storageclass to all StorageConsumer CRs on provider cluster
    2. Enables remote OBC on all client clusters
    3. At teardown, disables remote OBC

    Only runs when client clusters are present in the deployment.

    """
    client_indices = config.get_consumer_indexes_list()
    if not client_indices:
        log.info("No client clusters found, skipping remote OBC setup")
        return

    log.info(f"Setting up remote OBC for {len(client_indices)} client cluster(s)")
    enabled_clients = {}

    log.info(f"Will add noobaa storageclass: {constants.NOOBAA_SC}")

    # Step 1: Add noobaa storageclass to all StorageConsumer CRs on provider
    # This is required for remote OBC to function - fail if it doesn't work
    with config.RunWithProviderConfigContextIfAvailable():
        consumer_names = get_consumer_names()
        if consumer_names:
            log.info(
                f"Found {len(consumer_names)} StorageConsumer(s) on provider: {consumer_names}"
            )

            for consumer_name in consumer_names:
                add_storageclasses_to_storageconsumer(
                    consumer_name, constants.NOOBAA_SC
                )
        else:
            log.warning("No StorageConsumer CRs found on provider")

    # Step 2: Enable remote OBC on all client clusters
    # Fail if ANY client setup fails (fail-fast approach)
    for client_index in client_indices:
        with config.RunWithConfigContext(client_index):
            cluster_name = config.ENV_DATA.get("cluster_name")
            cluster_type = config.ENV_DATA.get("cluster_type", "").lower()

            if cluster_type != constants.HCI_CLIENT:
                log.warning(
                    f"Cluster '{cluster_name}' (index {client_index}) is '{cluster_type}', "
                    f"not {constants.HCI_CLIENT}, skipping"
                )
                continue

            log.info(
                f"Enabling remote OBC on client cluster '{cluster_name}' (index {client_index})"
            )
            odf_cli = odf_cli_setup_helper()
            odf_cli.run_object_enable_remote_obc()
            enabled_clients[client_index] = odf_cli
            log.info(
                f"Remote OBC enabled on client '{cluster_name}' (index {client_index})"
            )

    def teardown_remote_obc():
        """Disable remote OBC on all client clusters."""
        log.info("Tearing down remote OBC setup")

        for client_index, odf_cli in enabled_clients.items():
            with config.RunWithConfigContext(client_index):
                cluster_name = config.ENV_DATA.get("cluster_name")
                try:
                    log.info(
                        f"Disabling remote OBC on client '{cluster_name}' (index {client_index})"
                    )
                    odf_cli.run_object_disable_remote_obc()
                    log.info(
                        f"Remote OBC disabled on client '{cluster_name}' (index {client_index})"
                    )
                except Exception as e:
                    log.error(
                        f"Failed to disable remote OBC on client '{cluster_name}' "
                        f"(index {client_index}): {e}"
                    )

    request.addfinalizer(teardown_remote_obc)


@pytest.fixture(scope="session")
def object_browser_ca_cert_setup_client(request):
    """
    Session-scoped fixture to setup CA certificate for object browser on client clusters.

    This fixture handles the TLS/SSL trust configuration for the object browser
    on client clusters by creating/updating the required secret with CA certificates.

    For private/custom CA certificates, the object browser needs the CA cert chain
    to trust the S3 endpoint. This fixture automates the setup of:
    - Resource name: "ocs-client-operator-console-s3-endpoint-ca-certs"
    - Key: "ocs-s3-endpoints-list-<STORAGECLIENT_UID>-noobaaS3.crt"
    - Value: Root CA certificate for custom ingress (from get_root_ca_cert())

    The fixture runs at session scope and sets up certificates on all client clusters
    in a provider/client deployment.

    Only runs when client clusters are present in the deployment.

    Usage:
        Add to test module pytestmark:
        pytestmark = pytest.mark.usefixtures("object_browser_ca_cert_setup_client")

    """
    client_indices = config.get_consumer_indexes_list()
    if not client_indices:
        log.info("No client clusters found, skipping object browser CA cert setup")
        return

    log.info(
        "Setting up object browser CA certificates on %d client cluster(s)",
        len(client_indices),
    )

    # Get CA certificate from provider (root CA for custom ingress certificates)
    with config.RunWithProviderConfigContextIfAvailable():
        ca_cert_file = get_root_ca_cert()
        if not ca_cert_file:
            raise Exception(
                "No root CA certificate file found. "
                "Ensure DEPLOYMENT['ingress_ssl_ca_cert'] is configured or "
                "custom_ssl_cert_provider is set."
            )

        # Read certificate content from file
        with open(ca_cert_file, "r") as f:
            ca_cert = f.read()

    # Setup certificate on each client
    for client_index in client_indices:
        with config.RunWithConfigContext(client_index):
            cluster_name = config.ENV_DATA.get(
                "cluster_name", "client-%d" % client_index
            )
            cluster_type = config.ENV_DATA.get("cluster_type", "").lower()

            if cluster_type != constants.HCI_CLIENT:
                log.warning(
                    "Cluster '%s' (index %d) is '%s', not %s, skipping",
                    cluster_name,
                    client_index,
                    cluster_type,
                    constants.HCI_CLIENT,
                )
                continue

            log.info(
                "Setting up object browser CA cert on client cluster '%s'", cluster_name
            )
            setup_object_browser_ca_cert_on_client(ca_cert)
            log.info(
                "Object browser CA cert setup completed on client '%s'", cluster_name
            )

    log.info("Object browser CA certificate setup completed on all client clusters")
    # No teardown - the secret should persist for object browser functionality
