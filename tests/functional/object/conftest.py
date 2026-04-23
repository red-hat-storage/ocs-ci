import logging
import pytest

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.managedservice import get_consumer_names
from ocs_ci.ocs.resources.storageconsumer import add_storageclasses_to_storageconsumer
from ocs_ci.helpers.odf_cli import odf_cli_setup_helper

log = logging.getLogger(__name__)

# Default noobaa storageclass name
DEFAULT_NOOBAA_SC = "openshift-storage.noobaa.io"


@pytest.fixture(scope="session")
def remote_obc_setup_session(request):
    """
    Session-scoped fixture to enable remote OBC on all client clusters.

    This fixture:
    1. Adds noobaa storageclass(es) to all StorageConsumer CRs on provider cluster
    2. Enables remote OBC on all client clusters
    3. At teardown, disables remote OBC

    The fixture adds the default noobaa storageclass and any custom storageclasses
    specified in ENV_DATA['obc_storageclasses'].

    Configuration:
        ENV_DATA:
          obc_storageclasses:
            - openshift-storage.noobaa.io  # Added by default
            - my-custom-noobaa-sc          # Optional custom SC

    Only runs when client clusters are present in the deployment.

    """
    client_indices = config.get_consumer_indexes_list()
    if not client_indices:
        log.info("No client clusters found, skipping remote OBC setup")
        return

    log.info(f"Setting up remote OBC for {len(client_indices)} client cluster(s)")
    enabled_clients = {}

    # Get storageclasses to add (default + any custom ones from config)
    scs_to_add = config.ENV_DATA.get("obc_storageclasses", [DEFAULT_NOOBAA_SC])
    if isinstance(scs_to_add, str):
        scs_to_add = [scs_to_add]
    # Ensure default is always included
    if DEFAULT_NOOBAA_SC not in scs_to_add:
        scs_to_add.append(DEFAULT_NOOBAA_SC)

    log.info(f"Will add these noobaa storageclasses: {scs_to_add}")

    # Step 1: Add noobaa storageclasses to all StorageConsumer CRs on provider
    with config.RunWithProviderConfigContextIfAvailable():
        try:
            consumer_names = get_consumer_names()
            if consumer_names:
                log.info(
                    f"Found {len(consumer_names)} StorageConsumer(s) on provider: {consumer_names}"
                )

                for consumer_name in consumer_names:
                    add_storageclasses_to_storageconsumer(consumer_name, scs_to_add)

        except Exception as e:
            log.error(f"Failed to process StorageConsumer CRs on provider: {e}")

    # Step 2: Enable remote OBC on all client clusters
    for client_index in client_indices:
        with config.RunWithConfigContext(client_index):
            try:
                cluster_type = config.ENV_DATA.get("cluster_type", "").lower()

                if cluster_type != constants.HCI_CLIENT:
                    log.warning(
                        f"Cluster {client_index} is '{cluster_type}', not HCI_CLIENT, skipping"
                    )
                    continue

                log.info(f"Enabling remote OBC on client cluster {client_index}")
                odf_cli = odf_cli_setup_helper()
                odf_cli.run_object_enable_remote_obc()
                enabled_clients[client_index] = odf_cli
                log.info(f"Remote OBC enabled on client {client_index}")

            except Exception as e:
                log.error(f"Failed to enable remote OBC on client {client_index}: {e}")

    def teardown_remote_obc():
        """Disable remote OBC on all client clusters."""
        log.info("Tearing down remote OBC setup")

        for client_index, odf_cli in enabled_clients.items():
            with config.RunWithConfigContext(client_index):
                try:
                    log.info(f"Disabling remote OBC on client {client_index}")
                    odf_cli.run_object_disable_remote_obc()
                    log.info(f"Remote OBC disabled on client {client_index}")
                except Exception as e:
                    log.error(
                        f"Failed to disable remote OBC on client {client_index}: {e}"
                    )

    request.addfinalizer(teardown_remote_obc)
