import logging
import pytest

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.managedservice import get_consumer_names
from ocs_ci.ocs.resources.storageconsumer import StorageConsumer
from ocs_ci.helpers.odf_cli import odf_cli_setup_helper

log = logging.getLogger(__name__)

# Default noobaa storageclass name
DEFAULT_NOOBAA_SC = "openshift-storage.noobaa.io"


def add_storageclasses_to_storageconsumer(consumer_name, storageclasses):
    """
    Add storageclass(es) to a specific StorageConsumer on the provider cluster.

    This function runs on the provider cluster and adds the specified storageclasses
    to the StorageConsumer's spec.storageClasses list if they are not already present.

    Args:
        consumer_name (str): Name of the StorageConsumer CR
        storageclasses (str or list): Storageclass name(s) to add

    Returns:
        tuple: (success: bool, added_scs: list, current_scs: list)
            - success: True if operation completed without errors
            - added_scs: List of storageclasses that were added
            - current_scs: Final list of storageclasses in the StorageConsumer

    Example:
        # Add single storageclass
        success, added, current = add_storageclasses_to_storageconsumer(
            "consumer-c21-c5", "openshift-storage.noobaa.io"
        )

        # Add multiple storageclasses
        success, added, current = add_storageclasses_to_storageconsumer(
            "consumer-c21-c5",
            ["openshift-storage.noobaa.io", "my-custom-noobaa-sc"]
        )

    """
    # Normalize to list
    if isinstance(storageclasses, str):
        storageclasses = [storageclasses]

    if not isinstance(storageclasses, list):
        log.error("storageclasses must be a string or list of strings")
        return False, [], []

    added_scs = []
    current_scs = []

    # Must run on provider cluster
    with config.RunWithProviderConfigContextIfAvailable():
        try:
            consumer = StorageConsumer(consumer_name)
            current_scs = consumer.get_storage_classes()
            log.info(
                f"StorageConsumer '{consumer_name}' current storage classes: {current_scs}"
            )

            # Check which SCs need to be added
            scs_missing = [sc for sc in storageclasses if sc not in current_scs]

            if scs_missing:
                log.info(f"Adding {scs_missing} to StorageConsumer '{consumer_name}'")
                updated_scs = current_scs + scs_missing
                consumer.set_storage_classes(updated_scs)
                added_scs = scs_missing
                current_scs = updated_scs
                log.info(
                    f"Updated StorageConsumer '{consumer_name}' with storage classes: {updated_scs}"
                )
            else:
                log.info(
                    f"StorageConsumer '{consumer_name}' already has all specified storageclasses"
                )

            return True, added_scs, current_scs

        except Exception as e:
            log.error(f"Failed to update StorageConsumer '{consumer_name}': {e}")
            return False, [], current_scs


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
