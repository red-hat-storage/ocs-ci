import logging
import pytest

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.helpers.odf_cli import odf_cli_setup_helper

log = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def remote_obc_setup_session(request):
    """
    Session-scoped fixture to enable remote OBC on all client clusters.

    This fixture enables Object Bucket Claims (OBC) on all client clusters
    in Provider/Client (HCI) deployments. It switches to each client cluster,
    enables remote OBC, and restores the original context.

    Only runs when client clusters are present in the deployment.

    """
    client_indices = config.get_consumer_indexes_list()
    if not client_indices:
        log.info("No client clusters found, skipping remote OBC setup")
        return

    log.info(f"Enabling remote OBC on {len(client_indices)} client cluster(s)")
    original_index = config.cur_index
    enabled_clients = {}

    # Enable remote OBC on all client clusters
    for client_index in client_indices:
        try:
            config.switch_ctx(client_index)
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

    config.switch_ctx(original_index)

    def disable_remote_obc():
        """Disable remote OBC on all client clusters."""
        log.info("Disabling remote OBC on client clusters")
        original_index = config.cur_index

        for client_index, odf_cli in enabled_clients.items():
            try:
                config.switch_ctx(client_index)
                log.info(f"Disabling remote OBC on client {client_index}")
                odf_cli.run_object_disable_remote_obc()
                log.info(f"Remote OBC disabled on client {client_index}")
            except Exception as e:
                log.error(f"Failed to disable remote OBC on client {client_index}: {e}")

        config.switch_ctx(original_index)

    request.addfinalizer(disable_remote_obc)
