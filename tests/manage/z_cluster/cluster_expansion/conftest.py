import pytest
import logging

from ocs_ci.ocs import constants
from ocs_ci.framework import config
from ocs_ci.ocs.node import (
    get_worker_nodes,
    add_new_node_and_label_it,
    add_new_node_and_label_upi,
    add_new_nodes_and_label_upi_lso,
    taint_nodes,
)
from ocs_ci.ocs import machine as machine_utils

log = logging.getLogger(__name__)


@pytest.fixture()
def add_nodes():
    """
    Test for adding worker nodes to the cluster while IOs
    """

    def factory(ocs_nodes=False, node_count=3, taint_label=None):
        """
        Args:
            ocs_nodes (bool): True if new nodes are OCS, False otherwise
            node_count (int): Number of nodes to be added
            taint_label (str): Taint label to be added

        """

        new_nodes = []
        if config.ENV_DATA["platform"].lower() in constants.CLOUD_PLATFORMS:
            dt = config.ENV_DATA["deployment_type"]
            if dt == "ipi":
                machines = machine_utils.get_machinesets()
                log.info(
                    f"The worker nodes number before expansion {len(get_worker_nodes())}"
                )
                for machine in machines:
                    new_nodes.append(
                        add_new_node_and_label_it(machine, mark_for_ocs_label=ocs_nodes)
                    )

                log.info(
                    f"The worker nodes number after expansion {len(get_worker_nodes())}"
                )

            else:
                log.info(
                    f"The worker nodes number before expansion {len(get_worker_nodes())}"
                )
                if config.ENV_DATA.get("rhel_workers"):
                    node_type = constants.RHEL_OS
                else:
                    node_type = constants.RHCOS

                new_nodes.append(
                    add_new_node_and_label_upi(
                        node_type, node_count, mark_for_ocs_label=ocs_nodes
                    )
                )
                log.info(
                    f"The worker nodes number after expansion {len(get_worker_nodes())}"
                )

        elif config.ENV_DATA["platform"].lower() == constants.VSPHERE_PLATFORM:
            log.info(
                f"The worker nodes number before expansion {len(get_worker_nodes())}"
            )
            dt = config.ENV_DATA["deployment_type"]
            if dt == "ipi":
                machines = machine_utils.get_machinesets()
                for machine in machines:
                    new_nodes.append(
                        add_new_node_and_label_it(machine, mark_for_ocs_label=ocs_nodes)
                    )

            else:
                if config.ENV_DATA.get("rhel_user"):
                    node_type = constants.RHEL_OS
                else:
                    node_type = constants.RHCOS

                if config.DEPLOYMENT.get("local_storage"):
                    new_nodes.append(
                        add_new_nodes_and_label_upi_lso(
                            node_type, node_count, mark_for_ocs_label=ocs_nodes
                        )
                    )
                else:
                    new_nodes.append(
                        add_new_node_and_label_upi(
                            node_type, node_count, mark_for_ocs_label=ocs_nodes
                        )
                    )

            log.info(
                f"The worker nodes number after expansion {len(get_worker_nodes())}"
            )

        nodes = [node for sublist in new_nodes for node in sublist]

        if taint_label:
            taint_nodes(nodes=nodes, taint_label=taint_label), "Failed to taint nodes"
        log.info(f"Successfully Tainted nodes {new_nodes} with {taint_label}")

    return factory
