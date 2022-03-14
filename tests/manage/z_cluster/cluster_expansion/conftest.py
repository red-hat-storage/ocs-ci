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
    get_worker_nodes_not_in_ocs,
    label_nodes,
)
from ocs_ci.ocs import machine as machine_utils
from ocs_ci.ocs.resources.pod import get_osd_pods

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


@pytest.fixture
def add_capacity_setup(add_nodes):
    """
    Check that we have the right configurations before we start the test
    """
    log.info("Start add capacity setup")
    osd_pods_before = get_osd_pods()
    number_of_osd_pods_before = len(osd_pods_before)
    if number_of_osd_pods_before >= constants.MAX_OSDS:
        pytest.skip("We have maximum of OSDs in the cluster")

    # If we use vSphere we may need to add more worker nodes
    # to the cluster before starting the test
    if (
        config.ENV_DATA["platform"].lower() == constants.VSPHERE_PLATFORM
        and number_of_osd_pods_before >= 9
    ):
        num_of_expected_wnodes = 6
        wnodes = get_worker_nodes()
        num_of_wnodes = len(wnodes)
        log.info(
            f"We have {number_of_osd_pods_before} OSDs in the cluster, "
            f"and {num_of_wnodes} worker nodes in the cluster"
        )
        if num_of_wnodes < num_of_expected_wnodes:
            num_of_wnodes_to_add = num_of_expected_wnodes - num_of_wnodes
            log.info(f"Adding more {num_of_wnodes_to_add} worker nodes to the cluster")
            add_nodes(ocs_nodes=True, node_count=num_of_wnodes_to_add)

        wnodes_not_in_ocs = get_worker_nodes_not_in_ocs()
        if wnodes_not_in_ocs:
            log.info("Label the worker nodes that are not in OCS")
            label_nodes(wnodes_not_in_ocs)
