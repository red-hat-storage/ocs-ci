from ocs_ci.deployment.baremetal import clean_disk
from ocs_ci.ocs.node import get_nodes
from ocs_ci.ocs import constants


def test_fun():
    worker_node_objs = get_nodes(node_type=constants.WORKER_MACHINE)
    print(f"worker nodes: {worker_node_objs}")
    for worker in worker_node_objs:
        clean_disk(worker)
