import random

import logging
from ocs_ci.ocs import node
from ocs_ci.utility.vsphere_nodes import VSPHERENode


def test_luks_header():

    nodes = node.get_node_ips()
    node_interested = random.choice(nodes)
    logging.info("CHECKING USER OF NODE: {}".format(node_interested))
    vm_node = VSPHERENode(host=node_interested, user="mahesh")
    user_logged = vm_node.get_username()
    logging.info("OUTPUT: {}".format(user_logged))
