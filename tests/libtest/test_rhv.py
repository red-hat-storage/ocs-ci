import logging
import time

from ocs_ci.deployment.rhv import RHVIPI
from ocs_ci.framework.testlib import libtest
from ocs_ci.framework.pytest_customization.marks import rhv_platform_required

from ocs_ci.ocs.node import get_node_objs
from ocs_ci.ocs.platform_nodes import RHVNodes

logger = logging.getLogger(__name__)


@libtest
@rhv_platform_required
def test_get_vm_status():
    """
    Test of RHV get_vm_status() method implementation
    VM  of healthy OCS Cluster has 'up' status by default.
    """
    rhv_depl = RHVIPI()
    vm = rhv_depl.rhv_util.get_rhv_vm_instance(
        get_node_objs()[0].get().get("metadata").get("name")
    )
    logger.info(f"vm name is: {vm.name}")
    status = rhv_depl.rhv_util.get_vm_status(vm)
    assert "up" == str(status), f"Status of {vm.name} is {status}"


@libtest
@rhv_platform_required
def test_stop_and_start_rhv_vms():
    """
    Test of RHV stop_rhv_vms() method implementation
    VM has 'down' status after shutdown and 'up' after power on
    """
    rhv_depl = RHVIPI()
    vm = rhv_depl.rhv_util.get_rhv_vm_instance(
        get_node_objs()[0].get().get("metadata").get("name")
    )
    logger.info(f"vm name is: {vm.name}")
    rhv_depl.rhv_util.stop_rhv_vms([vm])
    status = rhv_depl.rhv_util.get_vm_status(vm)
    assert "down" == str(status), f"Status of {vm.name} is {status}"
    time.sleep(100)
    rhv_depl.rhv_util.start_rhv_vms([vm])
    status = rhv_depl.rhv_util.get_vm_status(vm)
    assert "up" == str(status), f"Status of {vm.name} is {status}"


@libtest
@rhv_platform_required
def test_p_stop_and_start():
    """
    Test of RHV stop_nodes & start Nodes method implementation
    """
    rhv_plfrm = RHVNodes()
    nodes = get_node_objs()
    logger.info(f"nodes are: {nodes}")
    node = [nodes[4]]
    rhv_plfrm.stop_nodes(node)
    vm_name = node[0].get().get("metadata").get("name")
    vm_obj = rhv_plfrm.rhv.get_rhv_vm_instance(vm_name)
    status = rhv_plfrm.rhv.get_vm_status(vm_obj)
    assert "down" == str(status), f"Status of {vm_name} is {status}"
    status = rhv_plfrm.rhv.get_vm_status(vm_obj)
    logger.info(f"Status of {vm_name} is {status}")
    rhv_plfrm.start_nodes(node)
    status = rhv_plfrm.rhv.get_vm_status(vm_obj)
    assert "up" == str(status), f"Status of {vm_name} is {status}"
