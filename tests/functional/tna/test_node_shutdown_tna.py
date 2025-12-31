import logging
import random
import pytest

from ocs_ci.helpers.cnv_helpers import cal_md5sum_vm
from ocs_ci.helpers.stretchcluster_helper import (
    recover_from_ceph_stuck,
    verify_vm_workload,
)
from ocs_ci.ocs.node import (
    wait_for_nodes_status,
    get_nodes,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import get_ceph_tools_pod
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.framework.pytest_customization.marks import tier2
from ocs_ci.utility.retry import retry

log = logging.getLogger(__name__)


def check_ceph_accessibility(timeout, delay=60, grace=180):
    """
    Check for Ceph accessibility for the given duration.

    Args:
        timeout (int): Total duration (in seconds) to check Ceph status
        delay (int): How often Ceph status should be checked (in seconds)
        grace (int): Extra grace time for Ceph to respond (in seconds)

    Returns:
        bool: True if no Ceph accessibility issues, False otherwise
    """
    command = (
        f"SECONDS=0;while true;do ceph -s;sleep {delay};duration=$SECONDS;"
        f"if [ $duration -ge {timeout} ];then break;fi;done"
    )
    ceph_tools_pod = get_ceph_tools_pod(wait=True)

    try:
        ceph_out = ceph_tools_pod.exec_sh_cmd_on_pod(
            command=command, timeout=timeout + grace
        )
        log.info(f"Ceph status output:\n{ceph_out}")
        if "monclient(hunting): authenticate timed out" in ceph_out:
            log.warning("Ceph was hung for some time.")
            return False
        return True
    except Exception as err:
        if (
            "TimeoutExpired" in err.args[0]
            or "monclient(hunting): authenticate timed out" in err.args[0]
        ):
            log.error("Ceph status check got timed out. Maybe Ceph is hung.")
            return False
        elif (
            "connect: no route to host" in err.args[0]
            or "error dialing backend" in err.args[0]
        ):
            ceph_tools_pod.delete(force=True)
        raise


@tier2
class TestNodeShutdownsAndCrashes:
    @pytest.mark.parametrize(
        argnames=["nodes_type"],
        argvalues=[
            pytest.param(*["master"]),
            pytest.param(*["arbiter"]),
        ],
    )
    def test_master_node_shutdown_tna(
        self,
        node_restart_teardown,
        nodes,
        nodes_type,
        cnv_workload,
        setup_cnv,
    ):
        """
        This test will test the shutdown scenarios when VM workloads are running.
        Steps:
            1) Create VM using standalone PVC. Create some data inside the VM instance
            2) Induce one of the master node shutdown.
            3) Make sure ceph is accessible during the crash duration
            4) Check VM data integrity is maintained post node shutdown.
            5) Check if New IO is possible in VM and out of VM.
            6) Make sure there is no data loss

        """

        # setup vm and write some data to the VM instance
        vm_obj = cnv_workload(volume_interface=constants.VM_VOLUME_PVC)
        vm_obj.run_ssh_cmd(command="mkdir /test && sudo chmod -R 777 /test")
        vm_obj.run_ssh_cmd(
            command="< /dev/urandom tr -dc 'A-Za-z0-9' | head -c 10485760 > /test/file_1.txt && sync"
        )
        md5sum_before = cal_md5sum_vm(vm_obj, file_path="/test/file_1.txt")
        log.debug(
            f"This is the file_1.txt content:\n{vm_obj.run_ssh_cmd(command='cat /test/file_1.txt')}"
        )

        # Get the node list
        node = get_nodes(node_type=nodes_type)
        nodes_to_shutdown = random.choice(node)
        nodes.stop_nodes(nodes=nodes_to_shutdown)
        node_name = [node.name for node in nodes_to_shutdown]
        wait_for_nodes_status(
            node_names=node_name,
            status=constants.NODE_NOT_READY,
            timeout=300,
        )
        log.info(f"Node {node_name} is shutdown successfully")

        # check ceph accessibility while the node is down
        if not check_ceph_accessibility(timeout=600):
            assert (
                recover_from_ceph_stuck()
            ), "Something went wrong. not expected. please check rook-ceph logs"
        log.info("There is no issue with ceph access seen")

        # start the nodes
        try:
            nodes.start_nodes(nodes=nodes_to_shutdown)
        except Exception:
            log.error("Something went wrong!")

        # Validate all nodes are in READY state and up
        wait_for_nodes_status(timeout=600)

        # check vm data written before the failure for integrity
        log.info("Waiting for VM SSH connectivity!")
        retry(CommandFailed, tries=5, delay=10)(vm_obj.wait_for_ssh_connectivity)()
        retry(CommandFailed, tries=5, delay=10)(verify_vm_workload)(
            vm_obj, md5sum_before
        )

        # check vm data written after the failure for integrity
        verify_vm_workload(vm_obj, md5sum_before)
        # stop the VM
        vm_obj.stop()
        log.info("Stoped the VM successfully")
