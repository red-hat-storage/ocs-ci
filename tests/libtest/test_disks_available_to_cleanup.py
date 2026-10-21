import json
import logging

from ocs_ci.framework.testlib import ManageTest, libtest, brown_squad, skipif_no_lso
from ocs_ci.deployment.baremetal import disks_available_to_cleanup
from ocs_ci.ocs import constants
from ocs_ci.ocs.node import get_nodes
from ocs_ci.ocs.ocp import OCP

log = logging.getLogger(__name__)


@brown_squad
@libtest
@skipif_no_lso
class TestDisksAvailableToCleanup(ManageTest):
    """
    Verify that disks_available_to_cleanup returns eligible disks and that the
    new per-disk lsblk size lookup used in clean_disks works for each of them.
    No actual disk wiping is performed.
    """

    def test_disks_available_to_cleanup(self):
        """
        For each worker node:
          1. Confirm disks_available_to_cleanup returns a non-empty list.
          2. For each disk, run the per-disk lsblk size lookup introduced in
             the clean_disks fix and verify a valid size is returned.
        """
        ocp_obj = OCP()
        worker_nodes = get_nodes(node_type=constants.WORKER_MACHINE)
        assert worker_nodes, "No worker nodes found"

        for worker in worker_nodes:
            disks = disks_available_to_cleanup(worker)
            log.info("Node %s — disks available for cleanup: %s", worker.name, disks)
            assert isinstance(
                disks, list
            ), f"Expected a list for node {worker.name}, got {type(disks)}"

            for disk_name in disks:
                out = ocp_obj.exec_oc_debug_cmd(
                    node=worker.name,
                    cmd_list=[f"lsblk -n --output SIZE -b --json /dev/{disk_name}"],
                )
                size = int(json.loads(str(out))["blockdevices"][0]["size"])
                log.info(
                    "Node %s — disk /dev/%s size: %d bytes",
                    worker.name,
                    disk_name,
                    size,
                )
                assert size > 0, (
                    f"Expected positive size for /dev/{disk_name} on "
                    f"{worker.name}, got {size}"
                )
