import logging

from ocs_ci.framework.testlib import (
    ManageTest,
    ignore_leftovers,
    libtest,
    brown_squad,
    runs_on_provider,
)
from ocs_ci.deployment.baremetal import download_script_to_node, run_script_on_node
from ocs_ci.ocs.node import get_nodes

log = logging.getLogger(__name__)


@brown_squad
@libtest
@ignore_leftovers
@runs_on_provider
class TestSimulateCephBlueStoreLabel(ManageTest):
    """
    Test that simulate_bluestore_label correctly stamps and verifies a BlueStore label on a test disk.
    """

    def test_simulate_bluestore_label(self):
        worker = get_nodes()[0]
        # Destination path on node
        script_path = "/tmp/simulate_bluestore_label.sh"
        url_prefix = "https://raw.githubusercontent.com/red-hat-storage/ocs-ci/master/"
        script_name = "scripts/bash/simulate_bluestore_label.sh"
        script_url = url_prefix + script_name

        # Step 1: Download the script directly on the node
        download_script_to_node(
            worker=worker,  # worker node object
            script_url=script_url,
            script_path=script_path,  # destination path on node
            namespace="default",
            timeout=300,
        )

        # Step 2: Run the script (optional verification stage)
        out = run_script_on_node(
            worker=worker,
            script_path=script_path,
            args="/dev/sda",
            namespace="default",
            timeout=600,
        )

        log.info(out)
        assert (
            "Verification PASSED" in out or ">>> BlueStore UUID:" in out
        ), "BlueStore label simulation failed"
