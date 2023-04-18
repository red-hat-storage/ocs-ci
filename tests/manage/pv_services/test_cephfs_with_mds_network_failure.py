import logging
import time
import pytest
from random import randint
from ocs_ci.ocs import constants
from threading import Thread
from ocs_ci.ocs.cluster import get_mds_standby_replay_info
from ocs_ci.helpers.helpers import disable_vm_network_for_duration
from ocs_ci.utility.utils import ceph_health_check
from ocs_ci.framework.pytest_customization.marks import (
    vsphere_platform_required,
    tier4b,
    skipif_external_mode,
)
from ocs_ci.ocs.resources.pod import search_pattern_in_pod_logs
from ocs_ci.helpers.helpers import change_vm_network_state

log = logging.getLogger(__name__)


@skipif_external_mode
@vsphere_platform_required
class TestCephFSWithMDSNetworkFailure:
    def teardown(self):
        """
        teardown function, Re-enabling the network connectivity of the VM if it was not done.
        """
        change_vm_network_state(self.standby_replay_node_ip, connect=True)

    @tier4b
    @pytest.mark.bugzilla("2130925")
    @pytest.mark.polarion_id("OCS-4858")
    def test_cephfs_network_interruption_standby_replay_MDS(self, pod_factory, request):
        """Test MDS crash by causing network interruption to the standby-replay daemon

        The test does the following:
        1. Deploy vmware upi cluster based on the test environment
        2. Identify the nodes running active and standby MDS by running commands like "ceph fs status"
        3. Run the cephFS IOs, if not running
        4. Disconnect the network of node from vmware side where ceph MDS standby daemon is runnning."
        5. Wait for maximum 15 secs
        6. reconnect the network of the node which was disconnected earlier.
        7. check for error messages like "respawn" and "Map removed me" in MDS logs. If they are found within 15 sec,
        then the main issue we are looking for is hit
        8. Bring the network down & up repeatedly for every 5sec, 10 sec and 15sec and 20sec and check logs
        9. Keep checking on ceph -s to identify any ceph daemon crash
        """

        # Get standby-replay daemon info
        log.info("Getting standby-replay daemon info...")
        ceph_standby_replay_info = get_mds_standby_replay_info()
        assert ceph_standby_replay_info, "Failed To get ceph mds daemon information."

        self.standby_replay_node_ip = ceph_standby_replay_info["node_ip"]
        request.addfinalizer(self.teardown)

        # Launch IO thread
        log.info("Launching IO thread...")
        pod_obj = pod_factory(interface=constants.CEPHFILESYSTEM)
        kwargs = {"storage_type": "fs", "size": "3G", "runtime": 240}
        io_thread = Thread(
            target=pod_obj.run_io,
            name="io_thread",
            kwargs=kwargs,
        )
        io_thread.start()

        # Cause network interruption to the standby-replay daemon three times
        log.info("Starting network interruptions...")
        for i in range(1, 4):
            disable_vm_network_for_duration(
                self.standby_replay_node_ip, duration=randint(5, 15)
            )
            time.sleep(5)

        # Wait for IO thread to finish
        log.info("Waiting for IO thread to finish...")
        io_thread.join()

        # Search MDS pod logs for pattern
        log.info("Searching MDS pod logs for pattern...")
        ceph_standby_replay_info = get_mds_standby_replay_info()
        pattern = r"respawn|Map removed me"
        matched_lines = search_pattern_in_pod_logs(
            ceph_standby_replay_info["standby_replay_pod"], pattern=pattern
        )

        # Assert that the pattern was found in the logs
        assert (
            len(matched_lines) == 0
        ), f"ceph MDS pod logs has lines with pattern '{pattern}'"

        # Check Ceph cluster health
        log.info("Checking Ceph cluster health...")
        assert ceph_health_check(), "Ceph cluster health is not OK"
