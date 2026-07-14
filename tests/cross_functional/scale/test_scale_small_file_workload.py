"""
Test to exercise Small File Workload in scale from 500K files to 5M files
and try to see if the CephFS storage pool is inflate after deleting the volume.

"""

# Builtin modules
import logging
import re
import time

# 3ed party modules
import pytest

# Local modules
from ocs_ci.framework.pytest_customization.marks import orange_squad
from ocs_ci.framework.testlib import E2ETest, scale
from ocs_ci.ocs import constants
from ocs_ci.ocs.cluster import CephCluster
from ocs_ci.ocs.elasticsearch import ElasticSearch
from ocs_ci.ocs.small_file_workload import SmallFiles

log = logging.getLogger(__name__)


@orange_squad
@scale
class TestSmallFileWorkloadScale(E2ETest):
    """
    Deploy benchmark operator and run different scale tests.
    Call common small files workload routine to run SmallFile workload
    """

    def setup(self):
        """
        Initialize the test environment

        """
        # Deploy internal ES server - not need to keep results,
        # so don't use production ES
        self.es = ElasticSearch()

        # Initial the Small Files workload, based on benchmark-operator
        self.small_files = SmallFiles(self.es)

        self.ceph_cluster = CephCluster()

        # Get the total storage capacity
        self.ceph_capacity = self.ceph_cluster.get_ceph_capacity()
        log.info(f"Total storage capacity is {self.ceph_capacity:,.2f} GiB")

        # Collect the pulls usage before the test is starting
        self.orig_data = self.get_cephfs_data()

    def teardown(self):
        """
        Teardown the test environment

        """
        self.small_files.cleanup()
        self.es.cleanup()

    def get_cephfs_data(self):
        """
        Look through ceph pods and find space usage on all ceph filesystem pods

        Returns:
            Dictionary of byte usage, indexed by pod name.
        """
        ceph_status = self.ceph_cluster.toolbox.exec_ceph_cmd(ceph_cmd="ceph df")
        ret_value = {}
        for pool in ceph_status["pools"]:
            # Only the data pool is in our interest (not metadata)
            if "cephfilesystem" in pool["name"]:
                ret_value[pool["name"]] = pool["stats"]["bytes_used"]
        return ret_value

    def display_ceph_usage(self, msg, data):
        """
        Display the pool usage in a pretty way

        Args:
            msg (str): the message string to display with the values
            data (dict): dictionary of pools -> capacity (in bytes)

        """
        log.info(f"The pools usage {msg} is :")
        for entry in data:
            log.info(f"{entry} now uses {data[entry]:,} bytes")

    @pytest.mark.parametrize(
        argnames=["file_size", "files", "threads", "interface"],
        argvalues=[
            # 500K Files, ~4GB
            pytest.param(*[8, 125000, 4, constants.CEPHFILESYSTEM]),
            # 5M Files, ~152GB
            pytest.param(*[32, 1250000, 4, constants.CEPHFILESYSTEM]),
        ],
    )
    def test_scale_smallfile_workload(self, file_size, files, threads, interface):
        # updating the benchmark parameters
        self.small_files.setup_storageclass(interface)
        self.small_files.setup_test_params(file_size, files, threads, 1)

        # Verify we have enough storage capacity to run the test.
        self.small_files.setup_vol_size(file_size, files, threads, self.ceph_capacity)

        # Run the benchmark to create files on the volume
        self.small_files.setup_operations("create")
        self.small_files.run()

        # Collect pools usage after creation is done.
        self.run_data = self.get_cephfs_data()

        # Delete the benchmark data
        self.small_files.delete()

        # Getting the usage capacity immediately after deletion
        self.now_data = self.get_cephfs_data()

        # Wait 3 minutes for the backend deletion actually start.
        time.sleep(180)

        # Quarry the storage usage every 2 Min. if no difference between two
        # samples, the backend cleanup is done.
        still_going_down = True
        while still_going_down:
            log.info("Waiting for Ceph to finish cleaning up")
            time.sleep(120)
            self.new_data = self.get_cephfs_data()
            still_going_down = False
            for entry in self.new_data:
                if self.new_data[entry] < self.now_data[entry]:
                    still_going_down = True
                    self.now_data[entry] = self.new_data[entry]

        self.display_ceph_usage("Before ths test", self.orig_data)
        self.display_ceph_usage("After data creation", self.run_data)

        # Make sure that the test actually wrote data to the volume
        # at least 1GiB.
        for entry in self.run_data:
            if re.search("metadata", entry):
                # Since we are interesting in the data written and not the metadata
                # skipping the metadata pool
                continue
            written = self.run_data[entry] - self.orig_data[entry]
            check = written > constants.GB
            errmsg = (
                f"{written:,.2f} bytes was written to {entry} -"
                "This is not enough for the test"
            )
            assert check, errmsg

        self.display_ceph_usage("After data deletion", self.now_data)

        for entry in self.now_data:
            # Leak indicated if over %20 more storage is used and more then 5 GiB.
            try:
                ratio = self.now_data[entry] / self.orig_data[entry]
            except ZeroDivisionError:
                ratio = self.now_data[entry]

            added_data = (self.now_data[entry] - self.orig_data[entry]) / constants.GB
            # in some cases (especially for metadata), it might be that after the
            # test there is less data in the pool than before the test.
            if added_data < 0:
                added_data = 0
                ratio = 1

            log.info(
                "The ratio between capacity before and after the test "
                f"on {entry} is : {ratio:.2f} ; {added_data:,.2f} GiB"
            )

            check = (ratio < 1.20) or (added_data < 3)
            errmsg = f"{entry} is over 20% (or 3 GiB) larger [{ratio} ; {added_data}]-- possible leak"
            assert check, errmsg
