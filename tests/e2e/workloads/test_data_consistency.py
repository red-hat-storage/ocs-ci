# -*- coding: utf8 -*-

import logging

import pytest

from ocs_ci.ocs.cephfs_workload import LogReaderWriterParallel
from ocs_ci.framework.pytest_customization import marks
from ocs_ci.framework.testlib import tier1, ManageTest, skipif_ms_provider


logger = logging.getLogger(__name__)


@skipif_ms_provider
class TestLogReaderWriterParallel(ManageTest):
    """
    Write and read logfile stored on cephfs volume, from all worker nodes of a
    cluster via k8s Deployment, while fetching content of the stored data via
    oc rsync to check the data locally.
    """

    @tier1
    @marks.bugzilla("1989301")
    @pytest.mark.polarion_id("OCS-2735")
    def test_log_reader_writer_parallel(self, project, tmp_path):
        """
        Write and read logfile stored on cephfs volume, from all worker nodes of a
        cluster via k8s Deployment, while fetching content of the stored data via
        oc rsync to check the data locally.

        Reproduces BZ 1989301. Test failure means new blocker high priority bug.
        """
        log_read_write = LogReaderWriterParallel(
            project, tmp_path, number_of_fetches=120
        )
        log_read_write.log_reader_writer_parallel()
        # while the workload is running, we will try to fetch and validate data
        # from the cephfs volume of the workload 120 times (this number of retries
        # is a bit larger than usual number required to reproduce bug from
        # BZ 1989301, but we need to be sure here)
        log_read_write.fetch_and_validate_data()
