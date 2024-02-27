"""
Module to perform IOs with several weights
"""
import pytest
import logging

from ocs_ci.framework.pytest_customization.marks import grey_squad
from ocs_ci.framework.testlib import ManageTest


logger = logging.getLogger(__name__)


@grey_squad
class TestIOPerformance(ManageTest):
    """
    Test IO performance
    """

    @pytest.fixture()
    def base_setup(self, request, interface_iterate, pod_factory):
        """
        A setup phase for the test

        Args:
            interface_iterate: will iterate over RBD and CephFS interfaces
                to parametrize the test
            pod_factory: A fixture to create everything needed for a running
                pod

        """
        self.interface = interface_iterate
        self.pod_obj = pod_factory(self.interface)

    @pytest.mark.parametrize(
        argnames=["size", "io_direction", "jobs", "runtime", "depth"],
        argvalues=[
            pytest.param(
                *["1GB", "rw", 1, 60, 4], marks=pytest.mark.polarion_id("OCS-676")
            ),
            pytest.param(
                *["1GB", "rw", 6, 60, 16], marks=pytest.mark.polarion_id("OCS-677")
            ),
            pytest.param(
                *["1GB", "rw", 12, 60, 32], marks=pytest.mark.polarion_id("OCS-678")
            ),
        ],
    )
    @pytest.mark.usefixtures(base_setup.__name__)
    def test_run_io(self, size, io_direction, jobs, runtime, depth):
        """
        Test IO
        """
        logger.info(
            f"Running FIO with:\nsize: {size}\njobs: {jobs}\n"
            f"runtime: {runtime}\nIO depth: {depth}\n"
        )
        self.pod_obj.run_io(
            "fs",
            size=size,
            io_direction=io_direction,
            jobs=jobs,
            runtime=runtime,
            depth=depth,
        )
        logger.info("Waiting for results")
        fio_result = self.pod_obj.get_fio_results()
        logger.info("IOPs after FIO:")
        reads = fio_result.get("jobs")[0].get("read").get("iops")
        writes = fio_result.get("jobs")[0].get("write").get("iops")
        w_bw = fio_result.get("jobs")[0].get("write").get("bw")
        r_bw = fio_result.get("jobs")[0].get("read").get("bw")
        logger.info(f"Read: {reads} IOPS , {r_bw} MiB/Sec")
        logger.info(f"Write: {writes} IOPS , {w_bw} MiB/Sec")
