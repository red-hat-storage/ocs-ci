"""
Module to measure IO Latency, IOPS and bandwidth with several weights
"""
import pytest
import logging
from ocs_ci.utility.spreadsheet.spreadsheet_api import GoogleSpreadSheetAPI
from ocs_ci.framework.testlib import (
    E2ETest, performance, google_api_required,
)


logger = logging.getLogger(__name__)


@google_api_required
@performance
class TestIORandReadProfile(E2ETest):
    """
    Test Random read IO performance using FIO
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
        argnames=[
            "size", "io_direction", "jobs",
            "runtime", "depth", "sheet_index"
        ],
        argvalues=[
            pytest.param(
                *['1G', 'ro', 2, 300, 64, 4]
            ),
            pytest.param(
                *['1G', 'ro', 2, 300, 16, 4]
            ),
            pytest.param(
                *['1G', 'ro', 2, 300, 4, 4]
            ),
            pytest.param(
                *['10M', 'ro', 2, 300, 64, 3]
            ),
            pytest.param(
                *['10M', 'ro', 2, 300, 16, 3]
            ),
            pytest.param(
                *['10M', 'ro', 2, 300, 4, 3]
            ),
            pytest.param(
                *['1M', 'ro', 2, 300, 64, 2]
            ),
            pytest.param(
                *['1M', 'ro', 2, 300, 16, 2]
            ),
            pytest.param(
                *['1M', 'ro', 2, 300, 4, 2]
            ),
            pytest.param(
                *['10k', 'ro', 2, 300, 64, 1]
            ),
            pytest.param(
                *['10k', 'ro', 2, 300, 16, 1]
            ),
            pytest.param(
                *['10k', 'ro', 2, 300, 4, 1]
            )
        ]
    )
    @pytest.mark.usefixtures(base_setup.__name__)
    def test_perf_read_latency(
        self, size, io_direction, jobs, runtime, depth, sheet_index
    ):
        """
        Test that profiles 4k block IO using fio for various file sizes.
        Results are updated to spreadsheets OCS_RBD_PROFILE &
        OCS_CEPHFS_PROFILE under ocs-qe drive
        """
        # todo: check to validate against benchmark and assert
        logging.info(
            f"Running FIO with:\nsize: {size}\njobs: {jobs}\n"
            f"runtime: {runtime}\nIO depth: {depth}\n"
        )
        self.pod_obj.run_io(
            'fs', size=size, io_direction=io_direction, jobs=jobs,
            runtime=runtime, depth=depth
        )
        logging.info("Waiting for results")
        fio_result = self.pod_obj.get_fio_results()
        logging.info("collecting clat&bw after FIO:")
        read_latency_ns = fio_result.get('jobs')[1].get('read').get('clat_ns').get('mean')
        read_latency_ms = read_latency_ns / 1000000
        read_iops = fio_result.get('jobs')[1].get('read').get('iops')
        read_bw_kb = fio_result.get('jobs')[1].get('read').get('bw_mean')
        read_bw_mb = read_bw_kb / 1024
        logging.info(f"Read_latency: {read_latency_ms}")
        logging.info(f"Read_bw: {read_bw_mb}")
        sheet = 'OCS_RBD_PROFILE' if self.interface == 'CephBlockPool' else 'OCS_CEPHFS_PROFILE'
        g_sheet = GoogleSpreadSheetAPI(sheet, sheet_index)
        g_sheet.insert_row([depth, read_latency_ms, read_bw_mb, read_iops], 2)
