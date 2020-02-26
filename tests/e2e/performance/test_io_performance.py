"""
Module to perform IOs with several weights
"""
import pytest
import logging
from ocs_ci.utility.spreadsheet.spreadsheet_api import GoogleSpreadSheetAPI
from ocs_ci.framework.testlib import (
    ManageTest, performance, google_api_required,
)


logger = logging.getLogger(__name__)


@google_api_required
@performance
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
        argnames=[
            "size", "io_direction", "jobs",
            "runtime", "depth", "sheet_index"
        ],
        argvalues=[
            pytest.param(
                *['1GB', 'rw', 1, 60, 4, 1],
                marks=pytest.mark.polarion_id("OCS-676")
            ),
            pytest.param(
                *['1GB', 'rw', 6, 60, 16, 2],
                marks=pytest.mark.polarion_id("OCS-677")
            ),
            pytest.param(
                *['1GB', 'rw', 12, 60, 32, 3],
                marks=pytest.mark.polarion_id("OCS-678")
            ),
        ]
    )
    @pytest.mark.usefixtures(base_setup.__name__)
    def test_run_io(
        self, size, io_direction, jobs, runtime, depth, sheet_index
    ):
        """
        Test IO
        """
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
        logging.info("IOPs after FIO:")
        reads = fio_result.get('jobs')[0].get('read').get('iops')
        writes = fio_result.get('jobs')[0].get('write').get('iops')
        logging.info(f"Read: {reads}")
        logging.info(f"Write: {writes}")
        g_sheet = GoogleSpreadSheetAPI("OCS FIO", sheet_index)
        g_sheet.insert_row([self.interface, reads, writes], 2)
