"""
Module to perform IOs with several weights
"""
import pytest
import logging
from ocs_ci.utility.spreadsheet.spreadsheet_api import GoogleSpreadSheetAPI

from ocs_ci.framework.testlib import ManageTest, tier1, google_api_required
from tests.fixtures import (
    create_rbd_storageclass, create_rbd_pod, create_pvc, create_ceph_block_pool,
    create_rbd_secret, create_project
)

logger = logging.getLogger(__name__)


@pytest.mark.usefixtures(
    create_rbd_secret.__name__,
    create_ceph_block_pool.__name__,
    create_rbd_storageclass.__name__,
    create_project.__name__,
    create_pvc.__name__,
    create_rbd_pod.__name__,
)
@google_api_required
@tier1
class TestIOPerformance(ManageTest):
    """
    Test IO performance
    """

    @pytest.mark.parametrize(
        argnames=[
            "size", "io_direction", "jobs", "runtime", "depth",
            "cell_to_update"
        ],
        argvalues=[
            pytest.param(
                *['1GB', 'rw', 1, 120, 4, (5, 2)],
                #marks=pytest.mark.polarion_id("OCS-555")
            ),
            pytest.param(
                *['1GB', 'rw', 6, 120, 16, (13, 2)],
                #marks=pytest.mark.polarion_id("OCS-558")
            ),
            pytest.param(
                *['1GB', 'rw', 12, 120, 32, (21, 2)],
                #marks=pytest.mark.polarion_id("OCS-559")
            ),
        ]
    )
    def test_run_io(
        self, size, io_direction, jobs, runtime, depth, cell_to_update
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
        g_sheet = GoogleSpreadSheetAPI("OCS FIO")
        g_sheet.update_sheet(cell_to_update[0], cell_to_update[1], reads)
        g_sheet.update_sheet(cell_to_update[0] + 1, cell_to_update[1], writes)
