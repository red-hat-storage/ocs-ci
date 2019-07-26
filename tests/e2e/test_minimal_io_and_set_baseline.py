"""
OCS-685

Demonstrate that the ceph configuration can handle a minimal level of I/O
and establish a baseline for comparision of later I/O tests.
"""
import pytest
import logging
import json

from ocs_ci.framework.testlib import ManageTest, tier1
from tests.fixtures import (
    create_rbd_storageclass, create_rbd_pod, create_pvc, create_ceph_block_pool,
    create_rbd_secret, create_project
)

logger = logging.getLogger(__name__)


def do_io_actions(io_type, io_info):
    """
    Display fio statistics and stash fio results somewhere.

    Args:
        io_type (str):  either "Read" or "Write"
        io_info (dict): results of fio run.
    """
    logging.info(
        f'{io_type}: {io_info.get("iops")} IOPS '
    )
    logging.info(
        f'{io_type}: {io_info.get("clat")} Completion Latency '
    )
    with open(f'/tmp/{io_type}_fio_data_baseline.json', 'w') as out_json:
        json.dump(io_info, out_json)


@pytest.mark.usefixtures(
    create_rbd_secret.__name__,
    create_ceph_block_pool.__name__,
    create_rbd_storageclass.__name__,
    create_project.__name__,
    create_pvc.__name__,
    create_rbd_pod.__name__,
)
@pytest.mark.polarion_id("OCS-685")
class TestMinimalLimit(ManageTest):

    @tier1
    def test_run_io(self):
        """
        Use fio to exercise the Ceph cluster
        """
        self.pod_obj.run_io('fs', '1G')
        logging.info("Waiting for results")
        fio_result = self.pod_obj.get_fio_results()
        read_job_info = fio_result.get('jobs')[0].get('read')
        write_job_info = fio_result.get('jobs')[0].get('write')
        do_io_actions("Read", read_job_info)
        do_io_actions("Write", write_job_info)
