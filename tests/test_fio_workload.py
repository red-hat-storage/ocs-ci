import pytest
import logging
import copy

from ocs import defaults, exceptions
from ocs import workload
from ocsci.testlib import ManageTest
from utility.utils import TimeoutSampler


from tests.fixtures import (
    create_rbd_storageclass, create_pod, create_pvc, create_ceph_block_pool,
    create_rbd_secret
)

logger = logging.getLogger(__name__)


@pytest.mark.usefixtures(
    create_rbd_secret.__name__,
    create_ceph_block_pool.__name__,
    create_rbd_storageclass.__name__,
    create_pvc.__name__,
    create_pod.__name__
)
class TestFIOWorkload(ManageTest):

    def test_fio_with_block_storage(self):
        name = 'test_workload'
        spec = self.pod_obj['spec']
        path = spec['containers'][0]['volumeMounts'][0]['mountPath']
        wrk_load = 'fio'
        storage_type = 'block'

        wl = workload.WorkLoad(
            name, path, wrk_load, storage_type, self.pod_obj
        )
        assert wl.setup()
        io_conf = copy.deep_copy(defaults.FIO_IO_PARAMS)
        future_result = wl.run(io_conf)

        timeout = 1200
        sample = TimeoutSampler(
            timeout=timeout, sleep=3, func=future_result.done
        )
        assert sample.wait_for_func_status(result=True)
        try:
            out = future_result.result()
        except exceptions.CommandFailed as ex:
            logger.error("FIO failed")
            logger.error(ex)

        logger.info(out)
