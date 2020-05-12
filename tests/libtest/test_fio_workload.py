import pytest
import logging

from ocs_ci.ocs import constants, exceptions
from ocs_ci.ocs import workload
from ocs_ci.framework.testlib import ManageTest, libtest
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.utility import templating


from tests.fixtures import (
    create_rbd_storageclass, create_rbd_pod, create_pvc, create_ceph_block_pool,
    create_rbd_secret, delete_pvc, delete_pod, create_project
)

logger = logging.getLogger(__name__)


@libtest
@pytest.mark.usefixtures(
    create_rbd_secret.__name__,
    create_ceph_block_pool.__name__,
    create_rbd_storageclass.__name__,
    create_project.__name__,
    create_pvc.__name__,
    create_rbd_pod.__name__,
    delete_pvc.__name__,
    delete_pod.__name__
)
class TestFIOWorkload(ManageTest):

    def test_fio_with_block_storage(self):
        name = 'test_workload'
        spec = self.pod_obj.data.get('spec')
        path = (
            spec.get('containers')[0].get('volumeMounts')[0].get('mountPath')
        )
        work_load = 'fio'
        storage_type = 'fs'
        # few io parameters for Fio
        runtime = 10
        size = '200M'

        wl = workload.WorkLoad(
            name, path, work_load, storage_type, self.pod_obj
        )
        assert wl.setup()
        io_params = templating.load_yaml(constants.FIO_IO_PARAMS_YAML)
        io_params['runtime'] = runtime
        io_params['size'] = size

        future_result = wl.run(**io_params)

        timeout = 1200
        sample = TimeoutSampler(
            timeout=timeout, sleep=3, func=future_result.done
        )
        assert sample.wait_for_func_status(result=True)

        try:
            logger.info(future_result.result())
        except exceptions.CommandFailed:
            logger.exception("FIO failed")
            raise
        except Exception:
            logger.exception("Found Exception")
            raise
