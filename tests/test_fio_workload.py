import logging

from ocs_ci.ocs import constants, exceptions
from ocs_ci.ocs import workload
from ocs_ci.framework.testlib import ManageTest
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.utility import templating

logger = logging.getLogger(__name__)


class TestFIOWorkload(ManageTest):

    def test_fio_with_block_storage(self, rbd_pod_factory):
        rbd_pod = rbd_pod_factory()
        name = 'test_workload'
        spec = rbd_pod.data.get('spec')
        path = (
            spec.get('containers')[0].get('volumeMounts')[0].get('mountPath')
        )
        work_load = 'fio'
        storage_type = 'fs'
        # few io parameters for Fio
        runtime = 10
        size = '200M'

        wl = workload.WorkLoad(
            name, path, work_load, storage_type, rbd_pod
        )
        assert wl.setup()
        io_params = templating.load_yaml_to_dict(constants.FIO_IO_PARAMS_YAML)
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
            logger.exception(f"FIO failed")
            raise
        except Exception:
            logger.exception(f"Found Exception")
            raise
