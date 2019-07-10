import pytest
import logging

from ocs_ci.ocs import constants, exceptions
from ocs_ci.ocs import workload
from ocs_ci.framework.testlib import ManageTest
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.utility import templating
from ocs_ci.ocs.resources import pvc


from tests.fixtures import (
    create_rbd_storageclass, create_pod, create_pvc, create_ceph_block_pool,
    create_rbd_secret
)

logger = logging.getLogger(__name__)


@pytest.fixture()
def pod_cleanup(request):
    self = request.node.cls

    def finalizer():
        if hasattr(self, 'pod_obj'):
            logger.info(f"Deleting pod {self.pod_obj.name}")
            self.pod_obj.delete()
        else:
            logger.info(f"No pod_obj is present")
    request.addfinalizer(finalizer)


@pytest.fixture()
def pvc_cleanup(request):
    self = request.node.cls

    def finalizer():
        if hasattr(self, 'pvc_obj'):
            self.pvc_obj.reload()
            pv = pvc.get_pv_for_pvc(self.pvc_obj)
            logger.info(f"PV for PVC is {pv.name}")
            logger.info(f"Deleting PVC {self.pvc_obj.name}")
            self.pvc_obj.delete()
            logger.info(f"Deleting PV {pv.name}")
            pv.delete()
        else:
            logger.warning(f"No PVC found")
    request.addfinalizer(finalizer)


@pytest.mark.usefixtures(
    create_rbd_secret.__name__,
    create_ceph_block_pool.__name__,
    create_rbd_storageclass.__name__,
    create_pvc.__name__,
    create_pod.__name__,
    pvc_cleanup.__name__,
    pod_cleanup.__name__,
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
        runtime = 50
        size = '200M'

        wl = workload.WorkLoad(
            name, path, work_load, storage_type, self.pod_obj
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
