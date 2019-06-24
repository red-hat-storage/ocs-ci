import logging
import os
import importlib
import concurrent.futures

from ocs import constants


log = logging.getLogger(__name__)


class WorkLoad(object):
    def __init__(
            self, name=None, path=None, wrk_load=None,
            storage_type='fs', pod=None
    ):
        """
        Args:
            name (str): name for this workload instance (for identifying in a
                test run)
            path (str): Mount point OR blk device on the pod where workload
                should do IO (note: this need not be known at this
                point in time)
            wrk_load (str): example fio, mongodb, pgsql etc.
            storage_type (str): type on which we will be running IOs,
                if type is 'fs' we will interpret 'path' as mount point else
                if type is 'block' we will interpret 'path' as a block device
            pod (Pod): pod on which we want to run this workload
        """
        self.name = name
        self.path = path
        self.wrk_load = wrk_load
        self.storage_type = storage_type
        # Pod on which we will be running IO
        self.pod = pod
        self.wrk_load_dir = os.path.join(
            constants.TEMPLATE_WORKLOAD_DIR, self.wrk_load
        )
        try:
            self.wrk_load_mod = importlib.import_module(
                self.wrk_load_dir.self.wrk_load
            )
        except ModuleNotFoundError as ex:
            log.error(f"No workload found with name {self.wrk_load}")
            log.error(ex)
            raise ModuleNotFoundError
        self.tp_exec = concurrent.futures.ThreadPoolExecutor(max_workers=2)

    def setup(self, **setup_conf):
        """
        perform wrk_load_mod.setup to setup the workload

        Args:
            setup_conf (dict): work load setup configuration, varies from
                workload to workload. Refer constants.TEMPLATE_WORKLOAD_DIR
                for various available workloads

        Returns:
            bool: True if setup is success else False
        """
        if self.pod:
            setup_conf['pod'] = self.pod
        return self.wrk_load_mod.setup(**setup_conf)

    def run(self, **run_conf):
        """
        perform wrk_load_mod.run in order to run actual io

        Args:
            run_conf (dict): run configuration a.k.a parameters for workload
                io runs

        Returns:
            result (Future): returns a concurrent.future object
        """
        run_conf['pod'] = self.pod
        run_conf['path'] = self.path
        run_conf['type'] = self.storage_type
        return self.tp_exec.submit(self.wrk_load_mod.run(**run_conf))
