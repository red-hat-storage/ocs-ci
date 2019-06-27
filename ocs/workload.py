import logging
import importlib
import concurrent.futures


log = logging.getLogger(__name__)


class WorkLoad(object):
    def __init__(
        self, name=None, path=None, work_load=None, storage_type='fs',
        pod=None
    ):
        """
        Args:
            name (str): name for this workload instance (for identifying in a
                test run)
            path (str): Mount point OR blk device on the pod where workload
                should do IO (note: this need not be known at this
                point in time)
            work_load (str): example fio, mongodb, pgsql etc.
            storage_type (str): type on which we will be running IOs,
                if type is 'fs' we will interpret 'path' as mount point else
                if type is 'block' we will interpret 'path' as a block device
            pod (Pod): pod on which we want to run this workload
        """
        self.name = name
        self.path = path
        self.work_load = work_load
        self.storage_type = storage_type
        self.pod = pod

        try:
            self.work_load_mod = importlib.import_module(
                f'workloads.{self.work_load}.{self.work_load}'
            )
        except ModuleNotFoundError as ex:
            log.error(f"No workload found with name {self.work_load}")
            log.error(ex)
            raise

        self.thread_exec = concurrent.futures.ThreadPoolExecutor(max_workers=4)

    def setup(self, **setup_conf):
        """
        perform work_load_mod.setup to setup the workload

        Args:
            setup_conf (dict): work load setup configuration, varies from
                workload to workload. Refer constants.TEMPLATE_WORKLOAD_DIR
                for various available workloads

        Returns:
            bool: True if setup is success else False
        """
        if self.pod:
            setup_conf['pod'] = self.pod
        return self.work_load_mod.setup(**setup_conf)

    def run(self, **conf):
        """
        perform work_load_mod.run in order to run actual io

        Args:
            **conf (dict): run configuration a.k.a parameters for workload
                io runs

        Returns:
            result (Future): returns a concurrent.future object
        """
        conf['pod'] = self.pod
        conf['path'] = self.path
        conf['type'] = self.storage_type
        future_obj = self.thread_exec.submit(self.work_load_mod.run, **conf)
        log.info("Done submitting.. ")
        return future_obj
