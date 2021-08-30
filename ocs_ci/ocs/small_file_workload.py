"""
Class to implement the Small Files benchmark as a subclass of the benchmark operator

This workload is required an elastic-search instance to run.

"""

# Builtin modules
import logging

# Local modules
from ocs_ci.framework import config
from ocs_ci.ocs import constants, benchmark_operator
from ocs_ci.ocs.benchmark_operator import BenchmarkOperator
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.utility import templating
from ocs_ci.utility.utils import TimeoutSampler

log = logging.getLogger(__name__)


class SmallFiles(BenchmarkOperator):
    """
    Small_Files workload benchmark
    """

    def __init__(self, es, **kwargs):
        """
        Initializer function

        Args:
            es (obj): elastic search instance object

        """
        self.es = es
        self.dev_mode = config.RUN["cli_params"].get("dev_mode")
        super().__init__(**kwargs)

        # Loading the main template yaml file for the benchmark
        log.info("Loading the CRD Template file")
        self.crd_data = templating.load_yaml(constants.SMALLFILE_BENCHMARK_YAML)
        assert (
            self._setup_elasticsearch()
        ), "Can not execute the workload without ES server"
        self.deploy()

    def _setup_elasticsearch(self):
        """
        Setting up the elastic search parameters in the CRD object.

        Return:
            bool : True if there is ES to connect, False otherwise

        """
        log.info("Setting up the elasticsearch configuration")
        self.crd_data["spec"]["elasticsearch"] = {}
        if not self.dev_mode and config.PERF.get("production_es"):
            log.info("Setting ES to production !")
            self.crd_data["spec"]["elasticsearch"] = {
                "server": config.PERF.get("production_es_server"),
                "port": config.PERF.get("production_es_port"),
            }
        elif self.dev_mode and config.PERF.get("dev_lab_es"):
            log.info("Setting ES to development one !")
            self.crd_data["spec"]["elasticsearch"] = {
                "server": config.PERF.get("dev_es_server"),
                "port": config.PERF.get("dev_es_port"),
            }

        if not self.crd_data["spec"]["elasticsearch"] == {}:
            self.crd_data["spec"]["elasticsearch"]["url"] = "http://{}:{}".format(
                self.crd_data["spec"]["elasticsearch"]["server"],
                self.crd_data["spec"]["elasticsearch"]["port"],
            )
            self.crd_data["spec"]["elasticsearch"]["parallel"] = True

        # Saving the Original elastic-search IP and PORT - if defined in yaml
        self.backup_es = self.crd_data["spec"]["elasticsearch"]

        # Use the internal define elastic-search server in the test - if exist
        if self.es:
            self.crd_data["spec"]["elasticsearch"] = {
                "url": f"http://{self.es.get_ip()}:{self.es.get_port()}",
                "server": self.es.get_ip(),
                "port": self.es.get_port(),
                "parallel": True,
            }
        if self.crd_data["spec"]["elasticsearch"] == {}:
            log.error(
                "No ElasticSearch server is available. workload can not be execute"
            )
            return False

        return True

    def setup_storageclass(self, interface):
        """
        Setting up the storageclass parameter in the CRD object

        Args:
            interface (str): the storage interface

        """
        if interface == constants.CEPHBLOCKPOOL:
            storageclass = constants.DEFAULT_STORAGECLASS_RBD
        else:
            storageclass = constants.DEFAULT_STORAGECLASS_CEPHFS
        log.info(f"Using {storageclass} Storageclass")
        self.crd_data["spec"]["workload"]["args"]["storageclass"] = storageclass

    def setup_test_params(self, file_size, files, threads, samples):
        """
        Setting up the parameters for this test

        Args:
            file_size (int): the file size in KB
            files (int): number of file to use in the test
            threads (int): number of threads to use in the test
            samples (int): number of sample to run the test

        """
        self.crd_data["spec"]["workload"]["args"]["file_size"] = file_size
        self.crd_data["spec"]["workload"]["args"]["files"] = files
        self.crd_data["spec"]["workload"]["args"]["threads"] = threads
        self.crd_data["spec"]["workload"]["args"]["samples"] = samples

    def setup_vol_size(self, file_size, files, threads, total_capacity):
        """
        Calculating the size of the volume that need to be test, it should
        be at least twice in the size then the size of the files, and at
        least 100Gi.

        Since the file_size is in Kb and the vol_size need to be in Gb, more
        calculation is needed.

        Args:
            file_size (int): the file size in KB
            files (int): number of file to use in the test
            threads (int): number of threads to use in the test
            total_capacity (int): The total usable storage capacity in GiB

        """
        vol_size = int(files * threads * file_size * 3)
        vol_size = int(vol_size / constants.GB2KB)
        if vol_size < 100:
            vol_size = 100
        errmsg = (
            "There is not enough storage to run the test. "
            f"Storage capacity : {total_capacity:,.2f} GiB, "
            f"Needed capacity is more then {vol_size:,.2f} GiB"
        )
        assert vol_size < total_capacity, errmsg
        self.crd_data["spec"]["workload"]["args"]["storagesize"] = f"{vol_size}Gi"

    def setup_operations(self, ops):
        """
        Setting up the test operations

        Args:
            ops : can be list of operations or a string of one operation

        """
        if isinstance(ops, list):
            self.crd_data["spec"]["workload"]["args"]["operation"] = ops
        elif isinstance(ops, str):
            self.crd_data["spec"]["workload"]["args"]["operation"] = [ops]

    def run(self):
        """
        Run the benchmark and wait until it completed

        """
        # Create the benchmark object
        self.sf_obj = OCS(**self.crd_data)
        self.sf_obj.create()

        # Wait for benchmark pods to get created - takes a while
        for bench_pod in TimeoutSampler(
            240,
            10,
            get_pod_name_by_pattern,
            "smallfile-client",
            benchmark_operator.BMO_NAME,
        ):
            try:
                if bench_pod[0] is not None:
                    small_file_client_pod = bench_pod[0]
                    break
            except IndexError:
                log.info("Bench pod not ready yet")

        bench_pod = OCP(kind="pod", namespace=benchmark_operator.BMO_NAME)
        log.info("Waiting for SmallFile benchmark to Run")
        assert bench_pod.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            resource_name=small_file_client_pod,
            sleep=30,
            timeout=600,
        )
        log.info("The SmallFiles benchmark is running, wait for completion")
        bench_pod.wait_for_resource(
            condition=constants.STATUS_COMPLETED,
            resource_name=small_file_client_pod,
            timeout=3600,
            sleep=60,
        )
        log.info("The SmallFiles benchmark is completed")

    def delete(self):
        """
        Delete the benchmark

        """
        log.info("Deleting The Small Files benchmark")
        self.sf_obj.delete()
