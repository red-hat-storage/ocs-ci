"""
Module to perform FIO benchmark
"""
import logging
import pytest
import time
import json

from ocs_ci.utility import templating
from ocs_ci.utility.utils import run_cmd
from ocs_ci.ocs import constants
from ocs_ci.utility.performance_dashboard import push_perf_dashboard
from ocs_ci.framework.testlib import performance, skipif_ocs_version
from ocs_ci.ocs.perfresult import PerfResult
from ocs_ci.helpers.helpers import get_full_test_logs_path
from ocs_ci.ocs.perftests import PASTest
from ocs_ci.ocs.cluster import CephCluster, calculate_compression_ratio
from ocs_ci.helpers.performance_lib import run_command

log = logging.getLogger(__name__)


class FIOResultsAnalyse(PerfResult):
    """
    This class is reading all test results from elasticsearch server (which the
    ripsaw running of the benchmark is generate), aggregate them by :
        test operation (e.g. create / delete etc.)
        sample (for test to be valid it need to run with more the one sample)
        host (test can be run on more then one pod {called host})

    It generates results for all tests as one unit which will be valid only
    if the deviation between samples is less the 5%

    """

    def __init__(self, uuid, crd, full_log_path, es_con):
        """
        Initialize the object by reading some of the data from the CRD file and
        by connecting to the ES server and read all results from it.

        Args:
            uuid (str): the unique uid of the test
            crd (dict): dictionary with test parameters - the test yaml file
                        that modify it in the test itself.
            full_log_path (str): the path of the results files to be found
            es_con (elasticsearch): an elasticsearch connection

        """

        super(FIOResultsAnalyse, self).__init__(uuid, crd)
        self.index = "ripsaw-fio-analyzed-result"
        self.new_index = "ripsaw-fio-fullres"
        self.full_log_path = full_log_path
        # make sure we have connection to the elastic search server
        self.es = es_con

    def read_results_from_file(self):
        """
        Reading all data from the output file that was dumped from the internal ES server

        """
        file_name = f"{self.full_log_path}/results/{self.index}.data.json"
        log.info(f"Reading the {self.index} data from the file")
        full_data = []
        with open(file_name) as json_file:
            while True:
                line = json_file.readline()
                if line:
                    full_data.append(json.loads(line))
                else:
                    break
        return full_data

    def analyze_results(self):
        """
        Analyzing the results of the test and creating one record with all test
        information

        """
        results = self.read_results_from_file()

        log.info("Test Results are :")
        for result in results:
            test_data = result["ceph_benchmark_test"]["test_data"]
            object_size = test_data["object_size"]
            operation = test_data["operation"]
            if operation == "rw":
                operation = "pre-fill"
            total_iops = f"{test_data['total-iops']:.2f}"
            total_iops = float(total_iops)
            std_dev = "std-dev-" + object_size
            variance = 0
            bs = int(object_size.replace("KiB", ""))
            if std_dev in test_data.keys():
                variance = f"{test_data[std_dev]:.2f}"
            if object_size in self.all_results.keys():
                self.all_results[object_size][operation] = {
                    "IOPS": total_iops,
                    "std_dev": float(variance),
                    "Throughput": int(int(total_iops) * bs / 1024),
                }
            else:
                self.all_results[object_size] = {
                    operation: {
                        "IOPS": total_iops,
                        "std_dev": float(variance),
                        "Throughput": int(int(total_iops) * bs / 1024),
                    }
                }

            log.info(
                f"IO_Pattern: {self.results['io_pattern']} : "
                f"BlockSize: {object_size} ; Operation: {operation} ; "
                f"IOPS: {total_iops} ; Throughput: {int(total_iops) * bs / 1024} MiB/Sec ; "
                f"Variance - {variance}"
            )
        # Todo: Fail test if 5% deviation from benchmark value

    def codespeed_push(self, dev_mode=False):
        """
        Pushing the results into codespeed, for random test only!

        Args:
            dev_mode (bool): does the test run in development mode, if true, don't push the results.
        """

        # do not push results in case of development run
        if dev_mode:
            return

        # in case of io pattern is sequential - do nothing
        if self.results["io_pattern"] == "sequential":
            return

        # in case of random test - push the results
        reads = self.all_results["4KiB"]["randread"]["IOPS"]
        writes = self.all_results["4KiB"]["randwrite"]["IOPS"]
        r_bw = self.all_results["1024KiB"]["randread"]["IOPS"]
        w_bw = self.all_results["1024KiB"]["randwrite"]["IOPS"]

        # Pushing the results into codespeed
        log.info(
            f"Pushing to codespeed : Read={reads} ; Write={writes} ; "
            f"R-BW={r_bw} ; W-BW={w_bw}"
        )
        push_perf_dashboard(self.results["storageclass"], reads, writes, r_bw, w_bw)


@performance
class TestFIOBenchmark(PASTest):
    """
    Run FIO perf test using ripsaw benchmark

    """

    def setup(self):
        """
        Setting up test parameters
        """
        log.info("Starting the test setup")
        super(TestFIOBenchmark, self).setup()
        self.benchmark_name = "FIO"
        self.client_pod_name = "fio-client"

    def setting_storage_usage(self):
        """
        Getting the storage capacity, calculate the usage of the storage and
        setting the workload CR rile parameters.

        """

        # for development mode - use parameters for short test run
        if self.dev_mode:
            log.info("Setting up parameters for development mode")
            self.crd_data["spec"]["workload"]["args"]["filesize"] = "1GiB"
            self.crd_data["spec"]["workload"]["args"]["storagesize"] = "5Gi"
            self.crd_data["spec"]["workload"]["args"]["servers"] = 2
            self.crd_data["spec"]["workload"]["args"]["samples"] = 2
            self.crd_data["spec"]["workload"]["args"]["read_runtime"] = 30
            self.crd_data["spec"]["workload"]["args"]["write_runtime"] = 30
            self.crd_data["spec"]["workload"]["args"]["bs"] = ["64KiB"]
            self.total_data_set = 20
            self.filesize = 3
            return

        ceph_cluster = CephCluster()
        ceph_capacity = ceph_cluster.get_ceph_capacity()
        log.info(f"Total storage capacity is {ceph_capacity} GiB")
        self.total_data_set = int(ceph_capacity * 0.4)
        self.filesize = int(
            self.crd_data["spec"]["workload"]["args"]["filesize"].replace("GiB", "")
        )
        # To make sure the number of App pods will not be more then 50, in case
        # of large data set, changing the size of the file each pod will work on
        if self.total_data_set > 500:
            self.filesize = int(ceph_capacity * 0.008)
            self.crd_data["spec"]["workload"]["args"][
                "filesize"
            ] = f"{self.filesize}GiB"
            # make sure that the storage size is larger then the file size
            self.crd_data["spec"]["workload"]["args"][
                "storagesize"
            ] = f"{int(self.filesize * 1.2)}Gi"
        self.crd_data["spec"]["workload"]["args"]["servers"] = int(
            self.total_data_set / self.filesize
        )
        log.info(f"Total Data set to work on is : {self.total_data_set} GiB")

    def setting_io_pattern(self, io_pattern):
        """
        Setting the test jobs according to the io pattern - random / sequential

        Args:
            io_pattern (str): the I/O pattern to run (random / sequential)

        """
        if io_pattern == "sequential":
            self.crd_data["spec"]["workload"]["args"]["jobs"] = ["write", "read"]
            self.crd_data["spec"]["workload"]["args"]["iodepth"] = 1
        if io_pattern == "random":
            self.crd_data["spec"]["workload"]["args"]["jobs"] = [
                "randwrite",
                "randread",
            ]

    def init_full_results(self, full_results):
        """
        Initialize the full results object which will send to the ES server

        Args:
            full_results (obj): an empty FIOResultsAnalyse object

        Returns:
            FIOResultsAnalyse (obj): the input object fill with data

        """
        for key in self.environment:
            full_results.add_key(key, self.environment[key])

        # Setting the global parameters of the test
        full_results.add_key("dataset", f"{self.total_data_set}GiB")
        full_results.add_key(
            "file_size", self.crd_data["spec"]["workload"]["args"]["filesize"]
        )
        full_results.add_key(
            "servers", self.crd_data["spec"]["workload"]["args"]["servers"]
        )
        full_results.add_key(
            "samples", self.crd_data["spec"]["workload"]["args"]["samples"]
        )
        full_results.add_key(
            "operations", self.crd_data["spec"]["workload"]["args"]["jobs"]
        )
        full_results.add_key(
            "block_sizes", self.crd_data["spec"]["workload"]["args"]["bs"]
        )
        full_results.add_key(
            "io_depth", self.crd_data["spec"]["workload"]["args"]["iodepth"]
        )
        full_results.add_key(
            "jobs", self.crd_data["spec"]["workload"]["args"]["numjobs"]
        )
        full_results.add_key(
            "runtime",
            {
                "read": self.crd_data["spec"]["workload"]["args"]["read_runtime"],
                "write": self.crd_data["spec"]["workload"]["args"]["write_runtime"],
            },
        )
        full_results.add_key(
            "storageclass", self.crd_data["spec"]["workload"]["args"]["storageclass"]
        )
        full_results.add_key(
            "vol_size", self.crd_data["spec"]["workload"]["args"]["storagesize"]
        )
        return full_results

    def cleanup(self):
        """
        Do cleanup in the benchmark-operator namespace.
        delete the benchmark, an make sure no PVC's an no PV's are left.

        """
        log.info("Deleting FIO benchmark")
        self.benchmark_obj.delete()
        time.sleep(180)

        # Getting all PVCs created in the test (if left).
        NL = "\\n"  # NewLine character
        command = ["oc", "get", "pvc", "-n"]
        command.append(constants.RIPSAW_NAMESPACE)
        command.append("-o")
        command.append("template")
        command.append("--template")
        command.append("'{{range .items}}{{.metadata.name}}{{\"" + NL + "\"}}{{end}}'")
        pvcs_list = run_command(command, out_format="list")
        log.info(f"list of all PVCs :{pvcs_list}")
        for pvc in pvcs_list:
            pvc = pvc.replace("'", "")
            run_command(f"oc -n {constants.RIPSAW_NAMESPACE} delete pvc {pvc}")

        # Getting all PVs created in the test (if left).
        command[2] = "pv"
        command[8] = (
            "'{{range .items}}{{.metadata.name}} {{.spec.claimRef.namespace}}{{\""
            + NL
            + "\"}}{{end}}'"
        )
        command.remove("-n")
        command.remove(constants.RIPSAW_NAMESPACE)
        pvs_list = run_command(command, out_format="list")
        log.info(f"list of all PVs :{pvs_list}")

        for line in pvs_list:
            pv, ns = line.split(" ")
            pv = pv.replace("'", "")
            if ns == constants.RIPSAW_NAMESPACE:
                log.info(f"Going to delete {pv}")
                run_command(f"oc delete pv {pv}")

    def run(self):
        """
        Run the test, and wait until it finished

        """
        self.deploy_and_wait_for_wl_to_start(timeout=900)
        # Getting the UUID from inside the benchmark pod
        self.uuid = self.ripsaw.get_uuid(self.client_pod)
        # Setting back the original elastic-search information
        if self.backup_es:
            self.crd_data["spec"]["elasticsearch"] = self.backup_es
        if self.dev_mode:
            timeout = 600
            sleeptime = 30
        else:
            timeout = 18000
            sleeptime = 300

        self.wait_for_wl_to_finish(timeout=timeout, sleep=sleeptime)

        try:
            if "Fio failed to execute" not in self.test_logs:
                log.info("FIO has completed successfully")
        except IOError:
            log.warning("FIO failed to complete")

    @pytest.mark.parametrize(
        argnames=["interface", "io_pattern"],
        argvalues=[
            pytest.param(
                *[constants.CEPHBLOCKPOOL, "sequential"],
                marks=pytest.mark.polarion_id("OCS-844"),
            ),
            pytest.param(
                *[constants.CEPHFILESYSTEM, "sequential"],
                marks=pytest.mark.polarion_id("OCS-845"),
            ),
            pytest.param(
                *[constants.CEPHBLOCKPOOL, "random"],
                marks=pytest.mark.polarion_id("OCS-846"),
            ),
            pytest.param(
                *[constants.CEPHFILESYSTEM, "random"],
                marks=pytest.mark.polarion_id("OCS-847"),
            ),
        ],
    )
    def test_fio_workload_simple(self, ripsaw, es, interface, io_pattern):
        """
        This is a basic fio perf test - non-compressed volumes

        Args:
            interface (str): the interface that need to be tested - CephFS / RBD
            io_pattern (str): the I/O pattern to do - random / sequential

        """

        # Getting the full path for the test logs
        self.full_log_path = get_full_test_logs_path(cname=self)
        self.full_log_path += f"-{interface}-{io_pattern}"
        log.info(f"Logs file path name is : {self.full_log_path}")

        # deploy the benchmark-operator (ripsaw)
        self.ripsaw = ripsaw
        self.ripsaw_deploy(self.ripsaw)

        log.info("Create resource file for fio workload")
        self.crd_data = templating.load_yaml(constants.FIO_CR_YAML)

        # Saving the Original elastic-search IP and PORT - if defined in yaml
        self.es_info_backup(es)

        self.set_storageclass(interface=interface)

        # Setting the data set to 40% of the total storage capacity
        self.setting_storage_usage()

        self.get_env_info()

        self.setting_io_pattern(io_pattern)

        self.run()

        # Initialize the results doc file.
        full_results = self.init_full_results(
            FIOResultsAnalyse(
                self.uuid, self.crd_data, self.full_log_path, self.main_es
            )
        )

        # Setting the global parameters of the test
        full_results.add_key("io_pattern", io_pattern)

        # Clean up fio benchmark
        self.cleanup()

        log.debug(f"Full results is : {full_results.results}")
        self.copy_es_data(es)

        full_results.analyze_results()  # Analyze the results

        # Writing the analyzed test results to the Elastic-Search server
        if self.main_es is not None:
            full_results.es_write()
            full_results.codespeed_push(
                dev_mode=self.dev_mode
            )  # Push results to codespeed
            # Creating full link to the results on the ES server
            log.info(f"The Result can be found at : {full_results.results_link()}")

    @skipif_ocs_version("<4.6")
    @pytest.mark.parametrize(
        argnames=["io_pattern", "bs", "cmp_ratio"],
        argvalues=[
            pytest.param(*["random", "1024KiB", 60]),
            pytest.param(*["random", "64KiB", 60]),
            pytest.param(*["random", "16KiB", 60]),
            pytest.param(*["sequential", "1024KiB", 60]),
            pytest.param(*["sequential", "64KiB", 60]),
            pytest.param(*["sequential", "16KiB", 60]),
        ],
    )
    def test_fio_compressed_workload(
        self, ripsaw, es, storageclass_factory, io_pattern, bs, cmp_ratio
    ):
        """
        This is a basic fio perf test which run on compression enabled volume

        Args:
            io_pattern (str): the I/O pattern to do - random / sequential
            bs (str): block size to use in the test
            cmp_ratio (int): the expected compression ratio

        """

        # Getting the full path for the test logs
        self.full_log_path = get_full_test_logs_path(cname=self)
        self.full_log_path += f"-{io_pattern}-{bs}-{cmp_ratio}"
        log.info(f"Logs file path name is : {self.full_log_path}")

        # deploy the benchmark-operator (ripsaw)
        self.ripsaw = ripsaw
        self.ripsaw_deploy(self.ripsaw)

        log.info("Create resource file for fio workload")
        self.crd_data = templating.load_yaml(
            "ocs_ci/templates/workloads/fio/benchmark_fio_cmp.yaml"
        )

        # Saving the Original elastic-search IP and PORT - if defined in yaml
        self.es_info_backup(es)

        log.info("Creating compressed pool & SC")
        sc_obj = storageclass_factory(
            interface=constants.CEPHBLOCKPOOL,
            new_rbd_pool=True,
            replica=3,
            compression="aggressive",
        )

        sc = sc_obj.name
        pool_name = run_cmd(f"oc get sc {sc} -o jsonpath={{'.parameters.pool'}}")
        # Create fio benchmark
        self.crd_data["spec"]["workload"]["args"]["bs"] = [bs]
        self.crd_data["spec"]["workload"]["args"]["prefill_bs"] = bs
        self.crd_data["spec"]["workload"]["args"]["cmp_ratio"] = cmp_ratio

        # Setting the data set to 40% of the total storage capacity
        self.setting_storage_usage()

        self.get_env_info()

        self.crd_data["spec"]["workload"]["args"]["storageclass"] = sc
        self.setting_io_pattern(io_pattern)
        self.run()

        # Initialize the results doc file.
        full_results = self.init_full_results(
            FIOResultsAnalyse(
                self.uuid, self.crd_data, self.full_log_path, self.main_es
            )
        )

        # Setting the global parameters of the test
        full_results.add_key("io_pattern", io_pattern)

        # Clean up fio benchmark
        self.copy_es_data(es)

        log.info("verifying compression ratio")
        ratio = calculate_compression_ratio(pool_name)

        full_results.add_key("cmp_ratio", {"expected": cmp_ratio, "actual": ratio})
        log.debug(f"Full results is : {full_results.results}")
        full_results.analyze_results()  # Analyze the results
        if (cmp_ratio + 5) < ratio or ratio < (cmp_ratio - 5):
            log.warning(
                f"The compression ratio is {ratio}% "
                f"while the expected ratio is {cmp_ratio}%"
            )
        else:
            log.info(f"The compression ratio is {ratio}%")

        # Writing the analyzed test results to the Elastic-Search server
        if self.main_es is not None:
            full_results.es_write()
            # Creating full link to the results on the ES server
            log.info(f"The Result can be found at : {full_results.results_link()}")

        self.cleanup()
        sc_obj.delete()
        sc_obj.ocp.wait_for_delete(resource_name=sc, timeout=300, sleep=5)
