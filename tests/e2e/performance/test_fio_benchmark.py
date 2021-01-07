"""
Module to perform FIO benchmark
"""
import logging
import pytest
import time
import json

from elasticsearch import Elasticsearch

from ocs_ci.ocs import defaults
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility import templating
from ocs_ci.utility.utils import run_cmd, TimeoutSampler
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.ocs.ripsaw import RipSaw
from ocs_ci.ocs import constants
from ocs_ci.utility.performance_dashboard import push_perf_dashboard
from ocs_ci.framework import config
from ocs_ci.framework.testlib import E2ETest, performance, skipif_ocs_version
from ocs_ci.ocs.perfresult import PerfResult
from ocs_ci.helpers.helpers import get_full_test_logs_path
from ocs_ci.ocs.elasticsearch import elasticsearch_load

from ocs_ci.ocs.cluster import CephCluster, calculate_compression_ratio
from ocs_ci.ocs.version import get_environment_info
from ocs_ci.helpers.performance_lib import run_command

log = logging.getLogger(__name__)
dev_mode = config.RUN["cli_params"].get("dev_mode")


@pytest.fixture(scope="function")
def ripsaw(request):

    # Create benchmark Operator (formerly ripsaw)
    ripsaw = RipSaw()

    def teardown():
        ripsaw.cleanup()
        time.sleep(10)

    request.addfinalizer(teardown)
    return ripsaw


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

    def codespeed_push(self):
        """
        Pushing the results into codespeed, for random test only!

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
class TestFIOBenchmark(E2ETest):
    """
    Run FIO perf test using ripsaw benchmark

    """

    def ripsaw_deploy(self, ripsaw):
        """
        Deploy the benchmark operator (formally ripsaw) CRD

        Args:
            ripsaw (obj): benchmark operator object

        """
        log.info("Deploying benchmark operator (ripsaw)")
        ripsaw.apply_crd("resources/crds/" "ripsaw_v1alpha1_ripsaw_crd.yaml")

    def es_info_backup(self, elasticsearch):
        """
        Saving the Original elastic-search IP and PORT - if defined in yaml

        Args:
            elasticsearch (obj): elasticsearch object

        """

        # for development mode use the Dev ES server
        if dev_mode:
            if "elasticsearch" in self.fio_cr["spec"]:
                self.fio_cr["spec"]["elasticsearch"] = {
                    "server": defaults.ELASTICSEARCH_DEV_IP,
                    "port": defaults.ELASTICSEARCE_PORT,
                    "url": f"http://{defaults.ELASTICSEARCH_DEV_IP}:{defaults.ELASTICSEARCE_PORT}",
                }

        if "elasticsearch" in self.fio_cr["spec"]:
            self.fio_cr["spec"]["elasticsearch"]["url"] = (
                f"http://{self.fio_cr['spec']['elasticsearch']['server']}:"
                f"{self.fio_cr['spec']['elasticsearch']['port']}"
            )
            self.backup_es = self.fio_cr["spec"]["elasticsearch"]
            log.info(
                f"Creating object for the Main ES server on {self.backup_es['url']}"
            )
            self.main_es = Elasticsearch([self.backup_es["url"]], verify_certs=True)
            if not self.main_es.ping():
                log.warning("Can not connect to Main elasticsearch server")
                self.main_es = None

        else:
            log.warning("Elastic Search information does not exists in YAML file")
            self.fio_cr["spec"]["elasticsearch"] = {}

        # Use the internal define elastic-search server in the test - if exist
        if elasticsearch:
            self.fio_cr["spec"]["elasticsearch"] = {
                "server": elasticsearch.get_ip(),
                "port": elasticsearch.get_port(),
                "url": f"http://{elasticsearch.get_ip()}:{elasticsearch.get_port()}",
            }

    def setting_storage_usage(self):
        """
        Getting the storage capacity, calculate the usage of the storage and
        setting the workload CR rile parameters.

        """

        ceph_cluster = CephCluster()
        ceph_capacity = ceph_cluster.get_ceph_capacity()
        log.info(f"Total storage capacity is {ceph_capacity} GiB")
        self.total_data_set = int(ceph_capacity * 0.4)
        self.filesize = int(
            self.fio_cr["spec"]["workload"]["args"]["filesize"].replace("GiB", "")
        )
        # To make sure the number of App pods will not be more then 50, in case
        # of large data set, changing the size of the file each pod will work on
        if self.total_data_set > 500:
            self.filesize = int(ceph_capacity * 0.008)
            self.fio_cr["spec"]["workload"]["args"]["filesize"] = f"{self.filesize}GiB"
            # make sure that the storage size is larger then the file size
            self.fio_cr["spec"]["workload"]["args"][
                "storagesize"
            ] = f"{int(self.filesize * 1.2)}Gi"
        self.fio_cr["spec"]["workload"]["args"]["servers"] = int(
            self.total_data_set / self.filesize
        )
        log.info(f"Total Data set to work on is : {self.total_data_set} GiB")

    def get_env_info(self):
        """
        Getting the environment information and update the workload RC if
        necessary.

        """
        self.environment = get_environment_info()
        if not self.environment["user"] == "":
            self.fio_cr["spec"]["test_user"] = self.environment["user"]
        self.fio_cr["spec"]["clustername"] = self.environment["clustername"]

        log.debug(f"Environment information is : {self.environment}")

    def setting_io_pattern(self, io_pattern):
        """
        Setting the test jobs according to the io pattern - random / sequential

        Args:
            io_pattern (str): the I/O pattern to run (random / sequential)

        """
        if io_pattern == "sequential":
            self.fio_cr["spec"]["workload"]["args"]["jobs"] = ["write", "read"]
            self.fio_cr["spec"]["workload"]["args"]["iodepth"] = 1
        if io_pattern == "random":
            self.fio_cr["spec"]["workload"]["args"]["jobs"] = ["randwrite", "randread"]

    def deploy_and_wait_for_wl_to_start(self):
        """
        Deploy the workload and wait until it start working

        Returns:
            obj : the FIO client pod object

        """
        log.info(f"The FIO CR file is {self.fio_cr}")
        self.fio_cr_obj = OCS(**self.fio_cr)
        self.fio_cr_obj.create()

        # Wait for fio client pod to be created
        for fio_pod in TimeoutSampler(
            900, 20, get_pod_name_by_pattern, "fio-client", constants.RIPSAW_NAMESPACE
        ):
            try:
                if fio_pod[0] is not None:
                    fio_client_pod = fio_pod[0]
                    break
            except IndexError:
                log.info("Bench pod not ready yet")

        # Getting the start time of the test
        self.start_time = time.strftime("%Y-%m-%dT%H:%M:%SGMT", time.gmtime())
        return fio_client_pod

    def wait_for_wl_to_finish(self, fio_client_pod):
        """
        Waiting until the workload is finished

        Args:
            fio_client_pod (obj): the FIO client pod object

        Returns:
            str: the end time of the workload

        """
        if dev_mode:
            timeout = 3600
            sleeptime = 30
        else:
            timeout = 18000
            sleeptime = 300

        log.info("Waiting for fio_client to complete")
        pod_obj = OCP(kind="pod")
        pod_obj.wait_for_resource(
            condition="Completed",
            resource_name=fio_client_pod,
            timeout=timeout,
            sleep=sleeptime,
        )

        # Getting the end time of the test
        end_time = time.strftime("%Y-%m-%dT%H:%M:%SGMT", time.gmtime())

        output = run_cmd(f"oc logs {fio_client_pod}")
        log_file_name = f"{self.full_log_path}/test-pod.log"
        with open(log_file_name, "w") as f:
            f.write(output)
        log.info(f"The Test log is can be found at : {log_file_name}")

        try:
            if "Fio failed to execute" not in output:
                log.info("FIO has completed successfully")
        except IOError:
            log.info("FIO failed to complete")

        return end_time

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
            "file_size", self.fio_cr["spec"]["workload"]["args"]["filesize"]
        )
        full_results.add_key(
            "servers", self.fio_cr["spec"]["workload"]["args"]["servers"]
        )
        full_results.add_key(
            "samples", self.fio_cr["spec"]["workload"]["args"]["samples"]
        )
        full_results.add_key(
            "operations", self.fio_cr["spec"]["workload"]["args"]["jobs"]
        )
        full_results.add_key(
            "block_sizes", self.fio_cr["spec"]["workload"]["args"]["bs"]
        )
        full_results.add_key(
            "io_depth", self.fio_cr["spec"]["workload"]["args"]["iodepth"]
        )
        full_results.add_key("jobs", self.fio_cr["spec"]["workload"]["args"]["numjobs"])
        full_results.add_key(
            "runtime",
            {
                "read": self.fio_cr["spec"]["workload"]["args"]["read_runtime"],
                "write": self.fio_cr["spec"]["workload"]["args"]["write_runtime"],
            },
        )
        full_results.add_key(
            "storageclass", self.fio_cr["spec"]["workload"]["args"]["storageclass"]
        )
        full_results.add_key(
            "vol_size", self.fio_cr["spec"]["workload"]["args"]["storagesize"]
        )
        return full_results

    def copy_es_data(self, elasticsearch):
        """
        Copy data from Internal ES (if exists) to the main ES

        Args:
            elasticsearch (obj): elasticsearch object (if exits)

        Returns:
            bool: True if data was copy to the main ES False otherwise

        """
        if elasticsearch:
            log.info("Copy all data from Internal ES to Main ES")
            log.info("Dumping data from the Internal ES to tar ball file")
            elasticsearch.dumping_all_data(self.full_log_path)
            es_connection = self.backup_es
            es_connection["host"] = es_connection.pop("server")
            es_connection.pop("url")
            if elasticsearch_load(self.main_es, self.full_log_path):
                # Adding this sleep between the copy and the analyzing of the results
                # since sometimes the results of the read (just after write) are empty
                time.sleep(10)
                return True
            else:
                log.warning("Can not upload data into the Main ES server")
                return False

    def cleanup(self):
        log.info("Deleting FIO benchmark")
        self.fio_cr_obj.delete()
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

        """

        self.full_log_path = get_full_test_logs_path(cname=self)
        self.full_log_path += f"-{interface}-{io_pattern}"
        log.info(f"Logs file path name is : {self.full_log_path}")

        self.ripsaw_deploy(ripsaw)

        if interface == "CephBlockPool":
            sc = constants.CEPHBLOCKPOOL_SC
        else:
            sc = constants.CEPHFILESYSTEM_SC

        # Create fio benchmark
        log.info("Create resource file for fio workload")
        self.fio_cr = templating.load_yaml(constants.FIO_CR_YAML)

        # Saving the Original elastic-search IP and PORT - if defined in yaml
        self.es_info_backup(es)

        # Setting the data set to 40% of the total storage capacity
        self.setting_storage_usage()

        self.get_env_info()

        self.fio_cr["spec"]["workload"]["args"]["storageclass"] = sc
        self.setting_io_pattern(io_pattern)
        fio_client_pod = self.deploy_and_wait_for_wl_to_start()

        # Getting the UUID from inside the benchmark pod
        uuid = ripsaw.get_uuid(fio_client_pod)
        # Setting back the original elastic-search information
        self.fio_cr["spec"]["elasticsearch"] = self.backup_es

        # Initialize the results doc file.
        full_results = self.init_full_results(
            FIOResultsAnalyse(uuid, self.fio_cr, self.full_log_path, self.main_es)
        )

        # Setting the global parameters of the test
        full_results.add_key("io_pattern", io_pattern)

        end_time = self.wait_for_wl_to_finish(fio_client_pod)
        full_results.add_key("test_time", {"start": self.start_time, "end": end_time})

        # Clean up fio benchmark
        self.cleanup()

        log.debug(f"Full results is : {full_results.results}")
        self.copy_es_data(es)

        full_results.analyze_results()  # Analyze the results

        # Writing the analyzed test results to the Elastic-Search server
        if self.main_es is not None:
            full_results.es_write()
            full_results.codespeed_push()  # Push results to codespeed
            # Creating full link to the results on the ES server
            log.info(f"The Result can be found at ; {full_results.results_link()}")

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

        self.full_log_path = get_full_test_logs_path(cname=self)
        self.full_log_path += f"-{io_pattern}-{bs}-{cmp_ratio}"
        log.info(f"Logs file path name is : {self.full_log_path}")

        self.ripsaw_deploy(ripsaw)

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
        log.info("Create resource file for fio workload")
        self.fio_cr = templating.load_yaml(
            "ocs_ci/templates/workloads/fio/benchmark_fio_cmp.yaml"
        )
        self.fio_cr["spec"]["workload"]["args"]["bs"] = [bs]
        self.fio_cr["spec"]["workload"]["args"]["prefill_bs"] = bs
        self.fio_cr["spec"]["workload"]["args"]["cmp_ratio"] = cmp_ratio

        # Saving the Original elastic-search IP and PORT - if defined in yaml
        self.es_info_backup(es)

        # Setting the data set to 40% of the total storage capacity
        self.setting_storage_usage()

        self.get_env_info()

        self.fio_cr["spec"]["workload"]["args"]["storageclass"] = sc
        self.setting_io_pattern(io_pattern)
        fio_client_pod = self.deploy_and_wait_for_wl_to_start()

        # Getting the UUID from inside the benchmark pod
        uuid = ripsaw.get_uuid(fio_client_pod)
        # Setting back the original elastic-search information
        self.fio_cr["spec"]["elasticsearch"] = self.backup_es

        # Initialize the results doc file.
        full_results = self.init_full_results(
            FIOResultsAnalyse(uuid, self.fio_cr, self.full_log_path, self.main_es)
        )

        # Setting the global parameters of the test
        full_results.add_key("io_pattern", io_pattern)

        end_time = self.wait_for_wl_to_finish(fio_client_pod)
        full_results.add_key("test_time", {"start": self.start_time, "end": end_time})

        # Clean up fio benchmark
        self.copy_es_data(es)

        log.info("verifying compression ratio")
        ratio = calculate_compression_ratio(pool_name)

        full_results.add_key("cmp_ratio", {"expected": cmp_ratio, "actual": ratio})
        full_results.analyze_results()  # Analyze the results
        # TODO: change the info message to Warning/Error after
        #  prefill at ripsaw will be fixed Ripsaw PR - #505
        if (cmp_ratio + 5) < ratio or ratio < (cmp_ratio - 5):
            log.info(
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
        log.debug(f"Full results is : {full_results.results}")
