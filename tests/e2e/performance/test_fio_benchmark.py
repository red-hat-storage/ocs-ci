"""
Module to perform FIO benchmark
"""
import logging
import pytest
import time

from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility import templating
from ocs_ci.utility.utils import run_cmd, TimeoutSampler
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.ocs.ripsaw import RipSaw
from ocs_ci.ocs import constants, node
from ocs_ci.utility.performance_dashboard import push_perf_dashboard
from ocs_ci.framework.testlib import E2ETest, performance, skipif_ocs_version
from ocs_ci.ocs.perfresult import PerfResult
from ocs_ci.ocs.elasticsearch import ElasticSearch
from ocs_ci.ocs.cluster import CephCluster, get_ceph_df_detail
from ocs_ci.ocs.version import get_environment_info

log = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def es(request):

    # Create internal ES only if Cloud platform is tested
    if node.get_provider().lower() in constants.CLOUD_PLATFORMS:
        es = ElasticSearch()
    else:
        es = None

    def teardown():
        if es is not None:
            es.cleanup()
            time.sleep(10)

    request.addfinalizer(teardown)
    return es


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

    def __init__(self, uuid, crd):
        """
        Initialize the object by reading some of the data from the CRD file and
        by connecting to the ES server and read all results from it.

        Args:
            uuid (str): the unique uid of the test
            crd (dict): dictionary with test parameters - the test yaml file
                        that modify it in the test itself.

        """

        super(FIOResultsAnalyse, self).__init__(uuid, crd)
        self.index = "ripsaw-fio-analyzed-result"
        self.new_index = "ripsaw-fio-fullres"
        # make sure we have connection to the elastic search server
        self.es_connect()

    def analyze_results(self):
        """
        Analyzing the results of the test and creating one record with all test
        information

        """

        for result in self.es_read():
            test_data = result["_source"]["ceph_benchmark_test"]["test_data"]
            object_size = test_data["object_size"]
            operation = test_data["operation"]
            total_iops = "{:.2f}".format(test_data["total-iops"])
            std_dev = "std-dev-" + object_size
            variance = "{:.2f}".format(test_data[std_dev])
            if object_size in self.all_results.keys():
                self.all_results[object_size][operation] = {
                    "IOPS": total_iops,
                    "std_dev": variance,
                }
            else:
                self.all_results[object_size] = {
                    operation: {"IOPS": total_iops, "std_dev": variance}
                }

            log.info(
                f"\nio_pattern: {self.results['io_pattern']} : "
                f"block_size: {object_size} ; operation: {operation} ; "
                f"total_iops: {total_iops} ; variance - {variance}\n"
            )
        # Todo: Fail test if 5% deviation from benchmark value

    def codespeed_push(self):
        """
        Pushing the results into codespeed, for random test only!

        """

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

    def es_info_backup(self, es):
        """
        Saving the Original elastic-search IP and PORT - if defined in yaml

        Args:
            es (obj): elasticsearch object

        """

        if "elasticsearch" in self.fio_cr["spec"]:
            self.backup_es = self.fio_cr["spec"]["elasticsearch"]
        else:
            log.warning("Elastic Search information does not exists in YAML file")
            self.fio_cr["spec"]["elasticsearch"] = {}

        # Use the internal define elastic-search server in the test - if exist
        if es:
            self.fio_cr["spec"]["elasticsearch"] = {
                "server": es.get_ip(),
                "port": es.get_port(),
            }

    def setting_storage_usage(self):
        """
        Getting the storage capacity, calculate the usage of the storage and
        setting the workload CR rile parameters.

        """
        ceph_cluster = CephCluster()
        ceph_capacity = ceph_cluster.get_ceph_capacity()
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
        if io_pattern == "sequential":
            self.fio_cr["spec"]["workload"]["args"]["jobs"] = ["write", "read"]
            self.fio_cr["spec"]["workload"]["args"]["iodepth"] = 1

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
            300, 20, get_pod_name_by_pattern, "fio-client", constants.RIPSAW_NAMESPACE
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

        Raises:
            IOError: in case of the FIO failed to finish
        Returns:
            str: the end time of the workload

        """
        log.info("Waiting for fio_client to complete")
        pod_obj = OCP(kind="pod")
        pod_obj.wait_for_resource(
            condition="Completed",
            resource_name=fio_client_pod,
            timeout=18000,
            sleep=300,
        )

        # Getting the end time of the test
        end_time = time.strftime("%Y-%m-%dT%H:%M:%SGMT", time.gmtime())

        output = run_cmd(f"oc logs {fio_client_pod}")
        log.info(f"The Test log is : {output}")

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

    def copy_es_data(self, es, full_results):
        """
        Copy data from Internal ES (if exists) to the main ES

        Args:
            es (obj): elasticsearch object (if exits)
            full_results (obj): the full results object

        """
        if es:
            log.info("Copy all data from Internal ES to Main ES")
            es._copy(full_results.es)
        # Adding this sleep between the copy and the analyzing of the results
        # since sometimes the results of the read (just after write) are empty
        time.sleep(10)

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

        ripsaw = RipSaw()
        self.ripsaw_deploy(ripsaw)

        if interface == "CephBlockPool":
            sc = constants.CEPHBLOCKPOOL_SC
        else:
            sc = constants.CEPHFILESYSTEM_SC

        # Create fio benchmark
        log.info("Create resource file for fio workload")
        self.fio_cr = templating.load_yaml(constants.fio_cr_YAML)

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
        full_results = self.init_full_results(FIOResultsAnalyse(uuid, self.fio_cr))

        # Setting the global parameters of the test
        full_results.add_key("io_pattern", io_pattern)

        end_time = self.wait_for_wl_to_finish(fio_client_pod)
        full_results.add_key("test_time", {"start": self.start_time, "end": end_time})

        # Clean up fio benchmark
        log.info("Deleting FIO benchmark")
        self.fio_cr_obj.delete()

        ripsaw.cleanup()

        log.debug(f"Full results is : {full_results.results}")

        self.copy_es_data(es, full_results)
        full_results.analyze_results()  # Analyze the results
        # Writing the analyzed test results to the Elastic-Search server
        full_results.es_write()
        full_results.codespeed_push()  # Push results to codespeed
        # Creating full link to the results on the ES server
        log.info(f"The Result can be found at ; {full_results.results_link()}")

    @skipif_ocs_version("<4.6")
    @pytest.mark.parametrize(
        argnames=["io_pattern"],
        argvalues=[
            pytest.param(
                *["random"],
                marks=pytest.mark.polarion_id("OCS-846"),
            ),
        ],
    )
    def test_fio_compressed_workload(self, es, storageclass_factory, io_pattern):
        """
        This is a basic fio perf test which run on compression enabled volume

        """

        ripsaw = RipSaw()
        self.ripsaw_deploy(ripsaw)

        log.info("Creating compressed pool & SC")
        sc_obj = storageclass_factory(
            interface=constants.CEPHBLOCKPOOL,
            new_rbd_pool=True,
            replica=3,
            compression="aggressive",
        )

        sc = sc_obj.name
        # Create fio benchmark
        log.info("Create resource file for fio workload")
        self.fio_cr = templating.load_yaml(
            "ocs_ci/templates/workloads/fio/benchmark_fio_cmp.yaml"
        )

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
        full_results = self.init_full_results(FIOResultsAnalyse(uuid, self.fio_cr))

        # Setting the global parameters of the test
        full_results.add_key("io_pattern", io_pattern)

        end_time = self.wait_for_wl_to_finish(fio_client_pod)
        full_results.add_key("test_time", {"start": self.start_time, "end": end_time})

        log.info("verifying compression ratio")
        expected_ratio = 50  # according to the yaml file parameter
        results = get_ceph_df_detail()
        for pool in results["pools"]:
            if "cbp-test-" in pool["name"]:
                stor = pool["stats"]["compress_under_bytes"]
                saved = pool["stats"]["compress_bytes_used"]
                ratio = int(100 / (stor / saved))
                if (expected_ratio + 5) > ratio > (expected_ratio - 5):
                    log.error(
                        f"The compression ratio is {ratio}% "
                        f"while the expected ratio is {expected_ratio}%"
                    )
                else:
                    log.info(f"The compression ratio is {ratio}%")
                break

        # Clean up fio benchmark
        log.info("Deleting FIO benchmark")
        self.fio_cr_obj.delete()

        ripsaw.cleanup()

        sc_obj.delete()
        sc_obj.ocp.wait_for_delete(resource_name=sc, timeout=300, sleep=5)
        log.debug(f"Full results is : {full_results.results}")

        self.copy_es_data(es, full_results)
        full_results.analyze_results()  # Analyze the results
        # Writing the analyzed test results to the Elastic-Search server
        full_results.es_write()
        full_results.codespeed_push()  # Push results to codespeed
        # Creating full link to the results on the ES server
        log.info(f"The Result can be found at ; {full_results.results_link()}")
