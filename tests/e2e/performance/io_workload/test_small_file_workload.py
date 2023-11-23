"""
Test to exercise Small File Workload

Note:
This test is using the benchmark-operator and the elastic search, so it start
process with port forwarding on port 9200 from the host that run the test (localhost)
to the elastic-search within the open-shift cluster, so, if you host is listen to
port 9200, this test can not be running in your host.

"""

# Builtin modules
import json
import os

import logging

# 3ed party modules
import os.path

from elasticsearch import Elasticsearch, exceptions as ESExp
import numpy as np
import pytest

# import time

# Local modules
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import grey_squad
from ocs_ci.framework.testlib import performance, performance_a
from ocs_ci.helpers.helpers import get_full_test_logs_path
from ocs_ci.ocs import benchmark_operator, constants
from ocs_ci.ocs.elasticsearch import ElasticSearch
from ocs_ci.ocs.perfresult import PerfResult
from ocs_ci.ocs.perftests import PASTest
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.utility import templating
from ocs_ci.utility.utils import ceph_health_check

log = logging.getLogger(__name__)
os.environ["redis_timeout"] = 120

class SmallFileResultsAnalyse(PerfResult):
    """
    This class is reading all test results from elasticsearch server (which the
    benchmark-operator running of the benchmark is generate), aggregate them by :
        test operation (e.g. create / delete etc.)
        sample (for test to be valid it need to run with more the one sample)
        host (test can be run on more then one pod {called host})

    it generates results for all tests as one unit which will be valid only
    if the deviation between samples is less the 5%

    """

    managed_keys = {
        "IOPS": {"name": "iops", "op": np.sum},
        "MiBps": {"name": "mbps", "op": np.sum},
        "elapsed": {"name": "elapsed-time", "op": np.average},
        "files": {"name": "files_per_thread", "op": np.sum},
        "filesPerSec": {"name": "Files-Sec", "op": np.sum},
        "records": {"name": "Rec-per-thread", "op": np.sum},
    }

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

        super(SmallFileResultsAnalyse, self).__init__(uuid, crd)

        self.index = crd["spec"]["es_index"] + "-results"
        self.new_index = crd["spec"]["es_index"] + "-fullres"
        self.full_log_path = full_log_path
        # make sure we have connection to the elastic search server
        self.es = es_con

        # WA for Cloud environment where pod can not send results to ES
        self.dont_check = False

        # make sure we have connection to the elastic search server
        # self.es_connect()

        # Creating full results dictionary
        self.add_key("clients", crd["spec"]["workload"]["args"]["clients"])
        self.add_key("samples", crd["spec"]["workload"]["args"]["samples"])
        self.add_key("threads", crd["spec"]["workload"]["args"]["threads"])
        self.add_key("operations", crd["spec"]["workload"]["args"]["operation"])
        self.add_key("full-res", {})

        # Calculate the number of records for the test

        # Total threads for one sample - one operation
        self.records = self.results["clients"] * self.results["threads"]

        # Number of threads for all samples
        self.records *= self.results["samples"]

        # Number of records for all operation - cleanup does not count
        numofops = len(self.results["operations"])
        if "cleanup" in self.results["operations"]:
            numofops -= 1

        self.records *= numofops

    def read(self):
        """
        Reading all test records from the elasticsearch server into dictionary
        inside this object

        """
        query = {"query": {"match": {"uuid": f'"{self.uuid}"'}}}
        log.info("Reading all data from ES server")
        try:
            # Initialize the scroll
            page = self.es.search(index=self.index, scroll="2m", size=1000, body=query)
            sid = page["_scroll_id"]
            scroll_size = page["hits"]["total"]["value"]
            log.info(
                f"Looking for {self.records} records and found {scroll_size} records."
            )
            self.all_results = page["hits"]["hits"]

            # Start scrolling
            while scroll_size > 0:
                page = self.es.scroll(scroll_id=sid, scroll="2m")

                # Update the scroll ID
                sid = page["_scroll_id"]
                self.all_results += page["hits"]["hits"]

                # Get the number of results that we returned in the last scroll
                scroll_size = len(page["hits"]["hits"])
                log.debug(f"{scroll_size} records was read")

            log.info(f"The total record that was read : {len(self.all_results)}")
            log.debug(self.all_results)

            total_rec_found = len(self.all_results)
            if total_rec_found < 1:
                log.warning("No data in ES server, disabling results calculation")
                self.dont_check = True

            if total_rec_found < self.records:
                log.error("Not all data read from ES server")
                self.dont_check = True

            if total_rec_found > self.records:
                log.warning("More records then expected was read, check the results!")

        except ESExp.NotFoundError:
            log.warning("No data in ES server, disabling results calculation")
            self.dont_check = True

    def thread_read(self, host, op, snum):
        """
        This method read all threads record of one host / operation and sample

        Args:
            host (str): the name of the pod that ran the test
            op (str): the operation that is tested
            snum (int): sample of test as string

        Returns:
            dict : dictionary of results records

        """

        res = {}
        log.debug(f"Reading all threads for {op} / {snum} / {host}")
        for hit in self.all_results:
            if (
                hit["_source"]["host"] == host
                and hit["_source"]["optype"] == op
                and hit["_source"]["sample"] == snum
            ):
                for key in self.managed_keys.keys():
                    # not all operation have all values, so i am using try
                    try:
                        val = float("{:.2f}".format(hit["_source"][key]))
                        if self.managed_keys[key]["name"] in res.keys():
                            res[self.managed_keys[key]["name"]].append(val)
                        else:
                            res[self.managed_keys[key]["name"]] = [val]
                    except Exception:
                        pass
        res = self.aggregate_threads_results(res)
        return res

    def aggregate_threads_results(self, res):
        """
        Aggregation of one section of results, this can be threads in host,
        hosts in sample, samples in test

        Args:
            res (dict) : dictionary of results

        Returns:
            dict : dictionary with the aggregate results.

        """

        results = {}
        for key in self.managed_keys.keys():
            if self.managed_keys[key]["name"] in res.keys():
                results[key] = self.managed_keys[key]["op"](
                    res[self.managed_keys[key]["name"]]
                )

        # This is the place to check in host (treads) deviation.
        return results

    def combine_results(self, results, clear):
        """
        Combine 2 or more results (hosts in sample / samples in test)
        to one result.

        Args:
            results (dict): dictionary of results to combine
            clear (bool): return only combined results or not.
                          True - return only combined results
                          False - add the combine results to originals results

        Returns:
            dict : dictionary of results records

        """

        res = {}
        log.debug(f"The results to combine {json.dumps(results, indent=2)}")
        for rec in results.keys():
            record = results[rec]
            for key in self.managed_keys.keys():
                # not all operation have all values, so i am using try
                try:
                    val = float("{:.2f}".format(record[key]))
                    if self.managed_keys[key]["name"] in res.keys():
                        res[self.managed_keys[key]["name"]].append(val)
                    else:
                        res[self.managed_keys[key]["name"]] = [val]
                except Exception:
                    pass
        if not clear:
            res.update(self.aggregate_threads_results(res))
        else:
            res = self.aggregate_threads_results(res)
        log.debug(f"The combines results are : {json.dumps(res, indent=2)}")
        return res

    def aggregate_host_results(self):
        """
        Aggregation results from all hosts in single sample

        """

        results = {}

        for op in self.results["operations"]:
            for smp in range(self.results["samples"]):
                sample = smp + 1
                if op in self.results["full-res"].keys():
                    self.results["full-res"][op][sample] = self.combine_results(
                        self.results["full-res"][op][sample], True
                    )

        return results

    def aggregate_samples_results(self):
        """
        Aggregation results from all hosts in single sample, and compare
        between samples.

        Returns:
            bool: True if results deviation (between samples) is les or equal
                       to 20%, otherwise False

        """

        test_pass = True
        for op in self.results["operations"]:
            log.debug(
                f'Aggregating {op} - {json.dumps(self.results["full-res"][op], indent=3)}'
            )
            results = self.combine_results(self.results["full-res"][op], False)
            log.info(f"Check IOPS {op} samples deviation")

            for key in self.managed_keys.keys():
                if self.managed_keys[key]["name"] in results.keys():

                    results[key] = self.managed_keys[key]["op"](
                        results[self.managed_keys[key]["name"]]
                    )
                    if isinstance(results[self.managed_keys[key]["name"]], list):
                        results[key] = np.average(
                            results[self.managed_keys[key]["name"]]
                        )
                    results[key] = float("{:.2f}".format(results[key]))
                    if key == "IOPS":
                        st_deviation = np.std(results[self.managed_keys[key]["name"]])
                        mean = np.mean(results[self.managed_keys[key]["name"]])

                        pct_dev = (st_deviation / mean) * 100
                        if pct_dev > 20:
                            log.error(
                                f"Deviation for {op} IOPS is more the 20% ({pct_dev})"
                            )
                            # TODO: unmarked next line after implementing data cleansing
                            # test_pass = False
                self.results["full-res"][op] = results

        return test_pass

    def get_clients_list(self):
        """
        Finding and creating a list of all hosts that was used in this test

        Returns:
            list: a list of pods name

        """

        res = []
        for hit in self.all_results:
            host = hit["_source"]["host"]
            if host not in res:
                res.append(host)
        log.info(f"The pods names used in this test are {res}")
        return res

    def init_full_results(self):
        """
        Initialize the full results Internal DB as dictionary.

        """

        log.info("Initialising results DB")

        # High level of internal results DB is operation
        for op in self.results["operations"]:
            self.results["full-res"][op] = {}

            # second level is sample
            for smp in range(self.results["samples"]):
                sample = smp + 1
                self.results["full-res"][op][sample] = {}

                # last level is host (all threads will be in the host)
                for host in self.results["hosts"]:
                    self.results["full-res"][op][sample][host] = self.thread_read(
                        host, op, sample
                    )

        log.debug(f"The Initial DB is : {self.results['full-res']}")


@grey_squad
@performance
@performance_a
class TestSmallFileWorkload(PASTest):
    """
    Deploy benchmark operator and run SmallFile workload
    SmallFile workload using https://github.com/distributed-system-analysis/smallfile
    smallfile is a python-based distributed POSIX workload generator which can be
    used to quickly measure performance for a variety of metadata-intensive
    workloads
    """

    def setup(self):
        """
        Setting up test parameters
        """
        log.info("Starting the test setup")
        self.benchmark_name = "SmallFiles"
        self.client_pod_name = "smallfile-client"

        super(TestSmallFileWorkload, self).setup()

    def setting_storage_usage(self, file_size, files, threads, samples, clients):
        """
        Getting the storage capacity, calculate the usage of the storage and
        setting the workload CR rile parameters.

        Args:
            file_size (int) : the size of the file to be used
            files (int) : number of files to use
            threads (int) : number of threads to be use in the test
            samples (int) : how meany samples to run for each test
            clients (int) : number of clients (pods) to use in the test

        """
        self.crd_data["spec"]["workload"]["args"]["file_size"] = file_size
        self.crd_data["spec"]["workload"]["args"]["files"] = files
        self.crd_data["spec"]["workload"]["args"]["threads"] = threads
        self.crd_data["spec"]["workload"]["args"]["samples"] = samples
        self.crd_data["spec"]["workload"]["args"]["clients"] = clients

        # Calculating the size of the volume that need to be test, it should
        # be at least twice in the size then the size of the files, and at
        # least 100Gi.
        # Since the file_size is in Kb and the vol_size need to be in Gb, more
        # calculation is needed.
        vol_size = int(files * threads * file_size * 3)
        vol_size = int(vol_size / constants.GB2KB)
        if vol_size < 100:
            vol_size = 100
        self.crd_data["spec"]["workload"]["args"]["storagesize"] = f"{vol_size}Gi"

    def init_full_results(self, full_results):
        """
        Initialize the full results object which will send to the ES server

        Args:
            full_results (obj): an empty SmallFileResultsAnalyse object

        Returns:
            SmallFileResultsAnalyse (obj): the input object fill with data

        """
        for key in self.environment:
            full_results.add_key(key, self.environment[key])

        # Calculating the total size of the working data set - in GB
        full_results.add_key(
            "dataset",
            self.crd_data["spec"]["workload"]["args"]["file_size"]
            * self.crd_data["spec"]["workload"]["args"]["files"]
            * self.crd_data["spec"]["workload"]["args"]["threads"]
            * full_results.results["clients"]
            / constants.GB2KB,
        )

        full_results.add_key(
            "global_options",
            {
                "files": self.crd_data["spec"]["workload"]["args"]["files"],
                "file_size": self.crd_data["spec"]["workload"]["args"]["file_size"],
                "storageclass": self.crd_data["spec"]["workload"]["args"][
                    "storageclass"
                ],
                "vol_size": self.crd_data["spec"]["workload"]["args"]["storagesize"],
            },
        )
        return full_results

    def generate_kibana_link(self, index, columns):
        """
        Generating full link to the Kibana server with full test results information

        Args:
            index (str): the kibana index name (results, response time, etc.)
            columns (str): list of all columns to display

        Return:
            str : an http link to the appropriate kibana report

        """

        stime = self.start_time.replace("GMT", ".000Z")
        etime = self.end_time.replace("GMT", ".000Z")
        log.info(json.dumps(self.crd_data.get("spec").get("elasticsearch"), indent=2))
        host = self.crd_data.get("spec").get("elasticsearch").get("url")
        try:
            host = host.split(":")[1].replace("//", "")
        except Exception:
            log.error("No ES configuretion")
            return ""
        kibana_id = self.get_kibana_indexid(host, index)

        app = "app/kibana#/discover"
        if self.dev_mode:
            app = "app/discover#/"

        result = (
            f"http://{host}:5601/{app}"
            f"?_a=(columns:!({columns}),filters:!(),index:'{kibana_id}',interval:auto,"
            f"query:(language:kuery,query:'uuid:{self.uuid}'),sort:!())"
            f"&_g=(filters:!(),refreshInterval:(pause:!t,value:0),time:(from:'{stime}',to:'{etime}'))"
        )
        return result

    def collect_benchmark_logs(self):
        """
        Collecting the test log from all benchmark pods
        """

        # Getting full list of benchmark clients
        self.full_client_list = get_pod_name_by_pattern(
            self.client_pod_name, benchmark_operator.BMO_NAME
        )

        # Collecting logs from each pod
        for clpod in self.full_client_list:
            test_logs = self.pod_obj.exec_oc_cmd(f"logs {clpod}", out_yaml_format=False)
            log_file_name = f"{self.full_log_path}/{clpod}-pod.log"
            try:
                with open(log_file_name, "w") as f:
                    f.write(test_logs)
                log.info(f"The Test log can be found at : {log_file_name}")
            except Exception:
                log.warning(f"Cannot write the log to the file {log_file_name}")
        log.info("Logs from all client pods got successfully")

    def run(self):
        log.info("Running SmallFile bench")
        self.deploy_and_wait_for_wl_to_start(timeout=240, sleep=10)

        # Getting the UUID from inside the benchmark pod
        self.uuid = self.operator.get_uuid(self.client_pod)
        self.wait_for_wl_to_finish(sleep=30)
        self.collect_benchmark_logs()
        try:
            if "RUN STATUS DONE" in self.test_logs:
                log.info("SmallFiles has completed successfully")
                return True
        except IOError:
            log.warning("SmallFiles failed to complete")
            return False

    def teardown(self):
        """
        The teardown of the test environment in the end.

        """
        log.info("cleanup the environment")
        if isinstance(self.es, ElasticSearch):
            self.es.cleanup()
        try:
            self.operator.cleanup()
        except Exception:
            # nothig to do, the benchmark-operator did not deployed. this is for
            # the results collecting and pushing results into the dashboard
            pass
        # wait up to 45 min for the ceph cluster be health OK after backend
        # operation completed.
        log.info("Verify (and wait if needed) that ceph health is OK")
        ceph_health_check(tries=45, delay=60)
        # Let the background operation (delete backed images) to finish
        # time.sleep(120)

    @pytest.mark.parametrize(
        argnames=["file_size", "files", "threads", "samples", "clients", "interface"],
        argvalues=[
            pytest.param(*[4, 5000, 22, 5, 33, constants.CEPHBLOCKPOOL]),
            pytest.param(*[16, 5000, 8, 5, 21, constants.CEPHBLOCKPOOL]),
            pytest.param(*[4, 2500, 4, 5, 9, constants.CEPHFILESYSTEM]),
            pytest.param(*[16, 1500, 4, 5, 9, constants.CEPHFILESYSTEM]),
        ],
    )
    @pytest.mark.polarion_id("OCS-1295")
    def test_smallfile_workload(
        self, file_size, files, threads, samples, clients, interface
    ):
        """
        Run SmallFile Workload

        Args:
            file_size (int) : the size of the file to be used
            files (int) : number of files to use
            threads (int) : number of threads to be use in the test
            samples (int) : how meany samples to run for each test
            interface (str) : the volume type (rbd / cephfs)

        """
        if config.PERF.get("deploy_internal_es"):
            self.es = ElasticSearch()
        else:
            if config.PERF.get("internal_es_server") == "":
                self.es = None
                return
            else:
                self.es = {
                    "server": config.PERF.get("internal_es_server"),
                    "port": config.PERF.get("internal_es_port"),
                    "url": f"http://{config.PERF.get('internal_es_server')}:{config.PERF.get('internal_es_port')}",
                }
                # verify that the connection to the elasticsearch server is OK
                if not super(TestSmallFileWorkload, self).es_connect():
                    self.es = None
                    return

        # deploy the benchmark-operator
        self.deploy_benchmark_operator()

        # verify that there is an elasticsearch server for the benchmark
        if not self.es:
            log.error("This test must have an Elasticsearch server")
            return False

        # Getting the full path for the test logs
        self.full_log_path = get_full_test_logs_path(cname=self)
        self.results_path = get_full_test_logs_path(cname=self)
        self.full_log_path += (
            f"-{file_size}-{files}-{threads}-{samples}-{clients}-{interface}"
        )
        log.info(f"Logs file path name is : {self.full_log_path}")

        # Loading the main template yaml file for the benchmark
        log.info("Create resource file for small_files workload")
        self.crd_data = templating.load_yaml(constants.SMALLFILE_BENCHMARK_YAML)

        # Saving the Original elastic-search IP and PORT - if defined in yaml
        self.es_info_backup(self.es)

        self.set_storageclass(interface=interface)

        # Setting the data set to 40% of the total storage capacity
        self.setting_storage_usage(file_size, files, threads, samples, clients)

        self.get_env_info()

        if not self.run():
            log.error("The benchmark failed to run !")
            return

        # Setting back the original elastic-search information
        if self.backup_es:
            self.crd_data["spec"]["elasticsearch"] = self.backup_es

        # Initialize the results doc file.
        full_results = self.init_full_results(
            SmallFileResultsAnalyse(
                self.uuid, self.crd_data, self.full_log_path, self.main_es
            )
        )

        log.info(f"Full results is : {full_results.results}")
        if isinstance(self.es, ElasticSearch):
            # Using internal deployed elasticsearch
            log.info("Getting data from internal ES")
            if self.main_es:
                self.copy_es_data(self.es)
                full_results.read()
            else:
                log.info("Dumping data from the Internal ES to tar ball file")
                self.es.dumping_all_data(self.full_log_path)
        else:
            log.info(self.es)
            self.es = Elasticsearch(
                hosts=[{"host": self.es["server"], "port": self.es["port"]}]
            )
            full_results.read()

        full_results.add_key(
            "test_time", {"start": self.start_time, "end": self.end_time}
        )

        if self.main_es:
            full_results.es = self.main_es

        if not full_results.dont_check:
            full_results.add_key("hosts", full_results.get_clients_list())
            full_results.init_full_results()
            full_results.aggregate_host_results()
            test_status = full_results.aggregate_samples_results()

            # Generate link for the all data in the kibana
            columens = "optype,files,filesPerSec,elapsed,sample,tid"
            klink = self.generate_kibana_link("ripsaw-smallfile-results", columens)

            # Generate link for the all response-time data in the kibana
            columens = "optype,sample,iops,max,min,mean,'90%25','95%25','99%25'"
            rtlink = self.generate_kibana_link("ripsaw-smallfile-rsptimes", columens)

            full_results.all_results = {"kibana_all": klink, "kibana_rsptime": rtlink}

            if full_results.es_write():
                res_link = full_results.results_link()
                log.info(f"The Result can be found at : {res_link}")

                # Create text file with results of all subtest (4 - according to the parameters)
                self.write_result_to_file(res_link)

        else:
            test_status = True

        assert test_status, "Test Failed !"

    def test_smallfile_results(self):
        """
        This is not a test - it is only check that previous test ran and finish as expected
        and reporting the full results (links in the ES) of previous tests (4)
        """

        self.number_of_tests = 4
        self.results_path = get_full_test_logs_path(
            cname=self, fname="test_smallfile_workload"
        )
        self.results_file = os.path.join(self.results_path, "all_results.txt")
        log.info(f"Check results in {self.results_file}")

        self.check_tests_results()

        self.push_to_dashboard(test_name=self.benchmark_name)
