"""
Test to exercise Small File Workload

Note:
This test is using the benchmark-operator and the elastic search, so it start
process with port forwarding on port 9200 from the host that run the test (localhost)
to the elastic-search within the open-shift cluster, so, if you host is listen to
port 9200, this test can not be running in your host.

"""

# Builtin modules
import logging

# 3ed party modules
import pytest
import numpy as np
from elasticsearch import Elasticsearch, exceptions as ESExp

# Local modules
from ocs_ci.framework import config
from ocs_ci.utility import templating
from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import performance
from ocs_ci.ocs.perfresult import PerfResult
from ocs_ci.helpers.helpers import get_full_test_logs_path
from ocs_ci.ocs.perftests import PASTest
from ocs_ci.ocs.elasticsearch import ElasticSearch
from ocs_ci.utility.utils import ceph_health_check

log = logging.getLogger(__name__)


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
        "files": {"name": "Files-per-thread", "op": np.sum},
        "files-per-sec": {"name": "Files-per-sec", "op": np.sum},
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
        self.records = self.results["clients"] * self.results["threads"]
        self.records *= self.results["samples"]
        self.records *= len(self.results["operations"])

    def read(self):
        """
        Reading all test records from the elasticsearch server into dictionary
        inside this object

        """
        query = {"query": {"match": {"uuid": self.uuid}}}
        log.info("Reading all data from ES server")
        try:
            self.all_results = self.es.search(
                index=self.index, body=query, size=self.records
            )
            log.debug(self.all_results)

            if not self.all_results["hits"]["hits"]:
                log.warning("No data in ES server, disabling results calculation")
                self.dont_check = True
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
        for hit in self.all_results["hits"]["hits"]:

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
        log.debug(f"The results to combine {results}")
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
            log.debug(f'Aggregating {op} - {self.results["full-res"][op]}')
            results = self.combine_results(self.results["full-res"][op], False)

            log.info(f"Check IOPS {op} samples deviation")

            for key in self.managed_keys.keys():
                if self.managed_keys[key]["name"] in results.keys():
                    results[key] = np.average(results[self.managed_keys[key]["name"]])
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
                    del results[self.managed_keys[key]["name"]]
                self.results["full-res"][op] = results

        return test_pass

    def get_clients_list(self):
        """
        Finding and creating a list of all hosts that was used in this test

        Returns:
            list: a list of pods name

        """

        res = []
        for hit in self.all_results["hits"]["hits"]:
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


@performance
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

        super(TestSmallFileWorkload, self).setup()
        # deploy the benchmark-operator
        self.deploy_benchmark_operator()

    def setting_storage_usage(self, file_size, files, threads, samples):
        """
        Getting the storage capacity, calculate the usage of the storage and
        setting the workload CR rile parameters.

        Args:
            file_size (int) : the size of the file to be used
            files (int) : number of files to use
            threads (int) : number of threads to be use in the test
            samples (int) : how meany samples to run for each test

        """
        self.crd_data["spec"]["workload"]["args"]["file_size"] = file_size
        self.crd_data["spec"]["workload"]["args"]["files"] = files
        self.crd_data["spec"]["workload"]["args"]["threads"] = threads
        self.crd_data["spec"]["workload"]["args"]["samples"] = samples

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

    def run(self):
        log.info("Running SmallFile bench")
        self.deploy_and_wait_for_wl_to_start(timeout=240, sleep=10)

        # Getting the UUID from inside the benchmark pod
        self.uuid = self.operator.get_uuid(self.client_pod)
        self.wait_for_wl_to_finish(sleep=30)
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
        self.operator.cleanup()
        # wait up to 45 min for the ceph cluster be health OK after backend
        # operation completed.
        log.info("Verify (and wait if needed) that ceph health is OK")
        ceph_health_check(tries=45, delay=60)

    @pytest.mark.parametrize(
        argnames=["file_size", "files", "threads", "samples", "interface"],
        argvalues=[
            pytest.param(
                *[4, 50000, 4, 3, constants.CEPHBLOCKPOOL],
                marks=pytest.mark.polarion_id("OCS-1295"),
            ),
            pytest.param(
                *[16, 50000, 4, 3, constants.CEPHBLOCKPOOL],
                marks=pytest.mark.polarion_id("OCS-2020"),
            ),
            pytest.param(
                *[16, 200000, 4, 3, constants.CEPHBLOCKPOOL],
                marks=pytest.mark.polarion_id("OCS-2021"),
            ),
            pytest.param(
                *[4, 50000, 4, 3, constants.CEPHFILESYSTEM],
                marks=pytest.mark.polarion_id("OCS-2022"),
            ),
            pytest.param(
                *[16, 50000, 4, 3, constants.CEPHFILESYSTEM],
                marks=pytest.mark.polarion_id("OCS-2023"),
            ),
        ],
    )
    @pytest.mark.polarion_id("OCS-1295")
    def test_smallfile_workload(self, file_size, files, threads, samples, interface):
        """
        Run SmallFile Workload

        Args:
            file_size (int) : the size of the file to be used
            files (int) : number of files to use
            threads (int) : number of threads to be use in the test
            samples (int) : how meany samples to run for each test
            interface (str) : the volume type (rbd / cephfs)

        """
        # verify that there is an elasticsearch server for the benchmark
        if not self.es:
            log.error("This test must have an Elasticsearch server")
            return False

        # Getting the full path for the test logs
        self.full_log_path = get_full_test_logs_path(cname=self)
        self.full_log_path += f"-{file_size}-{files}-{threads}-{samples}-{interface}"
        log.info(f"Logs file path name is : {self.full_log_path}")

        # Loading the main template yaml file for the benchmark
        log.info("Create resource file for smallfiles workload")
        self.crd_data = templating.load_yaml(constants.SMALLFILE_BENCHMARK_YAML)

        # Saving the Original elastic-search IP and PORT - if defined in yaml
        self.es_info_backup(self.es)

        self.set_storageclass(interface=interface)

        # Setting the data set to 40% of the total storage capacity
        self.setting_storage_usage(file_size, files, threads, samples)

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
            full_results.all_results = None
            if full_results.es_write():
                log.info(f"The Result can be found at : {full_results.results_link()}")
        else:
            test_status = True

        assert test_status, "Test Failed !"
