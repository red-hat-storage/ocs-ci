"""
Test to exercise Small File Workload

Note:
This test is using the ripsaw and the elastic search, so it start process with
port forwarding on port 9200 from the host that run the test (localhost) to
the elastic-search within the open-shift cluster, so, if you host is listen to
port 9200, this test can not be running in your host.

"""

# Builtin modules
import logging
import time

# 3ed party modules
import pytest
import numpy as np
from elasticsearch import exceptions as ESExp

# Local modules
from ocs_ci.utility import templating
from ocs_ci.ocs.ripsaw import RipSaw
from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import performance
from ocs_ci.ocs.perfresult import PerfResult
from ocs_ci.helpers.helpers import get_full_test_logs_path
from ocs_ci.ocs.perftests import PASTest

log = logging.getLogger(__name__)


class SmallFileResultsAnalyse(PerfResult):
    """
    This class is reading all test results from elasticsearch server (which the
    ripsaw running of the benchmark is generate), aggregate them by :
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

    def __init__(self, uuid, crd):
        """
        Initialize the object by reading some of the data from the CRD file and
        by connecting to the ES server and read all results from it.

        Args:
            uuid (str): the unique uid of the test
            crd (dict): dictionary with test parameters - the test yaml file
                        that modify it in the test itself.

        """

        super(SmallFileResultsAnalyse, self).__init__(uuid, crd)

        self.index = crd["spec"]["es_index"] + "-results"
        self.new_index = crd["spec"]["es_index"] + "-fullres"

        # WA for Cloud environment where pod can not send results to ES
        self.dont_check = False

        # make sure we have connection to the elastic search server
        self.es_connect()

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
                            test_pass = False
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


@pytest.fixture(scope="function")
def ripsaw(request, storageclass_factory):
    def teardown():
        ripsaw.cleanup()
        time.sleep(10)

    request.addfinalizer(teardown)

    ripsaw = RipSaw()

    return ripsaw


@performance
class TestSmallFileWorkload(PASTest):
    """
    Deploy Ripsaw operator and run SmallFile workload
    SmallFile workload using https://github.com/distributed-system-analysis/smallfile
    smallfile is a python-based distributed POSIX workload generator which can be
    used to quickly measure performance for a variety of metadata-intensive
    workloads
    """

    def setup(self):
        super(TestSmallFileWorkload, self).setup()
        self.benchmark_name = "SmallFiles"
        self.client_pod_name = "smallfile-client"

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
    def test_smallfile_workload(
        self, ripsaw, es, file_size, files, threads, samples, interface
    ):
        """
        Run SmallFile Workload
        """
        self.full_log_path = get_full_test_logs_path(cname=self)
        self.full_log_path += f"-{file_size}-{files}-{threads}-{samples}-{interface}"
        log.info(f"Logs file path name is : {self.full_log_path}")

        # Loading the main template yaml file for the benchmark
        self.crd_data = templating.load_yaml(constants.SMALLFILE_BENCHMARK_YAML)

        self.es_info_backup(es)

        self.ripsaw_deploy(ripsaw)
        self.set_storageclass(interface=interface)
        log.info("Running SmallFile bench")

        """
            Setting up the parameters for this test
        """
        self.crd_data["spec"]["workload"]["args"]["file_size"] = file_size
        self.crd_data["spec"]["workload"]["args"]["files"] = files
        self.crd_data["spec"]["workload"]["args"]["threads"] = threads
        self.crd_data["spec"]["workload"]["args"]["samples"] = samples
        """
        Calculating the size of the volume that need to be test, it should
        be at least twice in the size then the size of the files, and at
        least 100Gi.

        Since the file_size is in Kb and the vol_size need to be in Gb, more
        calculation is needed.
        """
        vol_size = int(files * threads * file_size * 3)
        vol_size = int(vol_size / constants.GB2KB)
        if vol_size < 100:
            vol_size = 100
        self.crd_data["spec"]["workload"]["args"]["storagesize"] = f"{vol_size}Gi"
        self.get_env_info()

        self.deploy_and_wait_for_wl_to_start(timeout=240, sleep=10)

        # Getting the UUID from inside the benchmark pod
        uuid = ripsaw.get_uuid(self.client_pod)

        # Setting back the original elastic-search information
        if self.backup_es:
            self.crd_data["spec"]["elasticsearch"] = self.backup_es

        full_results = SmallFileResultsAnalyse(uuid, self.crd_data)

        # Initialize the results doc file.
        for key in self.environment:
            full_results.add_key(key, self.environment[key])

        # Calculating the total size of the working data set - in GB
        full_results.add_key(
            "dataset",
            file_size
            * files
            * threads
            * full_results.results["clients"]
            / constants.GB2KB,
        )

        full_results.add_key(
            "global_options",
            {
                "files": files,
                "file_size": file_size,
                "storageclass": self.crd_data["spec"]["workload"]["args"][
                    "storageclass"
                ],
                "vol_size": self.crd_data["spec"]["workload"]["args"]["storagesize"],
            },
        )

        self.wait_for_wl_to_finish(timeout=3600, sleep=30)

        if "RUN STATUS DONE" in self.test_logs:
            # Getting the end time of the test
            full_results.add_key(
                "test_time", {"start": self.start_time, "end": self.end_time}
            )
            # if Internal ES is exists, Copy all data from the Internal to main ES
            self.copy_es_data(es)
            full_results.read()
            if not full_results.dont_check:
                full_results.add_key("hosts", full_results.get_clients_list())
                full_results.init_full_results()
                full_results.aggregate_host_results()
                test_status = full_results.aggregate_samples_results()
                full_results.es_write()

                # Creating full link to the results on the ES server
                log.info(f"The Result can be found at : {full_results.results_link()}")
            else:
                test_status = True

        assert test_status, "Test Failed !"
