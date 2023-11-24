# import pytest
import time
import logging
import os
import re
import tempfile
from uuid import uuid4
import yaml

import requests
import json

from elasticsearch import Elasticsearch, exceptions as esexp

from ocs_ci.framework import config
from ocs_ci.framework.testlib import BaseTest
from ocs_ci.helpers import helpers, performance_lib
from ocs_ci.helpers.performance_lib import run_oc_command

from ocs_ci.ocs import benchmark_operator, constants, defaults, exceptions, node
from ocs_ci.ocs.cluster import CephCluster
from ocs_ci.ocs.elasticsearch import elasticsearch_load
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    MissingRequiredConfigKeyError,
    PVCNotCreated,
    PodNotCreated,
)
from ocs_ci.ocs.ocp import OCP, switch_to_default_rook_cluster_project
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.ocs.version import get_environment_info
from ocs_ci.utility import templating
from ocs_ci.utility.perf_dash.dashboard_api import PerfDash
from ocs_ci.utility.utils import TimeoutSampler, get_running_cluster_id, ocsci_log_path

log = logging.getLogger(__name__)


class PASTest(BaseTest):
    """
    Base class for QPAS team - Performance and Scale tests

    This class contain functions which used by performance and scale test,
    and also can be used by E2E test which used the benchmark-operator (ripsaw)
    """

    def setup(self):
        """
        Setting up the environment for each performance and scale test

        Args:
            name (str): The test name that will use in the performance dashboard
        """
        log.info("Setting up test environment")
        self.es = None  # place holder for the incluster deployment elasticsearch
        self.es_backup = None  # place holder for the elasticsearch backup
        self.main_es = None  # place holder for the main elasticsearch object
        self.benchmark_obj = None  # place holder for the benchmark object
        self.client_pod = None  # Place holder for the client pod object
        self.dev_mode = config.RUN["cli_params"].get("dev_mode")
        self.pod_obj = OCP(kind="pod", namespace=benchmark_operator.BMO_NAME)
        self.initialize_test_crd()

        # Place holders for test results file (all sub-tests together)
        self.results_file = ""

        # All tests need a uuid for the ES results, benchmark-operator base test
        # will overrite it with uuid pulling from the benchmark pod
        self.uuid = uuid4().hex

        # Getting the full path for the test logs
        self.full_log_path = os.environ.get("PYTEST_CURRENT_TEST").split(" ")[0]
        self.full_log_path = (
            self.full_log_path.replace("::", "/").replace("[", "-").replace("]", "")
        )
        self.full_log_path = os.path.join(ocsci_log_path(), self.full_log_path)
        log.info(f"Logs file path name is : {self.full_log_path}")

        # Getting the results path as a list
        self.results_path = self.full_log_path.split("/")
        self.results_path.pop()

        # List of test(s) for checking the results
        self.workloads = []

        # Collecting all Environment configuration Software & Hardware
        # for the performance report.
        self.environment = get_environment_info()
        self.environment["clusterID"] = get_running_cluster_id()

        self.ceph_cluster = CephCluster()
        self.used_capacity = self.get_cephfs_data()

        self.get_osd_info()

        self.get_node_info(node_type="master")
        self.get_node_info(node_type="worker")

    def teardown(self):
        if hasattr(self, "operator"):
            self.operator.cleanup()

        now_data = self.get_cephfs_data()
        # Wait 1 minutes for the backend deletion actually start.
        log.info("Waiting for Ceph to finish cleaning up")
        time.sleep(60)

        # Quarry the storage usage every 2 Min. if no difference between two
        # samples, the backend cleanup is done.
        still_going_down = True
        while still_going_down:
            new_data = self.get_cephfs_data()
            # no deletion operation is in progress
            if abs(now_data - new_data) < 1:
                still_going_down = False
                # up to 2% inflation of usage is acceptable
                if new_data > (self.used_capacity * 1.02):
                    log.warning(
                        f"usage capacity after the test ({new_data:.2f} GiB) "
                        f"is more then in the begining of it ({self.used_capacity:.2f} GiB)"
                    )
            else:
                log.info(f"Last usage : {now_data}, Current usage {new_data}")
                now_data = new_data
                log.info("Waiting for Ceph to finish cleaning up")
                time.sleep(120)
                still_going_down = True
        log.info("Storage usage was cleandup")

        # Add delay of 15 sec. after each test.
        time.sleep(10)

    def initialize_test_crd(self):
        """
        Initializing the test CRD file.
        this include the Elasticsearch info, cluster name and user name which run the test
        """
        self.crd_data = {
            "spec": {
                "test_user": "Homer simpson",  # place holde only will be change in the test.
                "clustername": "test_cluster",  # place holde only will be change in the test.
                "elasticsearch": {
                    "server": config.PERF.get("production_es_server"),
                    "port": config.PERF.get("production_es_port"),
                    "url": f"http://{config.PERF.get('production_es_server')}:{config.PERF.get('production_es_port')}",
                },
            }
        }
        # during development use the dev ES so the data in the Production ES will be clean.
        if self.dev_mode:
            self.crd_data["spec"]["elasticsearch"] = {
                "server": config.PERF.get("dev_es_server"),
                "port": config.PERF.get("dev_es_port"),
                "url": f"http://{config.PERF.get('dev_es_server')}:{config.PERF.get('dev_es_port')}",
            }

    def create_new_pool(self, pool_name):
        """
        Creating new Storage pool for RBD / CephFS to use in a test so it can be
        deleted in the end of the test for fast cleanup

        Args:
            pool_name (str):  the name of the pool to create

        """
        if self.interface == constants.CEPHBLOCKPOOL:
            self.ceph_cluster.create_new_blockpool(pool_name=pool_name)
            self.ceph_cluster.set_pgs(poolname=pool_name, pgs=128)
        elif self.interface == constants.CEPHFILESYSTEM:
            self.ceph_cluster.create_new_filesystem(fs_name=pool_name)
            self.ceph_cluster.toolbox.exec_ceph_cmd(
                f"ceph fs subvolumegroup create {pool_name} csi"
            )
            self.ceph_cluster.set_pgs(poolname=f"{pool_name}-data0", pgs=128)

        self.ceph_cluster.set_target_ratio(
            poolname="ocs-storagecluster-cephblockpool", ratio=0.24
        )
        self.ceph_cluster.set_target_ratio(
            poolname="ocs-storagecluster-cephfilesystem-data0", ratio=0.24
        )
        return

    def delete_ceph_pool(self, pool_name):
        """
        Delete Storage pool (RBD / CephFS) that was created for the test for
        fast cleanup.

        Args:
            pool_name (str):  the name of the pool to be delete

        """
        if self.interface == constants.CEPHBLOCKPOOL:
            self.ceph_cluster.delete_blockpool(pool_name=pool_name)
        elif self.interface == constants.CEPHFILESYSTEM:
            self.ceph_cluster.delete_filesystem(fs_name=pool_name)

        self.ceph_cluster.set_target_ratio(
            poolname="ocs-storagecluster-cephblockpool", ratio=0.49
        )
        self.ceph_cluster.set_target_ratio(
            poolname="ocs-storagecluster-cephfilesystem-data0", ratio=0.49
        )
        return

    def get_cephfs_data(self):
        """
        Look through ceph pods and find space usage on all ceph pools

        Returns:
            int: total used capacity in GiB.
        """
        ceph_status = self.ceph_cluster.toolbox.exec_ceph_cmd(ceph_cmd="ceph df")
        total_used = 0
        for pool in ceph_status["pools"]:
            total_used += pool["stats"]["bytes_used"]
        return total_used / constants.GB

    def get_osd_info(self):
        """
        Getting the OSD's information and update the main environment
        dictionary.

        """
        ct_pod = pod.get_ceph_tools_pod()
        osd_info = ct_pod.exec_ceph_cmd(ceph_cmd="ceph osd df")
        self.environment["osd_size"] = osd_info.get("nodes")[0].get("crush_weight")
        self.environment["osd_num"] = len(osd_info.get("nodes"))
        self.environment["total_capacity"] = osd_info.get("summary").get(
            "total_kb_avail"
        )
        self.environment["ocs_nodes_num"] = len(node.get_ocs_nodes())

    def get_node_info(self, node_type="master"):
        """
        Getting node type hardware information and update the main environment
        dictionary.

        Args:
            node_type (str): the node type to collect data about,
              can be : master / worker - the default is master

        """
        if node_type == "master":
            nodes = node.get_master_nodes()
        elif node_type == "worker":
            nodes = node.get_worker_nodes()
        else:
            log.warning(f"Node type ({node_type}) is invalid")
            return

        oc_cmd = OCP(namespace=config.ENV_DATA["cluster_namespace"])
        self.environment[f"{node_type}_nodes_num"] = len(nodes)
        self.environment[f"{node_type}_nodes_cpu_num"] = oc_cmd.exec_oc_debug_cmd(
            node=nodes[0],
            cmd_list=["lscpu | grep '^CPU(s):' | awk '{print $NF}'"],
        ).rstrip()
        self.environment[f"{node_type}_nodes_memory"] = oc_cmd.exec_oc_debug_cmd(
            node=nodes[0], cmd_list=["free | grep Mem | awk '{print $2}'"]
        ).rstrip()

    def deploy_benchmark_operator(self):
        """
        Deploy the benchmark operator

        """
        self.operator = benchmark_operator.BenchmarkOperator()
        self.operator.deploy()

    def es_info_backup(self, elasticsearch):
        """
        Saving the Original elastic-search IP and PORT - if defined in yaml

        Args:
            elasticsearch (obj): elasticsearch object

        """

        self.crd_data["spec"]["elasticsearch"] = {}

        # for development mode use the Dev ES server
        if self.dev_mode and config.PERF.get("dev_lab_es"):
            log.info("Using the development ES server")
            self.crd_data["spec"]["elasticsearch"] = {
                "server": config.PERF.get("dev_es_server"),
                "port": config.PERF.get("dev_es_port"),
                "url": f"http://{config.PERF.get('dev_es_server')}:{config.PERF.get('dev_es_port')}",
                "parallel": True,
            }

        # for production mode use the Lab ES server
        if not self.dev_mode and config.PERF.get("production_es"):
            self.crd_data["spec"]["elasticsearch"] = {
                "server": config.PERF.get("production_es_server"),
                "port": config.PERF.get("production_es_port"),
                "url": f"http://{config.PERF.get('production_es_server')}:{config.PERF.get('production_es_port')}",
                "parallel": True,
            }

        # backup the Main ES info (if exists)
        if not self.crd_data["spec"]["elasticsearch"] == {}:
            self.backup_es = self.crd_data["spec"]["elasticsearch"]
            log.info(
                f"Creating object for the Main ES server on {self.backup_es['url']}"
            )
            self.main_es = Elasticsearch([self.backup_es["url"]], verify_certs=True)
        else:
            log.warning("Elastic Search information does not exists for this test")

        # Use the internal define elastic-search server in the test - if exist
        if elasticsearch:

            if not isinstance(elasticsearch, dict):
                # elasticsearch is an internally deployed server (obj)
                ip = elasticsearch.get_ip()
                port = elasticsearch.get_port()
            else:
                # elasticsearch is an existing server (dict)
                ip = elasticsearch.get("server")
                port = elasticsearch.get("port")

            self.crd_data["spec"]["elasticsearch"] = {
                "server": ip,
                "port": port,
                "url": f"http://{ip}:{port}",
                "parallel": True,
            }
            log.info(f"Going to use the ES : {self.crd_data['spec']['elasticsearch']}")
        elif config.PERF.get("internal_es_server"):
            # use an in-cluster elastic-search (not deployed by the test)
            self.crd_data["spec"]["elasticsearch"] = {
                "server": config.PERF.get("internal_es_server"),
                "port": config.PERF.get("internal_es_port"),
                "url": f"http://{config.PERF.get('internal_es_server')}:{config.PERF.get('internal_es_port')}",
                "parallel": True,
            }

    def set_storageclass(self, interface):
        """
        Setting the benchmark CRD storageclass

        Args:
            interface (str): The interface which will used in the test

        """
        if interface == constants.CEPHBLOCKPOOL:
            storageclass = constants.DEFAULT_STORAGECLASS_RBD
        else:
            storageclass = constants.DEFAULT_STORAGECLASS_CEPHFS
        log.info(f"Using [{storageclass}] Storageclass")
        self.crd_data["spec"]["workload"]["args"]["storageclass"] = storageclass

    def get_env_info(self):
        """
        Getting the environment information and update the workload RC if
        necessary.

        """
        if not self.environment["user"] == "":
            self.crd_data["spec"]["test_user"] = self.environment["user"]
        else:
            # since full results object need this parameter, initialize it from CR file
            self.environment["user"] = self.crd_data["spec"]["test_user"]
        self.crd_data["spec"]["clustername"] = self.environment["clustername"]

        log.debug(f"Environment information is : {self.environment}")

    def deploy_and_wait_for_wl_to_start(self, timeout=300, sleep=20):
        """
        Deploy the workload and wait until it start working

        Args:
            timeout (int): time in second to wait until the benchmark start
            sleep (int): Sleep interval seconds

        """
        log.debug(f"The {self.benchmark_name} CR file is {self.crd_data}")
        self.benchmark_obj = OCS(**self.crd_data)
        self.benchmark_obj.create()

        # This time is only for reporting - when the benchmark started.
        self.start_time = self.get_time()

        # Wait for benchmark client pod to be created
        log.info(f"Waiting for {self.client_pod_name} to Start")
        for bm_pod in TimeoutSampler(
            timeout,
            sleep,
            get_pod_name_by_pattern,
            self.client_pod_name,
            benchmark_operator.BMO_NAME,
        ):
            try:
                if bm_pod[0] is not None:
                    self.client_pod = bm_pod[0]
                    break
            except IndexError:
                log.info("Bench pod is not ready yet")
        # Sleeping for 15 sec for the client pod to be fully accessible
        time.sleep(15)
        log.info(f"The benchmark pod {self.client_pod_name} is Running")

    def wait_for_wl_to_finish(self, timeout=18000, sleep=300):
        """
        Waiting until the workload is finished and get the test log

        Args:
            timeout (int): time in second to wait until the benchmark start
            sleep (int): Sleep interval seconds

        Raise:
            exception for too much restarts of the test.
            ResourceWrongStatusException : test Failed / Error
            TimeoutExpiredError : test did not completed on time.

        """
        log.info(f"Waiting for {self.client_pod_name} to complete")
        timeout=24000
        Finished = 0
        restarts = 0
        total_time = timeout
        while not Finished and total_time > 0:
            log.info(f"total_time {total_time}")
            results = run_oc_command(
                "get pod --no-headers -o custom-columns=:metadata.name,:status.phase",
                namespace=benchmark_operator.BMO_NAME,
            )
            (fname, status) = ["", ""]
            for name in results:
                # looking for the pod which run the benchmark (not the IO)
                # this pod contain the `client` in his name, and there is only one
                # pod like this, other pods have the `server` in the name.
                (fname, status) = name.split()
                if re.search("client", fname):
                    break
                else:
                    (fname, status) = ["", ""]

            if fname == "":  # there is no `client` pod !
                err_msg = f"{self.client_pod} Failed to run !!!"
                log.error(err_msg)
                raise Exception(err_msg)

            if not fname == self.client_pod:
                # The client pod name is different from previous check, it was restarted
                log.info(
                    f"The pod {self.client_pod} was restart. the new client pod is {fname}"
                )
                self.client_pod = fname
                restarts += 1
                # in case of restarting the benchmark, reset the timeout as well
                total_time = timeout

            if restarts > 3:  # we are tolerating only 3 restarts
                err_msg = f"Too much restarts of the benchmark ({restarts})"
                log.error(err_msg)
                raise Exception(err_msg)

            if status == "Succeeded":
                # Getting the end time of the benchmark - for reporting.
                self.end_time = self.get_time()
                self.test_logs = self.pod_obj.exec_oc_cmd(
                    f"logs {self.client_pod}", out_yaml_format=False
                )
                log.info(f"{self.client_pod} completed successfully")
                Finished = 1
            elif (
                status != constants.STATUS_RUNNING
                and status != constants.STATUS_PENDING
            ):
                # if the benchmark pod is not in Running state (and not Completed/Pending),
                # no need to wait for timeout.
                # Note: the pod can be in pending state in case of restart.
                err_msg = f"{self.client_pod} Failed to run - ({status})"
                log.error(err_msg)
                raise exceptions.ResourceWrongStatusException(
                    self.client_pod,
                    describe_out=err_msg,
                    column="Status",
                    expected="Succeeded",
                    got=status,
                )
            else:
                log.info(
                    f"{self.client_pod} is in {status} State, and wait to Succeeded State."
                    f" wait another {sleep} sec. for benchmark to complete"
                )
                time.sleep(sleep)
                total_time -= sleep

        if not Finished:
            err_msg = (
                f"{self.client_pod} did not completed on time, "
                f"maybe timeout ({timeout}) need to be increase"
            )
            log.error(err_msg)
            raise exceptions.TimeoutExpiredError(
                self.client_pod, custom_message=err_msg
            )

        # Saving the benchmark internal log into a file at the logs directory
        log_file_name = f"{self.full_log_path}/test-pod.log"
        try:
            with open(log_file_name, "w") as f:
                f.write(self.test_logs)
            log.info(f"The Test log can be found at : {log_file_name}")
        except Exception:
            log.warning(f"Cannot write the log to the file {log_file_name}")
        log.info(f"The {self.benchmark_name} benchmark complete")

    def copy_es_data(self, elasticsearch):
        """
        Copy data from Internal ES (if exists) to the main ES

        Args:
            elasticsearch (obj): elasticsearch object (if exits)

        """
        log.info(f"In copy_es_data Function - {elasticsearch}")
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
                log.info(
                    f"All raw data for tests results can be found at : {self.full_log_path}"
                )
                return True
            else:
                log.warning("Cannot upload data into the Main ES server")
                return False

    def read_from_es(self, es, index, uuid):
        """
        Reading all results from elasticsearch server

        Args:
            es (dict): dictionary with elasticsearch info  {server, port}
            index (str): the index name to read from the elasticsearch server
            uuid (str): the test UUID to find in the elasticsearch server

        Returns:
            list : list of all results

        """

        con = Elasticsearch([{"host": es["server"], "port": es["port"]}])
        query = {"size": 1000, "query": {"match": {"uuid": uuid}}}

        try:
            results = con.search(index=index, body=query)
            full_data = []
            for res in results["hits"]["hits"]:
                full_data.append(res["_source"])
            return full_data

        except Exception as e:
            log.warning(f"{index} Not found in the Internal ES. ({e})")
            return []

    def es_connect(self):
        """
        Create elasticsearch connection to the server

        Return:
            bool : True if there is a connection to the ES, False if not.

        """

        OK = True  # the return value
        try:
            log.info(f"try to connect the ES : {self.es['server']}:{self.es['port']}")
            self.es_con = Elasticsearch(
                [{"host": self.es["server"], "port": self.es["port"]}]
            )
        except Exception:
            log.error(f"Cannot connect to ES server {self.es}")
            OK = False

        # Testing the connection to the elastic-search
        if not self.es_con.ping():
            log.error(f"Cannot connect to ES server {self.es}")
            OK = False

        return OK

    def get_kibana_indexid(self, server, name):
        """
        Get the kibana Index ID by its name.

        Args:
            server (str): the IP (or name) of the Kibana server
            name (str): the name of the index

        Returns:
            str : the index ID of the given name
                  return None if the index does not exist.

        """

        port = 5601
        http_link = f"http://{server}:{port}/api/saved_objects"
        search_string = f"_find?type=index-pattern&search_fields=title&search='{name}'"
        log.info(f"Connecting to Kibana {server} on port {port}")
        try:
            res = requests.get(f"{http_link}/{search_string}")
            res = json.loads(res.content.decode())
            for ind in res.get("saved_objects"):
                if ind.get("attributes").get("title") in [name, f"{name}*"]:
                    log.info(f"The Kibana indexID for {name} is {ind.get('id')}")
                    return ind.get("id")
        except esexp.ConnectionError:
            log.warning("Cannot connect to Kibana server {}:{}".format(server, port))
        log.warning(f"Can not find the Kibana index : {name}")
        return None

    def write_result_to_file(self, res_link):
        """
        Write the results link into file, to combine all sub-tests results
        together in one file, so it can be easily pushed into the performance dashboard

        Args:
            res_link (str): http link to the test results in the ES server

        """
        if not os.path.exists(self.results_path):
            os.makedirs(self.results_path)
        self.results_file = os.path.join(self.results_path, "all_results.txt")

        log.info(f"Try to push results into : {self.results_file}")
        try:
            with open(self.results_file, "a+") as f:
                f.write(f"{res_link}\n")
            f.close()
        except FileNotFoundError:
            log.info("The file does not exist, so create new one.")
            with open(self.results_file, "w+") as f:
                f.write(f"{res_link}\n")
            f.close()
        except OSError as err:
            log.error(f"OS error: {err}")

    @staticmethod
    def get_time(time_format=None):
        """
        Getting the current GMT time in a specific format for the ES report,
        or for seeking in the containers log

        Args:
            time_format (str): which thime format to return - None / CSI

        Returns:
            str : current date and time in formatted way

        """
        formated = "%Y-%m-%dT%H:%M:%SGMT"
        if time_format and time_format.lower() == "csi":
            formated = "%Y-%m-%dT%H:%M:%SZ"

        return time.strftime(formated, time.gmtime())

    def check_tests_results(self):
        """
        Check that all sub-tests (test multiplication by parameters) finished and
        pushed the data to the ElastiSearch server.
        It also generate the es link to push into the performance dashboard.
        """

        es_links = []
        try:
            with open(self.results_file, "r") as f:
                data = f.read().split("\n")
            data.pop()  # remove the last empty element
            if len(data) != self.number_of_tests:
                log.error("Not all tests finished")
                raise exceptions.BenchmarkTestFailed()
            else:
                log.info("All test finished OK, and the results can be found at :")
                for res in data:
                    log.info(res)
                    es_links.append(res)
        except OSError as err:
            log.error(f"OS error: {err}")
            raise err

        self.es_link = ",".join(es_links)

    def push_to_dashboard(self, test_name):
        """
        Pushing the test results into the performance dashboard, if exist

        Args:
            test_name (str): the test name as defined in the performance dashboard

        Returns:
            None in case of pushing the results to the dashboard failed

        """

        try:
            db = PerfDash()
        except MissingRequiredConfigKeyError as ex:
            log.error(
                f"Results cannot be pushed to the performance dashboard, no connection [{ex}]"
            )
            return None

        log.info(f"Full version is : {self.environment.get('ocs_build')}")
        version = self.environment.get("ocs_build").split("-")[0]
        try:
            build = self.environment.get("ocs_build").split("-")[1]
            build = build.split(".")[0]
        except Exception:
            build = "GA"

        # Getting the topology from the cluster
        az = node.get_odf_zone_count()
        if az == 0:
            az = 1
        topology = f"{az}-AZ"

        # Check if it is Arbiter cluster
        my_obj = OCP(
            kind="StorageCluster", namespace=config.ENV_DATA["cluster_namespace"]
        )
        arbiter = (
            my_obj.data.get("items")[0].get("spec").get("arbiter").get("enable", False)
        )

        if arbiter:
            topology = "Strech-Arbiter"

        # Check if run on LSO
        try:
            ns = OCP(kind="namespace", resource_name=defaults.LOCAL_STORAGE_NAMESPACE)
            ns.get()
            platform = f"{self.environment.get('platform')}-LSO"
        except Exception:
            platform = self.environment.get("platform")

        # Check if encrypted cluster
        encrypt = (
            my_obj.data.get("items")[0]
            .get("spec")
            .get("encryption")
            .get("enable", False)
        )
        kms = (
            my_obj.data.get("items")[0]
            .get("spec")
            .get("encryption")
            .get("kms")
            .get("enable", False)
        )
        if kms:
            platform = f"{platform}-KMS"
        elif encrypt:
            platform = f"{platform}-Enc"

        # Check the base storageclass on AWS
        if self.environment.get("platform").upper() == "AWS":
            osd_pod_list = pod.get_osd_pods()
            osd_pod = osd_pod_list[0].pod_data["metadata"]["name"]
            osd_pod_obj = OCP(
                kind="POD",
                resource_name=osd_pod,
                namespace=config.ENV_DATA["cluster_namespace"],
            )
            log.info(f"The First OSD pod nams is {osd_pod}")

            osd_pvc_name = osd_pod_obj.get()["spec"]["initContainers"][0][
                "volumeDevices"
            ][0]["name"]
            log.info(f"The First OSD name is : {osd_pvc_name}")
            osd_pvc_obj = OCP(
                kind="PersistentVolumeClaim",
                resource_name=osd_pvc_name,
                namespace=config.ENV_DATA["cluster_namespace"],
            )

            odf_back_storage = osd_pvc_obj.get()["spec"]["storageClassName"]
            log.info(f"The ODF deployment use {odf_back_storage} as back storage")
            if odf_back_storage != "gp2":
                platform = f"{platform}-{odf_back_storage}"

        if self.dev_mode:
            port = "8181"
        else:
            port = "8080"

        try:
            log.info(
                "Trying to push :"
                f"version={version},"
                f"build={build},"
                f"platform={platform},"
                f"topology={topology},"
                f"test={test_name},"
                f"eslink={self.es_link}, logfile=None"
            )

            db.add_results(
                version=version,
                build=build,
                platform=platform,
                topology=topology,
                test=test_name,
                eslink=self.es_link,
                logfile=None,
            )
            resultslink = (
                f"http://{db.creds['host']}:{port}/index.php?"
                f"version1={db.get_version_id(version)}"
                f"&build1={db.get_build_id(version, build)}"
                f"&platform1={db.get_platform_id(platform)}"
                f"&az_topology1={db.get_topology_id(topology)}"
                f"&test_name%5B%5D={db.get_test_id(test_name)}"
                "&submit=Choose+options"
            )
            log.info(f"Full results report can be found at : {resultslink}")
        except Exception as ex:
            log.error(f"Can not push results into the performance Dashboard! [{ex}]")

        db.cleanup()

    def add_test_to_results_check(self, test, test_count, test_name):
        """
        Adding test information to list of test(s) that we want to check the results
        and push them to the dashboard.

        Args:
            test (str): the name of the test function that we want to check
            test_count (int): number of test(s) that need to run - according to parametize
            test_name (str): the test name in the Performance dashboard

        """
        self.workloads.append(
            {"name": test, "tests": test_count, "test_name": test_name}
        )

    def check_results_and_push_to_dashboard(self):
        """
        Checking test(s) results - that all test(s) are finished OK, and push
        the results into the performance dashboard

        """

        for wl in self.workloads:
            self.number_of_tests = wl["tests"]

            self.results_file = os.path.join(
                "/", *self.results_path, wl["name"], "all_results.txt"
            )
            log.info(f"Check results for [{wl['name']}] in : {self.results_file}")
            self.check_tests_results()
            self.push_to_dashboard(test_name=wl["test_name"])

    def create_test_project(self):
        """
        Creating new project (namespace) for performance test
        """
        self.namespace = helpers.create_unique_resource_name("pas-test", "namespace")
        log.info(f"Creating new namespace ({self.namespace}) for the test")
        try:
            self.proj = helpers.create_project(project_name=self.namespace)
        except CommandFailed as ex:
            if str(ex).find("(AlreadyExists)"):
                log.warning("The namespace already exists !")
            log.error("Cannot create new project")
            raise CommandFailed(f"{self.namespace} was not created")

    def delete_test_project(self):
        """
        Deleting the performance test project (namespace)
        """
        log.info(f"Deleting the test namespace : {self.namespace}")
        switch_to_default_rook_cluster_project()
        try:
            self.proj.delete(resource_name=self.namespace)
            self.proj.wait_for_delete(
                resource_name=self.namespace, timeout=60, sleep=10
            )
        except CommandFailed:
            log.error(f"Cannot delete project {self.namespace}")
            raise CommandFailed(f"{self.namespace} was not created")

    def set_results_path_and_file(self, func_name):
        """
        Setting the results_path and results_file parameter for a specific test

        Args:
            func_name (str): the name of the function which use for the test
        """

        self.results_path = os.path.join("/", *self.results_path, func_name)
        self.results_file = os.path.join(self.results_path, "all_results.txt")

    def create_fio_pod_yaml(self, pvc_size=1, filesize=0):
        """
        This function create a new performance pod yaml file, which will trigger
        the FIO command on starting and getting into Compleat state when finish

        If the filesize argument is not provided, The FIO will fillup 70% of the
        PVC which will attached to the pod.

        Args:
            pvc_size (int/float): the size of the pvc_which will attach to the pod (in GiB)
            file_size (str): the filesize to write into (e.g 100Mi, 30Gi)

        """
        if filesize == 0:
            file_size = f"{int(pvc_size * 1024 * 0.7)}M"
        else:
            file_size = filesize

        # Creating the FIO command line parameters string
        command = (
            "--name=fio-fillup --filename=/mnt/test_file --rw=write --bs=1m"
            f" --direct=1 --numjobs=1 --time_based=0 --runtime=36000 --size={file_size}"
            " --ioengine=libaio --end_fsync=1 --output-format=json"
        )
        # Load the default POD yaml file and update it to run the FIO immediately
        pod_data = templating.load_yaml(constants.PERF_POD_YAML)
        pod_data["spec"]["containers"][0]["command"] = ["/usr/bin/fio"]
        pod_data["spec"]["containers"][0]["args"] = command.split(" ")
        pod_data["spec"]["containers"][0]["stdin"] = False
        pod_data["spec"]["containers"][0]["tty"] = False
        # FIO need to run only once
        pod_data["spec"]["restartPolicy"] = "Never"

        # Generate new POD yaml file
        self.pod_yaml_file = tempfile.NamedTemporaryFile(prefix="PerfPod")
        with open(self.pod_yaml_file.name, "w") as temp:
            yaml.dump(pod_data, temp)

    def create_testing_pvc_and_wait_for_bound(self):
        log.info("Creating PVC for the test")
        try:
            self.pvc_obj = helpers.create_pvc(
                sc_name=self.sc_obj.name,
                pvc_name="pvc-pas-test",
                size=f"{self.pvc_size}Gi",
                namespace=self.namespace,
                # access_mode=Interfaces_info[self.interface]["accessmode"],
            )
        except Exception as e:
            log.exception(f"The PVC was not created, exception [{str(e)}]")
            raise PVCNotCreated("PVC did not reach BOUND state.")
        # Wait for the PVC to be Bound
        performance_lib.wait_for_resource_bulk_status(
            "pvc", 1, self.namespace, constants.STATUS_BOUND, 600, 5
        )
        log.info(f"The PVC {self.pvc_obj.name} was created and in Bound state.")

    def cleanup_testing_pvc(self):
        try:
            pv = self.pvc_obj.get("spec")["spec"]["volumeName"]
            self.pvc_obj.delete()
            # Wait for the PVC to be deleted
            performance_lib.wait_for_resource_bulk_status(
                "pvc", 0, self.namespace, constants.STATUS_BOUND, 60, 5
            )
            log.info("The PVC was deleted successfully")
        except Exception:
            log.warning("The PVC failed to delete")
            pass

        # Delete the backend PV of the PVC
        try:
            log.info(f"Try to delete the backend PV : {pv}")
            performance_lib.run_oc_command(f"delete pv {pv}")
        except Exception as ex:
            err_msg = f"cannot delete PV [{ex}]"
            log.error(err_msg)

    def create_testing_pod_and_wait_for_completion(self, **kwargs):
        # Creating pod yaml file to run as a Job, the command to run on the pod and
        # arguments to it will replace in the create_pod function
        self.create_fio_pod_yaml(
            pvc_size=int(self.pvc_size), filesize=kwargs.pop("filesize", "1M")
        )
        # Create a pod
        log.info(f"Creating Pod with pvc {self.pvc_obj.name}")

        try:
            self.pod_object = helpers.create_pod(
                pvc_name=self.pvc_obj.name,
                namespace=self.namespace,
                interface_type=self.interface,
                pod_name="pod-pas-test",
                pod_dict_path=self.pod_yaml_file.name,
                **kwargs,
            )
        except Exception as e:
            log.exception(
                f"Pod attached to PVC {self.pod_object.name} was not created, exception [{str(e)}]"
            )
            raise PodNotCreated("Pod attached to PVC was not created.")

        # Confirm that pod is running on the selected_nodes
        log.info("Checking whether the pod is running")
        helpers.wait_for_resource_state(
            resource=self.pod_object,
            state=constants.STATUS_COMPLETED,
            timeout=600,
        )

    def cleanup_testing_pod(self):
        try:
            self.pod_object.delete()
            # Wait for the POD to be deleted
            performance_lib.wait_for_resource_bulk_status(
                "pod", 0, self.namespace, constants.STATUS_RUNNING, 60, 5
            )
            log.info("The POD was deleted successfully")
        except Exception:
            log.warning("The POD failed to delete")
            pass
