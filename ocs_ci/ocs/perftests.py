# import pytest
import time
import logging
import re

from elasticsearch import Elasticsearch

from ocs_ci.framework.testlib import BaseTest

from ocs_ci.ocs import defaults, constants, node
from ocs_ci.ocs import benchmark_operator
from ocs_ci.framework import config
from ocs_ci.ocs.version import get_environment_info
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.utility.utils import TimeoutSampler, get_running_cluster_id
from ocs_ci.ocs.elasticsearch import elasticsearch_load
from ocs_ci.ocs.resources import pod
from ocs_ci.helpers.performance_lib import run_oc_command

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

        """
        log.info("Setting up test environment")
        self.crd_data = None  # place holder for Benchmark CDR data
        self.es_backup = None  # place holder for the elasticsearch backup
        self.main_es = None  # place holder for the main elasticsearch object
        self.benchmark_obj = None  # place holder for the benchmark object
        self.client_pod = None  # Place holder for the client pod object
        self.dev_mode = config.RUN["cli_params"].get("dev_mode")
        self.pod_obj = OCP(kind="pod", namespace=benchmark_operator.BMO_NAME)

        # Collecting all Environment configuration Software & Hardware
        # for the performance report.
        self.environment = get_environment_info()
        self.environment["clusterID"] = get_running_cluster_id()

        self.get_osd_info()

        self.get_node_info(node_type="master")
        self.get_node_info(node_type="worker")

    def teardown(self):
        if hasattr(self, "operator"):
            self.operator.cleanup()

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

        oc_cmd = OCP(namespace=defaults.ROOK_CLUSTER_NAMESPACE)
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

    def ripsaw_deploy(self, ripsaw):
        """
        Deploy the benchmark operator (formally ripsaw) CRD

        Args:
            ripsaw (obj): benchmark operator object

        """
        log.info("Deploying benchmark operator (ripsaw)")
        ripsaw.apply_crd("resources/crds/ripsaw_v1alpha1_ripsaw_crd.yaml")

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
        self.start_time = time.strftime("%Y-%m-%dT%H:%M:%SGMT", time.gmtime())

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

        """
        log.info(f"Waiting for {self.client_pod_name} to complete")

        Finished = 0
        restarts = 0
        while not Finished:
            results = run_oc_command(
                "get pod --no-headers -o custom-columns=:metadata.name,:status.phase",
                namespace=benchmark_operator.BMO_NAME,
            )
            fname = ""
            for name in results:
                if re.search(self.client_pod_name, name):
                    (fname, status) = name.split()
                    continue
            if not fname == self.client_pod:
                log.info(
                    f"The pod {self.client_pod} was restart. the new client pod is {fname}"
                )
                self.client_pod = fname
                restarts += 1
            if restarts > 3:
                err_msg = f"Too much restarts of the benchmark ({restarts})"
                log.error(err_msg)
                raise Exception(err_msg)
            if status == "Succeeded":
                self.end_time = time.strftime("%Y-%m-%dT%H:%M:%SGMT", time.gmtime())
                self.test_logs = self.pod_obj.exec_oc_cmd(
                    f"logs {self.client_pod}", out_yaml_format=False
                )
                log.info(f"{self.client_pod} completed successfully")
                Finished = 1
            else:
                log.info(
                    f"{self.client_pod} is in {status} State, and wait to Succeeded State."
                    f" wait another {sleep} sec. for benchmark to complete"
                )
                time.sleep(sleep)

        self.pod_obj.wait_for_resource(
            condition=constants.STATUS_COMPLETED,
            resource_name=self.client_pod,
            timeout=timeout,
            sleep=sleep,
        )

        # Getting the end time of the benchmark - for reporting.
        self.end_time = time.strftime("%Y-%m-%dT%H:%M:%SGMT", time.gmtime())
        self.test_logs = self.pod_obj.exec_oc_cmd(
            f"logs {self.client_pod}", out_yaml_format=False
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
