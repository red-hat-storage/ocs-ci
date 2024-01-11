"""
Testing the Elasticsearch server deployment

"""
# Internal modules
import logging
import time

# 3rd party modules
from elasticsearch import Elasticsearch, exceptions as esexp

# Local modules
from ocs_ci.framework.pytest_customization.marks import libtest
from ocs_ci.helpers.helpers import get_full_test_logs_path
from ocs_ci.helpers.performance_lib import run_command
from ocs_ci.ocs import benchmark_operator, constants, defaults
from ocs_ci.ocs.elasticsearch import ElasticSearch, elasticsearch_load
from ocs_ci.ocs.exceptions import ElasticSearchNotDeployed
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.utility import templating
from ocs_ci.utility.utils import TimeoutSampler

log = logging.getLogger(__name__)


@libtest
class TestElasticsearch:
    """
    Testing the ElasticjSearch module used by ocs-ci

    """

    def setup(self):
        """
        Initialize the test by deploying a benchmark-operator

        """

        # Deploy the benchmark operator
        log.info("Apply Operator CRD")
        self.operator = benchmark_operator.BenchmarkOperator()
        self.operator.deploy()

    def teardown(self):
        """
        Clean up the cluster from the ES and Benchmark-Operator

        """
        self.es.cleanup()
        self.operator.cleanup()

    def smallfile_run(self, es):
        """
        Run the smallfiles workload so the elasticsearch server will have some data
        in it for copy

        Args:
            es (Elasticsearch): elastic search object

        Returns:
            str: the UUID of the test

        """

        # Loading the main template yaml file for the benchmark and update some
        # fields with new values
        sf_data = templating.load_yaml(constants.SMALLFILE_BENCHMARK_YAML)

        # Setting up the parameters for this test
        sf_data["spec"]["elasticsearch"]["server"] = es.get_ip()
        sf_data["spec"]["elasticsearch"]["port"] = es.get_port()
        sf_data["spec"]["elasticsearch"][
            "url"
        ] = f"http://{es.get_ip()}:{es.get_port()}"

        sf_data["spec"]["workload"]["args"]["samples"] = 1
        sf_data["spec"]["workload"]["args"]["operation"] = ["create"]
        sf_data["spec"]["workload"]["args"]["file_size"] = 4
        sf_data["spec"]["workload"]["args"]["files"] = 500000
        sf_data["spec"]["workload"]["args"]["threads"] = 4
        sf_data["spec"]["workload"]["args"][
            "storageclass"
        ] = constants.DEFAULT_STORAGECLASS_RBD
        sf_data["spec"]["workload"]["args"]["storagesize"] = "100Gi"

        # deploy the smallfile workload
        log.info("Running SmallFile bench")
        sf_obj = OCS(**sf_data)
        sf_obj.create()

        # wait for benchmark pods to get created - takes a while
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
        bench_pod.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            resource_name=small_file_client_pod,
            sleep=30,
            timeout=600,
        )
        for item in bench_pod.get()["items"][1]["spec"]["volumes"]:
            if "persistentVolumeClaim" in item:
                break
        uuid = self.operator.get_uuid(small_file_client_pod)
        timeout = 600
        while timeout >= 0:
            logs = bench_pod.get_logs(name=small_file_client_pod)
            if "RUN STATUS DONE" in logs:
                break
            timeout -= 30
            if timeout == 0:
                raise TimeoutError("Timed out waiting for benchmark to complete")
            time.sleep(30)
        return uuid

    def test_elasticsearch(self):
        """
        This test do the following operations:

            * deploy the elasticsearch module
            * connect to it
            * run a simple SmallFile benchmark (to verify usability)
            * dump the results to a file
            * push the results from the file to the Dev. ES.
            * teardown the environment

        """

        log.info("Test with 'Dummy' Storageclass")
        try:
            self.es = ElasticSearch(sc="dummy")
        except ElasticSearchNotDeployed:
            log.info("Raised as expected !")

        log.info("Test with 'Real' Storageclass")
        try:
            self.es = ElasticSearch()
        except ElasticSearchNotDeployed as ex:
            log.error("Raise as expected !")
            raise ex

        full_log_path = get_full_test_logs_path(cname=self)
        log.info(f"Logs file path name is : {full_log_path}")
        log.info("The ElasticSearch deployment test started.")
        if self.es.get_health():
            log.info("The Status of the elasticsearch is OK")
        else:
            log.warning("The Status of the elasticsearch is Not OK")
            log.info("Waiting another 30 sec.")
            time.sleep(30)
            if self.es.get_health():
                log.info("The Status of the elasticsearch is OK")
            else:
                log.error("The Status of the elasticsearch is Not OK ! Exiting.")

        if self.es.get_health():
            log.info("\nThe Elastic-Search server information :\n")
            log.info(f"The Elasticsearch IP is {self.es.get_ip()}")
            log.info(f"The Elasticsearch port is {self.es.get_port()}")
            log.info(f"The Password to connect is {self.es.get_password()}")

        else:
            assert False, "The Elasticsearch module is not ready !"

        log.info(f"Test UUDI is : {self.smallfile_run(self.es)}")

        assert self.es.dumping_all_data(full_log_path), "Can not Retrieve the test data"

        assert run_command(
            f"ls {full_log_path}/FullResults.tgz"
        ), "Results file did not retrieve from pod"

        # Try to use the development ES server for testing the elasticsearch_load
        # function to push data into ES server
        try:
            url = (
                f"{defaults.ELASTICSEARCE_SCHEME}://{defaults.ELASTICSEARCH_DEV_IP}"
                f":{defaults.ELASTICSEARCE_PORT}"
            )
            main_es = Elasticsearch(
                [
                    {
                        "host": defaults.ELASTICSEARCH_DEV_IP,
                        "port": defaults.ELASTICSEARCE_PORT,
                        "scheme": defaults.ELASTICSEARCE_SCHEME,
                        "url": url,
                    }
                ]
            )
        except esexp.ConnectionError:
            log.warning("Cannot connect to ES server in the LocalServer")
            main_es = None
        assert elasticsearch_load(
            main_es, full_log_path
        ), "Can not load data into Main ES server"
