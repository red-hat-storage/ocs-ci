"""
Deploying an Elasticsearch server for collecting logs from benchmark-operator
(ripsaw) benchmarks.

Interface for the Performance ElasticSearch server

"""
# Internal modules
import base64
import json
import logging
import os
import tempfile

# 3rd party modules
from elasticsearch import Elasticsearch, helpers, exceptions as esexp
from subprocess import run, CalledProcessError

# Local modules
from ocs_ci.helpers.helpers import create_pvc, wait_for_resource_state
from ocs_ci.helpers.performance_lib import run_command
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    ResourceWrongStatusException,
    ElasticSearchNotDeployed,
)
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.utility.utils import TimeoutSampler

log = logging.getLogger(__name__)

# mute the elasticsearch logging
es_log = logging.getLogger("elasticsearch")
es_log.setLevel(logging.CRITICAL)


def elasticsearch_load(connection, target_path):
    """
    Load all data from target_path/results into an elasticsearch (es) server.

    Args:
        connection (obj): an elasticsearch connection object
        target_path (str): the path where data was dumped into

    Returns:
        bool: True if loading data succeed, False otherwise

    """

    # define a function that will load a text file
    def get_data_from_text_file(json_file):
        """
        This function will return a list of docs stored in a text file.
        the function is working as a generator, and return the records
        one at a time.

        Args:
            json_file (str): the file name to look for docs in

        Returns:
             list : list of documents as json dicts

        """

        docs = [
            l.strip() for l in open(str(json_file), encoding="utf8", errors="ignore")
        ]
        log.info(f"String docs length: {len(docs)}")

        for num, doc in enumerate(docs):
            try:
                dict_doc = json.loads(doc)
                yield dict_doc
            except json.decoder.JSONDecodeError as err:
                # print the errors
                log.error(
                    f"ERROR for num: {num} -- JSONDecodeError: {err} for doc: {doc}"
                )

    all_files = run_command(f"ls {target_path}/results/", out_format="list")
    if "Error in command" in all_files:
        log.error("There is No data to load into ES server")
        return False
    else:
        if connection is None:
            log.warning("There is no elasticsearch server to load data into")
            return False
        log.info(f"The ES connection is {connection}")
        for ind in all_files:
            if ".data." in ind:  # load only data files and not mapping info
                file_name = f"{target_path}/results/{ind}"
                ind_name = ind.split(".")[0]
                log.info(f"Loading the {ind} data into the ES server")

                try:
                    resp = helpers.bulk(
                        connection, get_data_from_text_file(file_name), index=ind_name
                    )
                    log.info(f"helpers.bulk() RESPONSE: {resp}")
                except Exception as err:
                    log.error(f"Elasticsearch helpers.bulk() ERROR:{err}")
        return True


class ElasticSearch(object):
    """
    ElasticSearch Environment
    """

    def __init__(self, **kwargs):
        """
        Initializer function

        """
        log.info("Initializing the Elastic-Search environment object")
        self.args = kwargs
        self.namespace = "elastic-system"
        self.repo = self.args.get("repo", constants.OCS_WORKLOADS)
        self.branch = self.args.get("branch", "master")
        self.dir = tempfile.mkdtemp(prefix="eck_")

        # Clone the ECK repo locally
        self._clone()

        self.eck_path = os.path.join(self.dir, "ocs-workloads/eck")
        self.eck_file = os.path.join(self.eck_path, "crds.yaml")
        self.dumper_file = os.path.join(constants.TEMPLATE_APP_POD_DIR, "esclient.yaml")
        self.crd = os.path.join(constants.TEMPLATE_APP_POD_DIR, "esq.yaml")

        # Creating some different types of OCP objects
        self.ocp = OCP(
            kind="pod", resource_name="elastic-operator-0", namespace=self.namespace
        )
        self.ns_obj = OCP(kind="namespace", namespace=self.namespace)

        # Verify that the namespace dose not exist, delete it if it is exists.
        if self.ns_obj.get(dont_raise=True, resource_name=self.namespace) is not None:
            log.warning(
                f"The {self.namespace} namespace is already exists!, try to delete it"
            )
            self.ns_obj.delete_project(project_name=self.namespace)
            self.ns_obj.wait_for_delete(
                resource_name=self.namespace, timeout=180, sleep=5
            )

        self.es = OCP(resource_name="quickstart-es-http", namespace=self.namespace)
        self.elasticsearch = OCP(namespace=self.namespace, kind="elasticsearch")
        self.password = OCP(
            kind="secret",
            resource_name="quickstart-es-elastic-user",
            namespace=self.namespace,
        )

        # Deploy the ECK all-in-one.yaml file
        self._deploy_eck()

        # Deploy the Elastic-Search server
        if not self._deploy_es():
            self.cleanup()
            raise ElasticSearchNotDeployed("Elasticsearch deployment Failed")

        # Verify that ES is Up & Running
        sample = TimeoutSampler(timeout=180, sleep=10, func=self.get_health)
        if not sample.wait_for_func_status(True):
            self.cleanup()
            raise ElasticSearchNotDeployed("Elasticsearch deployment Failed")

        # Deploy the elasticsearch dumper pod
        self._deploy_data_dumper_client()

        # Connect to the server
        self.con = self._es_connect()

    def _clone(self):
        """
        clone the ECK repo into temp directory

        """
        try:
            log.info(f"Cloning ECK in {self.dir}")
            git_clone_cmd = f"git clone -b {self.branch} {self.repo} --depth 1"
            run(git_clone_cmd, shell=True, cwd=self.dir, check=True)
        except (CommandFailed, CalledProcessError) as cf:
            log.error("Error during cloning of ECK repository")
            raise cf

    def _pod_is_found(self, pattern):
        """
        Boolean function which check if pod (by pattern) is exist.

        Args:
            pattern (str): the pattern of the pod to look for

        Returns:
            bool : True if pod found, otherwise False
        """
        return len(get_pod_name_by_pattern(pattern, self.namespace)) > 0

    def _deploy_eck(self):
        """
        Deploying the ECK environment for the Elasticsearch, and make sure it
        is in Running mode

        """

        log.info("Deploying the ECK environment for the ES cluster")
        log.info("Deploy the ECK CRD's")
        self.ocp.apply(self.eck_file)
        log.info("deploy the ECK operator")
        self.ocp.apply(f"{self.eck_path}/operator.yaml")
        sample = TimeoutSampler(
            timeout=300, sleep=10, func=self._pod_is_found, pattern="elastic-operator"
        )
        if not sample.wait_for_func_status(True):
            err_msg = "ECK deployment Failed"
            log.error(err_msg)
            self.cleanup()
            raise Exception(err_msg)

        log.info("The ECK pod is ready !")

    def _deploy_data_dumper_client(self):
        """
        Deploying elastic search client pod with utility which dump all the data
        from the server to .tgz file

        """

        log.info("Deploying the es client for dumping all data")
        self.ocp.apply(self.dumper_file)

        sample = TimeoutSampler(
            timeout=300, sleep=10, func=self._pod_is_found, pattern="es-dumper"
        )
        if not sample.wait_for_func_status(True):
            self.cleanup()
            raise Exception("Dumper pod deployment Failed")
        self.dump_pod = get_pod_name_by_pattern("es-dumper", self.namespace)[0]
        log.info(f"The dumper client pod {self.dump_pod} is ready !")

    def get_ip(self):
        """
        This function return the IP address of the Elasticsearch cluster.
        this IP is to use inside the OCP cluster

        Return
            str : String that represent the Ip Address.

        """
        return self.es.get()["spec"]["clusterIP"]

    def get_port(self):
        """
        This function return the port of the Elasticsearch cluster.

        Return
            str : String that represent the port.

        """
        return self.es.get()["spec"]["ports"][0]["port"]

    def get_scheme(self):
        """
        This function return the schema of the Elasticsearch cluster.

        Return
            str : String that represent the schema (http or https).

        """
        return self.es.get()["spec"]["ports"][0]["name"]

    def _deploy_es(self):
        """
        Deploying the Elasticsearch server

        """

        # Creating PVC for the elasticsearch server and wait until it bound
        log.info("Creating 10 GiB PVC for the ElasticSearch cluster on")
        try:
            self.pvc_obj = create_pvc(
                sc_name=self.args.get("sc") or constants.CEPHBLOCKPOOL_SC,
                namespace=self.namespace,
                pvc_name="elasticsearch-data-quickstart-es-default-0",
                access_mode=constants.ACCESS_MODE_RWO,
                size="10Gi",
            )

            # Make sure the PVC bound, or delete it and raise exception
            wait_for_resource_state(self.pvc_obj, constants.STATUS_BOUND)
        except ResourceWrongStatusException:
            log.error("The PVC couldn't created")
            return False

        self.pvc_obj.reload()

        log.info("Deploy the ElasticSearch cluster")
        self.ocp.apply(self.crd)

        sample = TimeoutSampler(
            timeout=300,
            sleep=10,
            func=self._pod_is_found,
            pattern="quickstart-es-default",
        )
        if not sample.wait_for_func_status(True):
            log.error("The ElasticSearch pod deployment Failed")
            return False

        self.espod = get_pod_name_by_pattern("quickstart-es-default", self.namespace)[0]
        log.info(f"The ElasticSearch pod {self.espod} Started")

        es_pod = OCP(kind="pod", namespace=self.namespace)
        log.info("Waiting for ElasticSearch to Run")
        if not es_pod.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            resource_name=self.espod,
            sleep=30,
            timeout=600,
        ):
            log.error("TThe ElasticSearch pod is not running !")
            return False
        else:
            log.info("Elastic Search is ready !!!")
            return True

    def get_health(self):
        """
        This method return the health status of the Elasticsearch.

        Returns:
            bool : True if the status is green (OK) otherwise - False

        """
        return self.elasticsearch.get()["items"][0]["status"]["health"] == "green"

    def get_password(self):
        """
        This method return the password used to connect the Elasticsearch.

        Returns:
            str : The password as text

        """
        return base64.b64decode(self.password.get()["data"]["elastic"]).decode("utf-8")

    def cleanup(self):
        """
        Cleanup the environment from all Elasticsearch components.

        """
        log.info("Teardown the Elasticsearch environment")
        log.info("Deleting all resources")
        try:
            log.info("Deleting the dumper client pod")
            self.ocp.delete(yaml_file=self.dumper_file)
        except CommandFailed:
            # in case of the es-dumper did not deployed yet, trying to delete it
            # will failed.
            log.warning("es-dumper pod does not exist")
            pass

        try:
            log.info("Deleting the es resource")
            self.ocp.delete(yaml_file=self.crd)
        except CommandFailed:
            # in case of the elastic-search did not deployed yet, trying to
            # delete it will failed.
            log.warning("elastic-search pod does not exist")
            pass

        log.info("Deleting the es project")
        # self.ns_obj.delete_project(project_name=self.namespace)
        self.ocp.delete(f"{self.eck_path}/operator.yaml")
        self.ocp.delete(yaml_file=self.eck_file)
        self.ns_obj.wait_for_delete(resource_name=self.namespace, timeout=180)

        log.info("The ElasticSearch cleaned up from the cluster")

    def _es_connect(self):
        """
        Create a connection to the local ES

        Returns:
            Elasticsearch: elasticsearch connection object, None if Cannot connect to ES

        """
        try:
            es = Elasticsearch(
                [
                    {
                        "host": self.get_ip(),
                        "port": self.get_port(),
                        "scheme": self.get_scheme(),
                    }
                ]
            )
        except esexp.ConnectionError:
            log.warning("Cannot connect to ES server in the LocalServer")
            es = None
        return es

    def get_indices(self):
        """
        Getting list of all indices in the ES server - all created by the test,
        the installation of the ES was without any indexes pre-installed.

        Returns:
            list : list of all indices defined in the ES server

        """
        results = []
        log.info("Getting all indices")
        for ind in self.con.indices.get_alias("*"):
            results.append(ind)
        return results

    def dumping_all_data(self, target_path):
        """
        Dump All data from the internal ES server to .tgz file.

        Args:
            target_path (str): the path where the results file will be copy into

        Return:
            bool: True if the dump operation succeed and return the results data to the host
                  otherwise False
        """

        log.info("dumping data from ES server to .tgz file")
        rsh_cmd = f"rsh {self.dump_pod} /elasticsearch-dump/esdumper.py --ip {self.get_ip()} --port {self.get_port()}"
        result = self.ocp.exec_oc_cmd(rsh_cmd, out_yaml_format=False, timeout=1200)
        if "ES dump is done." not in result:
            log.error("There is no data in the Elasticsearch server")
            return False
        else:
            src_file = result.split()[-1]
            log.info(f"Copy {src_file} from the client pod")

            cp_command = f"cp {self.dump_pod}:{src_file} {target_path}/FullResults.tgz"
            result = self.ocp.exec_oc_cmd(cp_command, timeout=120)
            log.info(f"The output from the POD is {result}")
            log.info("Extracting the FullResults.tgz file")
            kwargs = {"cwd": target_path}
            results = run_command(f"tar zxvf {target_path}/FullResults.tgz", **kwargs)
            log.debug(f"The untar results is {results}")
            if "Error in command" in results:
                log.warning("Cannot untar the dumped file")
                return False

        return True
