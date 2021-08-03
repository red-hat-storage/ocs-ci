"""
Deploying an Elasticsearch server for collecting logs from ripsaw benchmarks.
Interface for the Performance ElasticSearch server

"""
import logging
import base64
import time
import json

from elasticsearch import Elasticsearch, helpers, exceptions as esexp

from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.helpers.performance_lib import run_command

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
        This function will return a list of docs stored in a text file

        Args:
            json_file (str): the file name to look for docs in

        Returns:
             list : list of documents as json dicts

        """

        docs = [
            l.strip() for l in open(str(json_file), encoding="utf8", errors="ignore")
        ]
        log.info(f"String docs length: {len(docs)}")
        doc_list = []

        for num, doc in enumerate(docs):
            try:
                dict_doc = json.loads(doc)
                doc_list += [dict_doc]
            except json.decoder.JSONDecodeError as err:
                # print the errors
                log.error(
                    f"ERROR for num: {num} -- JSONDecodeError: {err} for doc: {doc}"
                )

        log.info(f"Dict docs length: {len(doc_list)}")
        return doc_list

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
                docs_list = get_data_from_text_file(file_name)

                try:
                    log.info(
                        "Attempting to index the list of docs using helpers.bulk()"
                    )
                    resp = helpers.bulk(connection, docs_list, index=ind_name)
                    log.info(f"helpers.bulk() RESPONSE: {resp}")
                except Exception as err:
                    log.error(f"Elasticsearch helpers.bulk() ERROR:{err}")
        return True


class ElasticSearch(object):
    """
    ElasticSearch Environment
    """

    def __init__(self):
        """
        Initializer function

        """
        log.info("Initializing the Elastic-Search environment object")
        self.namespace = "elastic-system"
        # self.eck_file = "ocs_ci/templates/app-pods/eck.1.3.1-all-in-one.yaml"
        self.eck_file = "ocs_ci/templates/app-pods/eck.1.6.0-all-in-one.yaml"
        self.dumper_file = "ocs_ci/templates/app-pods/esclient.yaml"
        self.pvc = "ocs_ci/templates/app-pods/es-pvc.yaml"
        self.crd = "ocs_ci/templates/app-pods/esq.yaml"

        # Creating some different types of OCP objects
        self.ocp = OCP(
            kind="pod", resource_name="elastic-operator-0", namespace=self.namespace
        )
        self.ns_obj = OCP(kind="namespace", namespace=self.namespace)
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
        self._deploy_es()

        # Verify that ES is Up & Running
        timeout = 600
        while timeout > 0:
            if self.get_health():
                log.info("The ElasticSearch server is ready !")
                break
            else:
                log.warning("The ElasticSearch server is not ready yet")
                log.info("going to sleep for 30 sec. before next check")
                time.sleep(30)
                timeout -= 30

        self._deploy_data_dumper_client()

        # Connect to the server
        self.con = self._es_connect()

    def _deploy_eck(self):
        """
        Deploying the ECK environment for the Elasticsearch, and make sure it
        is in Running mode

        """

        log.info("Deploying the ECK environment for the ES cluster")
        self.ocp.apply(self.eck_file)

        for es_pod in TimeoutSampler(
            300, 10, get_pod_name_by_pattern, "elastic-operator", self.namespace
        ):
            try:
                if es_pod[0] is not None:
                    self.eckpod = es_pod[0]
                    log.info(f"The ECK pod {self.eckpod} is ready !")
                    break
            except IndexError:
                log.info("ECK operator pod not ready yet")

    def _deploy_data_dumper_client(self):
        """
        Deploying elastic search client pod with utility which dump all the data
        from the server to .tgz file

        """

        log.info("Deploying the es client for dumping all data")
        self.ocp.apply(self.dumper_file)

        for dmp_pod in TimeoutSampler(
            300, 10, get_pod_name_by_pattern, "es-dumper", self.namespace
        ):
            try:
                if dmp_pod[0] is not None:
                    self.dump_pod = dmp_pod[0]
                    log.info(f"The dumper client pod {self.dump_pod} is ready !")
                    break
            except IndexError:
                log.info("Dumper pod not ready yet")

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

    def _deploy_es(self):
        log.info("Deploy the PVC for the ElasticSearch cluster")
        self.ocp.apply(self.pvc)

        log.info("Deploy the ElasticSearch cluster")
        self.ocp.apply(self.crd)

        for es_pod in TimeoutSampler(
            300, 20, get_pod_name_by_pattern, "quickstart-es-default", self.namespace
        ):
            try:
                if es_pod[0] is not None:
                    self.espod = es_pod[0]
                    log.info(f"The ElasticSearch pod {self.espod} Started")
                    break
            except IndexError:
                log.info("elasticsearch pod not ready yet")

        es_pod = OCP(kind="pod", namespace=self.namespace)
        log.info("Waiting for ElasticSearch to Run")
        assert es_pod.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            resource_name=self.espod,
            sleep=30,
            timeout=600,
        )
        log.info("Elastic Search is ready !!!")

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
        Cleanup the environment from all Elasticsearch components, and from the
        port forwarding process.

        """
        log.info("Teardown the Elasticsearch environment")
        log.info("Deleting all resources")
        log.info("Deleting the dumper client pod")
        self.ocp.delete(yaml_file=self.dumper_file)
        log.info("Deleting the es resource")
        self.ocp.delete(yaml_file=self.crd)
        log.info("Deleting the es project")
        self.ns_obj.delete_project(project_name=self.namespace)
        self.ns_obj.wait_for_delete(resource_name=self.namespace, timeout=180)

    def _es_connect(self):
        """
        Create a connection to the local ES

        Returns:
            Elasticsearch: elasticsearch connection object, None if Cannot connect to ES

        """
        try:
            es = Elasticsearch([{"host": self.get_ip(), "port": self.get_port()}])
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

    def _copy(self, es):
        """
        Copy All data from the internal ES server to the main ES.

        **This is deprecated function** , use the dump function, and load
        the data from the files for the main ES server

        Args:
            es (obj): elasticsearch object which connected to the main ES
        """

        query = {"size": 1000, "query": {"match_all": {}}}
        for ind in self.get_indices():
            log.info(f"Reading {ind} from internal ES server")
            try:
                result = self.con.search(index=ind, body=query)
            except esexp.NotFoundError:
                log.warning(f"{ind} Not found in the Internal ES.")
                continue

            log.debug(f"The results from internal ES for {ind} are :{result}")
            log.info(f"Writing {ind} into main ES server")
            for doc in result["hits"]["hits"]:
                log.debug(f"Going to write : {doc}")
                es.index(index=ind, doc_type="_doc", body=doc["_source"])

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
