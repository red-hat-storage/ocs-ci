"""
Deploying en Elasticsearch server for collecting logs from ripsaw benchmarks

"""
import os
import logging
import tempfile
import urllib
import urllib.error
import base64
import signal
import subprocess
import time

from elasticsearch import (Elasticsearch, exceptions as esexp)

from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.utility.utils import TimeoutSampler

log = logging.getLogger(__name__)


class ElasticSearch(object):
    """
      ElasticSearch Environment
    """

    def __init__(self):
        """
        Initializer function

        """
        log.info('Initializing the Elastic-Search environment object')
        self.namespace = "elastic-system"
        self.eck_path = "https://download.elastic.co/downloads/eck/1.1.2"
        self.eck_file = "all-in-one.yaml"
        self.pvc = "ocs_ci/templates/app-pods/es-pvc.yaml"
        self.crd = "ocs_ci/templates/app-pods/esq.yaml"
        self.lspid = None

        # Creating some different types of OCP objects
        self.ocp = OCP(
            kind="pod",
            resource_name="elastic-operator-0",
            namespace=self.namespace
        )
        self.ns_obj = OCP(kind='namespace', namespace=self.namespace)
        self.es = OCP(
            resource_name="quickstart-es-http", namespace=self.namespace
        )
        self.elasticsearch = OCP(namespace=self.namespace, kind='elasticsearch')
        self.password = OCP(
            kind='secret',
            resource_name='quickstart-es-elastic-user',
            namespace=self.namespace
        )

        # Fetch the all-in-one.yaml from the official repository
        self._get_eck_file()
        # Deploy the ECK all-in-one.yaml file
        self._deploy_eck()
        # Deploy the Elastic-Search server
        self._deploy_es()

        # Verify that ES is Up & Running
        timeout = 600
        while timeout > 0:
            if self.get_health():
                log.info('The ElasticSearch server is ready !')
                break
            else:
                log.warning('The ElasticSearch server is not ready yet')
                log.info('going to sleep for 30 sec. before next check')
                time.sleep(30)
                timeout -= 30

        # Starting LocalServer process - port forwarding
        self.local_server()

        # Connect to the server
        self.con = self._es_connect()

    def _get_eck_file(self):
        """
        Getting the ECK file from the official Elasticsearch web site and store
        it as a temporary file.

        Current version is 1.1.2, this need to be update with new versions,
        after testing it, and also it may need to update the CRD file (esq.yaml)
        with the new version as well.

        """

        self.dir = tempfile.mkdtemp(prefix='elastic-system_')
        src_file = f'{self.eck_path}/{self.eck_file}'
        trg_file = f'{self.dir}/{self.eck_file}'
        log.info(f'Retrieving the ECK CR file from {src_file} into {trg_file}')
        try:
            urllib.request.urlretrieve(src_file, trg_file)
        except urllib.error.HTTPError as e:
            log.error(f'Can not connect to {src_file} : {e}')
            raise e

    def _deploy_eck(self):
        """
        Deploying the ECK environment for the Elasticsearch, and make sure it
        is in Running mode

        """

        log.info('Deploying the ECK environment for the ES cluster')
        self.ocp.apply(f'{self.dir}/{self.eck_file}')

        for es_pod in TimeoutSampler(
            300, 10, get_pod_name_by_pattern, 'elastic-operator', self.namespace
        ):
            try:
                if es_pod[0] is not None:
                    self.eckpod = es_pod[0]
                    log.info(f'The ECK pod {self.eckpod} is ready !')
                    break
            except IndexError:
                log.info('ECK operator pod not ready yet')

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
        log.info('Deploy the PVC for the ElasticSearch cluster')
        self.ocp.apply(self.pvc)

        log.info('Deploy the ElasticSearch cluster')
        self.ocp.apply(self.crd)

        for es_pod in TimeoutSampler(
            300, 20, get_pod_name_by_pattern, 'quickstart-es-default', self.namespace
        ):
            try:
                if es_pod[0] is not None:
                    self.espod = es_pod[0]
                    log.info(f'The ElasticSearch pod {self.espod} Started')
                    break
            except IndexError:
                log.info('elasticsearch pod not ready yet')

        es_pod = OCP(kind='pod', namespace=self.namespace)
        log.info('Waiting for ElasticSearch to Run')
        assert es_pod.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            resource_name=self.espod,
            sleep=30,
            timeout=600
        )
        log.info('Elastic Search is ready !!!')

    def get_health(self):
        """
        This method return the health status of the Elasticsearch.

        Returns:
            bool : True if the status is green (OK) otherwise - False

        """
        return self.elasticsearch.get()['items'][0]['status']['health'] == 'green'

    def get_password(self):
        """
        This method return the password used to connect the Elasticsearch.

        Returns:
            str : The password as text

        """
        return base64.b64decode(self.password.get()['data']['elastic']).decode('utf-8')

    def cleanup(self):
        """
        Cleanup the environment from all Elasticsearch components, and from the
        port forwarding process.

        """
        log.info('Teardown the Elasticsearch environment')
        log.info(f'Killing the local server process ({self.lspid})')
        os.kill(self.lspid, signal.SIGKILL)
        log.info('Deleting all resources')
        subprocess.run(f'oc delete -f {self.crd}', shell=True)
        subprocess.run(f'oc delete -f {self.eck_file}', shell=True, cwd=self.dir)
        self.ns_obj.wait_for_delete(resource_name=self.namespace)

    def local_server(self):
        """
        Starting sub-process that will do port-forwarding, to allow access from
        outside the open-shift cluster into the Elasticsearch server.

        """
        cmd = f'oc -n {self.namespace } '
        cmd += f'port-forward service/quickstart-es-http {self.get_port()}'
        log.info(f'Going to run : {cmd}')
        proc = subprocess.Popen(cmd, shell=True)
        log.info(f'Starting LocalServer with PID of {proc.pid}')
        self.lspid = proc.pid

    def _es_connect(self):
        """
        Create a connection to the ES via the localhost port-fwd

        Returns:
            Elasticsearch: elasticsearch connection object
        """
        try:
            es = Elasticsearch([{'host': 'localhost', 'port': self.get_port()}])
        except esexp.ConnectionError:
            log.error('Can not connect to ES server in the LocalServer')
            raise
        return es

    def get_indices(self):
        """
        Getting list of all indexes in the ES server - all created by the test,
        the installation of the ES was without any indexes pre-installed.

        Returns:
            list : list of all indexes defined in the ES server

        """
        results = []
        log.info("Getting all indexes")
        for ind in self.con.indices.get_alias("*"):
            results.append(ind)
        return results

    def _copy(self, es):
        """
        Copy All data from the internal ES server to the main ES

        Args:
            es (obj): elasticsearch object which connected to the main ES

        """

        query = {'size': 1000, 'query': {'match_all': {}}}
        for ind in self.get_indices():
            log.info(f'Reading {ind} from internal ES server')
            try:
                result = self.con.search(index=ind, body=query)
            except esexp.NotFoundError:
                log.warning(f'{ind} Not found in the Internal ES.')
                continue

            log.debug(f'The results from internal ES for {ind} are :{result}')
            log.info(f'Writing {ind} into main ES server')
            for doc in result['hits']['hits']:
                log.debug(f'Going to write : {doc}')
                es.index(index=ind, doc_type='_doc', body=doc['_source'])
