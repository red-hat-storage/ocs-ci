"""
Basic Module to manage performance results

"""

import logging
import json
import time

from elasticsearch import Elasticsearch, exceptions as ESExp
from ocs_ci.ocs.defaults import ELASTICSEARCE_SCHEME

log = logging.getLogger(__name__)


class PerfResult:
    """
    Basic Performance results object for Q-PAS team

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

        self.uuid = uuid

        # Initialize the Elastic-search server parameters
        self.server = crd["spec"]["elasticsearch"]["server"]
        self.port = crd["spec"]["elasticsearch"]["port"]
        self.scheme = crd["spec"]["elasticsearch"].get("scheme", ELASTICSEARCE_SCHEME)
        self.index = None  # place holder for the ES index name
        self.new_index = None  # place holder for the ES full result index name
        self.all_results = {}
        self.es = None  # place holder for the elastic-search connection

        # Creating full results dictionary
        self.results = {"clustername": crd["spec"]["clustername"], "uuid": uuid}

    def es_connect(self):
        """
        Create Elastic-Search server connection

        """

        # Creating the connection to the elastic-search
        log.info(f"Connecting to ES {self.server} on port {self.port}")
        try:
            self.es = Elasticsearch(
                [
                    {
                        "host": self.server,
                        "port": self.port,
                        "scheme": self.scheme,
                    }
                ]
            )
        except ESExp.ConnectionError:
            log.warning(
                "Cannot connect to ES server {}:{}".format(self.server, self.port)
            )

        # Testing the connection to the elastic-search
        if not self.es.ping():
            log.warning(
                "Cannot connect to ES server {}:{}".format(self.server, self.port)
            )

    def es_read(self):
        """
        Reading all test results from the elastic-search server

        Return:
            list: list of results

        Assert:
            if no data found in the server

        """

        query = {"query": {"match": {"uuid": self.uuid}}}
        results = self.es.search(index=self.index, body=query)
        assert results["hits"]["hits"], "Results not found in Elasticsearch"
        return results["hits"]["hits"]

    def dump_to_file(self):
        """
        Writing the test results data into a JSON file, which can be loaded
        into the ElasticSearch server

        """
        json_file = f"{self.full_log_path}/full_results.json"
        self.add_key("index_name", self.new_index)
        log.info(f"Dumping data to {json_file}")
        with open(json_file, "w") as outfile:
            json.dump(self.results, outfile, indent=4)

    def es_write(self):
        """
        Writing the results to the elastic-search server, and to a JSON file

        """

        # Adding the results to the ES document and JSON file
        self.add_key("all_results", self.all_results)
        log.debug(json.dumps(self.results, indent=4))
        self.dump_to_file()
        if self.es is None:
            log.warning("No elasticsearch server to write data to")
            return False

        log.info(f"Writing all data to ES server {self.es}")
        log.info(f"Params : index={self.new_index} body={self.results}, id={self.uuid}")
        retry = 3
        while retry > 0:
            try:
                self.es.index(
                    index=self.new_index,
                    body=self.results,
                    id=self.uuid,
                )
                return True
            except Exception as e:
                if retry > 1:
                    log.warning("Failed to write data to ES, retrying in 3 sec...")
                    retry -= 1
                    time.sleep(3)
                else:
                    log.warning(f"Failed writing data with : {e}")
                    return False
        return True

    def add_key(self, key, value):
        """
        Adding (key and value) to this object results dictionary as a new
        dictionary.

        Args:
            key (str): String which will be the key for the value
            value (*): value to add, can be any kind of data type

        """

        self.results.update({key: value})

    def results_link(self):
        """
        Create a link to the results of the test in the elasticsearch serer

        Return:
            str: http link to the test results in the elastic-search server

        """

        res_link = f"{self.scheme}://{self.server}:{self.port}/{self.new_index}/"
        res_link += f'_search?q=uuid:"{self.uuid}"'
        return res_link


class ResultsAnalyse(PerfResult):
    """
    This class generates results for all tests as one unit
    and saves them to an elastic search server on the cluster

    """

    def __init__(self, uuid, crd, full_log_path, index_name):
        """
        Initialize the object by reading some of the data from the CRD file and
        by connecting to the ES server and read all results from it.

        Args:
            uuid (str): the unique uid of the test
            crd (dict): dictionary with test parameters - the test yaml file
                        that modify it in the test itself.
            full_log_path (str): the path of the results files to be found
            index_name (str): index name in ES
        """
        super(ResultsAnalyse, self).__init__(uuid, crd)
        self.new_index = index_name
        self.full_log_path = full_log_path
        # make sure we have connection to the elastic search server
        self.es_connect()
