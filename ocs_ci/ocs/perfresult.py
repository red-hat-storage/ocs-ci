"""
Basic Module to manage performance results

"""

import logging

from elasticsearch import Elasticsearch, exceptions as ESExp

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
            self.es = Elasticsearch([{"host": self.server, "port": self.port}])
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

    def es_write(self):
        """
        Writing the results to the elastic-search server

        Raise:
            RequestError: in case of error writing data to the server

        """

        if self.es is None:
            log.warning("No elasticsearch server to write data to")
            return False

        log.info(f"Writing all data to ES server {self.es}")
        self.add_key("all_results", self.all_results)
        log.debug(
            f"Params : index={self.new_index}, "
            f"doc_type=_doc, body={self.results}, id={self.uuid}"
        )
        try:
            self.es.index(
                index=self.new_index, doc_type="_doc", body=self.results, id=self.uuid
            )
        except ESExp.RequestError as e:
            log.warning(f"Failed writhing data with {e}")
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

        res_link = f"http://{self.server}:{self.port}/{self.new_index}/"
        res_link += f"_search?q=uuid:{self.uuid}"
        return res_link
