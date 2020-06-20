"""
Testing the Elasticsearch server deployment

"""
import logging
import pytest
import time
from subprocess import run
from ocs_ci.ocs.elasticsearch import ElasticSearch

log = logging.getLogger(__name__)


@pytest.fixture(scope='function')
def es(request):
    def teardown():
        es.cleanup()

    request.addfinalizer(teardown)

    es = ElasticSearch()

    return es


class Test_Elasticsearch():

    def test_elasticsearch(self, es):
        """
        This test only deploy the elasticsearch module, connect to it with and
        without credentials and teardown the environment

        Args:
            es (fixture) : fixture that deploy / teardown the elasticsearch

        """

        log.info('The ElasticSearch deployment test started.')
        if es.get_health():
            log.info('The Status of the elasticsearch is OK')
        else:
            log.warning('The Status of the elasticsearch is Not OK')
            log.info('Waiting another 30 sec.')
            time.sleep(30)
            if es.get_health():
                log.info('The Status of the elasticsearch is OK')
            else:
                log.error('The Status of the elasticsearch is Not OK ! Exiting.')

        if es.get_health():
            log.info('\nThe Elastic-Search server information :\n')
            log.info(f'The Elasticsearch IP is {es.get_ip()}')
            log.info(f'The Elasticsearch port is {es.get_port()}')
            log.info(f'The Password to connect is {es.get_password()}')
            log.info(f'The local server PID is {es.lspid}')

            con_string = f'elastic:{es.get_password()}'
            server_string = f'localhost:{es.get_port()}"'
            curl_cmd = 'curl "http://'

            log.info("\nTesting the Local server Connecting with authentication")
            log.info(f'Going to run : {curl_cmd}{con_string}@{server_string}')
            log.info(run(f'{curl_cmd}{con_string}@{server_string}',
                         shell=True, capture_output=True))

            log.info("\nTesting the Local server Connecting without authentication")
            log.info(f'Going to run : {curl_cmd}{server_string}')
            log.info(run(f'{curl_cmd}{con_string}@{server_string}',
                         shell=True, capture_output=True))
