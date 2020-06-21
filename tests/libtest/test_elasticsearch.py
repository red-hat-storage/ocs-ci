"""
Testing the Elasticsearch server deployment

"""
import logging
import pytest
import time
from ocs_ci.ocs.elasticsearch import ElasticSearch
from elasticsearch import (Elasticsearch, exceptions as esexp)
from subprocess import run
log = logging.getLogger(__name__)


@pytest.fixture(scope='function')
def es(request):
    def teardown():
        es.cleanup()

    request.addfinalizer(teardown)

    es = ElasticSearch()

    return es


class Test_Elasticsearch():

    def curl_run(self, es, auth=True):

        if auth:
            con_string = f'elastic:{es.get_password()}@'
            msg = 'with authentication'
        else:
            con_string = ''
            msg = 'without authentication'

        log.info(f"\nTesting the Local server Connecting {msg}")

        try:
            if auth:
                test_es = Elasticsearch([{'host': 'localhost',
                                          'port': es.get_port()}],
                                        http_auth=('elastic', es.get_password()))
            else:
                test_es = Elasticsearch([{'host': 'localhost',
                                          'port': es.get_port()}])
        except esexp.ConnectionError:
            log.error(
                'can not connect to ES server {}:{} {}'.format(
                    es.get_ip, es.get_port, msg))
            raise

        log.info(f'testing ES object is {test_es}')
        server_string = f'localhost:{es.get_port()}"'
        curl_cmd = 'curl "http://'

        log.info(f'Going to run : {curl_cmd}{con_string}{server_string}')
        log.info(run(f'{curl_cmd}{con_string}{server_string}',
                     shell=True,
                     capture_output=True))

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

            self.curl_run(es, auth=True)

            self.curl_run(es, auth=False)

        else:
            assert False, ('The Elasticsearch module is not ready !')
