'''
A test program for instantiating an api-client and
perform a basic functionality check using api-client
'''


import logging
import yaml

from ocs_ci.ocs import api_client as ac
from ocs_ci.framework.testlib import libtest

log = logging.getLogger(__name__)


@libtest
def test_create_simple_service():
    """
    Create a simple openshift service

    Args:
        client(APIClient): api-client object

    """
    client = ac.get_api_client("OCRESTClient")
    log.info(f"Using api-client {client.name}")
    # For brevity, having inline service yaml
    # TODO: fetch from templates
    service = """
    kind: Service
    apiVersion: v1
    metadata:
      name: myservice
    spec:
      selector:
        app: MyApp
      ports:
        - protocol: TCP
          port: 8089
          targetPort: 9369
    """

    service_data = yaml.safe_load(service)
    res = client.create_service(body=service_data, namespace='default')
    log.info(res)
    log.info(f"Created service: {res['metadata']['name']}")
