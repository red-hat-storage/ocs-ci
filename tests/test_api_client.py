'''
A test program for instantiating an api-client and
perform a basic functionality check using api-client
'''


import logging
import yaml
import os


from ocs import api_client as ac
from ocsci.enums import TestStatus


log = logging.getLogger(__name__)


def create_simple_service(client):
    """
    Create a simple openshift service

    Args:
        client(APIClient): api-client object

    Returns:
        TestStatus(enum): PASSED or FAILED
    """

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
    try:
        res = client.create_service(body=service_data, namespace='default')
        log.info(res)
    except Exception as e:
        log.error(e)
        return TestStatus.FAILED

    log.info(f"Created service: {res['metadata']['name']}")
    return TestStatus.PASSED


def run():
    """
    A simple function to exercise a resource creation through api-client
    """
    client = ac.get_api_client("OCRESTClient")
    log.info(f"Using api-client {client.name}")
    return create_simple_service(client)

