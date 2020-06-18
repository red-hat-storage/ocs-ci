from ocs_ci.framework import config
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs import constants
from tests.helpers import storagecluster_independent_check


class RGW(object):
    """
    Wrapper class for interaction with a cluster's RGW service
    """

    def __init__(self, namespace=None):
        self.namespace = namespace if namespace else config.ENV_DATA['cluster_namespace']

        if storagecluster_independent_check():
            sc_name = constants.INDEPENDENT_DEFAULT_STORAGECLASS_RGW
        else:
            sc_name = constants.DEFAULT_STORAGECLASS_RGW

        self.storageclass = OCP(
            kind='storageclass', namespace=namespace,
            resource_name=sc_name
        )
        self.s3_endpoint = self.storageclass.get().get('parameters').get('endpoint')
        self.region = self.storageclass.get().get('parameters').get('region')
        # Todo: Implement retrieval in cases where CephObjectStoreUser is available
        self.key_id = None
        self.secret_key = None
        self.s3_resource = None
