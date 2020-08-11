from ocs_ci.framework import config
from ocs_ci.ocs.ocp import OCP
from tests.helpers import retrieve_rgw_storageclass_name


class RGW(object):
    """
    Wrapper class for interaction with a cluster's RGW service
    """

    def __init__(self, namespace=None):
        self.namespace = namespace if namespace else config.ENV_DATA['cluster_namespace']
        self.storageclass = OCP(
            kind='storageclass', namespace=namespace,
            resource_name=retrieve_rgw_storageclass_name()
        )
        self.s3_internal_endpoint = self.storageclass.get().get('parameters').get('endpoint')
        self.region = self.storageclass.get().get('parameters').get('region')
        # Todo: Implement retrieval in cases where CephObjectStoreUser is available
        self.key_id = None
        self.secret_key = None
        self.s3_resource = None
