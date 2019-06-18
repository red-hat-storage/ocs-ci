"""
A module for all rook functionalities and abstractions.

This module has rook related classes, support for functionalities to work with
rook cluster. This works with assumptions that an OCP cluster is already
functional and proper configurations are made for interaction.
"""

import logging
import os

import oc.openshift_ops as ac
import resources.pod as pod
from ocs import constants, defaults
from utility.templating import generate_yaml_from_jinja2_template_with_data
from ocsci import config

logger = logging.getLogger(__name__)


class Rook(object):
    """
    High level rook abstraction. This class should handle operator
    and cluster objects.

    As of now deligated rook environment to corresponding cluster object
    but in future rookenv should be handled here.Also few operator related ops
    will be included in this class. Keeping this class lightweight for the first
    cut.

    Attrs:
        self._cluster (RookCluster): current cluster object
    """

    def __init__(self, **config):
        self._cluster = RookCluster(**config)

    @property
    def cluster(self):
        return self._cluster


class RookCluster(object):
    """
    Handles all cluster related operations from ceph perspective

    This class has depiction of ceph cluster. Contains references to
    pod objects which represents ceph cluster entities.

    Attributes:
        _ocs_pods (list) : A list of  ceph cluster related pods
        _cluster_name (str): Name of ceph cluster
        _namespace (str): openshift Namespace where this cluster lives
        _api_client (APIClient): api-client used for interacting with openshift
        _url_prefix (str): url prefix for REST api interaction
    """

    def __init__(self, **config):
        # cluster_name is name of cluster in rook of type CephCluster
        self._cluster_name = config.get('cluster_name', 'rook-ceph')
        """
        TODO: Instantiate rook object somewhere higher after deployment
        Read namespace and cluster name from config.
        """
        self._namespace = config.get(
            'namespace', config.ENV_DATA['cluster_namespace']
        )
        # Keeping api_client_name for upcoming PR
        self._api_client_name = config.get('api_client_name', 'OCRESTClient')
        self.rook_crd_ver = config.get('rook_crd_ver', 'v1')
        self._api_client = None         # APIClient object
        self._url_prefix = config.get(
            'url_prefix',
            f'/apis/ceph.rook.io/{self.rook_crd_ver}'
            f'/namespaces/{self._namespace}/'
        )
        self._ocs_pods = pod.get_all_pods(self._namespace)
        self._api_client = ac.OCP()  # TODO: APIClient abstractions

    @property
    def cluster_name(self):
        return self._cluster_name

    @property
    def namespace(self):
        return self._namespace

    @property
    def pods(self):
        return self._ocs_pods

    def create_cephblockpool(
        self,
        cephblockpool_name,
        namespace,
        service_cbp,
        failureDomain,
        replica_count
    ):
        """
        Creates cephblock pool

        Args:
            cephblockpool_name (str): Name of cephblockpool
            namespace (str): Namespace to create cephblockpool
            service_cbp (class):  Dynamic client resource of kind cephblockpool
            failureDomain (str): The failure domain across which the
                                   replicas or chunks of data will be spread
            replica_count (int): The number of copies of the data in the pool.

        Returns:
            bool : True if cephblockpool created sucessfully

        Raises:
            Exception when error occured

        Examples:
            create_cephblockpool(
                cephblockpool_name",
                service_cbp,
                failureDomain="host",
                replica_count=3
            )

        """
        _rc = False
        template_path = os.path.join(
            constants.TEMPLATE_DEPLOYMENT_DIR,
            "cephblockpool.yaml"
        )
        # overwrite the namespace with openshift-storage, since cephblockpool
        # is tied-up with openshift-storage
        namespace = config.ENV_DATA['cluster_namespace']

        cephblockpool_data = {}
        cephblockpool_data['cephblockpool_name'] = cephblockpool_name
        cephblockpool_data['rook_api_version'] = defaults.ROOK_API_VERSION
        cephblockpool_data['failureDomain'] = failureDomain
        cephblockpool_data['replica_count'] = replica_count

        data = generate_yaml_from_jinja2_template_with_data(
            template_path,
            **cephblockpool_data
        )
        try:
            service_cbp.create(body=data, namespace=namespace)
            _rc = True
        except Exception as err:
            logger.error(
                "Error while creating cephblockpool %s", cephblockpool_name
            )
            raise Exception(err)

        return _rc
