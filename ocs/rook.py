"""
A module for all rook functionalities and abstractions.

This module has rook related classes, support for functionalities to work with
rook cluster. This works with assumptions that an OCP cluster is already
functional and proper configurations are made for interaction.
"""

import logging
import os
import yaml

from munch import munchify

import oc.openshift_ops as ac
from ocs.pod import Pod
from ocs import ocp
import ocs.defaults as default
from utility.templating import generate_yaml_from_jinja2_template_with_data

logger = logging.getLogger(__name__)


class OCS(object):
    """
    High level rook abstraction. This class should handle operator
    and cluster objects.

    As of now deligated rook environment to corresponding cluster object
    but in future rookenv should be handled here.Also few operator related ops
    will be included in this class. Keeping this class lightweight for the first
    cut.

    Attrs:
        self._cluster (OCSCluster): current cluster object
    """

    def __init__(self, **config):
        self._cluster = OCSCluster(**config)

    @property
    def cluster(self):
        return self._cluster


class OCSCluster(object):
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
            'namespace', default.ROOK_CLUSTER_NAMESPACE
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
        self._ocs_pods = list()
        self._api_client = ac.OCP()  # TODO: APIClient abstractions
        self.ocs_pod_init()
        self.cephfs = None

        """
        Following variables hold map of pod name to pod objects
        """
        self.mons = {}
        self.osds = {}
        self.mdss = {}
        self.toolbox = None

    @property
    def cluster_name(self):
        return self._cluster_name

    @property
    def namespace(self):
        return self._namespace

    @property
    def pods(self):
        # TODO: Decide whether to return list or yield
        for pod in self._ocs_pods:
            yield pod

    def get_mons(self):
        # TODO: return all mons belonging to this cluster
        pass

    def get_key(self, username):
        #TODO: Return key for cluster user, mostly client.***
        pass

    def get_client_users(self):
        #TODO: Return all client users, like client.admin..
        pass

    def get_mon_info(self):
        # TODO: Return info related to a mon ex: PORT
        pass

    def ocs_pod_init(self):
        """
        Not to be confused with actual pod init in oc cluster.
        This is just an initializer for ```Class Pod``` from ocs/pod module.
        """

        def _get_ocs_pods():
            """
            Fetch pod info from openshift rook cluster

            This function scans all mon, osd, client pods from the namespace
            `_namespace` and fills in the details in `_ocs_pods`

            Yields:
                pod (str): name of the pod
            """

            pod_list = self._api_client.get_pods(
                namespace=self._namespace,
                label_selector='app != rook-ceph-osd-prepare'
            )
            for each in pod_list:
                yield each

        for pod in _get_ocs_pods():
            pod_labels = self._get_pod_labels(
                pod_name=pod,
                namespace=self._namespace,
            )
            # Instantiate pod object for this pod
            podobj = Pod(pod, self._namespace, pod_labels)
            self._ocs_pods.append(podobj)

    def _get_pod_labels(self, pod_name, namespace):
        """
        Get labels(openshift labels) on a pod

        Args:
            pod_name (str): Name of the pod
            namespace (str): Namespace in which this pod lives

        Returns:
            dict: Labels from pod metadata

        Note: this function is only for internal consumption, end users would
        be using pod.labels to get labels on a given pod instance.
        """

        return self._api_client.get_labels(pod_name, namespace)

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
            default.TEMPLATE_DIR,
            "cephblockpool.yaml"
        )
        # overwrite the namespace with openshift-storage, since cephblockpool
        # is tied-up with openshift-storage
        namespace = default.ROOK_CLUSTER_NAMESPACE

        cephblockpool_data = {}
        cephblockpool_data['cephblockpool_name'] = cephblockpool_name
        cephblockpool_data['rook_api_version'] = default.ROOK_API_VERSION
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

    def create_cephfs(self, **kwargs):
        """
        A function to invoke cephfs creation via CephFS

        Args:
            **kwargs: Parameter which user want to override against defaults
                like name, mds count etc. Keys should match as per fs resource
                yaml.

        Returns:
            bool: True if fs healthy else False
        """
        if self.cephfs:
            logger.info(f"Cephfs {self.cephfs.name} already exists")
            return self.cephfs.health_ok()
        self.cephfs = CephFS(self.namespace, **kwargs)
        assert self.cephfs.health_ok()
        return True


class CephFS(object):
    """
    A class which handles create, delete and modify cephfs
    No MDS ops here, instead it will be handled by RookCluster class
    """

    def __init__(self, _api_client, namespace, **kwargs):
        self._name = kwargs.get('name', default.CEPHFS_NAME)
        self.namespace = namespace
        self.CEPHFS =  ocp.OCP(
            kind='CephFilesystem',
            namespace=namespace,
        )
        self.POD = ocp.OCP(
            kind='Pod',
            namespace=namespace,
        )
        self.CEPHFS_INITIAL_CONFIG = os.path.join(
            default.TEMPLATE_DIR, "cephfilesystem.yaml"
        )
        self.CEPHFS_CURRENT_CONFIG = "cephfilesystem_tmp.yaml"
        self.MDS_APP_LABEL = default.MDS_APP_LABEL
        self.mdss = {}
        self.active_count = 0
        #In memory fs configuration(cephfilesystem type)
        self.conf = None
        self.data_pool = kwargs.get('data-pool', default.CEPHFS_DATA_POOL)

        resource = generate_yaml_from_jinja2_template_with_data(
            self.CEPHFS_INITAL_CONFIG,
            **kwargs,
        )

        with open(self.CEPHFS_CURRENT_CONFIG, 'w') as conf:
            yaml.dump(resource, conf, default_flow_style=False)

        logger.info(f"Creating ceph FileSystem {self._name}")
        assert self.CEPHFS.create(yaml_file=self.CEPHFS_CURRENT_CONFIG)
        self.conf = self.get_current_conf()
        self.active_count = self.conf.spec.metadataServer.activeCount

    @property
    def name(self):
        return self._name

    def get_current_conf(self):
        """
        A function to create in memory represenation
        of fs config (Munchified data)

        Returns:
            Munchified object

        # TODO: Get current config from openshift rather than temp conf file
        """
        with open(self.CEPHFS_CURRENT_CONFIG, 'r') as conf:
            cur_conf = munchify(yaml.safe_load(conf))
        return cur_conf

    def set_current_conf(self, conf_obj):
        """
        After modification of in memory fs config we will call this function

        Params:
            modifiedconf(Munchobj): A munchified obj with new conf
        """
        with open(self.CEPHFS_CURRENT_CONFIG, 'w') as conf:
            yaml.dump(conf_obj.toDict(), conf, default_flow_style=False)

    def health_ok(self):
        """
        A all round health check of fs

        Args:
            None

        Returns:
            bool: True if healthy else False

        """
        assert self.check_mds_health()

    def check_mds_health(self):
        """
        Asses health of mds

        Returns:
            bool: True if mds healthy
        """
        self.get_mds_info()
        # TODO: need  checklist of health check parameters
        # as of now just return True
        return True

    def get_mds_info(self):
        """
        Get mds info and fill in the pod object in self.mdss
        """
        #TODO: Replace api_client with OCP before merge
        mdss = self._api_client.get_pods(
            namespace=self.namespace,
            label_selector=f'app == {self.MDS_APP_LABEL}'
        )

        for mds in mdss:
            labels = self._api_client.get_pod_labels(
                pod_name = mds,
                namespace = self.namespace,
            )
            mds_id = labels['mds']
            pod = Pod(mds, self.namespace, labels)
            self.mdss.update({mds_id: pod})

    def delete(self):
        """
        Destroy this fs

        Returns:
            bool: True if success else False
        """
        logger.info(f"Deleting the fs {self._name}")
        out = self.CEPHFS.delete(resource_name=self._name)
        if f'"{self._name}" deleted' in out:
            # wait for mds pod termination
            return self.POD.wait_for_resource(
                condition='',
                selector=self.MDS_APP_LABEL,
                to_delete=True,
            )
        logger.error(f"Failed to delete cephfs {self._name}")
        return False
