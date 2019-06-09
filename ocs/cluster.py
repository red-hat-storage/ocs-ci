"""
A module for all rook functionalities and abstractions.

This module has rook related classes, support for functionalities to work with
rook cluster. This works with assumptions that an OCP cluster is already
functional and proper configurations are made for interaction.
"""

import logging
import os
import base64

import oc.openshift_ops as ac
import resources.pod as pod
from resources import ocs
import ocs.defaults as default
from utility.templating import generate_yaml_from_jinja2_template_with_data
from ocsci.config import ENV_DATA
from ocs import ocp
from ocs.exceptions import *

POD = ocp.OCP(kind='Pod', namespace=ENV_DATA['cluster_namespace'])
CEPHCLUSTER = ocp.OCP(
    kind='CephCluster', namespace=ENV_DATA['cluster_namespace']
)

logger = logging.getLogger(__name__)


class CephCluster(object):
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
        TODO: Instantiate CephCluster object somewhere higher after deployment
        Read namespace and cluster name from config.
        """
        self._namespace = config.get(
            'namespace', ENV_DATA['cluster_namespace']
        )
        self._ocs_pods = pod.get_all_pods(self._namespace)

        #self.cluster_resource = os.path.join(
        #    default.TEMPLATE_DIR, "cluster.yaml")
        # Once we figure out where to initialize this CephCluster object we
        # will pass **config
        #self.cluster_config = generate_yaml_from_jinja2_template_with_data(
        #    self.cluster_resource, **config
        #)
        self.cluster_config = CEPHCLUSTER.get()['items'][0]
        self.cephfs_config = default.CEPHFILESYSTEM_DICT.copy()
        #Below is just for testing purpose
        #self.cephfs_config['metadata']['name'] = 'cephfs-test-0913235459'

        # We are not invoking ocs.create() here
        # assuming cluster creation is done somewhere after deployment
        # So just load ocs with existing cluster details
        self.cluster = ocs.OCS(**self.cluster_config)
        self.cephfs = ocs.OCS(**self.cephfs_config)

        self.cluster.reload()
        self.cephfs.reload()

        self.mon_selector = default.MON_APP_LABEL
        self.mds_selector = default.MDS_APP_LABEL
        self.tool_selector = default.TOOL_APP_LABEL

        self.mons = []
        self.mdss = []
        self.toolbox = []

        self.scan_cluster()
        self.mon_count = len(self.mons)
        self.mds_count = len(self.mdss)

    @property
    def cluster_name(self):
        return self._cluster_name

    @property
    def namespace(self):
        return self._namespace

    @property
    def pods(self):
        return self._ocs_pods

    def scan_cluster(self):
        """
        Get accurate info on current state of pods
        """
        self._ocs_pods = pod.get_all_pods(self._namespace)
        self.mons = self._filter_pods(self.mon_selector, self.pods)
        self.mdss = self._filter_pods(self.mds_selector, self.pods)
        self.toolbox = self._filter_pods(self.tool_selector, self.pods)

        # set port attrib on mon pods
        self.mons = list(map(self.set_port, self.mons))

    @staticmethod
    def set_port(pod):
        """
        Set port attribute on pod
        Args:
            pod(Pod): Pod object
        """
        l1 = pod.pod_data.get('spec').get('containers')
        l2 = l1[0]['ports'][0]['containerPort']
        # Dynamically added attribute 'port'
        pod.port = l2
        logging.info(f"port={pod.port}")
        return pod

    def mon_change_count(self, new_count):
        """
        Change mon count in the cluster

        Args:
            new_count(int): Absolute number of mons required
        """
        self.cluster.data['spec']['mon']['count'] = new_count
        self.cluster.apply(**self.cluster.data)
        self.mon_health_check(new_count)
        logger.info(f"Mon count changed to {new_count}")
        self.mon_count = new_count
        self.scan_cluster()
        self.cluster.reload()

    def mon_health_check(self, count):
        """
        TODO: Mon pod count alone can't tell monitor health
            need other parameters as well
        """
        try:
            assert POD.wait_for_resource(
                condition='Running', selector=self.mon_selector,
                resource_count=count,
            )
        except AssertionError as e:
            logger.error(e)
            raise MonCountException(
                f"Failed to achieve desired Mon count"
                f" {count}"
            )

    def mds_change_count(self, new_count):
        """
        Change mds count in the cluster

        Args:
            new_count(int): Absolute number of mds required
        """
        self.cephfs.data['spec']['metadataServer']['activeCount'] = new_count
        logger.info(self.cephfs.data)
        self.cephfs.apply(**self.cephfs.data)
        self.mds_health_check(new_count)
        logger.info(f"MDS count changed to {new_count}")
        self.mds_count = new_count
        self.scan_cluster()
        self.cephfs.reload()

    def mds_health_check(self, count):
        """
        TODO: mds count alone can't tell whether fs is healthy
                need some more parameters as well
        """
        try:
            assert POD.wait_for_resource(
                condition='Running', selector=self.mds_selector,
                resource_count=count
        )
        except AssertionError as e:
            logger.error(e)
            raise MDSCountException(
                f"Failed to achieve desired MDS count"
                f" {count}"
            )

    def get_admin_key(self):
        """
        Returns:
            adminkey(str): base64 encoded key
        """
        return self.get_user_key('client.admin')

    def get_user_key(self, user):
        """
        Args:
            user(str): ceph username ex: client.user1
        Returns:
            key(str): base64 encoded user key
        """
        out = self.toolbox[0].exec_cmd_on_pod(
            f"ceph auth get-key {user} --format json"
        )
        if 'ENOENT' in out:
            return False
        key_base64 = base64.b64encode(out['key'].encode()).decode()
        return key_base64

    def create_user(self, username, caps):
        """
        Create a ceph user in the cluster

        Args:
            username(str): ex client.user1
            caps(str): ceph caps ex: mon 'allow r' osd 'allow rw'

        Return:
            return value of get_user_key()
        """
        cmd = f"ceph auth add {username} {caps}"
        # As of now ceph auth command gives output to stderr
        # To be handled
        out = self.toolbox[0].exec_cmd_on_pod(cmd)
        logging.info(type(out))
        return self.get_user_key(username)

    @staticmethod
    def _filter_pods(selector, pods):
        """
        Filter pods based on label match

        Args:
             selector (str): for ex: "app=rook-ceph-mon"
             pods (list): list of Pod objects

        Returns:
            list: of pod objects which matches labels
        """
        def _filter(each):
            key, val = selector.split("=")
            return key in each.labels and each.labels[key] == val
        return list(filter(_filter, pods))
