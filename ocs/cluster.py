"""
A module for all rook functionalities and abstractions.

This module has rook related classes, support for functionalities to work with
rook cluster. This works with assumptions that an OCP cluster is already
functional and proper configurations are made for interaction.
"""

import logging
import base64

import resources.pod as pod
from resources import ocs
import ocs.constants as constant
from utility.utils import TimeoutSampler
from ocsci import config
from ocs import ocp
from ocs import exceptions


logger = logging.getLogger(__name__)


class CephCluster(object):
    """
    Handles all cluster related operations from ceph perspective

    This class has depiction of ceph cluster. Contains references to
    pod objects which represents ceph cluster entities.

    Attributes:
        pods (list) : A list of  ceph cluster related pods
        cluster_name (str): Name of ceph cluster
        namespace (str): openshift Namespace where this cluster lives
    """

    def __init__(self):
        """
        Cluster object initializer, this object needs to be initialized
        after cluster deployment. However its harmless to do anywhere.
        """
        # cluster_name is name of cluster in rook of type CephCluster

        self.POD = ocp.OCP(
            kind='Pod', namespace=config.ENV_DATA['cluster_namespace']
        )
        self.CEPHCLUSTER = ocp.OCP(
            kind='CephCluster', namespace=config.ENV_DATA['cluster_namespace']
        )
        self.CEPHFS = ocp.OCP(
            kind='CephFilesystem',
            namespace=config.ENV_DATA['cluster_namespace']
        )

        self.cluster_resource_config = self.CEPHCLUSTER.get().get('items')[0]
        try:
            self.cephfs_config = self.CEPHFS.get().get('items')[0]
        except IndexError as e:
            logging.warning(e)
            logging.warning("No CephFS found")
            self.cephfs_config = None

        self._cluster_name = (
            self.cluster_resource_config.get('metadata').get('name')
        )
        self._namespace = (
            self.cluster_resource_config.get('metadata').get('namespace')
        )

        # We are not invoking ocs.create() here
        # assuming cluster creation is done somewhere after deployment
        # So just load ocs with existing cluster details
        self.cluster = ocs.OCS(**self.cluster_resource_config)
        if self.cephfs_config:
            self.cephfs = ocs.OCS(**self.cephfs_config)

        self.mon_selector = constant.MON_APP_LABEL
        self.mds_selector = constant.MDS_APP_LABEL
        self.tool_selector = constant.TOOL_APP_LABEL
        self.mgr_selector = constant.MGR_APP_LABEL
        self.osd_selector = constant.OSD_APP_LABEL
        self.mons = []
        self._ceph_pods = []
        self.mdss = []
        self.mgrs = []
        self.osds = []
        self.toolbox = None
        self.mds_count = 0
        self.mon_count = 0
        self.mgr_count = 0
        self.osd_count = 0

        self.scan_cluster()
        logging.info(f"Number of mons = {self.mon_count}")
        logging.info(f"Number of mds = {self.mds_count}")

    @property
    def cluster_name(self):
        return self._cluster_name

    @property
    def namespace(self):
        return self._namespace

    @property
    def pods(self):
        return self._ceph_pods

    def scan_cluster(self):
        """
        Get accurate info on current state of pods
        """
        self._ceph_pods = pod.get_all_pods(self._namespace)
        self.mons = pod.get_mon_pods(self.mon_selector, self.namespace)
        self.mdss = pod.get_mds_pods(self.mds_selector, self.namespace)
        self.mgrs = pod.get_mgr_pods(self.mgr_selector, self.namespace)
        self.osds = pod.get_osd_pods(self.osd_selector, self.namespace)
        self.toolbox = pod.get_ceph_tools_pod()

        # set port attrib on mon pods
        self.mons = list(map(self.set_port, self.mons))
        self.cluster.reload()
        if self.cephfs_config:
            self.cephfs.reload()

        self.mon_count = len(self.mons)
        self.mds_count = len(self.mdss)
        self.mgr_count = len(self.mgrs)
        self.osd_count = len(self.osds)

    @staticmethod
    def set_port(pod):
        """
        Set port attribute on pod.
        port attribute for mon is required for secrets and this attrib
        is not a member for original pod class.

        Args:
            pod(Pod): Pod object without 'port' attribute

        Returns:
            pod(Pod): A modified pod object with 'port' attribute set
        """
        container = pod.pod_data.get('spec').get('containers')
        port = container[0]['ports'][0]['containerPort']
        # Dynamically added attribute 'port'
        pod.port = port
        logging.info(f"port={pod.port}")
        return pod

    def is_health_ok(self):
        """
        Returns:
            bool: True if "HEALTH_OK" else False
        """
        self.cluster.reload()
        return self.cluster.data['status']['ceph']['health'] == "HEALTH_OK"

    def cluster_health_check(self, timeout=0):
        """
        Check overall cluster health.
        Relying on health reported by CephCluster.get()

        Args:
            timeout (int): in seconds. By default timeout value will be scaled
                based on number of ceph pods in the cluster. This is just a
                crude number. Its been observed that as the number of pods
                increases it takes more time for cluster's HEALTH_OK.

        Returns:
            bool: True if "HEALTH_OK"  else False

        Raises:
            CephHealthException: if cluster is not healthy
        """
        timeout = 10 * len(self.pods)
        sample = TimeoutSampler(
            timeout=timeout, sleep=3, func=self.is_health_ok
        )

        if not sample.wait_for_func_status(result=True):
            raise exceptions.CephHealthException("Cluster health is NOT OK")
        # This way of checking health of different cluster entities and
        # raising only CephHealthException is not elegant.
        # TODO: add an attribute in CephHealthException, called "reason"
        # which should tell because of which exact cluster entity health
        # is not ok ?
        expected_mon_count = self.mon_count
        expected_mds_count = self.mds_count

        self.scan_cluster()
        try:
            self.mon_health_check(expected_mon_count)
        except exceptions.MonCountException as e:
            logger.error(e)
            raise exceptions.CephHealthException("Cluster health is NOT OK")

        try:
            if not expected_mds_count:
                pass
            else:
                self.mds_health_check(expected_mds_count)
        except exceptions.MDSCountException as e:
            logger.error(e)
            raise exceptions.CephHealthException("Cluster health is NOT OK")
        # TODO: OSD and MGR health check
        logger.info("Cluster HEALTH_OK")
        # This scan is for reconcilation on *.count
        # because during first scan in this function some of the
        # pods may not be up and would have set count to lesser number
        self.scan_cluster()
        return True

    def mon_change_count(self, new_count):
        """
        Change mon count in the cluster

        Args:
            new_count(int): Absolute number of mons required
        """
        self.cluster.reload()
        self.cluster.data['spec']['mon']['count'] = new_count
        logger.info(self.cluster.data)
        self.cluster.apply(**self.cluster.data)
        self.mon_count = new_count
        self.cluster_health_check()
        logger.info(f"Mon count changed to {new_count}")
        self.cluster.reload()

    def mon_health_check(self, count):
        """
        Mon health check based on pod count

        Args:
            count (int): Expected number of mon pods

        Raises:
            MonCountException: if mon pod count doesn't match
        """
        timeout = 10 * len(self.pods)
        logger.info(f"Expected MONs = {count}")
        try:
            assert self.POD.wait_for_resource(
                condition='Running', selector=self.mon_selector,
                resource_count=count, timeout=timeout, sleep=3,
            )
        except exceptions.TimeoutExpiredError as e:
            logger.error(e)
            raise exceptions.MonCountException(
                f"Failed to achieve desired Mon count"
                f" {count}"
            )

    def mds_change_count(self, new_count):
        """
        Change mds count in the cluster

        Args:
            new_count(int): Absolute number of active mdss required
        """
        self.cephfs.data['spec']['metadataServer']['activeCount'] = new_count
        self.cephfs.apply(**self.cephfs.data)
        logger.info(f"MDS active count changed to {new_count}")
        if self.cephfs.data['spec']['metadataServer']['activeStandby']:
            expected = new_count * 2
        else:
            expected = new_count
        self.mds_count = expected
        self.cluster_health_check()
        self.cephfs.reload()

    def mds_health_check(self, count):
        """
        MDS health check based on pod count

        Args:
            count (int): number of pods expected

        Raises:
            MDACountException: if pod count doesn't match
        """
        timeout = 10 * len(self.pods)
        try:
            assert self.POD.wait_for_resource(
                condition='Running', selector=self.mds_selector,
                resource_count=count, timeout=timeout, sleep=3,
            )
        except AssertionError as e:
            logger.error(e)
            raise exceptions.MDSCountException(
                f"Failed to achieve desired MDS count"
                f" {count}"
            )

    def get_admin_key(self):
        """
        Returns:
            adminkey (str): base64 encoded key
        """
        return self.get_user_key('client.admin')

    def get_user_key(self, user):
        """
        Args:
            user (str): ceph username ex: client.user1

        Returns:
            key (str): base64 encoded user key
        """
        out = self.toolbox.exec_cmd_on_pod(
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
            username (str): ex client.user1
            caps (str): ceph caps ex: mon 'allow r' osd 'allow rw'

        Return:
            return value of get_user_key()
        """
        cmd = f"ceph auth add {username} {caps}"
        # As of now ceph auth command gives output to stderr
        # To be handled
        out = self.toolbox.exec_cmd_on_pod(cmd)
        logging.info(type(out))
        return self.get_user_key(username)
