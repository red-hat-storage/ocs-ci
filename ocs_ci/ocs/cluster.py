"""
A module for all rook functionalities and abstractions.

This module has rook related classes, support for functionalities to work with
rook cluster. This works with assumptions that an OCP cluster is already
functional and proper configurations are made for interaction.
"""

import base64
import logging
import random
import re
import threading
import yaml
from time import sleep

import ocs_ci.ocs.resources.pod as pod
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.ocs.resources import ocs, storage_cluster
import ocs_ci.ocs.constants as constant
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import TimeoutSampler, run_cmd
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.framework import config
from ocs_ci.ocs import ocp, constants, exceptions
from ocs_ci.ocs.resources.pvc import get_all_pvc_objs

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
        self.DEP = ocp.OCP(
            kind='Deployment',
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
        else:
            self.cephfs = None

        self.mon_selector = constant.MON_APP_LABEL
        self.mds_selector = constant.MDS_APP_LABEL
        self.tool_selector = constant.TOOL_APP_LABEL
        self.mgr_selector = constant.MGR_APP_LABEL
        self.osd_selector = constant.OSD_APP_LABEL
        self.noobaa_selector = constant.NOOBAA_APP_LABEL
        self.noobaa_core_selector = constant.NOOBAA_CORE_POD_LABEL
        self.mons = []
        self._ceph_pods = []
        self.mdss = []
        self.mgrs = []
        self.osds = []
        self.noobaas = []
        self.toolbox = None
        self.mds_count = 0
        self.mon_count = 0
        self.mgr_count = 0
        self.osd_count = 0
        self.noobaa_count = 0
        self.health_error_status = None
        self.health_monitor_enabled = False
        self.health_monitor = None
        self.scan_cluster()
        logging.info(f"Number of mons = {self.mon_count}")
        logging.info(f"Number of mds = {self.mds_count}")

        self.used_space = 0

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
        # TODO: Workaround for BZ1748325:
        mons = pod.get_mon_pods(self.mon_selector, self.namespace)
        for mon in mons:
            if mon.ocp.get_resource_status(mon.name) == constant.STATUS_RUNNING:
                self.mons.append(mon)
        # TODO: End of workaround for BZ1748325
        self.mdss = pod.get_mds_pods(self.mds_selector, self.namespace)
        self.mgrs = pod.get_mgr_pods(self.mgr_selector, self.namespace)
        self.osds = pod.get_osd_pods(self.osd_selector, self.namespace)
        self.noobaas = pod.get_noobaa_pods(self.noobaa_selector, self.namespace)
        self.toolbox = pod.get_ceph_tools_pod()

        # set port attrib on mon pods
        self.mons = list(map(self.set_port, self.mons))
        self.cluster.reload()
        if self.cephfs:
            self.cephfs.reload()
        else:
            try:
                self.cephfs_config = self.CEPHFS.get().get('items')[0]
                self.cephfs = ocs.OCS(**self.cephfs_config)
                self.cephfs.reload()
            except IndexError as e:
                logging.warning(e)
                logging.warning("No CephFS found")

        self.mon_count = len(self.mons)
        self.mds_count = len(self.mdss)
        self.mgr_count = len(self.mgrs)
        self.osd_count = len(self.osds)
        self.noobaa_count = len(self.noobaas)

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

    def cluster_health_check(self, timeout=None):
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
        # Scale timeout only if user hasn't passed any value
        timeout = timeout or (10 * len(self.pods))
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

        self.noobaa_health_check()
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

            # TODO: Workaround for BZ1748325:
            actual_mons = pod.get_mon_pods()
            actual_running_mons = list()
            for mon in actual_mons:
                if mon.ocp.get_resource_status(mon.name) == constant.STATUS_RUNNING:
                    actual_running_mons.append(mon)
            actual = len(actual_running_mons)
            # TODO: End of workaround for BZ1748325

            assert count == actual, f"Expected {count},  Got {actual}"
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

    def noobaa_health_check(self):
        """
        Noobaa health check based on pods status
        """
        timeout = 10 * len(self.pods)
        assert self.POD.wait_for_resource(
            condition='Running', selector=self.noobaa_selector,
            timeout=timeout, sleep=3,
        ), "Failed to achieve desired Noobaa Operator Status"

        assert self.POD.wait_for_resource(
            condition='Running', selector=self.noobaa_core_selector,
            timeout=timeout, sleep=3,
        ), "Failed to achieve desired Noobaa Core Status"

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

    def get_mons_from_cluster(self):
        """
        Getting the list of mons from the cluster

        Returns:
            available_mon (list): Returns the mons from the cluster
        """

        ret = self.DEP.get(
            resource_name='', out_yaml_format=False, selector='app=rook-ceph-mon'
        )
        available_mon = re.findall(r'[\w-]+mon-+[\w-]', ret)
        return available_mon

    def remove_mon_from_cluster(self):
        """
        Removing the mon pod from deployment

        Returns:
            remove_mon(bool): True if removal of mon is successful, False otherwise
        """
        mons = self.get_mons_from_cluster()
        after_delete_mon_count = len(mons) - 1
        random_mon = random.choice(mons)
        remove_mon = self.DEP.delete(resource_name=random_mon)
        assert self.POD.wait_for_resource(
            condition=constant.STATUS_RUNNING,
            resource_count=after_delete_mon_count,
            selector='app=rook-ceph-mon'
        )
        logging.info(f"Removed the mon {random_mon} from the cluster")
        return remove_mon

    @retry(UnexpectedBehaviour, tries=20, delay=10, backoff=1)
    def check_ceph_pool_used_space(self, cbp_name):
        """
        Check for the used space of a pool in cluster

         Returns:
            used_in_gb (float): Amount of used space in pool (in GBs)

         Raises:
            UnexpectedBehaviour: If used size keeps varying in Ceph status
        """
        ct_pod = pod.get_ceph_tools_pod()
        rados_status = ct_pod.exec_ceph_cmd(ceph_cmd=f"rados df -p {cbp_name}")
        assert rados_status is not None
        used = rados_status['pools'][0]['size_bytes']
        used_in_gb = format(used / constants.GB, '.4f')
        if self.used_space and self.used_space == used_in_gb:
            return float(self.used_space)
        self.used_space = used_in_gb
        raise UnexpectedBehaviour(
            f"In Rados df, Used size is varying"
        )

    def get_ceph_health(self, detail=False):
        """
        Exec `ceph health` cmd on tools pod and return the status of the ceph
        cluster.

        Args:
            detail (bool): If True the 'ceph health detail' is executed

        Returns:
            str: Output of the ceph health command.

        """
        ceph_health_cmd = "ceph health"
        if detail:
            ceph_health_cmd = f"{ceph_health_cmd} detail"

        return self.toolbox.exec_cmd_on_pod(
            ceph_health_cmd, out_yaml_format=False,
        )

    def get_ceph_status(self):
        """
        Exec `ceph status` cmd on tools pod and return its output.

        Returns:
            str: Output of the ceph status command.

        """
        return self.toolbox.exec_cmd_on_pod(
            "ceph status", out_yaml_format=False,
        )

    def enable_health_monitor(self, sleep=5):
        """
        Enable monitoring for ceph health status.

        Args:
            sleep (int): Number of seconds to sleep between health checks.

        """
        self.monitor = HealthMonitorThread(self, sleep)
        self.monitor.start()

    def disable_health_monitor(self):
        self.health_monitor_enabled = False


class HealthMonitorThread(threading.Thread):
    """
    Class for monitoring ceph health status of CephCluster. If CephCluster will
    get to HEALTH_ERROR state it will save the ceph status to
    health_error_status variable in ceph_cluster object and will stop
    monitoring. It's up to user how to handle the error.
    """

    def __init__(self, ceph_cluster, sleep=5):
        """
        Constructor for ceph health status thread.

        Args:
            ceph_cluster (CephCluster): Reference to CephCluster object.
            sleep (int): Number of seconds to sleep between health checks.

        """
        self.ceph_cluster = ceph_cluster
        self.sleep = sleep
        super(HealthMonitorThread, self).__init__()

    def run(self):
        self.ceph_cluster.health_monitor_enabled = True
        while self.ceph_cluster.health_monitor_enabled and (
            not self.ceph_cluster.health_error_status
        ):
            sleep(self.sleep)
            health_status = self.ceph_cluster.get_ceph_health(detail=True)
            if "HEALTH_ERROR" in health_status:
                self.ceph_cluster.health_error_status = (
                    self.ceph_cluster.get_ceph_status()
                )


def validate_cluster_on_pvc():
    """
    Validate creation of PVCs for MON and OSD pods.
    Also validate that those PVCs are attached to the OCS pods

    Raises:
         AssertionError: If PVC is not mounted on one or more OCS pods

    """
    # Get the PVCs for selected label (MON/OSD)
    ns = config.ENV_DATA['cluster_namespace']
    ocs_pvc_obj = get_all_pvc_objs(namespace=ns)

    # Check all pvc's are in bound state

    pvc_names = []
    for pvc_obj in ocs_pvc_obj:
        if (pvc_obj.name.startswith(constants.DEFAULT_DEVICESET_PVC_NAME)
                or pvc_obj.name.startswith(constants.DEFAULT_MON_PVC_NAME)):
            assert pvc_obj.status == constants.STATUS_BOUND, (
                f"PVC {pvc_obj.name} is not Bound"
            )
            logger.info(f"PVC {pvc_obj.name} is in Bound state")
            pvc_names.append(pvc_obj.name)

    mon_pods = get_pod_name_by_pattern('rook-ceph-mon', ns)
    osd_pods = get_pod_name_by_pattern('rook-ceph-osd', ns, filter='prepare')
    assert len(mon_pods) + len(osd_pods) == len(pvc_names), (
        "Not enough PVC's available for all Ceph Pods"
    )
    for ceph_pod in mon_pods + osd_pods:
        out = run_cmd(f'oc -n {ns} get pods {ceph_pod} -o yaml')
        out_yaml = yaml.safe_load(out)
        for vol in out_yaml['spec']['volumes']:
            if vol.get('persistentVolumeClaim'):
                claimName = vol.get('persistentVolumeClaim').get('claimName')
                logger.info(f"{ceph_pod} backed by pvc {claimName}")
                assert claimName in pvc_names, (
                    "Ceph Internal Volume not backed by PVC"
                )


def count_cluster_osd():
    """
    Returns:
         osc_count (Integer): number of OSD pods in current cluster

    """
    storage_cluster_obj = storage_cluster.StorageCluster(
        resource_name=config.ENV_DATA['storage_cluster_name'],
        namespace=config.ENV_DATA['cluster_namespace'],
    )
    storage_cluster_obj.reload_data()
    osd_count = (
        int(storage_cluster_obj.data['spec']['storageDeviceSets'][0]['count'])
        * int(storage_cluster_obj.data['spec']['storageDeviceSets'][0]['replica'])
    )
    return osd_count


def validate_pdb_creation():
    """
    Validate creation of PDBs for MON, MDS and OSD pods.

    Raises:
        AssertionError: If required PDBs were not created.

    """
    pdb_obj = ocp.OCP(kind='PodDisruptionBudget')
    item_list = pdb_obj.get().get('items')
    pdb_list = [item['metadata']['name'] for item in item_list]
    osd_count = count_cluster_osd()
    pdb_required = [constants.MDS_PDB, constants.MON_PDB]
    for num in range(osd_count):
        pdb_required.append(constants.OSD_PDB + str(num))

    pdb_list.sort()
    pdb_required.sort()
    for required, given in zip(pdb_required, pdb_list):
        assert required == given, f"{required} was not created"

    logger.info(f"All required PDBs created: {pdb_required}")
