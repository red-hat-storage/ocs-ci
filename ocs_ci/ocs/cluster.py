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
import time

import ocs_ci.ocs.resources.pod as pod
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.ocs.resources import ocs, storage_cluster
import ocs_ci.ocs.constants as constant
from ocs_ci.ocs.resources.mcg import MCG
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import (
    TimeoutSampler, run_cmd, convert_device_size, get_trim_mean
)
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
        self.rgws = []
        self.toolbox = None
        self.mds_count = 0
        self.mon_count = 0
        self.mgr_count = 0
        self.osd_count = 0
        self.noobaa_count = 0
        self.rgw_count = 0
        self._mcg_obj = None
        self.scan_cluster()
        logging.info(f"Number of mons = {self.mon_count}")
        logging.info(f"Number of mds = {self.mds_count}")

        self.used_space = 0

    @property
    def mcg_obj(self):
        if not self._mcg_obj:
            self._mcg_obj = MCG()
        return self._mcg_obj

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
        self.rgws = pod.get_rgw_pods()
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
        self.rgw_count = len(self.rgws)

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

        # TODO: OSD and MGR health check
        logger.info("Cluster HEALTH_OK")
        # This scan is for reconcilation on *.count
        # because during first scan in this function some of the
        # pods may not be up and would have set count to lesser number
        self.scan_cluster()

        # Check Noobaa health
        self.wait_for_noobaa_health_ok()

    def noobaa_health_check(self):
        """
        Check Noobaa health

        """
        if not self.mcg_obj.status:
            raise exceptions.NoobaaHealthException("Cluster health is NOT OK")

    def wait_for_noobaa_health_ok(self, tries=60, delay=5):
        """
        Wait for Noobaa health to be OK
        """
        return retry(
            exceptions.NoobaaHealthException,
            tries=tries, delay=delay, backoff=1
        )(self.noobaa_health_check)()

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

    def get_admin_key(self):
        """
        Returns:
            adminkey (str): base64 encoded key
        """
        return self.get_user_key('client.admin')

    def set_noout(self):
        """
        Set noout flag for maintainance
        """
        self.toolbox.exec_cmd_on_pod("ceph osd set noout")

    def unset_noout(self):
        """
        unset noout flag for peering
        """
        self.toolbox.exec_cmd_on_pod("ceph osd unset noout")

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
            "In Rados df, Used size is varying"
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

    def get_ceph_status(self, format=None):
        """
        Exec `ceph status` cmd on tools pod and return its output.

        Args:
            format (str) : Format of the output (e.g. json-pretty, json, plain)

        Returns:
            str: Output of the ceph status command.

        """
        cmd = "ceph status"
        if format:
            cmd += f" -f {format}"
        return self.toolbox.exec_cmd_on_pod(cmd, out_yaml_format=False)

    def get_ceph_capacity(self):
        """
        The function gets the total mount of storage capacity of the ocs cluster.
        the calculation is <Num of OSD> * <OSD size> / <replica number>
        it will not take into account the current used capacity.

        Returns:
            int : Total storage capacity in GiB (GiB is for development environment)

        """
        storage_cluster_obj = storage_cluster.StorageCluster(
            resource_name=config.ENV_DATA['storage_cluster_name'],
            namespace=config.ENV_DATA['cluster_namespace'],
        )
        replica = int(storage_cluster_obj.data['spec']['storageDeviceSets'][0]['replica'])

        ceph_pod = pod.get_ceph_tools_pod()
        ceph_status = ceph_pod.exec_ceph_cmd(ceph_cmd="ceph df")
        usable_capacity = int(ceph_status['stats']['total_bytes']) / replica / constant.GB

        return usable_capacity

    def get_ceph_cluster_iops(self):
        """
        The function gets the IOPS from the ocs cluster

        Returns:
            Total IOPS in the cluster

        """

        ceph_pod = pod.get_ceph_tools_pod()
        ceph_status = ceph_pod.exec_ceph_cmd(ceph_cmd="ceph status")
        read_ops = ceph_status['pgmap']['read_op_per_sec']
        write_ops = ceph_status['pgmap']['write_op_per_sec']
        cluster_iops = read_ops + write_ops
        return cluster_iops

    def get_iops_percentage(self, osd_size=2):
        """
        The function calculates the IOPS percentage
        of the cluster depending on number of osds in the cluster

        Args:
            osd_size (int): Size of 1 OSD in Ti

        Returns:
            IOPS percentage of the OCS cluster

        """

        osd_count = count_cluster_osd()
        iops_per_osd = osd_size * constants.IOPS_FOR_1TiB_OSD
        iops_in_cluster = self.get_ceph_cluster_iops()
        osd_iops_limit = iops_per_osd * osd_count
        iops_percentage = (iops_in_cluster / osd_iops_limit) * 100
        logging.info(f"The IOPS percentage of the cluster is {iops_percentage}%")
        return iops_percentage

    def get_cluster_throughput(self):
        """
        Function to get the throughput of ocs cluster

        Returns:
            float: The write throughput of the cluster in MiB/s

        """
        ceph_status = self.get_ceph_status()
        for item in ceph_status.split("\n"):
            if 'client' in item:
                throughput_data = item.strip('client: ').split(",")
                throughput_data = throughput_data[:2:1]
                # Converting all B/s and KiB/s to MiB/s
                throughput = 0
                for val in throughput_data:
                    throughput += [
                        float(re.findall(r'\d+', val)[0]) * constants.TP_CONVERSION[key]
                        for key in constants.TP_CONVERSION.keys() if key in val
                    ][0]
                    logger.info(f"The {val[-2:].upper()} throughput is {throughput} MiB/s")
                return throughput

    def get_throughput_percentage(self):
        """
        Function to get throughput percentage of the ocs cluster

        Returns:
            Throughput percentage of the cluster

        """

        throughput_of_cluster = self.get_cluster_throughput()
        throughput_percentage = (throughput_of_cluster / constants.THROUGHPUT_LIMIT_OSD) * 100
        logging.info(f"The throughput percentage of the cluster is {throughput_percentage}%")
        return throughput_percentage

    def calc_trim_mean_throughput(self, samples=8):
        """
        Calculate the cluster average throughput out of a few samples

        Args:
            samples (int): The number of samples to take

        Returns:
            float: The average cluster throughput

        """
        throughput_vals = [
            self.get_cluster_throughput() for _ in range(samples)
        ]
        return round(get_trim_mean(throughput_vals), 3)

    def get_rebalance_status(self):
        """
        This function gets the rebalance status

        Returns:
            bool: True if rebalance is completed, False otherwise

        """

        ceph_pod = pod.get_ceph_tools_pod()
        ceph_status = ceph_pod.exec_ceph_cmd(ceph_cmd="ceph status")
        ceph_health = ceph_pod.exec_ceph_cmd(ceph_cmd="ceph health")
        total_pg_count = ceph_status['pgmap']['num_pgs']
        pg_states = ceph_status['pgmap']['pgs_by_state']
        logger.info(ceph_health)
        logger.info(pg_states)
        for states in pg_states:
            return (
                states['state_name'] == 'active+clean'
                and states['count'] == total_pg_count
            )

    def wait_for_rebalance(self, timeout=600):
        """
        Wait for re-balance to complete

        Args:
            timeout (int): Time to wait for the completion of re-balance

        Returns:
            bool: True if rebalance completed, False otherwise

        """
        try:
            for rebalance in TimeoutSampler(
                timeout=timeout, sleep=10, func=self.get_rebalance_status
            ):
                if rebalance:
                    logging.info("Re-balance is completed")
                    return True
        except exceptions.TimeoutExpiredError:
            logger.error(
                f"Data re-balance failed to complete within the given "
                f"timeout of {timeout} seconds"
            )
            return False

    def time_taken_to_complete_rebalance(self, timeout=600):
        """
        This function calculates the time taken to complete
        rebalance

        Args:
            timeout (int): Time to wait for the completion of rebalance

        Returns:
            int : Time taken in minutes for the completion of rebalance

        """
        start_time = time.time()
        assert self.wait_for_rebalance(timeout=timeout), (
            f"Data re-balance failed to complete within the given "
            f"timeout of {timeout} seconds"
        )
        time_taken = time.time() - start_time
        return time_taken / 60


class CephHealthMonitor(threading.Thread):
    """
    Context manager class for monitoring ceph health status of CephCluster.
    If CephCluster will get to HEALTH_ERROR state it will save the ceph status
    to health_error_status variable and will stop monitoring.

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
        self.health_error_status = None
        self.health_monitor_enabled = False
        self.latest_health_status = None
        super(CephHealthMonitor, self).__init__()

    def run(self):
        self.health_monitor_enabled = True
        while self.health_monitor_enabled and (
            not self.health_error_status
        ):
            time.sleep(self.sleep)
            self.latest_health_status = self.ceph_cluster.get_ceph_health(
                detail=True
            )
            if "HEALTH_ERROR" in self.latest_health_status:
                self.health_error_status = (
                    self.ceph_cluster.get_ceph_status()
                )
                self.log_error_status()

    def __enter__(self):
        self.start()

    def __exit__(self, exception_type, value, traceback):
        """
        Exit method for context manager

        Raises:
            CephHealthException: If no other exception occurred during
                execution of context manager and HEALTH_ERROR is detected
                during the monitoring.
            exception_type: In case of exception raised during processing of
                the context manager.

        """
        self.health_monitor_enabled = False
        if self.health_error_status:
            self.log_error_status()
        if exception_type:
            raise exception_type.with_traceback(value, traceback)
        if self.health_error_status:
            raise exceptions.CephHealthException(
                f"During monitoring of Ceph health status hit HEALTH_ERROR: "
                f"{self.health_error_status}"
            )

        return True

    def log_error_status(self):
        logger.error(
            f"ERROR HEALTH STATUS DETECTED! "
            f"Status: {self.health_error_status}"
        )


def validate_ocs_pods_on_pvc(pods, pvc_names):
    """
    Validate if ocs pod has PVC. This validation checking if there is the pvc
    like: rook-ceph-mon-a for the pod rook-ceph-mon-a-56f67f5968-6j4px.

    Args:
        pods (list): OCS pod names
        pvc_names (list): names of all PVCs

    Raises:
         AssertionError: If no PVC found for one of the pod

    """
    logger.info(
        f"Validating if each pod from: {pods} has PVC from {pvc_names}."
    )
    for pod_name in pods:
        found_pvc = ""
        for pvc in pvc_names:
            if pvc in pod_name:
                found_pvc = pvc
        if found_pvc:
            logger.info(f"PVC {found_pvc} found for pod {pod_name}")
            continue
        assert found_pvc, f"No PVC found for pod: {pod_name}!"


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
    if not config.DEPLOYMENT.get('local_storage'):
        logger.info("Validating all mon pods have PVC")
        validate_ocs_pods_on_pvc(mon_pods, pvc_names)
    else:
        logger.debug(
            "Skipping validation if all mon pods have PVC because in LSO "
            "deployment we don't have mon pods backed by PVC"
        )
    logger.info("Validating all osd pods have PVC")
    osd_deviceset_pods = get_pod_name_by_pattern(
        'rook-ceph-osd-prepare-ocs-deviceset', ns
    )
    validate_ocs_pods_on_pvc(osd_deviceset_pods, pvc_names)
    osd_pods = get_pod_name_by_pattern('rook-ceph-osd', ns, filter='prepare')
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
    The function returns the number of cluster OSDs

    Returns:
         osd_count (int): number of OSD pods in current cluster

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


def get_osd_utilization():
    """
    Get osd utilization value

    Returns:
        osd_filled (dict): Dict of osd name and its used value
        i.e {'osd.1': 15.276289408185841, 'osd.0': 15.276289408185841, 'osd.2': 15.276289408185841}

    """
    osd_filled = {}
    ceph_cmd = "ceph osd df"
    ct_pod = pod.get_ceph_tools_pod()
    output = ct_pod.exec_ceph_cmd(ceph_cmd=ceph_cmd)
    for osd in output.get('nodes'):
        osd_filled[osd['name']] = osd['utilization']

    return osd_filled


def validate_osd_utilization(osd_used=80):
    """
    Validates osd utilization matches osd_used value

    Args:
        osd_used (int): osd used value

    Returns:
        bool: True if all osd values is equal or greater to osd_used.
              False Otherwise.

    """
    _rc = True
    osd_filled = get_osd_utilization()
    for osd, value in osd_filled.items():
        if int(value) >= osd_used:
            logger.info(f"{osd} used value {value}")
        else:
            _rc = False
            logger.warning(f"{osd} used value {value}")

    return _rc


def get_pgs_per_osd():
    """
    Function to get ceph pg count per OSD

    Returns:
        osd_dict (dict): Dict of osd name and its used value
        i.e {'osd.0': 136, 'osd.2': 136, 'osd.1': 136}

    """
    osd_dict = {}
    ceph_cmd = "ceph osd df"
    ct_pod = pod.get_ceph_tools_pod()
    output = ct_pod.exec_ceph_cmd(ceph_cmd=ceph_cmd)
    for osd in output.get('nodes'):
        osd_dict[osd['name']] = osd['pgs']

    return osd_dict


def get_balancer_eval():
    """
    Function to get ceph pg balancer eval value

    Returns:
        eval_out (float): Eval output of pg balancer

    """
    ceph_cmd = "ceph balancer eval"
    ct_pod = pod.get_ceph_tools_pod()
    eval_out = ct_pod.exec_ceph_cmd(ceph_cmd=ceph_cmd).split(' ')
    return float(eval_out[3])


def get_pg_balancer_status():
    """
    Function to check pg_balancer active and mode is upmap

    Returns:
        bool: True if active and upmap is set else False

    """
    # Check either PG balancer is active or not
    ceph_cmd = "ceph balancer status"
    ct_pod = pod.get_ceph_tools_pod()
    output = ct_pod.exec_ceph_cmd(ceph_cmd=ceph_cmd)

    # Check 'mode' is 'upmap', based on suggestion from Ceph QE
    # TODO: Revisit this if mode needs change.
    if output['active'] and output['mode'] == 'upmap':
        logging.info("PG balancer is active and mode is upmap")
        return True
    else:
        logging.error("PG balancer is not active")
        return False


def validate_pg_balancer():
    """
    Validate either data is equally distributed to OSDs

    Returns:
        bool: True if avg PG's per osd difference is <=10 else False

    """
    # Check OSD utilization either pg balancer is active
    # TODO: Revisit this if pg difference value needs change
    # TODO: Revisit eval value if pg balancer mode changes from 'upmap'
    if get_pg_balancer_status():
        eval = get_balancer_eval()
        osd_dict = get_pgs_per_osd()
        osd_avg_pg_value = round(sum(osd_dict.values()) / len(osd_dict))
        osd_pg_value_flag = True
        for key, value in osd_dict.items():
            diff = abs(value - osd_avg_pg_value)
            if diff <= 10:
                logging.info(f"{key} PG difference {diff} is acceptable")
            else:
                logging.error(f"{key} PG difference {diff} is not acceptable")
                osd_pg_value_flag = False
        if osd_pg_value_flag and eval <= 0.025:
            logging.info(
                f"Eval value is {eval} and pg distribution "
                f"average difference is <=10 which is acceptable"
            )
            return True
        else:
            logging.error(
                f"Eval value is {eval} and pg distribution "
                f"average difference is >=10 which is high and not acceptable"
            )
            return False
    else:
        logging.info("pg_balancer is not active")


def get_percent_used_capacity():
    """
    Function to calculate the percentage of used capacity in a cluster

    Returns:
        float: The percentage of the used capacity in the cluster

    """
    ct_pod = pod.get_ceph_tools_pod()
    output = ct_pod.exec_ceph_cmd(ceph_cmd='ceph df')
    total_used = (output.get('stats').get('total_used_raw_bytes'))
    total_avail = (output.get('stats').get('total_bytes'))
    return 100.0 * total_used / total_avail


def get_osd_pods_memory_sum():
    """
    Get the sum of memory of all OSD pods. This is used to determine the size
    needed for a PVC so when IO will be running over it the OSDs cache will be filled

    Returns:
        int: The sum of the OSD pods memory in GB

    """
    osd_pods = pod.get_osd_pods()
    num_of_osd_pods = len(osd_pods)
    osd_pod_mem_size_str = osd_pods[0].get_memory().get('osd')
    osd_pod_mem_size = convert_device_size(
        unformatted_size=osd_pod_mem_size_str, units_to_covert_to='GB'
    )
    return num_of_osd_pods * osd_pod_mem_size


def get_child_nodes_osd_tree(node_id, osd_tree):
    """
    This function finds the children of a node from the 'ceph osd tree' and returns them as list

    Args:
        node_id (int): the id of the node for which the children to be retrieved
        osd_tree (dict): dictionary containing the output of 'ceph osd tree'

    Returns:
        list: of 'children' of a given node_id

    """
    for i in range(len(osd_tree['nodes'])):
        if osd_tree['nodes'][i]['id'] == node_id:
            return osd_tree['nodes'][i]['children']


def check_osds_in_hosts_osd_tree(hosts, osd_tree):
    """
    Checks if osds are formed correctly after cluster expansion

    Args:
        hosts (list) : List of hosts
        osd_tree (str) : 'ceph osd tree' command output

    Returns:
        bool : True if osd tree formatted correctly

    """
    for each_host in hosts:
        osd_in_each_host = get_child_nodes_osd_tree(each_host, osd_tree)
        if len(osd_in_each_host) > 1 or len(osd_in_each_host) <= 0:
            logger.error("Error. ceph osd tree is NOT formed correctly after cluster expansion")
            return False

    logger.info("osd tree verification Passed")
    return True


def check_osd_tree_1az_vmware(osd_tree, number_of_osds):
    """
    Checks whether an OSD tree is created/modified correctly. This can be used as a verification step for
    deployment and cluster expansion tests.
    This function is specifically for ocs cluster created on 1 AZ VMWare setup

    Args:
        osd_tree (dict): Dictionary of the values which represent 'osd tree'.
        number_of_osds (int): total number of osds in the cluster

    Returns:
        bool: True, if the ceph osd tree is formed correctly. Else False

    """
    # in case of vmware, there will be only one zone as of now. The OSDs are arranged as follows:
    # ID  CLASS WEIGHT  TYPE NAME                            STATUS REWEIGHT PRI-AFF
    # -1       0.99326 root default
    # -8       0.33109     rack rack0
    # -7       0.33109         host ocs-deviceset-0-0-dktqc
    #  1   hdd 0.33109             osd.1                        up  1.00000 1.00000
    # There will be 3 racks - rack0, rack1, rack2.
    # When cluster expansion is successfully done, a host and an osd are added in each rack.
    # The number of hosts will be equal to the number osds the cluster has. Each rack can
    # have multiple hosts but each host will have only one osd under it.
    number_of_hosts_expected = int(number_of_osds / 3)
    all_hosts = []
    racks = osd_tree['nodes'][0]['children']

    for rack in racks:
        hosts = get_child_nodes_osd_tree(rack, osd_tree)
        if len(hosts) != number_of_hosts_expected:
            logging.error(f"Number of hosts under rack {rack} "
                          f"is not matching the expected ={number_of_hosts_expected} ")
            return False
        else:
            all_hosts.append(hosts)

    all_hosts_flatten = [item for sublist in all_hosts for item in sublist]
    return check_osds_in_hosts_osd_tree(all_hosts_flatten, osd_tree)


def check_osd_tree_3az_aws(osd_tree, number_of_osds):
    """
    Checks whether an OSD tree is created/modified correctly. This can be used as a verification step for
    deployment and cluster expansion tests.
    This function is specifically for ocs cluster created on 3 AZ AWS config

    Args:
        osd_tree (dict): Dictionary of the values which represent 'osd tree'.
        number_of_osds (int): total number of osds in the cluster

    Returns:
        Boolean: True, if the ceph osd tree is formed correctly. Else False

    """
    all_hosts = []
    region = osd_tree['nodes'][0]['children']

    zones = get_child_nodes_osd_tree(region[0], osd_tree)
    for each_zone in zones:
        hosts_in_each_zone = get_child_nodes_osd_tree(each_zone, osd_tree)
        if len(hosts_in_each_zone) != number_of_osds / 3:  # 3 is replica_factor
            logger.error("number of hosts in zone is incorrect")
            return False
        else:
            all_hosts.append(hosts_in_each_zone)

    all_hosts_flatten = [item for sublist in all_hosts for item in sublist]

    return check_osds_in_hosts_osd_tree(all_hosts_flatten, osd_tree)


def check_osd_tree_1az_aws(osd_tree, number_of_osds):
    """
    Checks whether an OSD tree is created/modified correctly. This can be used as a verification step for
    deployment and cluster expansion tests.
    This function is specifically for ocs cluster created on 1 AZ AWS config

    Args:
        osd_tree (dict): Dictionary of the values which represent 'osd tree'.
        number_of_osds (int): total number of osds in the cluster

    Returns:
        Boolean: True, if the ceph osd tree is formed correctly. Else False

    """
    all_hosts = []
    region = osd_tree['nodes'][0]['children']
    zones = get_child_nodes_osd_tree(region[0], osd_tree)
    racks = get_child_nodes_osd_tree(zones[0], osd_tree)
    logging.info(f"racks = {racks}")
    if len(racks) != 3:
        logging.error(f"Expected 3 racks but got {len(racks)}")
    for each_rack in racks:
        hosts_in_each_rack = get_child_nodes_osd_tree(each_rack, osd_tree)
        if len(hosts_in_each_rack) != number_of_osds / 3:  # 3 is replica_factor
            logging.error("number of hosts in rack is incorrect")
            return False
        else:
            logging.info(f"adding host...{hosts_in_each_rack}")
            all_hosts.append(hosts_in_each_rack)
    all_hosts_flatten = [item for sublist in all_hosts for item in sublist]

    return check_osds_in_hosts_osd_tree(all_hosts_flatten, osd_tree)


class CephClusterExternal(CephCluster):
    """
    Handle all external ceph cluster related functionalities
    Assumption: Cephcluster Kind resource exists

    """

    def __init__(self):
        self.POD = ocp.OCP(
            kind='Pod', namespace=config.ENV_DATA['cluster_namespace']
        )
        self.CEPHCLUSTER = ocp.OCP(
            kind='CephCluster', namespace=config.ENV_DATA['cluster_namespace']
        )

        self.wait_for_cluster_cr()
        self._cluster_name = (
            self.cluster_resource.get('metadata').get('name')
        )
        self._namespace = (
            self.cluster_resource.get('metadata').get('namespace')
        )
        self.cluster = ocs.OCS(**self.cluster_resource)
        self.wait_for_nooba_cr()

    @property
    def cluster_name(self):
        return self._cluster_name

    @property
    def namespace(self):
        return self._namespace

    @retry(IndexError, 10, 3, 1)
    def wait_for_cluster_cr(self):
        """
        we have to wait for cluster cr to appear else
        it leads to list index out of range error

        """
        cluster_cr = self.CEPHCLUSTER.get()
        self.cluster_resource = cluster_cr.get('items')[0]

    @retry((IndexError, AttributeError, TypeError), 100, 3, 1)
    def wait_for_nooba_cr(self):
        self._mcg_obj = MCG()

    def cluster_health_check(self, timeout=300):
        """
        This would be a comprehensive cluster health check
        which includes checking pods, external ceph cluster health.
        raise exceptions.CephHealthException("Cluster health is NOT OK")
        """
        sample = TimeoutSampler(
            timeout=timeout,
            sleep=3,
            func=self.is_health_ok
        )
        if not sample.wait_for_func_status(result=True):
            raise exceptions.CephHealthException("Cluster health is NOT OK")

        self.wait_for_noobaa_health_ok()
        self.validate_pvc()

    def validate_pvc(self):
        """
        Check whether all PVCs are in bound state

        """
        ocs_pvc_obj = get_all_pvc_objs(namespace=self.namespace)

        for pvc_obj in ocs_pvc_obj:
            assert pvc_obj.status == constants.STATUS_BOUND, {
                f"PVC {pvc_obj.name} is not Bound"
            }
            logger.info(f"PVC {pvc_obj.name} is in Bound state")
