"""
A module for all rook functionalities and abstractions.

This module has rook related classes, support for functionalities to work with
rook cluster. This works with assumptions that an OCP cluster is already
functional and proper configurations are made for interaction.
"""

import base64
import json
import logging
import random
import threading
import yaml
import time
import os
import pandas as pd
import re
import math

from datetime import datetime
from semantic_version import Version
from ocs_ci.utility.decorators import enable_high_recovery_during_rebalance_flag

from ocs_ci.ocs.utils import thread_init_class

import ocs_ci.ocs.resources.pod as pod
from ocs_ci.ocs.exceptions import (
    UnexpectedBehaviour,
    PDBNotCreatedException,
    PoolSizeWrong,
    PoolCompressionWrong,
    CommandFailed,
    LvDataPercentSizeWrong,
    ThinPoolUtilityWrong,
    TimeoutExpiredError,
    ResourceWrongStatusException,
    CephHealthException,
    ActiveMdsValueNotMatch,
)
from ocs_ci.ocs.resources import ocs, storage_cluster
import ocs_ci.ocs.constants as constant
from ocs_ci.ocs.resources.mcg import MCG
from ocs_ci.utility import version
from ocs_ci.utility.prometheus import PrometheusAPI
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import (
    TimeoutSampler,
    run_cmd,
    convert_device_size,
    convert_bytes_to_unit,
    get_trim_mean,
    ceph_health_check,
)
from ocs_ci.ocs.node import get_node_ip_addresses, wait_for_nodes_status
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.framework import config
from ocs_ci.ocs import ocp, constants, exceptions, defaults
from ocs_ci.ocs.exceptions import PoolNotFound
from ocs_ci.ocs.resources.pvc import get_all_pvc_objs
from ocs_ci.ocs.ocp import OCP, wait_for_cluster_connectivity
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.resources.pvc import PVC
from ocs_ci.utility.connection import Connection
from ocs_ci.utility.lvmo_utils import get_lvm_cluster_name
from ocs_ci.ocs.resources.pod import (
    get_mds_pods,
    wait_for_pods_to_be_in_statuses,
)

logger = logging.getLogger(__name__)


class CephClusterMultiCluster(object):
    """
    TODO: Implement this class later
    This class will be used in case of multicluster scenario
    and current cluster is ACM hence this cluster should point to
    the ODF which is not in current context

    """

    def __init__(self, cluster_conf=None):
        pass


class CephCluster(object):
    """
    Handles all cluster related operations from ceph perspective

    This class has depiction of ceph cluster. Contains references to
    pod objects which represents ceph cluster entities.

    Parameters
    ----------
    Attributes:
        pods (list): A list of ceph cluster related pods
        cluster_name (str): Name of ceph cluster
        namespace (str): openshift Namespace where this cluster lives
    """

    def __init__(self, cluster_config=None):
        """
        Cluster object initializer, this object needs to be initialized
        after cluster deployment. However its harmless to do anywhere.
        """
        if cluster_config:
            logger.info(
                "CephClusterMulticluster will be used to handle multicluster case"
            )
            return CephClusterMultiCluster()

        if config.ENV_DATA["mcg_only_deployment"] or (
            config.ENV_DATA.get("platform") == constants.FUSIONAAS_PLATFORM
            and config.ENV_DATA["cluster_type"].lower() == "consumer"
        ):
            return
        # cluster_name is name of cluster in rook of type CephCluster
        self.POD = ocp.OCP(kind="Pod", namespace=config.ENV_DATA["cluster_namespace"])
        self.CEPHCLUSTER = ocp.OCP(
            kind="CephCluster", namespace=config.ENV_DATA["cluster_namespace"]
        )
        self.CEPHFS = ocp.OCP(
            kind="CephFilesystem", namespace=config.ENV_DATA["cluster_namespace"]
        )
        self.RBD = ocp.OCP(
            kind="CephBlockPool", namespace=config.ENV_DATA["cluster_namespace"]
        )

        self.DEP = ocp.OCP(
            kind="Deployment", namespace=config.ENV_DATA["cluster_namespace"]
        )

        self.cluster_resource_config = self.CEPHCLUSTER.get().get("items")[0]
        try:
            self.cephfs_config = self.CEPHFS.get().get("items")[0]
        except IndexError as e:
            logger.warning(e)
            logger.warning("No CephFS found")
            self.cephfs_config = None

        try:
            self.rbd_config = self.RBD.get().get("items")[0]
        except IndexError as e:
            logger.warning(e)
            logger.warning("No RBD found")
            self.rbd_config = None

        self._cluster_name = self.cluster_resource_config.get("metadata").get("name")
        self._namespace = self.cluster_resource_config.get("metadata").get("namespace")

        # We are not invoking ocs.create() here
        # assuming cluster creation is done somewhere after deployment
        # So just load ocs with existing cluster details
        self.cluster = ocs.OCS(**self.cluster_resource_config)
        if self.cephfs_config:
            self.cephfs = ocs.OCS(**self.cephfs_config)
        else:
            self.cephfs = None

        if self.rbd_config:
            self.block = ocs.OCS(**self.rbd_config)
        else:
            self.block = None

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
        logger.info(f"Number of mons = {self.mon_count}")
        logger.info(f"Number of mds = {self.mds_count}")

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

    @retry(CommandFailed, tries=3, delay=10, backoff=1)
    def scan_cluster(self):
        """
        Get accurate info on current state of pods
        """
        self._ceph_pods = pod.get_all_pods(self._namespace)
        # TODO: Workaround for BZ1748325:
        mons = pod.get_mon_pods(self.mon_selector, self.namespace)
        self.mons = []
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
                self.cephfs_config = self.CEPHFS.get().get("items")[0]
                self.cephfs = ocs.OCS(**self.cephfs_config)
                self.cephfs.reload()
            except IndexError as e:
                logger.warning(e)
                logger.warning("No CephFS found")

        self.mon_count = len(set([mon.name for mon in self.mons]))
        self.mds_count = len(set([mds.name for mds in self.mdss]))
        self.mgr_count = len(set([mgr.name for mgr in self.mgrs]))
        self.osd_count = len(set([osd.name for osd in self.osds]))
        self.noobaa_count = len(set([noobaa.name for noobaa in self.noobaas]))
        self.rgw_count = len(set([rgw.name for rgw in self.rgws]))

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
        container = pod.pod_data.get("spec").get("containers")
        port = container[0]["ports"][0]["containerPort"]
        # Dynamically added attribute 'port'
        pod.port = port
        logger.info(f"port={pod.port}")
        return pod

    def is_health_ok(self):
        """
        Returns:
            bool: True if "HEALTH_OK" else False
        """
        self.cluster.reload()
        return self.cluster.data["status"]["ceph"]["health"] == "HEALTH_OK"

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
        sample = TimeoutSampler(timeout=timeout, sleep=3, func=self.is_health_ok)

        if not sample.wait_for_func_status(result=True):
            raise exceptions.CephHealthException("Cluster health is NOT OK")
        # This way of checking health of different cluster entities and
        # raising only CephHealthException is not elegant.
        # TODO: add an attribute in CephHealthException, called "reason"
        # which should tell because of which exact cluster entity health
        # is not ok ?
        self.scan_cluster()

        expected_mon_count = self.mon_count
        expected_mds_count = self.mds_count

        if config.ENV_DATA[
            "platform"
        ] in constants.HCI_PC_OR_MS_PLATFORM and config.ENV_DATA["cluster_type"] in [
            constants.MS_CONSUMER_TYPE,
            constant.HCI_CLIENT,
        ]:
            # on Managed Service Consumer cluster and HCI Client cluster,
            # check that there are no extra Ceph pods
            mon_pods = pod.get_mon_pods()
            if mon_pods:
                raise exceptions.CephHealthException(
                    "Managed Service Consumer cluster or HCI Client Cluster"
                    " shouldn't have any mon pods!"
                )
            osd_pods = pod.get_osd_pods()
            if osd_pods:
                raise exceptions.CephHealthException(
                    "Managed Service Consumer cluster or HCI Client Cluster "
                    "shouldn't have any osd pods!"
                )
            mds_pods = pod.get_mds_pods()
            if mds_pods:
                raise exceptions.CephHealthException(
                    "Managed Service Consumer cluster or HCI Client Cluster "
                    "shouldn't have any mds pods!"
                )
            return True

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
        if (
            config.ENV_DATA["platform"] not in constants.MANAGED_SERVICE_PLATFORMS
            and not config.COMPONENTS["disable_noobaa"]
        ):
            # skip noobaa healthcheck due to bug https://bugzilla.redhat.com/show_bug.cgi?id=2075422
            ocp_version = version.get_semantic_ocp_version_from_config()
            ocs_version = version.get_semantic_ocs_version_from_config()
            if config.DEPLOYMENT.get("live_deployment") and (
                (
                    ocp_version == version.VERSION_4_10
                    and ocs_version == version.VERSION_4_9
                )
                or (
                    ocp_version == version.VERSION_4_11
                    and ocs_version == version.VERSION_4_10
                )
            ):
                logger.info("skipping noobaa health check due to bug 2075422")
                return
            else:
                self.wait_for_noobaa_health_ok()

    def noobaa_health_check(self):
        """
        Check Noobaa health

        """
        if not self.mcg_obj.status:
            raise exceptions.NoobaaHealthException(
                "Noobaa is not in HEALTHY state. Please check noobaa, noobaa-default-backing-store"
                " and noobaa-default-bucket-class status"
            )

    def wait_for_noobaa_health_ok(self, tries=120, delay=5):
        """
        Wait for Noobaa health to be OK
        """
        return retry(
            (exceptions.NoobaaHealthException, exceptions.CommandFailed),
            tries=tries,
            delay=delay,
            backoff=1,
        )(self.noobaa_health_check)()

    def mon_change_count(self, new_count):
        """
        Change mon count in the cluster

        Args:
            new_count(int): Absolute number of mons required
        """
        self.cluster.reload()
        self.cluster.data["spec"]["mon"]["count"] = new_count
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
                condition="Running",
                selector=self.mon_selector,
                resource_count=count,
                timeout=timeout,
                sleep=3,
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
                f"Failed to achieve desired Mon count" f" {count}"
            )

    def mds_change_count(self, new_count):
        """
        Change mds count in the cluster

        Args:
            new_count(int): Absolute number of active mdss required
        """
        self.cephfs.data["spec"]["metadataServer"]["activeCount"] = new_count
        self.cephfs.apply(**self.cephfs.data)
        logger.info(f"MDS active count changed to {new_count}")
        if self.cephfs.data["spec"]["metadataServer"]["activeStandby"]:
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
                condition="Running",
                selector=self.mds_selector,
                resource_count=count,
                timeout=timeout,
                sleep=3,
            )
        except AssertionError as e:
            logger.error(e)
            raise exceptions.MDSCountException(
                f"Failed to achieve desired MDS count" f" {count}"
            )

    def get_admin_key(self):
        """
        Returns:
            adminkey (str): base64 encoded key
        """
        return self.get_user_key("client.admin")

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
        out = self.toolbox.exec_cmd_on_pod(f"ceph auth get-key {user} --format json")
        if "ENOENT" in out:
            return False
        key_base64 = base64.b64encode(out["key"].encode()).decode()
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
        logger.info(type(out))
        return self.get_user_key(username)

    def get_mons_from_cluster(self):
        """
        Getting the list of mons from the cluster

        Returns:
            available_mon (list): Returns the mons from the cluster
        """

        ret = self.DEP.get(
            resource_name="", out_yaml_format=False, selector="app=rook-ceph-mon"
        )
        available_mon = re.findall(r"[\w-]+mon-+[\w-]", ret)
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
            selector="app=rook-ceph-mon",
        )
        logger.info(f"Removed the mon {random_mon} from the cluster")
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
        used = rados_status["pools"][0]["size_bytes"]
        used_in_gb = format(used / constants.GB, ".4f")
        if self.used_space and self.used_space == used_in_gb:
            return float(self.used_space)
        self.used_space = used_in_gb
        raise UnexpectedBehaviour("In Rados df, Used size is varying")

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
            ceph_health_cmd,
            out_yaml_format=False,
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

    def get_ceph_default_replica(self):
        """
        The function return the default replica count in the system,
        taken from 'ceph status'. in case no parameter found, return '0'.

        Returns:
             int : the default replica count - 0 if not found.
        """
        ceph_pod = pod.get_ceph_tools_pod()
        ceph_status = ceph_pod.exec_ceph_cmd(ceph_cmd="ceph mgr dump")
        av_mod = ceph_status.get("available_modules")
        for mod in av_mod:
            if mod["name"] == "localpool":
                return mod.get("module_options").get("num_rep").get("default_value")
        logger.error("Replica count number did not found !")
        # if there is an error in the output of `ceph status` command and localpool
        # module does not exist, return 0 as number of replica.
        return 0

    def get_ceph_capacity(self, replica_divide=True):
        """
        The function gets the total mount of storage capacity of the ocs cluster.
        the calculation is <total bytes> / <replica number> depends if replia_divide is true
        it will not take into account the current used capacity.
        Args:
            replica_divide (bool): if true it will divide the capacity in to replica else return the capacity as it.
        Returns:
            int : Total storage capacity in GiB (GiB is for development environment)

        """
        ceph_pod = pod.get_ceph_tools_pod()
        ceph_status = ceph_pod.exec_ceph_cmd(ceph_cmd="ceph df")
        if replica_divide:
            replica = int(self.get_ceph_default_replica())
            logger.info(f"Number of replica : {replica}")
            usable_capacity = (
                int(ceph_status["stats"]["total_bytes"]) / replica / constant.GB
            )
        else:
            usable_capacity = int(ceph_status["stats"]["total_bytes"]) / constant.GB
        return usable_capacity

    def get_ceph_free_capacity(self):
        """
        Function to calculate the free capacity of a cluster

        Returns:
            float: The free capacity of a cluster (in GB)

        """
        replica = int(self.get_ceph_default_replica())
        if replica > 0:
            logger.info(f"Number of replica : {replica}")
            ct_pod = pod.get_ceph_tools_pod()
            output = ct_pod.exec_ceph_cmd(ceph_cmd="ceph df")
            total_avail = output.get("stats").get("total_bytes")
            total_used = output.get("stats").get("total_used_raw_bytes")
            total_free = total_avail - total_used
            return total_free / replica / constants.BYTES_IN_GB
        else:
            # if the replica number is 0, usable capacity can not be calculate
            # so, return 0 as usable capacity.
            return 0

    def get_ceph_cluster_iops(self):
        """
        The function gets the IOPS from the ocs cluster

        Returns:
            Total IOPS in the cluster

        """

        ceph_pod = pod.get_ceph_tools_pod()
        ceph_status = ceph_pod.exec_ceph_cmd(ceph_cmd="ceph status")
        read_ops = ceph_status["pgmap"]["read_op_per_sec"]
        write_ops = ceph_status["pgmap"]["write_op_per_sec"]
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
        logger.info(f"The IOPS percentage of the cluster is {iops_percentage}%")
        return iops_percentage

    def get_cluster_throughput(self):
        """
        Function to get the throughput of ocs cluster

        Returns:
            float: The write throughput of the cluster in MiB/s

        """
        ceph_status = self.get_ceph_status()
        for item in ceph_status.split("\n"):
            if "client" in item:
                throughput_data = item.strip("client: ").split(",")
                throughput_data = throughput_data[:2:1]
                # Converting all B/s and KiB/s to MiB/s
                throughput = 0
                for val in throughput_data:
                    throughput += [
                        float(re.findall(r"\d+", val)[0]) * constants.TP_CONVERSION[key]
                        for key in constants.TP_CONVERSION.keys()
                        if key in val
                    ][0]
                    logger.info(
                        f"The {val[-2:].upper()} throughput is {throughput} MiB/s"
                    )
                return throughput

    def get_throughput_percentage(self):
        """
        Function to get throughput percentage of the ocs cluster

        Returns:
            Throughput percentage of the cluster

        """

        throughput_of_cluster = self.get_cluster_throughput()
        throughput_percentage = (
            throughput_of_cluster / constants.THROUGHPUT_LIMIT_OSD
        ) * 100
        logger.info(
            f"The throughput percentage of the cluster is {throughput_percentage}%"
        )
        return throughput_percentage

    def calc_trim_mean_throughput(self, samples=8):
        """
        Calculate the cluster average throughput out of a few samples

        Args:
            samples (int): The number of samples to take

        Returns:
            float: The average cluster throughput

        """
        throughput_vals = [self.get_cluster_throughput() for _ in range(samples)]
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
        total_pg_count = ceph_status["pgmap"]["num_pgs"]
        pg_states = ceph_status["pgmap"]["pgs_by_state"]
        logger.info(ceph_health)
        logger.info(pg_states)
        for states in pg_states:
            return (
                states["state_name"] == "active+clean"
                and states["count"] == total_pg_count
            )

    @enable_high_recovery_during_rebalance_flag
    def wait_for_rebalance(self, timeout=600, repeat=3):
        """
        Wait for re-balance to complete

        Args:
            timeout (int): Time to wait for the completion of re-balance
            repeat (int): How many times to repeat the check to make sure, it's
                really completed.

        Returns:
            bool: True if re-balance completed, False otherwise

        """
        try:
            sleep_time = 10
            start_time = time.time()
            for attempt in range(1, repeat + 1):
                new_timeout = timeout - int(time.time() - start_time)
                if new_timeout < sleep_time:
                    new_timeout = sleep_time + 5
                logger.debug(f"Attempt {attempt} out of {repeat} repeats.")
                for rebalance in TimeoutSampler(
                    timeout=new_timeout,
                    sleep=sleep_time,
                    func=self.get_rebalance_status,
                ):
                    if rebalance:
                        logger.info(
                            f"Re-balance completed! This is attempt {attempt} out of {repeat} repeats. "
                            f"This rebalance check needs to prove it {repeat} times in row."
                        )
                        if repeat == attempt:
                            return True
                        else:
                            logger.debug(
                                f"Wait {sleep_time} seconds before next attempt to check re-balance has completed."
                            )
                            time.sleep(sleep_time)
                            break
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

    def set_pgs(self, poolname, pgs):
        """
        Setting up the PG / PGP / PG_MIN number of a pool
        if the pg_num_min is not setting to the pg_num number, the autoscale will
        set automaticlly the pg_num to 32 (incase you try to set pg_num > 32)

        Args:
            poolname (str): the pool name that need to be modify
            pgs (int): new number of PG's

        """
        for key in ["pg_num", "pgp_num", "pg_num_min"]:
            cmd = f"ceph osd pool set {poolname} {key} {pgs}"
            try:
                logger.debug(f"Try to set {key} to {pgs}")
                _ = self.toolbox.exec_ceph_cmd(ceph_cmd=cmd, format=None)
            except Exception as ex:
                logger.error(f"Failed to setup {key} : {ex}")

    def set_target_ratio(self, poolname, ratio):
        """
        Setting the target_size_ratio of a ceph pool

        Args:
            poolname (str): the pool name
            ratio (float): the new ratio to set

        """
        cmd = f"ceph osd pool set {poolname} target_size_ratio {ratio}"
        try:
            logger.debug(f"Try to set target_size_ratio on {poolname} to : {ratio}")
            _ = self.toolbox.exec_ceph_cmd(ceph_cmd=cmd, format=None)
        except Exception as ex:
            logger.error(f"Failed to change the ratio : {ex}")

    def get_cephfilesystem_status(self, fsname=None):
        """
        Getting the ceph filesystem status

        Args:
            fsname (str): The filesystem name

        Returnes:
            bool : true if the filesystem status is `Ready`, false otherwise
        """
        res = self.CEPHFS.get(resource_name=fsname)
        return res.get("status").get("phase") == constants.STATUS_READY

    def create_new_filesystem(self, fs_name):
        """
        Creating new filesystem to use in the tests insted of the default one.
        the new filesystem is identical (parameters wise) to the default filesystem

        Args:
            fs_name (str):  The name of the filesystem to create

        """
        # Creating the new filesystem using the default parameters
        self.cephfs.data["metadata"]["name"] = fs_name
        self.cephfs.apply(**self.cephfs.data)

        # Verify that the filesystem was created and the cluster if healthy
        sample = TimeoutSampler(
            timeout=120, sleep=3, func=self.get_cephfilesystem_status, fsname=fs_name
        )
        if not sample.wait_for_func_status(result=True):
            err_msg = "Can not create new filesystem"
            logger.error(err_msg)
            raise exceptions.CephHealthException(err_msg)

    def delete_filesystem(self, fs_name="ocs-storagecluster-cephfilesystem"):
        """
        Delete the ceph filesystem from the cluster, and wait until it recreated,
        then create the subvolumegroup on it.

        """

        # Delete the filesystem
        try:
            self.CEPHFS.delete(resource_name=fs_name)
        except Exception as ex:
            logger.warning(f"Cephfs filesystem deletion failed ({ex}).")

        if fs_name != "ocs-storagecluster-cephfilesystem":
            # don't need to wait until filesystem is recreated.
            return

        # The ceph filesystem is re-created automaticly
        logger.info(f"Wait until the CephFS {fs_name} is re-created")
        # wait 20 Sec. until the filesystem is fully created.
        time.sleep(20)
        try:
            self.CEPHFS.wait_for_resource(
                resource_name=fs_name,
                timeout=120,
                condition=constants.STATUS_READY,
                column="PHASE",
            )
        except Exception as ex:
            logger.warning(f"The CephFS filesystem failed to re-creste ({ex})")
        logger.info(f"The CephFS {fs_name} re-created.")
        logger.info("Try to re-create the subvolumegroup")
        cmd = f"ceph fs subvolumegroup create {fs_name} csi"
        self.toolbox.exec_cmd_on_pod(cmd, out_yaml_format=False)
        logger.info("The subvolumegroup was created.")

    def get_blockpool_status(self, poolname=None):
        """
        Getting the RBD pool status

        Args:
            fsname (str): The RBD pool name

        Returnes:
            bool : true if the RBD pool status is `Ready`, false otherwise
        """

        res = self.RBD.get(resource_name=poolname)
        return res.get("status").get("phase") == constants.STATUS_READY

    def create_new_blockpool(self, pool_name):
        """
        Creating new RBD pool to use in the tests insted of the default one.
        the new RBD pool is identical (parameters wise) to the default RBD pool

        Args:
            pool_name (str):  The name of the RBD pool to create

        """
        # Creating the new RBD pool using the default parameters
        self.block.data["metadata"]["name"] = pool_name
        self.block.apply(**self.block.data)

        # Verify that the RBD pool was created and the cluster if healthy
        sample = TimeoutSampler(
            timeout=120, sleep=3, func=self.get_blockpool_status, poolname=pool_name
        )
        if not sample.wait_for_func_status(result=True):
            err_msg = "Can not create new Block Pool"
            logger.error(err_msg)
            raise exceptions.CephHealthException(err_msg)

    def delete_blockpool(self, pool_name):
        """
        Delete a ceph RBD pool - not the default one - from the cluster

        Args:
            pool_name (str): the name of the RBD pool to delete
        """
        # Make sure the the default RBD pool is not deleted.
        if pool_name == "ocs-storagecluster-cephblockpool":
            return

        # To make the deletion time faster, delete the created pool brutally
        patch = (
            f"cephblockpool {pool_name} --type=merge -p "
            '\'{"metadata":{"finalizers":null}}\''
        )
        # Delete the RBD pool
        try:
            self.RBD.delete(resource_name=pool_name, wait=False)
        except Exception:
            logger.warning(f"BlockPoool {pool_name} couldnt delete")
            logger.info("Try to force delete it")
        # Wait for 30 seconds before brutally delete the bool.
        time.sleep(30)
        self.RBD.exec_oc_cmd(f"patch {patch}")


class MulticlusterCephHealthMonitor(object):
    # TODO: This will be a placeholder for now
    def __init__(self, ceph_cluster=None):
        pass

    def __enter__(self):
        pass

    def __exit__(self, exception_type, value, traceback):
        pass


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
        if isinstance(ceph_cluster, CephClusterMultiCluster):
            return MulticlusterCephHealthMonitor()
        self.ceph_cluster = ceph_cluster
        self.sleep = sleep
        self.health_error_status = None
        self.health_monitor_enabled = False
        self.latest_health_status = None
        super(CephHealthMonitor, self).__init__()

    def run(self):
        self.health_monitor_enabled = True
        while self.health_monitor_enabled and (not self.health_error_status):
            time.sleep(self.sleep)
            self.latest_health_status = self.ceph_cluster.get_ceph_health(detail=True)
            if "HEALTH_ERROR" in self.latest_health_status:
                self.health_error_status = self.ceph_cluster.get_ceph_status()
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
            f"ERROR HEALTH STATUS DETECTED! " f"Status: {self.health_error_status}"
        )


def validate_ocs_pods_on_pvc(pods, pvc_names, pvc_label=None):
    """
    Validate if ocs pod has PVC. This validation checking if there is the pvc
    like: rook-ceph-mon-a for the pod rook-ceph-mon-a-56f67f5968-6j4px.

    Args:
        pods (list): OCS pod names
        pvc_names (list): names of all PVCs
        pvc_label (str): label of PVC name for the pod. If None, we will verify
            pvc based on name of pod.

    Raises:
         AssertionError: If no PVC found for one of the pod

    """
    logger.info(f"Validating if each pod from: {pods} has PVC from {pvc_names}.")
    for pod_name in pods:
        if not pvc_label:
            found_pvc = ""
            for pvc in pvc_names:
                if pvc in pod_name:
                    found_pvc = pvc
            if found_pvc:
                logger.info(f"PVC {found_pvc} found for pod {pod_name}")
                continue
            assert found_pvc, f"No PVC found for pod: {pod_name}!"
        else:
            pod_obj = ocp.OCP(
                kind="Pod",
                namespace=config.ENV_DATA["cluster_namespace"],
                resource_name=pod_name,
            )
            pod_data = pod_obj.get()
            pod_labels = pod_data["metadata"].get("labels", {})
            pvc_name = pod_labels[pvc_label]
            assert (
                pvc_name in pvc_names
            ), f"No PVC {pvc_name} found for pod: {pod_name} in PVCs: {pvc_names}!"


@retry(CommandFailed, tries=3, delay=10, backoff=1)
def validate_claim_name_match_pvc(pvc_names, validated_pods=None):
    """
    Validate if OCS pods have mathching PVC and Claim name

    Args:
        pvc_names (list): names of all PVCs you would like to validate with.
        validated_pods(set): set to store already validated pods - if you pass
            an empty set from outside of this function, it will speed up the next
            validation when re-tries, as it will skip those already validated
            pods added to this set by previous run of this function.
    Raises:
        AssertionError: when the claim name does not match one of PVC name.

    """
    if validated_pods is None:
        validated_pods = set()
    ns = config.ENV_DATA["cluster_namespace"]
    mon_pods = get_pod_name_by_pattern("rook-ceph-mon", ns)
    osd_pods = get_pod_name_by_pattern("rook-ceph-osd", ns, filter="prepare")
    for ceph_pod in set(mon_pods + osd_pods) - validated_pods:
        out = run_cmd(f"oc -n {ns} get pods {ceph_pod} -o yaml")
        out_yaml = yaml.safe_load(out)
        for vol in out_yaml["spec"]["volumes"]:
            if vol.get("persistentVolumeClaim"):
                claimName = vol.get("persistentVolumeClaim").get("claimName")
                logger.info(f"{ceph_pod} backed by pvc {claimName}")
                assert claimName in pvc_names, "Ceph Internal Volume not backed by PVC"
        validated_pods.add(ceph_pod)


def validate_cluster_on_pvc():
    """
    Validate creation of PVCs for MON and OSD pods.
    Also validate that those PVCs are attached to the OCS pods

    Raises:
         AssertionError: If PVC is not mounted on one or more OCS pods

    """
    # Get the PVCs for selected label (MON/OSD)
    ns = config.ENV_DATA["cluster_namespace"]
    ocs_pvc_obj = get_all_pvc_objs(namespace=ns)

    # Check all pvc's are in bound state

    pvc_names = []
    for pvc_obj in ocs_pvc_obj:
        if pvc_obj.name.startswith(
            constants.DEFAULT_DEVICESET_PVC_NAME
        ) or pvc_obj.name.startswith(constants.DEFAULT_MON_PVC_NAME):
            assert (
                pvc_obj.status == constants.STATUS_BOUND
            ), f"PVC {pvc_obj.name} is not Bound"
            logger.info(f"PVC {pvc_obj.name} is in Bound state")
            pvc_names.append(pvc_obj.name)

    mon_pods = get_pod_name_by_pattern("rook-ceph-mon", ns)
    if not config.DEPLOYMENT.get("local_storage"):
        logger.info("Validating all mon pods have PVC")
        mon_pvc_label = constants.ROOK_CEPH_MON_PVC_LABEL
        if Version.coerce(config.ENV_DATA["ocs_version"]) < Version.coerce("4.6"):
            mon_pvc_label = None
        validate_ocs_pods_on_pvc(
            mon_pods,
            pvc_names,
            mon_pvc_label,
        )
    else:
        logger.debug(
            "Skipping validation if all mon pods have PVC because in LSO "
            "deployment we don't have mon pods backed by PVC"
        )
    logger.info("Validating all osd pods have PVC")
    osd_deviceset_pods = get_pod_name_by_pattern(
        "rook-ceph-osd-prepare-ocs-deviceset", ns
    )
    validate_ocs_pods_on_pvc(
        osd_deviceset_pods,
        pvc_names,
        constants.CEPH_ROOK_IO_PVC_LABEL,
    )
    validated_pods = set()
    validate_claim_name_match_pvc(pvc_names, validated_pods)


def count_cluster_osd():
    """
    The function returns the number of cluster OSDs

    Returns:
         osd_count (int): number of OSD pods in current cluster

    """
    storage_cluster_obj = storage_cluster.StorageCluster(
        resource_name=config.ENV_DATA["storage_cluster_name"],
        namespace=config.ENV_DATA["cluster_namespace"],
    )
    storage_cluster_obj.reload_data()
    osd_count = int(
        storage_cluster_obj.data["spec"]["storageDeviceSets"][0]["count"]
    ) * int(storage_cluster_obj.data["spec"]["storageDeviceSets"][0]["replica"])
    return osd_count


@retry((PDBNotCreatedException, AssertionError), tries=9, backoff=2)
def validate_pdb_creation():
    """
    Validate creation of PDBs for MON, MDS and OSD pods.

    Raises:
        AssertionError: If required PDBs were not created.

    """
    pdb_obj = ocp.OCP(
        kind="PodDisruptionBudget", namespace=config.ENV_DATA["cluster_namespace"]
    )
    item_list = pdb_obj.get().get("items")

    pdb_count = constants.PDB_COUNT_2_MGR
    pdb_required = [
        constants.MDS_PDB,
        constants.MON_PDB,
        constants.OSD_PDB,
        constants.MGR_PDB,
    ]

    # 4.19.0-59 is the stable build which doesn't contain the updated PDB count for Noobaa DB
    odf_running_version = version.get_ocs_version_from_csv(only_major_minor=True)

    if config.DEPLOYMENT.get("arbiter_deployment"):
        pdb_count = constants.PDB_COUNT_ARBITER
        pdb_required = [
            constants.MDS_PDB,
            constants.MON_PDB,
            constants.OSD_PDB,
            constants.MGR_PDB,
        ]
        if config.ENV_DATA["platform"].lower() == constants.VSPHERE_PLATFORM:
            pdb_count = constants.PDB_COUNT_ARBITER_VSPHERE
            pdb_required.append(constants.RGW_PDB)

    if odf_running_version >= version.VERSION_4_19:
        pdb_count += 1
        pdb_required.append(constants.NOOBAA_DB_PG_PDB)
    else:
        logger.info(f"Required PDB count is {pdb_count}")

    if len(item_list) != pdb_count:
        raise PDBNotCreatedException(
            f"Not All PDB's created. Expected {pdb_count} PDB's but found {len(item_list)}"
        )
    pdb_list = [item["metadata"]["name"] for item in item_list]

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
    for osd in output.get("nodes"):
        osd_filled[osd["name"]] = osd["utilization"]

    return osd_filled


def get_ceph_df_detail(format="json-pretty", out_yaml_format=True):
    """
    Get ceph osd df detail

    Returns:
         dict: 'ceph df details' command output

    """
    ceph_cmd = "ceph df detail"
    ct_pod = pod.get_ceph_tools_pod()
    return ct_pod.exec_ceph_cmd(
        ceph_cmd=ceph_cmd, format=format, out_yaml_format=out_yaml_format
    )


def parse_ceph_df_pools(raw_output: str) -> pd.DataFrame:
    """
    Parse the 'ceph df detail' command output and extract the POOLS section into a pandas DataFrame.

    Args:
        raw_output (str): The raw output string from the 'ceph df detail' command.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the parsed POOLS section data.
            The DataFrame includes columns for POOL, ID, PGS, STORED, OBJECTS, USED,
            %USED, MAX AVAIL, QUOTA OBJECTS, QUOTA BYTES, DIRTY, USED COMPR, and UNDER COMPR.

    Note:
        This function assumes a specific format for the 'ceph df detail' output.
        It extracts the POOLS section, processes the header and data rows,
        and returns a structured DataFrame for further analysis.

    """
    pools_section = (
        re.search(r"--- POOLS ---\n(.*)", raw_output, re.DOTALL).group(1).strip()
    )
    pools_lines = [line.strip() for line in pools_section.split("\n") if line.strip()]
    header = [
        "POOL",
        "ID",
        "PGS",
        "STORED",
        "(DATA)",
        "(OMAP)",
        "OBJECTS",
        "USED",
        "(DATA)",
        "(OMAP)",
        "%USED",
        "MAX AVAIL",
        "QUOTA OBJECTS",
        "QUOTA BYTES",
        "DIRTY",
        "USED COMPR",
        "UNDER COMPR",
    ]
    logger.info(f"Number of columns: {len(header)}")
    data = []
    for line in pools_lines[1:]:
        parts = re.split(r"\s{2,}", line.strip())
        if len(parts) == len(header):
            data.append(parts)
        else:
            logger.warning(f"Mismatch in column count for line: {line}")
            logger.warning(f"Expected {len(header)} columns, got {len(parts)}")
    df = pd.DataFrame(data, columns=header)

    return df


def ceph_details_df_to_dict(df: pd.DataFrame) -> dict:
    """
    Convert the DataFrame to a dictionary where the POOL column is the key
    and the rest of the columns form a nested dictionary.

    Args:
        df (pd.DataFrame): A pandas DataFrame containing Ceph pool information.

    Returns:
        dict: A dictionary where each key is a pool name, and the corresponding value
              is a nested dictionary containing the rest of the columns' data for that pool.

    """
    return {row["POOL"]: row.drop("POOL").to_dict() for _, row in df.iterrows()}


def validate_num_of_pgs(expected_pgs: dict[str, int]) -> bool:
    """
    Validate the number of PGs for each pool against expected values.

    Args:
        expected_pgs (dict[pool_name(str), expected_pg_num(int)]): A dictionary where keys
        are pool names and values are expected PG numbers.

    Returns:
        bool: True if all pools have the expected number of PGs, False otherwise.
    """

    ceph_df_output = get_ceph_df_detail(format=None, out_yaml_format=False)
    pools_df = parse_ceph_df_pools(ceph_df_output)
    pools_dict = ceph_details_df_to_dict(pools_df)

    for pool_name, expected_pg_num in expected_pgs.items():
        if pool_name not in pools_dict:
            logger.error(f"Pool {pool_name} not found in the cluster.")
            return False

        actual_pg_num = int(pools_dict[pool_name]["PGS"])
        if actual_pg_num != expected_pg_num:
            logger.error(
                f"Pool {pool_name} has {actual_pg_num} PGs, expected {expected_pg_num}."
            )
            return False

    logger.info("All pools have the expected number of PGs.")
    return True


def get_ceph_pool_property(pool_name, prop):
    """
    The fuction preform ceph osd pool get on a specific property.

    Args:
        pool_name (str): The pool name to get the property.
        prop (str): The property to get for example size, compression_mode etc.
    Returns:
        (str) property value as string and incase there is no property None.

    """
    ceph_cmd = f"ceph osd pool get {pool_name} {prop}"
    ct_pod = pod.get_ceph_tools_pod()
    try:
        ceph_pool_prop_output = ct_pod.exec_ceph_cmd(ceph_cmd=ceph_cmd)
        if ceph_pool_prop_output[prop]:
            return ceph_pool_prop_output[prop]
    except CommandFailed as err:
        logger.info(f"there was an error with the command {err}")
        return None


def check_pool_compression_replica_ceph_level(pool_name, compression, replica):
    """
    Validate compression and replica values in ceph level

    Args:
         pool_name (str): The pool name to check values.
         compression (bool): True for compression otherwise False.
         replica (int): size of pool to verify.

    Returns:
        (bool) True if replica and compression are validated. Otherwise raise Exception.

    """
    compression_output = None
    expected_compression_output = None

    if compression:
        expected_compression_output = "aggressive"
        compression_output = get_ceph_pool_property(pool_name, "compression_mode")
    else:
        if get_ceph_pool_property(pool_name, "compression_mode") is None:
            expected_compression_output = True
            compression_output = True

    replica_output = get_ceph_pool_property(pool_name, "size")
    if compression_output == expected_compression_output and replica_output == replica:
        logger.info(
            f"Pool {pool_name} was validated in ceph level with compression {compression}"
            f" and replica {replica}"
        )
        return True
    else:
        if compression_output != expected_compression_output:
            raise PoolCompressionWrong(
                f"Expected compression to be {expected_compression_output} but found {compression_output}"
            )
        if replica_output != replica:
            raise PoolSizeWrong(f"Replica should be {replica} but is {replica_output}")


def validate_replica_data(pool_name, replica):
    """
    Check if data is replica 2 or 3

    Args:
        replica (int): size of the replica(2,3)
        pool_name (str): name of the pool to check replica

    Returns:
        Bool: True if replicated data size is meet rep config and False if dont

    """

    ceph_df_detail_output = get_ceph_df_detail()
    pool_list = ceph_df_detail_output.get("pools")
    for pool in pool_list:
        if pool.get("name") == pool_name:
            logger.info(f"{pool_name}")
            stored = pool["stats"]["stored"]
            byte_used = pool["stats"]["bytes_used"]
            compress_bytes_used = pool["stats"]["compress_bytes_used"]
            compress_under_bytes = pool["stats"]["compress_under_bytes"]
            byte_used = byte_used + compress_under_bytes - compress_bytes_used
            store_ratio = byte_used / stored
            if (replica + 0.2) > store_ratio > (replica - 0.2):
                logger.info(f"pool {pool_name} meet rep {replica} size")
                return True
            else:
                logger.info(
                    f"pool {pool_name} meet do not meet rep {replica}"
                    f" size Store ratio is {store_ratio}"
                )

                return False
    raise PoolNotFound(f"Pool {pool_name} not found on cluster")


def get_byte_used_by_pool(pool_name):
    """
    Check byte_used value for specific pool

    Args:
        pool_name (str): name of the pool to check replica

    Returns:
        integer: The amount of byte stored from pool.

    """
    ceph_df_detail_output = get_ceph_df_detail()
    pool_list = ceph_df_detail_output.get("pools")
    for pool in pool_list:
        if pool.get("name") == pool_name:
            byte_used = pool["stats"]["bytes_used"]
            return byte_used
    raise PoolNotFound(f"Pool {pool_name} not found on cluster")


def calculate_compression_ratio(pool_name):
    """
    Calculating the compression of data on RBD pool

    Args:
        pool_name (str): the name of the pool to calculate the ratio on

    Returns:
        int: the compression ratio in percentage

    """
    results = get_ceph_df_detail()
    for pool in results["pools"]:
        if pool["name"] == pool_name:
            used = pool["stats"]["bytes_used"]
            used_cmp = pool["stats"]["compress_bytes_used"]
            stored = pool["stats"]["stored"]
            ratio = int((used_cmp * 100) / (used_cmp + used))
            logger.info(f"pool name is {pool_name}")
            logger.info(f"net stored data is {stored}")
            logger.info(f"total used data is {used}")
            logger.info(f"compressed data is {used_cmp}")
            return ratio

    logger.warning(f"the pool {pool_name} does not exits !")
    return None


def validate_compression(pool_name):
    """
    Check if data was compressed

    Args:
        pool_name (str): name of the pool to check replica

    Returns:
        bool: True if compression works. False if not

    """
    pool_replica = get_ceph_pool_property(pool_name, "size")
    ceph_df_detail_output = get_ceph_df_detail()
    pool_list = ceph_df_detail_output.get("pools")
    for pool in pool_list:
        if pool.get("name") == pool_name:
            logger.info(f"{pool_name}")
            stored = pool["stats"]["stored"]
            used_without_compression = stored * pool_replica
            compress_under_bytes = pool["stats"]["compress_under_bytes"]
            compression_ratio = compress_under_bytes / used_without_compression
            logger.info(f"this is the comp_ratio {compression_ratio}")
            if 0.6 < compression_ratio:
                logger.info(
                    f"Compression ratio {compression_ratio} is " f"larger than 0.6"
                )
                return True
            else:
                logger.info(
                    f"Compression ratio {compression_ratio} is " f"smaller than 0.6"
                )
                return False
    raise PoolNotFound(f"Pool {pool_name} not found on cluster")


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
    for osd in output.get("nodes"):
        osd_dict[osd["name"]] = osd["pgs"]

    return osd_dict


def get_balancer_eval():
    """
    Function to get ceph pg balancer eval value

    Returns:
        eval_out (float): Eval output of pg balancer

    """
    ceph_cmd = "ceph balancer eval"
    ct_pod = pod.get_ceph_tools_pod()
    eval_out = ct_pod.exec_ceph_cmd(ceph_cmd=ceph_cmd).split(" ")
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
    if output["active"] and output["mode"] == "upmap":
        logger.info("PG balancer is active and mode is upmap")
        return True
    else:
        logger.error("PG balancer is not active")
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
                logger.info(f"{key} PG difference {diff} is acceptable")
            else:
                logger.error(f"{key} PG difference {diff} is not acceptable")
                osd_pg_value_flag = False
        if osd_pg_value_flag and eval <= 0.025:
            logger.info(
                f"Eval value is {eval} and pg distribution "
                f"average difference is <=10 which is acceptable"
            )
            return True
        else:
            logger.error(
                f"Eval value is {eval} and pg distribution "
                f"average difference is >=10 which is high and not acceptable"
            )
            return False
    else:
        logger.info("pg_balancer is not active")


@retry((ZeroDivisionError, CommandFailed))
def get_percent_used_capacity():
    """
    Function to calculate the percentage of used capacity in a cluster

    Returns:
        float: The percentage of the used capacity in the cluster

    """
    ct_pod = pod.get_ceph_tools_pod()
    output = ct_pod.exec_ceph_cmd(ceph_cmd="ceph df")
    total_used = output.get("stats").get("total_used_raw_bytes")
    total_avail = output.get("stats").get("total_bytes")
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
    osd_pod_mem_size_str = osd_pods[0].get_memory(container_name=constants.OSD)
    osd_pod_mem_size = convert_device_size(
        unformatted_size=osd_pod_mem_size_str, units_to_covert_to="GB"
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
    for i in range(len(osd_tree["nodes"])):
        if osd_tree["nodes"][i]["id"] == node_id:
            return osd_tree["nodes"][i]["children"]


def get_nodes_osd_tree(osd_tree, node_ids=None):
    """
    This function gets the 'ceph osd tree' nodes, which have the ids 'node_ids', and returns
    them as a list. If 'node_ids' are not passed, it returns all the 'ceph osd tree' nodes.

    Args:
        osd_tree (dict): Dictionary containing the output of 'ceph osd tree'
        node_ids (list): The ids of the nodes for which we want to retrieve

    Returns:
        list: The nodes of a given 'node_ids'. If 'node_ids' are not passed,
            it returns all the nodes.

    """
    if not node_ids:
        return osd_tree["nodes"]

    # Convert to set to reduce complexity
    node_ids = set(node_ids)
    return [node for node in osd_tree["nodes"] if node["id"] in node_ids]


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
            logger.error(
                "Error. ceph osd tree is NOT formed correctly after cluster expansion"
            )
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
    racks = osd_tree["nodes"][0]["children"]

    for rack in racks:
        hosts = get_child_nodes_osd_tree(rack, osd_tree)
        if len(hosts) != number_of_hosts_expected:
            logger.error(
                f"Number of hosts under rack {rack} "
                f"is not matching the expected ={number_of_hosts_expected} "
            )
            return False
        else:
            all_hosts.append(hosts)

    all_hosts_flatten = [item for sublist in all_hosts for item in sublist]
    return check_osds_in_hosts_osd_tree(all_hosts_flatten, osd_tree)


def check_osd_tree_3az_cloud(osd_tree, number_of_osds):
    """
    Checks whether an OSD tree is created/modified correctly. This can be used as a verification step for
    deployment and cluster expansion tests.
    This function is specifically for ocs cluster created on 3 AZ config

    Args:
        osd_tree (dict): Dictionary of the values which represent 'osd tree'.
        number_of_osds (int): total number of osds in the cluster

    Returns:
        Boolean: True, if the ceph osd tree is formed correctly. Else False

    """
    all_hosts = []
    region = osd_tree["nodes"][0]["children"]

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


def check_osd_tree_1az_cloud(osd_tree, number_of_osds):
    """
    Checks whether an OSD tree is created/modified correctly. This can be used as a verification step for
    deployment and cluster expansion tests.
    This function is specifically for ocs cluster created on 1 AZ config

    Args:
        osd_tree (dict): Dictionary of the values which represent 'osd tree'.
        number_of_osds (int): total number of osds in the cluster

    Returns:
        Boolean: True, if the ceph osd tree is formed correctly. Else False

    """
    all_hosts = []
    region = osd_tree["nodes"][0]["children"]
    zones = get_child_nodes_osd_tree(region[0], osd_tree)
    racks = get_child_nodes_osd_tree(zones[0], osd_tree)
    logger.info(f"racks = {racks}")
    if len(racks) != 3:
        logger.error(f"Expected 3 racks but got {len(racks)}")
    for each_rack in racks:
        hosts_in_each_rack = get_child_nodes_osd_tree(each_rack, osd_tree)
        if len(hosts_in_each_rack) != number_of_osds / 3:  # 3 is replica_factor
            logger.error("number of hosts in rack is incorrect")
            return False
        else:
            logger.info(f"adding host...{hosts_in_each_rack}")
            all_hosts.append(hosts_in_each_rack)
    all_hosts_flatten = [item for sublist in all_hosts for item in sublist]

    return check_osds_in_hosts_osd_tree(all_hosts_flatten, osd_tree)


def check_osd_tree_1az_vmware_flex(osd_tree, number_of_osds):
    """
    Checks whether an OSD tree is created/modified correctly. This can be used as a verification step for
    deployment and cluster expansion tests.
    This function is specifically for ocs cluster created on 1 AZ VMWare LSO setup

    Args:
        osd_tree (dict): Dictionary of the values which represent 'osd tree'.
        number_of_osds (int): total number of osds in the cluster

    Returns:
        bool: True, if the ceph osd tree is formed correctly. Else False

    """
    # in case of vmware, there will be only one zone as of now.
    # If it's also an lso we use failure domain 'host'. The OSDs are arranged as follows:
    # ID CLASS WEIGHT  TYPE NAME          STATUS REWEIGHT PRI-AFF
    # -1       0.29306 root default
    # -7       0.09769     host compute-0
    #  2   hdd 0.09769         osd.2          up  1.00000 1.00000
    # -3       0.09769     host compute-1
    #  0   hdd 0.09769         osd.0          up  1.00000 1.00000
    # -5       0.09769     host compute-2
    #  1   hdd 0.09769         osd.1          up  1.00000 1.00000
    # There will be no racks, and we will have a failure domain 'host'.
    # When cluster expansion is successfully done, an osd are added in each host.
    # Each host will have one or multiple osds under it
    hosts = osd_tree["nodes"][0]["children"]
    host_nodes = get_nodes_osd_tree(osd_tree, hosts)
    osd_ids = []

    for node in host_nodes:
        node_name = node["name"]
        node_type = node["type"]
        expected_node_type = "host"
        if node_type != expected_node_type:
            logger.warning(
                f"The node with the name '{node_name}' is with type '{node_type}' instead of "
                f"the expected type '{expected_node_type}'"
            )
            return False

        node_osd_ids = get_child_nodes_osd_tree(node["id"], osd_tree)
        if len(node_osd_ids) <= 0:
            logger.warning(
                f"Error. Ceph osd tree is NOT formed correctly. "
                f"The node with the name '{node_name}' has no osds"
            )
            return False
        osd_ids.extend(node_osd_ids)

    if len(osd_ids) != number_of_osds:
        logger.warning(
            f"The number of osd ids in the ceph osd tree is {len(osd_ids)} instead of "
            f"the expected number {number_of_osds}"
        )
        return False

    logger.info("Ceph osd tree is formed correctly")
    return True


def check_osds_in_hosts_are_up(osd_tree):
    """
    Check if all the OSD's in status 'up'

    Args:
        osd_tree (dict): The ceph osd tree

    Returns:
        bool: True if all the OSD's in status 'up'. Else False

    """
    for n in osd_tree["nodes"]:
        if n["type"] == "osd":
            if n["status"] != "up":
                logger.warning(f"osd with name {n['name']} is not up")
                return False

    return True


def check_ceph_osd_tree():
    """
    Checks whether an OSD tree is created/modified correctly.
    It is a summary of the previous functions: 'check_osd_tree_1az_vmware',
    'check_osd_tree_3az_cloud', 'check_osd_tree_1az_cloud'.

    Returns:
         bool: True, if the ceph osd tree is formed correctly. Else False

    """
    osd_pods = pod.get_osd_pods()
    number_of_osds = len(osd_pods)
    # 'ceph osd tree' should show the new osds under right nodes/hosts
    #  Verification is different for 3 AZ and 1 AZ configs
    ct_pod = pod.get_ceph_tools_pod()
    tree_output = ct_pod.exec_ceph_cmd(ceph_cmd="ceph osd tree")
    if config.ENV_DATA["platform"].lower() == constants.VSPHERE_PLATFORM:
        if is_flexible_scaling_enabled():
            return check_osd_tree_1az_vmware_flex(tree_output, number_of_osds)
        else:
            return check_osd_tree_1az_vmware(tree_output, number_of_osds)

    number_of_zones = 3
    if config.ENV_DATA["platform"].lower() in constants.CLOUD_PLATFORMS:
        # parse the osd tree. if it contains a node 'rack' then it's a
        # 1AZ cluster. Else, 3 3AZ cluster
        for i in range(len(tree_output["nodes"])):
            if tree_output["nodes"][i]["name"] in "rack0":
                number_of_zones = 1
        if number_of_zones == 1:
            return check_osd_tree_1az_cloud(tree_output, number_of_osds)
        else:
            return check_osd_tree_3az_cloud(tree_output, number_of_osds)


def check_ceph_osd_tree_after_node_replacement():
    """
    Check the ceph osd tree after the process of node replacement.

    Returns:
        bool: True if the ceph osd tree formation is correct,
        and all the OSD's are up. Else False

    """
    ct_pod = pod.get_ceph_tools_pod()
    osd_tree = ct_pod.exec_ceph_cmd(ceph_cmd="ceph osd tree")
    if not check_ceph_osd_tree():
        logger.warning("Incorrect ceph osd tree formation found")
        return False

    if not check_osds_in_hosts_are_up(osd_tree):
        logger.warning("Not all the osd's are in status 'up'")
        return False

    return True


def silence_ceph_osd_crash_warning(osd_pod_name):
    """
    Silence the osd crash warning of a specific osd pod

    Args:
        osd_pod_name (str): The name of the osd pod which we need to
            silence the crash warning

    Returns:
        bool: True if it found the osd crash with name 'osd_pod_name'. False otherwise

    """
    ct_pod = pod.get_ceph_tools_pod()
    new_crash_objects_list = ct_pod.exec_ceph_cmd(ceph_cmd="ceph crash ls-new")
    for crash_obj in new_crash_objects_list:
        if crash_obj.get("utsname_hostname") == osd_pod_name:
            logger.info(f"Found osd crash with name {osd_pod_name}")
            obj_crash_id = crash_obj.get("crash_id")
            crash_info = ct_pod.exec_ceph_cmd(
                ceph_cmd=f"ceph crash info {obj_crash_id}"
            )
            logger.info(f"ceph crash info: {crash_info}")

            logger.info("silence the osd crash warning")
            ct_pod.exec_ceph_cmd(ceph_cmd=f"ceph crash archive {obj_crash_id}")
            return True

    logger.info(
        f"Didn't find osd crash with name {osd_pod_name} in ceph crash warnings"
    )
    return False


def wait_for_silence_ceph_osd_crash_warning(osd_pod_name, timeout=900):
    """
    Wait for 'timeout' seconds to check for the ceph osd crash warning,
    and silence it.

    Args:
        osd_pod_name (str): The name of the osd pod which we need to
            silence the crash warning
        timeout (int): time in seconds to wait for silence the osd crash warning

    Returns:
        bool: True if it found the osd crash with name 'osd_pod_name'. False otherwise

    """
    try:
        for silence_old_osd_crash_warning in TimeoutSampler(
            timeout=timeout,
            sleep=30,
            func=silence_ceph_osd_crash_warning,
            osd_pod_name=osd_pod_name,
        ):
            if silence_old_osd_crash_warning:
                return True
    except TimeoutExpiredError:
        return False


def get_mon_config_value(key):
    """
    Gets the default value of a specific ceph monitor config

    Args:
        key (str): Configuration key. Ex: mon_max_pg_per_osd

    Returns:
        any: Ceph monitor configuration value

    """
    ct_pod = pod.get_ceph_tools_pod()
    mon_dump_dict = ct_pod.exec_ceph_cmd("ceph mon dump")
    ceph_mon_name = mon_dump_dict.get("mons")[0].get("name")
    mon_config_value = ct_pod.exec_ceph_cmd(
        f"ceph config show mon.{ceph_mon_name} {key}"
    )
    return mon_config_value


def get_mds_cache_memory_limit():
    """
    Get the default value of mds

    Returns:
        int: Value of mds cache memory limit

    Raises:
        UnexpectedBehaviour: if MDS-a and MDS-b cache memory limit doesn't match
        IOError if fail to read configuration

    """
    pod_obj = pod.get_ceph_tools_pod()
    ceph_cmd = "ceph config show mds.ocs-storagecluster-cephfilesystem-a mds_cache_memory_limit"
    mds_a_cache_memory_limit = pod_obj.exec_ceph_cmd(ceph_cmd=ceph_cmd)
    ceph_cmd = "ceph config show mds.ocs-storagecluster-cephfilesystem-b mds_cache_memory_limit"
    mds_b_cache_memory_limit = pod_obj.exec_ceph_cmd(ceph_cmd=ceph_cmd)

    if mds_a_cache_memory_limit != mds_b_cache_memory_limit:
        raise UnexpectedBehaviour(
            f"mds_a_cache_memory_limit: {mds_a_cache_memory_limit}. "
            f"mds_b_cache_memory_limit: {mds_b_cache_memory_limit}"
        )
    return int(mds_a_cache_memory_limit)


def is_lso_cluster():
    """
    Check if the cluster is an lso cluster

    Returns:
        bool: True, if the cluster is an lso cluster. False, otherwise

    """
    return config.DEPLOYMENT.get("local_storage", False)


def is_flexible_scaling_enabled():
    """
    Check if flexible scaling is enabled

    Returns:
        bool: True if failure domain is "host" and flexible scaling is enabled. False otherwise

    """
    ocs_storage_cluster = storage_cluster.get_storage_cluster().get()["items"][0]

    failure_domain = ocs_storage_cluster.get("status").get("failureDomain")
    flexible_scaling = ocs_storage_cluster.get("spec").get("flexibleScaling")
    return failure_domain == "host" and flexible_scaling


def check_ceph_health_after_add_capacity(
    ceph_health_tries=80, ceph_rebalance_timeout=2400
):
    """
    Check Ceph health after adding capacity to the cluster

    Args:
        ceph_health_tries (int): The number of tries to wait for the Ceph health to be OK.
        ceph_rebalance_timeout (int): The time to wait for the Ceph cluster rebalanced.

    """
    if config.RUN.get("io_in_bg"):
        logger.info(
            "Increase the time to wait for Ceph health to be health OK, "
            "because we run IO in the background"
        )
        additional_ceph_health_tries = int(config.RUN.get("io_load") * 1.3)
        ceph_health_tries += additional_ceph_health_tries

        additional_ceph_rebalance_timeout = int(config.RUN.get("io_load") * 100)
        ceph_rebalance_timeout += additional_ceph_rebalance_timeout

    ceph_health_check(
        namespace=config.ENV_DATA["cluster_namespace"], tries=ceph_health_tries
    )
    ceph_cluster_obj = CephCluster()
    assert ceph_cluster_obj.wait_for_rebalance(
        timeout=ceph_rebalance_timeout
    ), "Data re-balance failed to complete"


def change_ceph_backfillfull_ratio(backfillfull_ratio):
    """
    Change Ceph Backfillfull Ratio

    Args:
        backfillfull_ratio (int): backfillfull_ratio

    """
    ceph_cmd = f"ceph osd set-backfillfull-ratio {str(float(backfillfull_ratio / 100))}"
    ct_pod = pod.get_ceph_tools_pod()
    ct_pod.exec_ceph_cmd(ceph_cmd=ceph_cmd)


def change_ceph_full_ratio(full_ratio):
    """
    Change Ceph full_ratio

    Args:
        full_ratio (int): backfillfull_ratio

    """
    ceph_cmd = f"ceph osd set-full-ratio {str(float(full_ratio / 100))}"
    ct_pod = pod.get_ceph_tools_pod()
    ct_pod.exec_ceph_cmd(ceph_cmd=ceph_cmd)


def validate_existence_of_blocking_pdb():
    """
    Validate creation of PDBs for OSDs.
    1. Versions lesser than ocs-operator.v4.6.2 have PDBs for each osds
    2. Versions greater than or equal to ocs-operator.v4.6.2-233.ci have
    PDBs collectively for osds like rook-ceph-osd

    Returns:
        bool: True if blocking PDBs are present, false otherwise

    """
    pdb_obj = ocp.OCP(
        kind=constants.POD_DISRUPTION_BUDGET,
        namespace=config.ENV_DATA["cluster_namespace"],
    )
    pdb_obj_get = pdb_obj.get()
    osd_pdb = []
    for pdb in pdb_obj_get.get("items"):
        # blocking OSD PDBs are in the format of rook-ceph-osd-zone-data-1
        if constants.OSD_PDB in pdb["metadata"]["name"]:
            osd_pdb.append(pdb)
    blocking_pdb_exist = False
    for osd in range(len(osd_pdb)):
        allowed_disruptions = osd_pdb[osd].get("status").get("disruptionsAllowed")
        maximum_unavailable = osd_pdb[osd].get("spec").get("maxUnavailable")
        if allowed_disruptions & (maximum_unavailable != 1):
            logger.info(
                f"Blocking PDB {osd_pdb[osd].get('metadata').get('name')} are created"
            )
            blocking_pdb_exist = True
            return blocking_pdb_exist
        else:
            logger.info(
                f"No blocking PDBs created, OSD PDB is {osd_pdb[osd].get('metadata').get('name')}"
            )
    return blocking_pdb_exist


def is_managed_service_cluster():
    """
    Check if the cluster is a managed service cluster

    Returns:
        bool: True, if the cluster is a managed service cluster. False, otherwise

    """
    return config.ENV_DATA["platform"].lower() in constants.MANAGED_SERVICE_PLATFORMS


def is_hci_cluster():
    """
    Check if the cluster is an hci provider or client cluster

    Returns:
        bool: True, if the cluster is an hci cluster. False, otherwise

    """
    return (
        config.ENV_DATA["platform"].lower() in constants.HCI_PROVIDER_CLIENT_PLATFORMS
    )


def is_ms_consumer_cluster():
    """
    Check if the cluster is a managed service consumer cluster

    Returns:
        bool: True, if the cluster is a managed service consumer cluster. False, otherwise

    """
    return (
        config.ENV_DATA["platform"].lower() in constants.MANAGED_SERVICE_PLATFORMS
        and config.ENV_DATA["cluster_type"].lower() == "consumer"
    )


def is_hci_client_cluster():
    """
    Check if the cluster is a Fusion HCI Client cluster

    Returns:
        bool: True, if the cluster is a Fusion HCI client cluster. False, otherwise

    """
    return (
        config.ENV_DATA["platform"].lower() in constants.HCI_PROVIDER_CLIENT_PLATFORMS
        and config.ENV_DATA["cluster_type"].lower() == constants.HCI_CLIENT
    )


def is_ms_provider_cluster():
    """
    Check if the cluster is a managed service provider cluster

    Returns:
        bool: True, if the cluster is a managed service provider cluster. False, otherwise

    """
    return (
        config.ENV_DATA["platform"].lower() in constants.MANAGED_SERVICE_PLATFORMS
        and config.ENV_DATA["cluster_type"].lower() == "provider"
    )


def is_hci_provider_cluster():
    """
    Check if the cluster is a Fusion HCI provider cluster

    Returns:
        bool: True, if the cluster is a Fusion HCI provider cluster. False, otherwise

    """
    return (
        config.ENV_DATA["platform"].lower() in constants.HCI_PROVIDER_CLIENT_PLATFORMS
        and config.ENV_DATA["cluster_type"].lower() == constants.HCI_PROVIDER
    )


def get_osd_dump(pool_name):
    """
    Get the osd dump part of a given pool

    Args:
        pool_name (str): ceph pool name

    Returns:
        dict: pool information from osd dump

    """
    osd_dump_dict = pod.get_ceph_tools_pod().exec_ceph_cmd("ceph osd dump")
    for pool in osd_dump_dict["pools"]:
        if pool["pool_name"] == pool_name:
            return pool
    assert False, "Failed to get the pool information from osd dump"


def get_pool_num(pool_name):
    """
    Get the pool number of a given pool (e.g., ocs-storagecluster-cephblockpool -> 2)

    Args:
        pool_name (str): ceph pool name

    Returns:
        int: pool number

    """
    return int(get_osd_dump(pool_name)["pool"])


def get_pgs_brief_dump():
    """
    Get pgs_brief dump from ceph pg dump

    Returns:
        dict: pgs_brief dump output

    """
    pgs_brief_dict = pod.get_ceph_tools_pod().exec_ceph_cmd("ceph pg dump pgs_brief")

    return pgs_brief_dict


def get_all_pgid():
    """
    Get all the pgid's listed in pgs_brief dump

    Returns:
        list: List of all the pgid's in pgs_brief dump

    """
    pg_dump_dict = get_pgs_brief_dump()

    return [pgid["pgid"] for pgid in pg_dump_dict["pg_stats"]]


def get_specific_pool_pgid(pool_name):
    """
    Get all the pgid's of a specific pool

    Args:
        pool_name (str): ceph pool name

    Returns:
        list: List of all the pgid's of a given pool

    """
    pool_num = get_pool_num(pool_name)
    all_pgid = get_all_pgid()

    return list(filter(lambda x: x.startswith(f"{pool_num}."), all_pgid))


def get_osd_pg_log_dups_tracked():
    """
    Get the default tracked number of osd pg log dups

    Returns:
        int: Number of default tracked osd pg log dups

    """
    ct_pod = pod.get_ceph_tools_pod()
    osd_pg_log_dups_count = ct_pod.exec_ceph_cmd(
        "ceph config get osd osd_pg_log_dups_tracked"
    )
    logger.info(f"Default number of osd pg log dups:{osd_pg_log_dups_count}")

    return int(osd_pg_log_dups_count)


def is_vsphere_ipi_cluster():
    """
    Check if the cluster is a vSphere IPI cluster

    Returns:
        bool: True, if the cluster is a vSphere IPI cluster. False, otherwise

    """
    return (
        config.ENV_DATA["platform"].lower() == constants.VSPHERE_PLATFORM
        and config.ENV_DATA["deployment_type"] == "ipi"
    )


class CephClusterExternal(CephCluster):
    """
    Handle all external ceph cluster related functionalities
    Assumption: Cephcluster Kind resource exists

    """

    def __init__(self):
        if config.DEPLOYMENT.get("multi_storagecluster"):
            namespace = constants.OPENSHIFT_STORAGE_EXTENDED_NAMESPACE
        else:
            namespace = config.ENV_DATA["cluster_namespace"]
        self.POD = ocp.OCP(kind="Pod", namespace=namespace)
        self.CEPHCLUSTER = ocp.OCP(kind="CephCluster", namespace=namespace)

        self.wait_for_cluster_cr()
        self._cluster_name = self.cluster_resource.get("metadata").get("name")
        self._namespace = self.cluster_resource.get("metadata").get("namespace")
        self.cluster = ocs.OCS(**self.cluster_resource)
        if not config.DEPLOYMENT.get("multi_storagecluster"):
            # Decrease chance that we will hit issue:
            # https://github.com/red-hat-storage/ocs-ci/issues/5186
            logger.info("Sleep for 60 seconds before verifying MCG")
            time.sleep(60)
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
        self.cluster_resource = cluster_cr.get("items")[0]

    @retry((IndexError, AttributeError, TypeError), 60, 20, 1)
    def wait_for_nooba_cr(self):
        self._mcg_obj = MCG()

    def cluster_health_check(self, timeout=300):
        """
        This would be a comprehensive cluster health check
        which includes checking pods, external ceph cluster health.
        raise exceptions.CephHealthException("Cluster health is NOT OK")
        """
        sample = TimeoutSampler(timeout=timeout, sleep=3, func=self.is_health_ok)
        if not sample.wait_for_func_status(result=True):
            raise exceptions.CephHealthException("Cluster health is NOT OK")
        if not config.DEPLOYMENT.get("multi_storagecluster"):
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


class LVM(object):
    """
    class for lvm cluster

    """

    def __init__(
        self, fstrim=False, fail_on_thin_pool_not_empty=False, threading_lock=None
    ):
        """
        Initiate the class, gets 2 parameters.
        Args:
            fstrim (bool): If to run fstrim on all disks
            fail_on_thin_pool_not_empty (bool): In init we are checking thinpool util percentage, if percentage
                if not 0 will fail test on True.
        Return:
            (LVM) object
        """
        self.lv_data = None
        self.lvmcluster = None
        self.pv_data = None
        self.version = None
        self.ip = None
        self.vg_data = None
        self.node_ssh = None
        self.new_prom = None
        self.threading_lock = threading_lock
        func_list = [
            self.cluster_ip(),
            self.get_lvmcluster(),
            self.get_lvm_version(),
            self.get_and_parse_pvs(),
            self.get_and_parse_lvs(),
            self.get_and_parse_vgs(),
        ]
        extend_func_list = []
        if fstrim and fail_on_thin_pool_not_empty:
            extend_func_list = [
                self.fstrim(),
                self.compare_thin_pool_data_percent(
                    data_percent=0,
                    sampler=True,
                    timeout=15,
                    fail=True,
                    diff_allowed=0,
                ),
            ]
        elif fstrim and not fail_on_thin_pool_not_empty:
            extend_func_list = [
                self.fstrim(),
                self.compare_thin_pool_data_percent(
                    data_percent=0,
                    sampler=False,
                    timeout=1,
                    fail=False,
                    diff_allowed=0,
                ),
            ]
        elif not fstrim and not fail_on_thin_pool_not_empty:
            extend_func_list = [
                self.compare_thin_pool_data_percent(
                    data_percent=0,
                    sampler=False,
                    timeout=1,
                    fail=False,
                    diff_allowed=0,
                ),
            ]
        elif not fstrim and fail_on_thin_pool_not_empty:
            extend_func_list = [
                self.compare_thin_pool_data_percent(
                    data_percent=0,
                    sampler=True,
                    timeout=15,
                    fail=True,
                    diff_allowed=0,
                ),
            ]
        func_list.extend(extend_func_list)

        thread_init_class(func_list, shutdown=0)

    def init_prom(self):
        self.new_prom = PrometheusAPI(threading_lock=self.threading_lock)

    def get_lvmcluster(self):
        """
        Get OCP object of lvm cluster and sets self.lvmcluster

        """
        lvmc_cop = OCP(
            namespace=config.ENV_DATA["cluster_namespace"],
            kind="lvmcluster",
            resource_name=get_lvm_cluster_name(),
        )
        lvmc_ocs = lvmc_cop.data
        self.lvmcluster = lvmc_ocs

    def get_lvm_version(self):
        """
        Get redhat-operators version (4.10, 4.11)
        returns:
            (str) lvmo version
        """
        redhat_operators_catalogesource_ocp = OCP(
            namespace=constants.MARKETPLACE_NAMESPACE,
            kind="catalogsource",
            resource_name="redhat-operators",
        )
        redhat_operators_catalogesource_ocs = OCS(
            **redhat_operators_catalogesource_ocp.data
        )
        image = getattr(redhat_operators_catalogesource_ocs, "data")["spec"]["image"]
        full_version = image.split(":")[1]
        pattern = re.compile(r"([^\.]*\.[^\.]*)")
        result = pattern.search(full_version)
        short_ver = result.group(1)
        self.version = short_ver

    def get_lvm_thin_pool_config_overprovision_ratio(self):
        """
        Get overprovisionRatio from lvmcluster
        returns:
            (int) overprovisionRatio

        """

        return self.lvmcluster["spec"]["storage"]["deviceClasses"][0]["thinPoolConfig"][
            "overprovisionRatio"
        ]

    def get_lvm_thin_pool_config_size_percent(self):
        """
        get sizePercent from lvmcluster
        returns:
            (int) sizePercent
        """
        return self.lvmcluster["spec"]["storage"]["deviceClasses"][0]["thinPoolConfig"][
            "sizePercent"
        ]

    def get_lvm_thin_pool(self):
        """
        get thinpool name.
        returns:
            (str) thinpool name
        """
        return self.lvmcluster["spec"]["storage"]["deviceClasses"][0]["thinPoolConfig"][
            "name"
        ]

    def get_and_parse_pvs(self):
        """
        get "pvs --reportformat json" from server and parse some data, sets self.pv_data

        """
        cmd = "sudo pvs --reportformat json"
        pvs_output = self.exec_cmd_on_cluster_node(cmd=cmd)

        pvs_json = json.loads(pvs_output)
        items = pvs_json["report"][0]["pv"]
        self.pv_data = {"pv_number": len(items)}
        char_to_replace_in_size = {"<": "", "g": ""}
        self.pv_data["pv_list"] = []
        for pv in items:
            size = pv["pv_size"].translate(str.maketrans(char_to_replace_in_size))
            self.pv_data[pv["pv_name"]] = {"pv_size": size, "vg_name": pv["vg_name"]}
            self.pv_data["pv_list"].append(pv["pv_name"])

    def get_and_parse_lvs(self):
        """
        get "lvs --reportformat json" from server and parse some data, sets self.lv_data

        """
        cmd = "sudo lvs --reportformat json"
        lvs_output = self.exec_cmd_on_cluster_node(cmd=cmd)
        lvs_json = json.loads(lvs_output)
        items = lvs_json["report"][0]["lv"]
        self.lv_data = {"lv_number": len(items)}
        char_to_replace_in_size = {"<": "", "g": ""}
        self.lv_data["lv_list"] = []
        for lv in items:
            size = lv["lv_size"].translate(str.maketrans(char_to_replace_in_size))
            self.lv_data[lv["lv_name"]] = {
                "lv_size": size,
                "vg_name": lv["vg_name"],
                "lv_attr": lv["lv_attr"],
                "pool_lv": lv["pool_lv"],
                "origin": lv["origin"],
                "data_percent": lv["data_percent"],
                "metadata_percent": lv["metadata_percent"],
                "move_pv": lv["move_pv"],
                "mirror_log": lv["mirror_log"],
                "copy_percent": lv["copy_percent"],
                "convert_lv": lv["convert_lv"],
            }
            self.lv_data["lv_list"].append(lv["lv_name"])

    def get_thin_pool_metadata(self):
        """
        Get thin pool metdata percent
        Returns:
            (str) metadata percent

        """
        self.get_and_parse_lvs()
        return self.lv_data["thin-pool-1"]["metadata_percent"]

    def get_and_parse_vgs(self):
        """
        get "vgs --reportformat json" from server and parse some data, sets self.vg_data

        """
        cmd = "sudo vgs --reportformat json"
        lvs_output = self.exec_cmd_on_cluster_node(cmd=cmd)
        lvs_json = json.loads(lvs_output)
        char_to_replace_in_size = {"<": "", "g": ""}
        items = lvs_json["report"][0]["vg"]
        self.vg_data = {}
        for vg in items:
            vg_size = vg["vg_size"].translate(str.maketrans(char_to_replace_in_size))
            vg_free = vg["vg_free"].translate(str.maketrans(char_to_replace_in_size))
            self.vg_data[vg["vg_name"]] = {
                "pv_count": vg["pv_count"],
                "lv_count": vg["lv_count"],
                "snap_count": vg["snap_count"],
                "vg_attr": vg["vg_attr"],
                "vg_size": vg_size,
                "vg_free": vg_free,
            }

    def fstrim(self):
        """
        perform fstrim on all disks

        """
        cmd = "sudo fstrim -av"
        self.exec_cmd_on_cluster_node(cmd)

    def compare_thin_pool_data_percent(
        self,
        data_percent,
        sampler=True,
        timeout=10,
        wait=1,
        fail=True,
        diff_allowed=0.5,
    ):
        """
        Check thin pool data percent against data_percent
        Args:
            data_percent (float): The expected data percent
            sampler (bool): use sampler for compare
            timeout (int): the time the sampler should run
            wait (int): wait between sampling
            fail (bool): if to fail the test or just give warning
            diff_allowed (float): The difference allowed between expected and real data percentage

        """

        if not sampler:
            timeout = 1
            wait = 0
        thin_util_data = 0
        try:
            for thin_util_data in TimeoutSampler(
                timeout=timeout, sleep=wait, func=self.get_thin_pool1_data_percent
            ):
                if float(abs(float(thin_util_data) - float(data_percent))) <= float(
                    diff_allowed
                ):
                    logger.info(
                        f"✅ Expected thin pool utility is {data_percent} and real {thin_util_data}"
                    )
                    break
                else:
                    logger.info(
                        f"⌛❎ Written percent data for thin pool should be {data_percent}  but is  "
                        f"{thin_util_data} "
                    )
        except TimeoutExpiredError:
            if fail:
                raise ThinPoolUtilityWrong(
                    f"❌ Thin pool utility expected {data_percent} "
                    f"but real utility {thin_util_data}\nlv data is {self.lv_data}"
                    f"Lv list is {self.lv_data['lv_list']}"
                )
            else:
                logger.info(
                    f"❌ Written percent data for thin pool should be {data_percent}  but is  "
                    f"{thin_util_data}\nlv data is {self.lv_data}, still not failing because fail=False"
                )

    def get_vg_size(self):
        """
        gets vgs size
        Returns:
            (str) vg_size

        """
        self.get_and_parse_vgs()
        return self.vg_data["vg1"]["vg_size"]

    def get_vg_free(self):
        """
        gets vg free
        Returns:
            (str) vg_free

        """
        self.get_and_parse_vgs()
        return self.vg_data["vg1"]["vg_free"]

    def get_lv_data_percent_of_pvc(self, pvc_obj):
        """
        Get lv data percent by pvc obj
        Args:
            pvc_obj (PVC,OCS): pvc, snapshot or restored pvc obj
        Returns:
            (str): data percent of lv under pvc
        """
        self.get_and_parse_lvs()
        pvc_obj.reload()
        pv_volume_handle = ""
        if type(pvc_obj) is PVC:
            pv_volume_handle = pvc_obj.get_pv_volume_handle_name
        elif type(pvc_obj) is OCS:
            pv_volume_handle_dummy = self.get_lv_name_from_snapshot(pvc_obj)
            pv_volume_handle = self.lv_data[pv_volume_handle_dummy]["origin"]

        return self.lv_data[pv_volume_handle]["data_percent"]

    def get_lv_size_of_pvc(self, pvc_obj):
        """
        Get lv size by pvc obj
        Args:
            pvc_obj (PVC,OCS): pvc, snapshot or restored pvc obj
        Returns:
            (str): size of lv under pvc
        """

        self.get_and_parse_lvs()
        pvc_obj.reload()
        pv_volume_handle = ""
        if type(pvc_obj) is PVC:
            pv_volume_handle = pvc_obj.get_pv_volume_handle_name
        elif type(pvc_obj) is OCS:
            pv_volume_handle_dummy = self.get_lv_name_from_snapshot(pvc_obj)
            pv_volume_handle = self.lv_data[pv_volume_handle_dummy]["origin"]
        return self.lv_data[pv_volume_handle]["lv_size"]

    def get_thin_pool1_size(self):
        """
        gets thin-pool size
        Returns:
            (str) thin pool size

        """
        return self.lv_data["thin-pool-1"]["lv_size"]

    def get_thin_pool1_data_percent(self):
        """
        Get thin-pool-1 data percent
        Returns:
            (str) of the data percent
        """
        self.get_and_parse_lvs()
        return self.lv_data["thin-pool-1"]["data_percent"]

    def get_lv_name_from_pvc(self, pvc_obj):
        """
        Get lv name by pvc obj
        Args:
            pvc_obj (PVC,OCS): pvc, snapshot or restored pvc obj
        Returns:
            (str): lv name under pvc

        """
        self.get_and_parse_lvs()
        pvc_obj.reload()

        return pvc_obj.get_pv_volume_handle_name

    @staticmethod
    def get_lv_name_from_snapshot(snap_obj):
        """
        Get lv name by snapshot obj
        Args:
            snap_obj (OCS): snapshot to find lv name
        Returns:
            (str): lv name

        """

        snapcontent_name = snap_obj.data["status"]["boundVolumeSnapshotContentName"]
        snapcontent = OCP(
            kind=constants.VOLUMESNAPSHOTCONTENT, resource_name=snapcontent_name
        )
        return snapcontent.data["status"]["snapshotHandle"]

    def compare_percent_data_from_pvc(self, pvc_obj, data_size):
        """
        Compare data percentage from data send to lv data measure in lvs
        Args:
            pvc_obj (PVC, OCS): pvc or snaphost or restored pvc
            data_size (float): the expected data to have on the pvc
        Raise:
            (exception): LvDataPercentSizeWrong


        """
        pvc_data_percent_float = self.get_lv_data_percent_of_pvc(pvc_obj=pvc_obj)
        pvc_size = 0
        if type(pvc_obj) is PVC:
            raw_size = pvc_obj.data["spec"]["resources"]["requests"]["storage"]
            if raw_size.isdigit():
                pvc_size = (
                    float(pvc_obj.data["spec"]["resources"]["requests"]["storage"])
                    / 1024
                    / 1024
                    / 1024
                )
            else:
                pvc_size = float("".join([i for i in raw_size if i.isdigit()]))
        if type(pvc_obj) is OCS:
            snapcontent_name = pvc_obj.data["status"]["boundVolumeSnapshotContentName"]
            snapcontent = OCP(
                kind=constants.VOLUMESNAPSHOTCONTENT, resource_name=snapcontent_name
            )
            sp_content_restored_size = snapcontent.data["status"]["restoreSize"]
            pvc_size = sp_content_restored_size / 1024 / 1024 / 1024
        pvc_expected_data_percent = data_size / pvc_size * 100

        pvc_data_percent = float(pvc_data_percent_float)
        if abs(pvc_expected_data_percent - pvc_data_percent) > 0.5:
            failed_lv_name = self.get_lv_name_from_pvc(pvc_obj=pvc_obj)
            raise LvDataPercentSizeWrong(
                f"❌ Written percent data for pvc {pvc_obj.name} should be "
                f"{pvc_expected_data_percent} but is lv {failed_lv_name} data percent"
                f"is {pvc_data_percent}"
            )
        else:
            logger.info(
                f"✅ Pvc {pvc_obj.name} utilization is {pvc_data_percent} and expected is {pvc_expected_data_percent}"
            )

    def cluster_ip(self):
        """
        Get cluster ip address for ssh connections, sets self.ip

        """
        node_ip_dict = get_node_ip_addresses(ipkind="InternalIP")
        self.ip = node_ip_dict[constants.SNO_NODE_NAME]

    def create_ssh_object(self):
        """
        Get ssh object ready, sets self.node_ssh
        """
        self.node_ssh = Connection(
            host=self.ip,
            user="core",
            private_key=f"{os.path.expanduser('~')}/.ssh/openshift-dev.pem",
            stdout=True,
        )

    def exec_cmd_on_cluster_node(self, cmd):
        """
        Exec cmd on SNO node with ssh
        Args:
            cmd (str): command to send to server
        Return:
            (str) output from server.
        """
        if not self.node_ssh:
            self.create_ssh_object()
        return_output = self.node_ssh.exec_cmd(cmd=cmd)
        return_stdout = return_output[1]
        return return_stdout

    def get_thin_provisioning_alerts(self):
        """
        Get the list of alerts that active in the cluster

        Returns:
            list: alrets name

        """
        if not isinstance(self.new_prom, PrometheusAPI):
            self.init_prom()

        alert_full = self.new_prom.get("alerts")
        alerts_data = alert_full.json().get("data").get("alerts")
        alerts_names = list()
        for entity in alerts_data:
            logger.debug(entity.get("labels").get("alertname"))
            alerts_names.append(entity.get("labels").get("alertname"))

        return alerts_names

    def check_for_alert(self, alert_name):
        """
        Check to see if a given alert is available

        Args:
            alert_name (str): Alert name

        Returns:
            bool: True if alert is available else False

        """
        if alert_name in self.get_thin_provisioning_alerts():
            return True

        return False

    def parse_topolvm_metrics(self, metrics):
        """
        Returns the name and value of topolvm metrics

        Args:
            metric_name (list): metrics name to be paesed

        Returns:
            dict: topolvm metrics by: names: value
        """
        if not isinstance(self.new_prom, PrometheusAPI):
            self.init_prom()

        metrics_short = dict()
        for metric_name in metrics:
            metric_full = self.new_prom.query(metric_name)
            metric_value = metric_full[0].get("value")[1]
            logger.info(f"metric: {metric_name} : {metric_value}")
            metrics_short[metric_name] = metric_value

        return metrics_short

    def validate_metrics_vs_operating_system_stats(self, metric, expected_os_value):
        """
        Validate metrics vs operating system stats

        Args:
            metric (str): tololvm metric name
            expected_os_value (str): linux "lvs" equivalent value

        Returns:
            bool: True if metric equals expected_os_value, False otherwise

        """
        logger.info(f"Comparing {metric} vs linux lvs output")
        metric_value = self.parse_topolvm_metrics(constants.TOPOLVM_METRICS).get(metric)
        converted_metric_value = convert_bytes_to_unit(metric_value)
        if (abs(float(metric_value) - float(expected_os_value)) < 0.2) or (
            abs(float(converted_metric_value[:-2]) - float(expected_os_value)) < 0.2
        ):
            return True
        else:
            logger.error(f"{metric} is not equal to os stat: {expected_os_value}")
            return False


def check_clusters():
    """
    Test if lvm or cephcluster is installed and set config.RUN values for conditions

    """

    try:
        lvmcluster_obj = OCP(
            kind="lvmcluster",
            resource_name=get_lvm_cluster_name(),
            namespace=config.ENV_DATA["cluster_namespace"],
            silent=True,
        )
        if isinstance(lvmcluster_obj.data, dict):
            config.RUN["lvm"] = True
            logger.info("Detected LVMcluster is installed")
    except CommandFailed:
        config.RUN["lvm"] = False
    except FileNotFoundError:
        if "install_lvmo" in config.DEPLOYMENT:
            config.RUN["lvm"] = True
        else:
            config.RUN["lvm"] = False
    try:
        cephcluster_obj = OCP(
            kind="cephcluster",
            namespace=config.ENV_DATA["cluster_namespace"],
            silent=True,
        )
        if isinstance(cephcluster_obj.data, dict):
            config.RUN["cephcluster"] = True
            logger.info("Detected CephCluster is installed")
    except CommandFailed:
        config.RUN["cephcluster"] = False
    except FileNotFoundError:
        logger.info(
            "This is deployment, will try to check from ENV_DATA and DEPLOYMENT"
        )
        if config.ENV_DATA["skip_ocs_deployment"]:
            config.RUN["cephcluster"] = False
        else:
            config.RUN["cephcluster"] = True


def get_lvm_full_version():
    """
    Get redhat-operators version (4.11-xxx)
    returns:
        (str) lvmo full version
    """
    redhat_operators_catalogesource_ocp = OCP(
        namespace=constants.MARKETPLACE_NAMESPACE,
        kind="catalogsource",
        resource_name="redhat-operators",
    )
    redhat_operators_catalogesource_ocs = OCS(
        **redhat_operators_catalogesource_ocp.data
    )
    image = getattr(redhat_operators_catalogesource_ocs, "data")["spec"]["image"]
    full_version = image.split(":")[1]
    return full_version


def get_mds_standby_replay_info():
    """Return information about the Ceph MDS standby replay node.

    Returns:
        dict: A dictionary containing information about the standby-replay MDS daemon,
        including the following keys in case of success, otherwise None.
        - "node_ip": The IP address of the node running the standby-replay MDS daemon.
        - "mds_daemon": The name of the MDS daemon.
        - "standby_replay_pod": The name of the standby replay pod.
        - "standby_replay_pod_obj": The object of standby replay pod.
    """
    ct_pod = pod.get_ceph_tools_pod()
    ceph_mdsmap = ct_pod.exec_ceph_cmd("ceph fs status")

    # Find ceph daemon state as 'standby-replay'
    ceph_daemon_name = next(
        (
            daemon["name"]
            for daemon in ceph_mdsmap["mdsmap"]
            if daemon["state"] == "standby-replay"
        ),
        None,
    )

    if ceph_daemon_name is None:
        logger.error("No standby-replay MDS daemon found")
        return None

    logger.info(f"Found standby-replay MDS daemon: {ceph_daemon_name}")

    # Find ceph MDS pod running 'standby-replay' daemon.
    mds_pods = get_mds_pods()
    standby_replay_pod = next(
        (srp for srp in mds_pods if ceph_daemon_name in srp.name), None
    )

    if standby_replay_pod is None:
        logger.error(
            f"No standby-replay MDS Pod found with running daemon '{ceph_daemon_name}'"
        )
        return None

    logger.info(f"Found standby-replay MDS pod: {standby_replay_pod.name}")

    # Get the node name of running pod.
    node_ip = standby_replay_pod.data["status"].get("hostIP")
    if not node_ip:
        logger.error(
            f"Unable to determine IP address of node running standby-replay MDS pod '{standby_replay_pod.name}'"
        )
        return None
    node_name = standby_replay_pod.data["spec"].get("nodeName")
    if not node_name:
        logger.error(
            f"Unable to determine Name of the node running standby-replay MDS pod '{standby_replay_pod.name}'"
        )
        return None
    return {
        "node_ip": node_ip,
        "mds_daemon": ceph_daemon_name,
        "standby_replay_pod": standby_replay_pod.name,
        "node_name": node_name,
        "standby_replay_pod_obj": standby_replay_pod,
    }


def set_osd_op_complaint_time(osd_op_complaint_time_val: float) -> dict:
    """
    Set osd_op_complaint_time to the given value

    Args:
        osd_op_complaint_time_val (float): Value in seconds to set osd_op_complaint_time to

    Returns:
        dict: output of the command
    """
    ct_pod = pod.get_ceph_tools_pod()
    return ct_pod.exec_ceph_cmd(
        f"ceph config set osd osd_op_complaint_time {osd_op_complaint_time_val}"
    )


def get_full_ratio_from_osd_dump():
    """
    Get the full ratio value from osd map

    Returns:
        float: full ratio value
    """
    ct_pod = pod.get_ceph_tools_pod()
    logger.info("Checking the values of ceph osd full ratios in osd map")
    osd_dump_dict = ct_pod.exec_ceph_cmd("ceph osd dump")
    return float(osd_dump_dict["full_ratio"])


def fetch_connection_scores_for_mon(mon_pod):
    """
    This will fetch connection scores for each mons

    Args:
        mon_pod (Pod): Pod object for the respective mon pod

    Returns:
        String: Represeting connection score dump for the mon

    """
    mon_pod_id = pod.get_mon_pod_id(mon_pod)
    cmd = f"ceph daemon mon.{mon_pod_id} connection scores dump"
    return mon_pod.exec_cmd_on_pod(command=cmd, out_yaml_format=False)


def get_mon_quorum_ranks():
    """
    This will return the map representing each mon's quorum ranks

    Returns:
        Dict: Mon quorum ranks

    """
    ceph_tools_pod = pod.get_ceph_tools_pod()
    out = dict(ceph_tools_pod.exec_cmd_on_pod(command="ceph quorum_status"))
    mon_quorum_ranks = {}
    for rank in list(out["quorum"]):
        mon_quorum_ranks[list(out["quorum_names"])[rank]] = rank
    return mon_quorum_ranks


def client_cluster_health_check():
    """
    Check the client cluster health.

    The function will check the following:
    1. Wait for the cluster connectivity
    2. Wait for the nodes to be in a Ready state
    3. Checking that there are no extra Ceph pods on the cluster
    4. Wait for the pods to be running in the cluster namespace
    5. Checking that the storageclient is connected

    Raises:
        ResourceWrongStatusException: In case not all the nodes are ready, not all the pods are running, or
            the storageclient is not connected
        CephHealthException: In case there are extra Ceph pods on the cluster

    """
    wait_for_cluster_connectivity(tries=120, delay=5)
    logger.info("Checking the cluster health")
    wait_for_nodes_status(timeout=420, sleep=10)

    logger.info("Checking that there are no extra Ceph pods on the cluster")
    mon_pods = pod.get_mon_pods()
    if mon_pods:
        raise exceptions.CephHealthException(
            "The client Cluster shouldn't have any mon pods!"
        )
    osd_pods = pod.get_osd_pods()
    if osd_pods:
        raise exceptions.CephHealthException(
            "The client Cluster shouldn't have any osd pods!"
        )
    mds_pods = pod.get_mds_pods()
    if mds_pods:
        raise exceptions.CephHealthException(
            "The client Cluster shouldn't have any mds pods!"
        )

    logger.info("Wait for the pods to be running")
    expected_statuses = [constants.STATUS_RUNNING, constants.STATUS_COMPLETED]
    exclude_pod_name_prefixes = ["rook-ceph-tools"]
    res = wait_for_pods_to_be_in_statuses(
        expected_statuses=expected_statuses,
        exclude_pod_name_prefixes=exclude_pod_name_prefixes,
        timeout=480,
        sleep=20,
    )
    if not res:
        raise ResourceWrongStatusException("Not all the pods in running state")

    logger.info("Checking that the storageclient is connected")
    storage_cluster.wait_for_storage_client_connected()

    logger.info("The client cluster health check passed successfully")


def client_clusters_health_check():
    """
    Check the client clusters health using the function 'client_cluster_health_check'.
    This function will be used when running a multi-cluster job, and we want to verify
    that all the client clusters are in a good health.

    Raises:
        ResourceWrongStatusException: In case not all the nodes are ready or not all the pods are running
        CephHealthException: In case there are extra Ceph pods on the cluster

    """
    for client_context in config.get_client_contexts_if_available():
        with client_context:
            client_cluster_health_check()

    logger.info("The client clusters health check passed successfully")


def check_cephcluster_status(
    desired_phase="Connected",
    desired_health="HEALTH_OK",
    name=constants.EXTERNAL_CEPHCLUSTER_NAME,
    namespace=constants.OPENSHIFT_STORAGE_EXTENDED_NAMESPACE,
):
    """
    Check cephcluster health and phase.

    Args:
        desired_phase (string): The cephcluster desired phase.
        desired_health (string): The cephcluster desired health.
        name (string): name of the cephcluster.
        namespace (string): namespace of the cephcluster.

    Returns:
        bool: True incase cluster is healthy and connected.

    Raises:
        CephHealthException incase phase or health are not as expected.

    """
    cephcluster = OCP(
        kind=constants.CEPH_CLUSTER, resource_name=name, namespace=namespace
    )
    cc_resource = cephcluster.get()
    if (
        cc_resource["status"]["phase"] == desired_phase
        and cc_resource["status"]["ceph"]["health"] == desired_health
    ):
        logger.info(
            f"Cephcluster health is {desired_health} and phase is {desired_phase}"
        )
        return True
    else:
        logger.warning(
            f'Cephcluster not healthy - phase is {cc_resource["status"]["phase"]} and health is'
            f' {cc_resource["status"]["ceph"]["health"]}'
        )
        raise CephHealthException()


def ceph_config_set_debug(debug_level):
    """
    This function will be useful to set default debug level for mds i.e 1/5

    Args:
        debug_level (str): The debug level to set in ceph config
        ex: debug_level='1/5'

    """
    logger.info(f"Setting debug level with user defined value {debug_level}")
    ceph_tools_pod = pod.get_ceph_tools_pod()
    ceph_tools_pod.exec_cmd_on_pod("ceph config set mds debug_mds " + debug_level)


def ceph_health_detail():
    """
    Get ceph health detail

    Returns:
        str: the output of the cmd
    """
    ceph_tools_pod = pod.get_ceph_tools_pod()
    return ceph_tools_pod.exec_cmd_on_pod("ceph health detail", out_yaml_format=False)


def get_active_mds_info():
    """Return information about the active Ceph MDS.

    Returns:
        dict: A dictionary containing information about the active MDS daemon,
        including the following keys in case of success, otherwise None.
        - "node_ip": The IP address of the node running the active MDS daemon.
        - "mds_daemon": The name of the MDS daemon.
        - "active_pod": The name of the active pod.
        - "node_name": The name of the node where active mds pod is running.
    """

    ct_pod = pod.get_ceph_tools_pod()
    ceph_mdsmap = ct_pod.exec_ceph_cmd("ceph fs status")
    logger.info("Find ceph daemon state as 'active'")
    ceph_daemon_name = next(
        (
            daemon["name"]
            for daemon in ceph_mdsmap["mdsmap"]
            if daemon["state"] == "active"
        ),
        None,
    )

    if ceph_daemon_name is None:
        logger.error("No active MDS daemon found")
        return None

    logger.info(f"Found active MDS daemon: {ceph_daemon_name}")

    logger.info("Find ceph MDS pod name where the active MDS daemon is running.")
    mds_pods = get_mds_pods()
    active_pod = next((pod for pod in mds_pods if ceph_daemon_name in pod.name), None)

    if active_pod is None:
        logger.error(
            f"No active MDS Pod found with running daemon '{ceph_daemon_name}'"
        )
        return None

    logger.info(f"Found active MDS pod: {active_pod.name}")
    logger.info("Get the node IP of active mds running pod")
    node_ip = active_pod.data["status"].get("hostIP")
    if not node_ip:
        logger.error(
            f"Unable to determine IP address of node running active MDS pod '{active_pod.name}'"
        )
        return None
    logger.info("Get the node name of of active mds  running pod")
    node_name = active_pod.data["spec"].get("nodeName")
    if not node_name:
        logger.error(
            f"Unable to determine Name of the node running active MDS pod '{active_pod.name}'"
        )
        return None

    return {
        "node_ip": node_ip,
        "mds_daemon": ceph_daemon_name,
        "active_pod": active_pod.name,
        "node_name": node_name,
        "active_pod_obj": active_pod,
    }


def clear_active_mds_load():
    """
    This function executes a ceph cmd to fail active mds daemon instantly.
    So that the existing load on active mds will be cleared off immediately.

    """
    ct_pod = pod.get_ceph_tools_pod()
    ct_pod.exec_ceph_cmd("ceph mds fail 0")


def get_active_mds_memory_utilisation_in_percentage():
    """
    This function gets total and used memory of active mds in Mebibytes and calculates the value in percentage.

    Returns:
         int: mds used memory in percentage

    """
    active_mds_pod_obj = get_active_mds_info()["active_pod_obj"]
    get_total_memory = active_mds_pod_obj.get_memory(container_name="mds")
    total_memory_in_mebibytes = int(get_total_memory[:-2]) * 1024
    used_memory = pod.get_pod_used_memory_in_mebibytes(active_mds_pod_obj.name)
    utilisation_in_percentage = (used_memory / total_memory_in_mebibytes) * 100
    return utilisation_in_percentage


def get_standby_replay_mds_memory_utilisation_in_percentage():
    """
    This function gets total and used memory of active mds in Mebibytes and calculates the value in percentage.

    Returns:
         int: mds used memory in percentage

    """
    standby_replay_mds_pod_obj = get_mds_standby_replay_info()["standby_replay_pod_obj"]
    get_total_memory = standby_replay_mds_pod_obj.get_memory(container_name="mds")
    total_memory_in_mebibytes = int(get_total_memory[:-2]) * 1024
    used_memory = pod.get_pod_used_memory_in_mebibytes(standby_replay_mds_pod_obj.name)
    utilisation_in_percentage = (used_memory / total_memory_in_mebibytes) * 100
    return utilisation_in_percentage


def bring_down_mds_memory_usage_gradually():
    """
    This function will monitor the mds memory usage for 18 minutes to make sure it is <=10%.
    Even if the memory usage is still high after 18 mins,
    it will fail the mds daemon and look for the same <=10% in memory utilisation.
    This will repeat the process until the time_elapsed reaches 30mins
    And it breaks if memory utilisation reduced in between.

    """
    logger.info("Continue monitoring mds memory usage until it get reduced to 10%")
    time_interval = 180
    time_elapsed = 0
    while time_elapsed <= 1800:
        logger.info("Check memory usage and sleep if usage is higher than 10%")
        if (
            get_active_mds_memory_utilisation_in_percentage() >= 10
            or get_standby_replay_mds_memory_utilisation_in_percentage() >= 10
        ):
            if time_elapsed <= 900:
                logger.info("Memory usage is high. Sleeping for 3 minutes...")
                time.sleep(time_interval)
                time_elapsed += time_interval
            else:
                clear_active_mds_load()
                logger.info("clearing the existing load on MDS by failing mds daemon ")
                logger.info(
                    "Failed MDS.0 daemon to clear load. Sleeping for 3 minutes..."
                )
                time.sleep(time_interval)
                continue
        else:
            logger.info("Memory usage is within the acceptable limits.")
            break

    assert (
        time_elapsed <= 1800
    ), "Memory usage remained high for more than 30 minutes. Failed to bring down the memory usage of MDS"


def parse_ceph_table_output(raw_output: str) -> pd.DataFrame:
    """
    Parse the Ceph command table output and extract the data into a pandas DataFrame.
    The function assumes that the first row contains the header, with at least two spaces
    separating each column value.

    Args:
        raw_output (str): The raw output string from any Ceph command that provides tabular output.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the parsed data, where the columns are
        derived from the header row and the data rows are parsed accordingly.

    """
    # Known units for sizes (e.g., GiB, TiB, MiB)
    known_units = ["GiB", "MiB", "KiB", "TiB"]

    # Step 1: Join size values with their units (e.g., '894 GiB' -> '894GiB')
    for unit in known_units:
        raw_output = re.sub(rf"(\d+)\s+{unit}", rf"\1{unit}", raw_output)

    # Split the raw output into lines
    lines = raw_output.strip().split("\n")
    # Use the first line as the header
    header_line = lines[0].strip()
    header = re.split(r"\s{2,}", header_line)
    logger.info(f"Extracted Header: {header}")

    data_lines = lines[1:]
    # Now process the collected lines into parts
    data = []
    for line in data_lines:
        # Split by any whitespace
        parts = re.split(r"\s+", line.strip())
        if len(parts) >= len(header) - 1:
            data.append(parts[: len(header)])
        else:
            logger.warning(
                f"Skipping line due to mismatch in number of columns: {line}"
            )

    # Create DataFrame
    df = pd.DataFrame(data, columns=header)

    return df


def get_ceph_osd_df_tree_weight_and_size():
    """
    Extract the 'ID', 'WEIGHT', and 'SIZE' values from the Ceph 'osd df tree' command output.

    Returns:
        list: A list of dictionaries where each dictionary contains 'ID', 'WEIGHT', and 'SIZE'.

    """
    ceph_cmd = "ceph osd df tree"
    ct_pod = storage_cluster.get_ceph_tools_pod()
    output = ct_pod.exec_ceph_cmd(
        ceph_cmd=ceph_cmd, format=False, out_yaml_format=False
    )
    logger.info(f"ceph osd df tree output = {output}")
    # Parse the raw output using the modified parse_ceph_osd_df_tree function
    df = parse_ceph_table_output(output)

    # Initialize the result list
    result = []

    for _, row in df.iterrows():
        # Extract WEIGHT and SIZE
        weight = row["WEIGHT"]
        if weight == "-":
            # If the weight value with '-' we need to get the next row value
            weight = row["CLASS"]
            size = row["REWEIGHT"]
        else:
            weight = row["WEIGHT"]
            size = row["SIZE"]

        result.append({"ID": row["ID"], "WEIGHT": weight, "SIZE": size})

    return result


def check_ceph_osd_df_tree():
    """
    Check that the ceph osd df tree output values are correct

    Returns:
        bool: True, if the ceph osd df tree output values are correct. False, otherwise.

    """
    logger.info("Verify ceph osd df tree values")
    storage_size_param = storage_cluster.get_storage_size()
    logger.info(f"storage size = {storage_size_param}")
    ceph_output_lines = get_ceph_osd_df_tree_weight_and_size()
    logger.info(f"ceph output lines = {ceph_output_lines}")

    for line in ceph_output_lines:
        osd_id = line["ID"]
        weight = float(line["WEIGHT"])
        # Regular expression to match the numeric part and the unit
        match = re.match(r"([0-9.]+)([a-zA-Z]+)", line["SIZE"])
        size = float(match.group(1))
        units = match.group(2)
        if units.startswith("Ti"):
            storage_size = convert_device_size(storage_size_param, "TB", 1024)
        elif units.startswith("Gi"):
            storage_size = convert_device_size(storage_size_param, "GB", 1024)
            weight = weight * 1024
        elif units.startswith("Mi"):
            storage_size = convert_device_size(storage_size_param, "MB", 1024)
            weight = weight * (1024**2)
        else:
            storage_size = float(storage_size_param[0:-2])

        logger.info(f"OSD size = {size}, weight = {weight}")
        # Check if the weight and size are equal ignoring a small diff
        diff = size * 0.04
        if not (size - diff <= weight <= size + diff):
            logger.warning(
                f"OSD weight {weight} (converted) does not match the OSD size {size} "
                f"for OSD ID {osd_id}. Expected OSD weight within [{size - diff}, {size + diff}]"
            )
            return False
        # If it's a regular OSD entry, check if the expected osd size
        # and the current size are equal ignoring a small diff
        diff = size * 0.02
        if not osd_id.startswith("-") and not (
            size - diff <= storage_size <= size + diff
        ):
            logger.warning(
                f"The storage size {storage_size} does not match the OSD size {size} "
                f"for OSD ID {osd_id}. Expected storage size within [{size - diff}, {size + diff}]"
            )
            return False

    return True


def get_used_and_total_capacity_in_gibibytes():
    """
    Get used capacity and total capacity of the cluster from the ceph tools pod
    Convert the storage values from bytes to gibibytes

    Returns:
        tuple: (total_used_in_gibibytes, total_capacity_in_gibibytes) ex: Used capacity, Total capacity

    """
    ct_pod = pod.get_ceph_tools_pod()
    output = ct_pod.exec_ceph_cmd(ceph_cmd="ceph df")
    total_used = output.get("stats").get("total_used_raw_bytes")
    total_capacity = output.get("stats").get("total_bytes")
    total_used_in_gibibytes = total_used / (2**30)
    total_capacity_in_gibibytes = total_capacity / (2**30)
    return (total_used_in_gibibytes, total_capacity_in_gibibytes)


def get_age_of_cluster_in_days():
    """
    Get age of the cluster in days.
    1. Get creation time by executing oc cmd on cluster
    2. Get current time from the ceph tools pod
    3. Calculate time difference between two times
    4. Convert the time into days

    Returns:
        int: returns number of days the cluster has been running

    """
    cmd = "get namespace kube-system -o jsonpath='{.metadata.creationTimestamp}'"
    creation_time = OCP().exec_oc_cmd(command=cmd, out_yaml_format=False)
    logger.info(f"The cluster creation time is: {creation_time}")
    ct_pod = pod.get_ceph_tools_pod()
    cephcmd = 'date -u +"%Y-%m-%dT%H:%M:%SZ"'
    current_time = ct_pod.exec_cmd_on_pod(command=cephcmd, out_yaml_format=False)
    logger.info(f"Current time in the cluster is: {current_time}")
    d1 = datetime.fromisoformat(creation_time[:-1])
    d2 = datetime.fromisoformat(current_time.strip()[:-1])
    time_difference_in_sec = (d2 - d1).total_seconds()
    seconds_per_day = 24 * 60 * 60
    time_diff_in_days = time_difference_in_sec / seconds_per_day
    return math.ceil(time_diff_in_days)


def get_active_mds_count_cephfilesystem():
    """
    Get the active mds pod count from cephfilesystem yaml.

    Returns:
         mds_active_count (int): Active mds pod count.

    """
    cephfs = ocp.OCP(
        kind=constants.CEPHFILESYSTEM,
        namespace=config.ENV_DATA["cluster_namespace"],
    )
    fs_data = cephfs.get(defaults.CEPHFILESYSTEM_NAME)
    mds_active_count = fs_data.get("spec").get("metadataServer").get("activeCount")
    return mds_active_count


def adjust_active_mds_count_storagecluster(target_count):
    """
    Adjust the activeMetadataServers count in the Storage cluster to the target_count.
    The function increases or decreases the count to match the target value sequentially.

    Args:
        target_count (int): The desired count for activeMetadataServers.

    Raises:
        ActiveMdsValueNotMatch: if activeMetadataServers count does not match.

    """
    # Retrieve the current activeMetadataServers count
    current_count_cephfilesystem = get_active_mds_count_cephfilesystem()
    sc = storage_cluster.get_storage_cluster(
        namespace=config.ENV_DATA["cluster_namespace"]
    )
    resource_name = sc.get()["items"][0]["metadata"]["name"]

    step = 1 if current_count_cephfilesystem < target_count else -1
    for _ in range(current_count_cephfilesystem, target_count + step, step):
        if current_count_cephfilesystem == target_count:
            logger.info("The current count is equal to the target count.")
        else:
            # Determine the new count by incrementing or decrementing
            current_count_cephfilesystem = current_count_cephfilesystem + step
            param = (
                f'{{"spec": {{"managedResources": {{"cephFilesystems": '
                f'{{"activeMetadataServers": {current_count_cephfilesystem}}}}}}}}}'
            )
            sc.patch(resource_name=resource_name, params=param, format_type="merge")

    # Retrieve the updated count
    current_params = sc.get(resource_name=resource_name)
    current_count_cephfilesystem = current_params["spec"]["managedResources"][
        "cephFilesystems"
    ]["activeMetadataServers"]
    if current_count_cephfilesystem != target_count:
        raise ActiveMdsValueNotMatch(
            f"Failed to update activeMetadataServers to {target_count}"
        )

    logger.info(
        "Wait until the active mds pod count from cephfilesystem matches the target count"
    )
    try:
        TimeoutSampler(
            timeout=300,
            sleep=10,
            func=get_active_mds_count_cephfilesystem,
        ).wait_for_func_value(target_count)
        logger.info(f"Target activeMetadataServers count {target_count} reached.")
    except TimeoutExpiredError:
        raise ActiveMdsValueNotMatch(
            f"Failed to change the active count to {target_count} within timeout."
        )


def get_active_mds_pod_objs():
    """
    Gets active mds pods objs.

    Returns:
        active_mds_pods (list): Active mds pod objs.

    """
    ct_pod = pod.get_ceph_tools_pod()
    ceph_mdsmap = ct_pod.exec_ceph_cmd("ceph fs status")
    # Extract the mdsmap list from the data
    mdsmap = ceph_mdsmap["mdsmap"]

    # Filter and get the names of active MDS pods
    ceph_daemon_name = [mds["name"] for mds in mdsmap if mds["state"] == "active"]
    mds_pods = get_mds_pods()
    active_mds_pods = [
        mdspod
        for mdspod in mds_pods
        if any(daemon_name in mdspod.name for daemon_name in ceph_daemon_name)
    ]
    return active_mds_pods


def get_mds_counts():
    """
    Fetch active and standby-replay MDS counts.

    Returns:
        tuple: A tuple containing two integers:
            - active_pod_count (int): The number of active MDS pods.
            - standby_replay_count (int): The number of standby-replay MDS pods.
    """
    ct_pod = pod.get_ceph_tools_pod()
    ceph_mdsmap = ct_pod.exec_ceph_cmd("ceph fs status")["mdsmap"]
    active_pod_count = sum(1 for mds in ceph_mdsmap if mds["state"] == "active")
    standby_replay_count = sum(
        1 for mds in ceph_mdsmap if mds["state"] == "standby-replay"
    )
    return active_pod_count, standby_replay_count


def is_lower_requirements():
    """
    Determine if the cluster meets lower hardware requirements.

    The conditions are:
    1. allow_lower_instance_requirements is set to True in the config.
    2. Any worker instance type is 'm4.4xlarge' or 'bx2-8x32'.

    Returns:
        bool: True if lower requirements are satisfied, otherwise False.

    """
    # Worker instance types considered as lower requirements
    lower_requirements_worker_types = {"m4.4xlarge", "bx2-8x32"}

    # Check if allow_lower_instance_requirements is explicitly set
    if config.DEPLOYMENT.get("allow_lower_instance_requirements", False):
        logger.info(
            "Lower requirements are allowed by configuration (allow_lower_instance_requirements=True)."
        )
        return True

    # Check if the Machine resources exist
    machine_obj = OCP(kind="Machine", namespace="openshift-machine-api")
    try:
        machines_data = machine_obj.get()
        machines = machines_data.get("items", [])
    except CommandFailed as ex:
        logger.warning(f"Could not fetch Machines (falling back to config only): {ex}")
        machines = []

    if not machines:
        logger.info(
            "No Machine resources found. Assuming standard (non-lower) requirements."
        )
        return False

    # Check worker instance types
    for machine in machines:
        role = machine["metadata"]["labels"].get(
            "machine.openshift.io/cluster-api-machine-role"
        )
        if role == "worker":
            instance_type = machine["metadata"]["labels"].get(
                "machine.openshift.io/instance-type"
            )
            if instance_type in lower_requirements_worker_types:
                logger.info(
                    f"Detected lower requirement worker instance type: {instance_type} "
                    f"(machine: {machine['metadata']['name']})."
                )
                return True

    logger.info(
        "Cluster worker machines use standard or larger instance types. No lower requirements detected."
    )
    return False
