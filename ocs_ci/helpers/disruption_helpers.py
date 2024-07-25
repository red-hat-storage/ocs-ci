import logging
import os
import random

from ocs_ci.ocs.resources import pod
from ocs_ci.ocs import constants, ocp
from ocs_ci.framework import config
from ocs_ci.utility.utils import TimeoutSampler, run_async, run_cmd
from ocs_ci.utility.retry import retry
from ocs_ci.ocs.exceptions import TimeoutExpiredError

log = logging.getLogger(__name__)

CEPH_PODS = ["mds", "mon", "mgr", "osd"]


class Disruptions:
    """
    This class contains methods of disrupt operations
    """

    resource = None
    resource_obj = None
    resource_count = 0
    selector = None
    daemon_pid = None
    cluster_kubeconfig = ""

    def kubeconfig_parameter(self):
        """
        Returns the '--kubeconfig <value>' parameter for the oc command

        Returns:
            str: The '--kubeconfig <value>' parameter for oc command if the attribute 'cluster_kubeconfig' is not empty.
                Empty string if the the attribute 'cluster_kubeconfig' is empty.
        """
        kubeconfig_parameter = (
            f"--kubeconfig {self.cluster_kubeconfig} "
            if self.cluster_kubeconfig
            else ""
        )
        return kubeconfig_parameter

    def set_resource(self, resource, leader_type="provisioner", cluster_index=None):
        self.resource = resource
        if (
            config.ENV_DATA["platform"].lower() in constants.HCI_PC_OR_MS_PLATFORM
        ) and (resource in CEPH_PODS):
            # If the platform is Managed Services, then the ceph pods will be present in the provider cluster.
            # Consumer cluster will be the primary cluster context in a multicluster run. Setting 'cluster_kubeconfig'
            # attribute to use as the value of the parameter '--kubeconfig' in the 'oc' commands to get ceph pods.
            provider_kubeconfig = os.path.join(
                config.clusters[config.get_provider_index()].ENV_DATA["cluster_path"],
                config.clusters[config.get_provider_index()].RUN.get(
                    "kubeconfig_location"
                ),
            )
            self.cluster_kubeconfig = provider_kubeconfig
        elif config.ENV_DATA["platform"] in constants.HCI_PC_OR_MS_PLATFORM:
            # cluster_index is used to identify the  cluster in which the pod is residing. If cluster_index is not
            # passed, assume that the context is already changed to the cluster where the pod is residing.
            cluster_index = (
                cluster_index if cluster_index is not None else config.cur_index
            )
            self.cluster_kubeconfig = os.path.join(
                config.clusters[cluster_index].ENV_DATA["cluster_path"],
                config.clusters[cluster_index].RUN.get("kubeconfig_location"),
            )
        resource_count = 0
        if self.resource == "mgr":
            self.resource_obj = pod.get_mgr_pods()
            self.selector = constants.MGR_APP_LABEL
        if self.resource == "mon":
            self.resource_obj = pod.get_mon_pods()
            self.selector = constants.MON_APP_LABEL
        if self.resource == "osd":
            self.resource_obj = pod.get_osd_pods()
            self.selector = constants.OSD_APP_LABEL
        if self.resource == "mds":
            self.resource_obj = pod.get_mds_pods()
            self.selector = constants.MDS_APP_LABEL
        if self.resource == "cephfsplugin":
            self.resource_obj = pod.get_plugin_pods(interface=constants.CEPHFILESYSTEM)
            self.selector = constants.CSI_CEPHFSPLUGIN_LABEL
        if self.resource == "rbdplugin":
            self.resource_obj = pod.get_plugin_pods(interface=constants.CEPHBLOCKPOOL)
            self.selector = constants.CSI_RBDPLUGIN_LABEL
        if self.resource == "cephfsplugin_provisioner":
            self.resource_obj = [
                pod.get_plugin_provisioner_leader(
                    interface=constants.CEPHFILESYSTEM, leader_type=leader_type
                )
            ]
            self.selector = constants.CSI_CEPHFSPLUGIN_PROVISIONER_LABEL
            resource_count = len(pod.get_cephfsplugin_provisioner_pods())
        if self.resource == "rbdplugin_provisioner":
            self.resource_obj = [
                pod.get_plugin_provisioner_leader(
                    interface=constants.CEPHBLOCKPOOL, leader_type=leader_type
                )
            ]
            self.selector = constants.CSI_RBDPLUGIN_PROVISIONER_LABEL
            resource_count = len(pod.get_rbdfsplugin_provisioner_pods())
        if self.resource == "operator":
            self.resource_obj = pod.get_operator_pods()
            self.selector = constants.OPERATOR_LABEL
        if self.resource == "ocs_operator":
            self.resource_obj = [pod.get_ocs_operator_pod()]
            self.selector = constants.OCS_OPERATOR_LABEL
        if self.resource == "noobaa_operator":
            self.resource_obj = [pod.get_noobaa_operator_pod()]
            self.selector = constants.NOOBAA_OPERATOR_POD_LABEL
        if self.resource == "odf_operator":
            self.resource_obj = [pod.get_odf_operator_controller_manager()]
            self.selector = constants.ODF_OPERATOR_CONTROL_MANAGER_LABEL
        if self.resource == "alertmanager_managed_ocs_alertmanager":
            self.resource_obj = pod.get_alertmanager_managed_ocs_alertmanager_pods()
            self.selector = constants.MANAGED_ALERTMANAGER_LABEL
        if self.resource == "ocs_osd_controller_manager":
            self.resource_obj = [pod.get_ocs_osd_controller_manager_pod()]
            self.selector = constants.MANAGED_CONTROLLER_LABEL
            # Setting resource_count because odf-operator-controller-manager pod also have the same label.
            resource_count = len(
                pod.get_pods_having_label(
                    constants.MANAGED_CONTROLLER_LABEL,
                    config.ENV_DATA["cluster_namespace"],
                )
            )
        if self.resource == "prometheus_managed_ocs_prometheus":
            self.resource_obj = [pod.get_prometheus_managed_ocs_prometheus_pod()]
            self.selector = constants.MANAGED_PROMETHEUS_LABEL
        if self.resource == "prometheus_operator":
            self.resource_obj = [pod.get_prometheus_operator_pod()]
            self.selector = constants.PROMETHEUS_OPERATOR_LABEL
        if self.resource == "ocs_provider_server":
            self.resource_obj = [pod.get_ocs_provider_server_pod()]
            self.selector = constants.PROVIDER_SERVER_LABEL

        self.resource_count = resource_count or len(self.resource_obj)

    def delete_resource(self, resource_id=0):
        pod_ocp = ocp.OCP(
            kind=constants.POD,
            namespace=self.resource_obj[resource_id].namespace
            or config.ENV_DATA["cluster_namespace"],
        )
        if self.cluster_kubeconfig:
            # Setting 'cluster_kubeconfig' attribute to use as the value of the
            # parameter '--kubeconfig' in the 'oc' commands.
            self.resource_obj[
                resource_id
            ].ocp.cluster_kubeconfig = self.cluster_kubeconfig
            pod_ocp.cluster_kubeconfig = self.cluster_kubeconfig
        self.resource_obj[resource_id].delete(force=True)
        assert pod_ocp.wait_for_resource(
            condition="Running",
            selector=self.selector,
            resource_count=self.resource_count,
            timeout=300,
        )

    @retry(AssertionError, tries=5, delay=3, backoff=1)
    def select_daemon(self, node_name=None):
        """
        Select pid of self.resource daemon

        Args:
            node_name (str): Name of node in which the resource daemon has
                to be selected.
        """
        node_name = node_name or self.resource_obj[0].pod_data.get("spec").get(
            "nodeName"
        )
        pid_cmd = (
            f"oc {self.kubeconfig_parameter()}debug node/{node_name}"
            f" --to-namespace={config.ENV_DATA['cluster_namespace']} -- chroot /host pidof ceph-{self.resource}"
        )
        pid_proc = run_async(pid_cmd)
        ret, pid, err = pid_proc.async_communicate()
        pid = pid.strip()

        # Consider scenario where more than one self.resource pod is running
        # on one node. eg: More than one osd on same node.
        pids = pid.split()
        self.pids = [pid.strip() for pid in pids]
        assert self.pids, f"Obtained pid values of ceph-{self.resource} is empty."
        pid = self.pids[0]

        # ret will be 0 and err will be None if command is success
        assert not any([ret, err, not pid.isdigit()]), (
            f"Failed to fetch pid of ceph-{self.resource} "
            f"from {node_name}. ret:{ret}, pid:{pid}, err:{err}"
        )

        self.daemon_pid = pid

    def kill_daemon(self, node_name=None, check_new_pid=True, kill_signal="9"):
        """
        Kill self.resource daemon

        Args:
            node_name (str): Name of node in which the resource daemon has
                to be killed
            check_new_pid (bool): True to check for new pid after killing the
                daemon. False to skip the check.
            kill_signal (str): kill signal type
        """
        node_name = node_name or self.resource_obj[0].pod_data.get("spec").get(
            "nodeName"
        )
        if not self.daemon_pid:
            self.select_daemon(node_name=node_name)

        # Command to kill the daemon
        kill_cmd = (
            f"oc {self.kubeconfig_parameter()}debug node/{node_name} "
            f"--to-namespace={config.ENV_DATA['cluster_namespace']} -- chroot /host  "
            f"kill -{kill_signal} {self.daemon_pid}"
        )
        daemon_kill = run_cmd(kill_cmd)

        # 'daemon_kill' will be an empty string if command is success
        assert isinstance(daemon_kill, str) and (not daemon_kill), (
            f"Failed to kill ceph-{self.resource} daemon in {node_name}. "
            f"Daemon kill command output - {daemon_kill}"
        )
        log.info(f"Killed ceph-{self.resource} daemon on node {node_name}")

        if check_new_pid:
            self.check_new_pid(node_name=node_name)

    def check_new_pid(self, node_name=None):
        """
        Check if the pid of the daemon has changed from the initially selected pid(daemon_pid attribute)

        Args:
            node_name (str): Name of node in which the resource daemon is running

        """
        node_name = node_name or self.resource_obj[0].pod_data.get("spec").get(
            "nodeName"
        )
        pid_cmd = (
            f"oc {self.kubeconfig_parameter()}debug node/{node_name} "
            f"--to-namespace={config.ENV_DATA['cluster_namespace']} -- chroot /host pidof ceph-{self.resource}"
        )
        try:
            for pid_proc in TimeoutSampler(60, 2, run_async, command=pid_cmd):
                ret, pid, err = pid_proc.async_communicate()

                # Consider scenario where more than one self.resource pod
                # is running on one node. eg:More than one osd on same node
                pids = pid.strip().split()
                pids = [pid.strip() for pid in pids]
                if len(pids) != len(self.pids):
                    continue
                new_pid = [pid for pid in pids if pid not in self.pids]
                assert (
                    len(new_pid) == 1
                ), f"Found more than one new pid of ceph-{self.resource} in the node {node_name}"
                new_pid = new_pid[0]
                if new_pid.isdigit() and (new_pid != self.daemon_pid):
                    log.info(f"New pid of ceph-{self.resource} is {new_pid}")
                    break
        except TimeoutExpiredError:
            raise TimeoutExpiredError(
                f"Waiting for pid of ceph-{self.resource} in {node_name}"
            )


def delete_resource_multiple_times(resource_name, num_of_iterations):
    """
    Delete a specific resource(osd, rook-operator, mon, etc,.) multiple times.

    Args:
        resource_name (str): The resource name to delete
        num_of_iterations (int): The number of iterations we delete the resource

    """
    d = Disruptions()
    d.set_resource(resource_name)
    resource_id = random.randrange(d.resource_count)

    for i in range(num_of_iterations):
        log.info(
            f"Iteration {i}: Delete resource {resource_name} with id {resource_id}"
        )
        d.set_resource(resource_name)
        d.delete_resource(resource_id)
