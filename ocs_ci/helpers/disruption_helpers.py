import logging
import os
import random
import time

import yaml

from ocs_ci.helpers.helpers import get_provisioner_label, get_node_plugin_label
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.resources.pod import cal_md5sum
from ocs_ci.ocs import constants, ocp
from ocs_ci.framework import config
from ocs_ci.utility.utils import TimeoutSampler, run_async, run_cmd
from ocs_ci.utility.retry import retry
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.ocs.exceptions import CommandFailed

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
            log.info(f"Setting provider kubeconfig for the resource {self.resource}")
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
            self.selector = get_node_plugin_label(constants.CEPHFILESYSTEM)
        if self.resource == "rbdplugin":
            self.resource_obj = pod.get_plugin_pods(interface=constants.CEPHBLOCKPOOL)
            self.selector = get_node_plugin_label(constants.CEPHBLOCKPOOL)
        if self.resource == "cephfsplugin_provisioner":
            self.resource_obj = [
                pod.get_plugin_provisioner_leader(
                    interface=constants.CEPHFILESYSTEM, leader_type=leader_type
                )
            ]
            self.selector = get_provisioner_label(constants.CEPHFILESYSTEM)
            resource_count = len(pod.get_cephfsplugin_provisioner_pods())
        if self.resource == "rbdplugin_provisioner":
            self.resource_obj = [
                pod.get_plugin_provisioner_leader(
                    interface=constants.CEPHBLOCKPOOL, leader_type=leader_type
                )
            ]
            self.selector = get_provisioner_label(constants.CEPHBLOCKPOOL)
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
        if self.resource == "ceph_csi_controller_manager":
            self.resource_obj = [pod.get_ceph_csi_controller_manager()]
            self.selector = constants.CEPH_CSI_CONTROLLER_MANAGER_LABEL
        if self.resource == "ocs_client_operator_controller_manager":
            self.resource_obj = [pod.get_ocs_client_operator_controller_manager()]
            self.selector = constants.OCS_CLIENT_OPERATOR_LABEL

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
            self.resource_obj[resource_id].ocp.cluster_kubeconfig = (
                self.cluster_kubeconfig
            )
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
            f" --to-namespace=default -- chroot /host pidof ceph-{self.resource}"
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
            f"--to-namespace=default -- chroot /host  "
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
            f"--to-namespace=default -- chroot /host pidof ceph-{self.resource}"
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


class FIOIntegrityChecker:
    """
    Verifies data integrity on RBD and CephFS PVCs during disruptive
    operations using a three-phase approach:

    1. **Write phase**: Write a known file with FIO (size-based).
       Compute and store md5sum of each file.
    2. **Background IO phase**: Start a time-based FIO (randrw)
       that runs throughout the disruptive operation. FIO results
       are written to a file inside the pod so they can be
       recovered if the oc rsh connection drops.
    3. **Verify phase**: Wait for FIO to complete, check IO errors,
       p99 latency, and verify FIO runtime covered the operation.
       If oc rsh connection dropped during the operation, recover
       FIO results from the output file. Re-compute md5sum of the
       integrity file and compare with the stored checksum.

    Usage::

        checker = FIOIntegrityChecker(pvc_factory, pod_factory)
        checker.start_io(bg_runtime=500)

        # ... perform disruptive operation ...

        checker.wait_and_verify()

    For node removal operations where IO pods may be killed::

        checker.verify_md5sum_only()
    """

    INTEGRITY_FILE = "integrity_data"
    BG_IO_FILE = "bg_io_data"
    FIO_RESULTS_FILE = "fio_results.json"

    DEFAULT_MAX_LATENCY_SEC = 10

    def __init__(
        self,
        pvc_factory,
        pod_factory,
        interfaces=None,
        pvc_size=5,
        max_latency_sec=None,
    ):
        """
        Args:
            pvc_factory: Pytest fixture for creating PVCs.
            pod_factory: Pytest fixture for creating pods.
            interfaces (list): Storage interfaces to test.
                Defaults to [CEPHBLOCKPOOL, CEPHFILESYSTEM].
            pvc_size (int): Size of each PVC in GiB.
            max_latency_sec (int): Maximum allowed p99 completion
                latency in seconds for background FIO. If the p99
                latency exceeds this value, the test fails indicating
                IO was stalled. Defaults to 10.
        """
        self.pvc_factory = pvc_factory
        self.pod_factory = pod_factory
        self.interfaces = interfaces or [
            constants.CEPHBLOCKPOOL,
            constants.CEPHFILESYSTEM,
        ]
        self.pvc_size = pvc_size
        self.max_latency_sec = (
            max_latency_sec
            if max_latency_sec is not None
            else self.DEFAULT_MAX_LATENCY_SEC
        )
        self.io_pods = []
        self._md5sums = {}
        self._bg_runtime = None
        self._start_time = None

    def start_io(
        self,
        size="1G",
        bg_runtime=900,
        bs="4K",
        rate="4m",
        node_name=None,
    ):
        """
        Create PVCs and pods, write integrity files with FIO, compute
        md5sums, then start background FIO (time-based, randrw).

        Background FIO writes results to a file inside the pod
        (fio_results.json) so they can be recovered if the oc rsh
        connection drops during the disruptive operation.

        Args:
            size (str): Size of the integrity file to write.
            bg_runtime (int): Background FIO runtime in seconds.
                Should be longer than the expected operation
                duration to ensure IO coverage.
            bs (str): Block size for background FIO.
            rate (str): IO rate limit for background FIO.
            node_name (str): Pin IO pods to this specific node.
                Useful when other nodes may be removed during
                the operation.
        """
        self._start_time = time.time()
        self._bg_runtime = bg_runtime

        if node_name:
            log.info(
                "-------- FIOIntegrityChecker: pinning IO pods "
                f"to node '{node_name}' --------"
            )

        log.info(
            "-------- FIOIntegrityChecker: creating PVCs and "
            "writing integrity files --------"
        )
        for interface in self.interfaces:
            pvc_obj = self.pvc_factory(interface=interface, size=self.pvc_size)
            io_pod = self.pod_factory(
                pvc=pvc_obj,
                interface=interface,
                node_name=node_name,
            )
            self.io_pods.append(io_pod)

            log.info(
                f"Writing {size} integrity file on pod "
                f"'{io_pod.name}' ({interface})"
            )
            io_pod.run_io(
                storage_type="fs",
                size=size,
                io_direction="wo",
                runtime=0,
                bs="1M",
                depth=4,
                rate="100m",
                fio_filename=self.INTEGRITY_FILE,
            )

        for io_pod in self.io_pods:
            io_pod.get_fio_results()
            log.info(f"FIO write completed on pod '{io_pod.name}'")

        log.info("-------- FIOIntegrityChecker: computing md5sums --------")
        for io_pod in self.io_pods:
            md5 = cal_md5sum(io_pod, self.INTEGRITY_FILE)
            self._md5sums[io_pod.name] = md5
            log.info(f"md5sum for pod '{io_pod.name}': {md5}")

        log.info(
            "-------- FIOIntegrityChecker: starting background "
            f"FIO (runtime={bg_runtime}s) --------"
        )
        for io_pod in self.io_pods:
            if not io_pod.wl_setup_done:
                io_pod.workload_setup(storage_type="fs", jobs=1)
            mount_path = (
                io_pod.get()
                .get("spec", {})
                .get("containers", [{}])[0]
                .get("volumeMounts", [{}])[0]
                .get("mountPath", "/var/lib/www/html")
            )
            results_file = f"{mount_path}/{self.FIO_RESULTS_FILE}"
            io_pod.io_params = {
                "name": "fio-rand-readwrite",
                "readwrite": "randrw",
                "bs": bs,
                "direct": 0,
                "numjobs": 1,
                "time_based": 1,
                "runtime": bg_runtime,
                "size": size,
                "iodepth": 4,
                "invalidate": 1,
                "fsync_on_close": 1,
                "rwmixread": 75,
                "ioengine": "libaio",
                "rate": rate,
                "rate_process": "poisson",
                "filename": self.BG_IO_FILE,
                "output": results_file,
            }
            io_pod.fio_thread = io_pod.wl_obj.run(**io_pod.io_params)
            log.info(
                "Background FIO started on pod "
                f"'{io_pod.name}' (results -> {results_file})"
            )

    def _collect_pod_diagnostics(self, io_pod):
        """
        Collect diagnostic information when FIO fails on a pod.
        Logs pod status, events, container state, process status,
        and mount point health.
        """
        log.info(f"======== IO Failure Diagnostics for '{io_pod.name}' ========")

        # 1. Pod status and container state
        try:
            pod_data = io_pod.get()
            phase = pod_data.get("status", {}).get("phase", "Unknown")
            node_name = pod_data.get("spec", {}).get("nodeName", "Unknown")
            ns = pod_data.get("metadata", {}).get("namespace", "Unknown")
            log.info(f"Pod phase: {phase}, node: {node_name}, namespace: {ns}")
            for cs in pod_data.get("status", {}).get("containerStatuses", []):
                log.info(
                    f"Container '{cs.get('name')}': "
                    f"ready={cs.get('ready')}, "
                    f"restarts={cs.get('restartCount', 0)}, "
                    f"state={cs.get('state')}, "
                    f"lastState={cs.get('lastState')}"
                )
        except Exception as ex:
            log.warning(f"Failed to get pod status: {ex}")
            return

        # 2. Pod events (timestamps of warnings/errors)
        try:
            events = io_pod.ocp.exec_oc_cmd(
                f"get events -n {ns} "
                f"--field-selector "
                f"involvedObject.name={io_pod.name} "
                f"--sort-by='.lastTimestamp'",
                out_yaml_format=False,
            )
            if events:
                log.info(f"Pod events:\n{events}")
            else:
                log.info("No events found for this pod")
        except Exception as ex:
            log.warning(f"Failed to get pod events: {ex}")

        # 3. Check if FIO process is still running
        try:
            ps_out = io_pod.exec_cmd_on_pod(
                command="ps aux",
                out_yaml_format=False,
                timeout=30,
            )
            fio_procs = [
                line
                for line in str(ps_out).split("\n")
                if "fio" in line and "grep" not in line
            ]
            if fio_procs:
                log.info(f"FIO process still running: {fio_procs}")
            else:
                log.info("FIO process is NOT running in the pod")
        except Exception as ex:
            log.warning(f"Failed to check FIO process: {ex}")

        # 4. Check mount point health
        try:
            df_out = io_pod.exec_cmd_on_pod(
                command="df -h /var/lib/www/html",
                out_yaml_format=False,
                timeout=30,
            )
            log.info(f"Mount point status:\n{df_out}")
        except Exception as ex:
            log.warning(f"Mount point check failed (volume may be unavailable): {ex}")

        # 5. Pod logs (FIO stderr)
        try:
            logs = io_pod.ocp.exec_oc_cmd(
                f"logs {io_pod.name} --tail=50",
                out_yaml_format=False,
            )
            if logs:
                log.info(f"Last 50 lines of pod logs:\n{logs}")
            else:
                log.info("Pod logs are empty")
        except Exception as ex:
            log.warning(f"Failed to get pod logs: {ex}")

        # 7. All pods in openshift-storage namespace
        try:
            storage_ns = config.ENV_DATA.get("cluster_namespace", "openshift-storage")
            from ocs_ci.ocs.ocp import OCP

            pod_ocp = OCP(kind=constants.POD, namespace=storage_ns)
            pods_out = pod_ocp.exec_oc_cmd(
                f"get pods -n {storage_ns} -o wide",
                out_yaml_format=False,
            )
            log.info(
                f"All pods in {storage_ns} namespace (client cluster):\n{pods_out}"
            )
        except Exception as ex:
            log.warning(f"Failed to list storage pods: {ex}")

        log.info(f"======== End diagnostics for '{io_pod.name}' ========")

    def _is_fio_running(self, io_pod):
        """Check if FIO process is running inside the pod."""
        try:
            ps_out = io_pod.exec_cmd_on_pod(
                command="ps aux",
                out_yaml_format=False,
                timeout=30,
            )
            return any(
                "fio" in line and "grep" not in line for line in str(ps_out).split("\n")
            )
        except CommandFailed:
            return False

    def _wait_for_fio_to_finish(self, io_pod, timeout=300):
        """
        Wait for FIO process to exit inside the pod.

        Args:
            io_pod: Pod object.
            timeout (int): Max seconds to wait.
        """
        log.info(f"Waiting up to {timeout}s for FIO to finish on pod '{io_pod.name}'")
        for sample in TimeoutSampler(
            timeout=timeout,
            sleep=15,
            func=self._is_fio_running,
            io_pod=io_pod,
        ):
            if not sample:
                log.info(f"FIO finished on pod '{io_pod.name}'")
                return
            log.info(f"FIO still running on pod '{io_pod.name}', waiting...")

    def _read_fio_results_from_pod(self, io_pod):
        """
        Read FIO JSON results from the output file inside the pod.
        Use this when oc rsh dropped but FIO completed and wrote
        results to the file.

        Returns:
            dict: Parsed FIO JSON results, or None if unreadable.
        """
        try:
            mount_path = (
                io_pod.get()
                .get("spec", {})
                .get("containers", [{}])[0]
                .get("volumeMounts", [{}])[0]
                .get("mountPath", "/var/lib/www/html")
            )
            results_file = f"{mount_path}/{self.FIO_RESULTS_FILE}"
            raw = io_pod.exec_cmd_on_pod(
                command=f"cat {results_file}",
                out_yaml_format=False,
                timeout=30,
            )
            if raw:
                fio_result = yaml.safe_load(raw)
                log.info(
                    f"Successfully read FIO results from file on pod '{io_pod.name}'"
                )
                return fio_result
        except (CommandFailed, yaml.YAMLError, KeyError) as ex:
            log.warning(
                f"Failed to read FIO results file from pod '{io_pod.name}': {ex}"
            )
        return None

    def wait_and_verify(self):
        """
        Wait for background FIO to complete, validate results, and
        verify data integrity.

        Checks performed:
        - FIO IO error counters (read/write errors == 0)
        - p99 completion latency under max_latency_sec threshold
        - FIO runtime covered the disruptive operation duration
        - md5sum of integrity file matches pre-operation checksum

        If oc rsh connection dropped during the operation, FIO
        results are recovered from the output file inside the pod.

        Raises:
            AssertionError: If FIO reported IO errors, latency
                exceeded threshold, or md5sum does not match.
            CommandFailed: If FIO results cannot be recovered.
        """
        log.info(
            "-------- FIOIntegrityChecker: waiting for background "
            "FIO to complete --------"
        )
        max_latency_ns = self.max_latency_sec * 1_000_000_000
        check_failures = []
        fio_failed_pods = []
        for io_pod in self.io_pods:
            try:
                fio_result = io_pod.get_fio_results(timeout=self._bg_runtime + 300)
            except CommandFailed as ex:
                log.error(f"FIO failed on pod '{io_pod.name}': {ex}")
                self._collect_pod_diagnostics(io_pod)
                if self._is_fio_running(io_pod):
                    log.warning(
                        "FIO still running on pod "
                        f"'{io_pod.name}' -- oc rsh "
                        "connection was likely disrupted. "
                        "Waiting for FIO to finish."
                    )
                    self._wait_for_fio_to_finish(io_pod)
                fio_result = self._read_fio_results_from_pod(io_pod)
                if fio_result:
                    jobs = fio_result.get("jobs", [])
                    log.info(
                        "Recovered FIO results from file "
                        f"for pod '{io_pod.name}' "
                        f"({len(jobs)} job(s))"
                    )
                    fio_failed_pods.append(io_pod)
                else:
                    log.error(f"Could not recover FIO results from pod '{io_pod.name}'")
                    raise
            job = fio_result["jobs"][0]

            read = job["read"]
            write = job["write"]
            read_bw = read.get("bw", 0)
            write_bw = write.get("bw", 0)
            read_iops = read["iops"]
            write_iops = write["iops"]
            read_total_ios = read.get("total_ios", 0)
            write_total_ios = write.get("total_ios", 0)
            read_io_kb = read.get("io_kbytes", 0)
            write_io_kb = write.get("io_kbytes", 0)
            read_err = read.get("io_error", 0)
            write_err = write.get("io_error", 0)
            fio_elapsed_sec = job.get("elapsed", 0)

            read_clat = read.get("clat_ns", {})
            write_clat = write.get("clat_ns", {})
            read_clat_mean = read_clat.get("mean", 0)
            write_clat_mean = write_clat.get("mean", 0)
            read_clat_max = read_clat.get("max", 0)
            write_clat_max = write_clat.get("max", 0)
            read_clat_p99 = read_clat.get("percentile", {}).get("99.000000", 0)
            write_clat_p99 = write_clat.get("percentile", {}).get("99.000000", 0)

            log.info(
                f"FIO on pod '{io_pod.name}': "
                f"runtime={fio_elapsed_sec}s, "
                f"Read: IOPS={read_iops:.1f} "
                f"BW={read_bw}KB/s "
                f"total_ios={read_total_ios} "
                f"data={read_io_kb / 1024:.1f}MB "
                f"clat mean={read_clat_mean / 1e6:.1f}ms "
                f"p99={read_clat_p99 / 1e6:.1f}ms "
                f"max={read_clat_max / 1e9:.2f}s | "
                f"Write: IOPS={write_iops:.1f} "
                f"BW={write_bw}KB/s "
                f"total_ios={write_total_ios} "
                f"data={write_io_kb / 1024:.1f}MB "
                f"clat mean={write_clat_mean / 1e6:.1f}ms "
                f"p99={write_clat_p99 / 1e6:.1f}ms "
                f"max={write_clat_max / 1e9:.2f}s | "
                f"Errors: read={read_err} write={write_err}"
            )

            if read_err == 0 and write_err == 0:
                log.info(
                    f"[PASS] IO errors check on pod '{io_pod.name}': read=0, write=0"
                )
            else:
                msg = (
                    "[FAIL] IO errors on pod "
                    f"'{io_pod.name}': read_errors="
                    f"{read_err}, write_errors={write_err}"
                )
                log.error(msg)
                check_failures.append(msg)

            worst_p99 = max(read_clat_p99, write_clat_p99)
            worst_p99_sec = worst_p99 / 1e9
            if worst_p99 <= max_latency_ns:
                log.info(
                    "[PASS] p99 latency check on pod "
                    f"'{io_pod.name}': "
                    f"{worst_p99_sec:.2f}s <= "
                    f"{self.max_latency_sec}s threshold"
                )
            else:
                msg = (
                    "[FAIL] IO stall on pod "
                    f"'{io_pod.name}': p99 latency "
                    f"{worst_p99_sec:.2f}s exceeds "
                    f"{self.max_latency_sec}s threshold"
                )
                log.error(msg)
                check_failures.append(msg)

        elapsed_since_start = time.time() - self._start_time
        if self._bg_runtime <= elapsed_since_start:
            log.info(
                "[PASS] FIO duration coverage: "
                f"bg_runtime={self._bg_runtime}s, "
                f"elapsed={elapsed_since_start:.0f}s"
            )
        else:
            msg = (
                "[FAIL] FIO duration gap: bg_runtime="
                f"{self._bg_runtime}s but only "
                f"{elapsed_since_start:.0f}s elapsed -- "
                "FIO may not have covered the full "
                "operation"
            )
            log.error(msg)
            check_failures.append(msg)

        log.info(
            "-------- FIOIntegrityChecker: verifying data "
            "integrity via md5sum --------"
        )
        for io_pod in self.io_pods:
            md5_verify = cal_md5sum(io_pod, self.INTEGRITY_FILE)
            original = self._md5sums[io_pod.name]
            if md5_verify == original:
                log.info(f"[PASS] md5sum on pod '{io_pod.name}': {md5_verify}")
            else:
                msg = (
                    "[FAIL] md5sum mismatch on pod "
                    f"'{io_pod.name}': original="
                    f"{original}, current={md5_verify}"
                )
                log.error(msg)
                check_failures.append(msg)

        if fio_failed_pods:
            log.warning(
                "-------- FIO results recovered from "
                f"file for {len(fio_failed_pods)} pod(s) "
                "due to oc rsh connection drop: "
                f"{[p.name for p in fio_failed_pods]} "
                "--------"
            )

        elapsed = time.time() - self._start_time
        if check_failures:
            log.error(
                "-------- FIOIntegrityChecker: "
                f"{len(check_failures)} check(s) FAILED "
                f"(elapsed: {elapsed:.0f}s) --------"
            )
            for failure in check_failures:
                log.error(f"  - {failure}")
            assert False, (
                "FIOIntegrityChecker: "
                f"{len(check_failures)} check(s) failed: " + "; ".join(check_failures)
            )

        log.info(
            "-------- FIOIntegrityChecker: all checks "
            f"PASSED (elapsed: {elapsed:.0f}s) --------"
        )

    def verify_md5sum_only(self):
        """
        Re-compute md5sum of the integrity files and compare with
        the stored checksums. Does not check FIO results.

        Use this after a disruptive operation where IO pods may
        have been killed (e.g. node removal that removed the pod's
        node).

        Raises:
            AssertionError: If md5sum does not match or no pods
                were reachable for verification.
        """
        log.info(
            "-------- FIOIntegrityChecker: verifying data "
            "integrity via md5sum only --------"
        )
        verified_count = 0
        for io_pod in self.io_pods:
            try:
                md5_verify = cal_md5sum(io_pod, self.INTEGRITY_FILE)
            except CommandFailed:
                log.warning(
                    f"Pod '{io_pod.name}' is not reachable, skipping md5sum check"
                )
                continue
            original = self._md5sums[io_pod.name]
            log.info(
                f"Pod '{io_pod.name}': original "
                f"md5={original}, current md5={md5_verify}"
            )
            assert md5_verify == original, (
                "Data integrity check FAILED on pod "
                f"'{io_pod.name}': md5sum changed from "
                f"{original} to {md5_verify}"
            )
            verified_count += 1
            log.info(f"Data integrity verified on pod '{io_pod.name}'")
        assert verified_count > 0, "No IO pods were reachable for md5sum verification"
        log.info(
            f"-------- md5sum verified on {verified_count}/"
            f"{len(self.io_pods)} pods --------"
        )
