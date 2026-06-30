import concurrent.futures as futures
from io import StringIO
import logging
import os
import shlex
from tempfile import mkdtemp
import threading
import time
from urllib.parse import urlparse

import pandas as pd

from ocs_ci.framework import config
from ocs_ci.helpers import helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed, UnexpectedBehaviour
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.resources.pod import Pod, get_pods_having_label
from ocs_ci.utility import templating
from ocs_ci.ocs.ui.workload_ui import wait_for_container_status_ready

log = logging.getLogger(__name__)


def warp_host_and_tls_from_noobaa_endpoint(endpoint_url, storage_namespace=None):
    """
    Map a NooBaa S3 base URL to warp ``--host`` (``host:port``) and the ``--tls`` flag.

    Accepts ``MCG.s3_endpoint`` (external / Route) or ``MCG.s3_internal_endpoint``
    (CR internalDNS). Explicit ports and scheme matter for SigV4 and TLS.

    Args:
        endpoint_url: HTTPS or HTTP URL, or host:port without scheme
        storage_namespace: OpenShift namespace where NooBaa runs (default: openshift-storage)

    Returns:
        tuple: (host_colon_port, use_tls)
    """
    ns = storage_namespace or constants.OPENSHIFT_STORAGE_NAMESPACE
    default_host = f"s3.{ns}.svc:443"
    if not endpoint_url or not str(endpoint_url).strip():
        log.warning("No S3 endpoint; defaulting Warp --host to %s", default_host)
        return default_host, True
    raw = str(endpoint_url).strip()
    if "://" not in raw:
        raw = "https://" + raw
    parsed = urlparse(raw)
    hostport = parsed.netloc
    if not hostport:
        hostport = parsed.path.strip("/")
    if not hostport:
        log.warning(
            "Could not parse S3 endpoint %r; defaulting Warp --host to %s",
            endpoint_url,
            default_host,
        )
        return default_host, True
    use_tls = parsed.scheme == "https"
    if use_tls and ":" not in hostport:
        hostport = f"{hostport}:443"
    elif not use_tls and ":" not in hostport:
        hostport = f"{hostport}:80"
    return hostport, use_tls


class Warp(object):
    """
    Warp - S3 benchmarking tool: https://github.com/minio/warp
    WARP is an open-source full-featured S3 performance assessment software
    built to conduct tests between WARP clients and object storage hosts.
    WARP measures GET and PUT performance from multiple clients against a MinIO cluster.

    """

    def __init__(self, pod_name_suffix=None, namespace=None, s3_host=None):
        """
        Initializer to create warp pod to running warp benchmark

        Args:
            pod_name_suffix (str): Optional suffix for pod name to make it unique
            namespace (str): Namespace to deploy Warp pods (default: cluster_namespace from config)
            s3_host (str): S3 endpoint host (default: s3.{cluster_namespace}.svc)
        """
        self.pod_dic_path = constants.WARP_YAML
        # Use provided namespace or fall back to cluster_namespace
        self.namespace = namespace or config.ENV_DATA.get(
            "cluster_namespace", "openshift-storage"
        )
        self.ocp_obj = OCP(namespace=self.namespace)
        self.bucket_name = None
        self.warp_cr = templating.load_yaml(constants.WARP_OBJ_YAML)
        # Use provided S3 host or construct from cluster namespace
        if s3_host:
            self.host = s3_host
        else:
            # For default namespace, use openshift-storage for S3 endpoint
            storage_namespace = config.ENV_DATA.get(
                "cluster_namespace", "openshift-storage"
            )
            self.host = f"s3.{storage_namespace}.svc"
        self.duration = self.warp_cr["duration"]
        self.concurrent = self.warp_cr["concurrent"]
        self.obj_size = self.warp_cr["obj.size"]
        self.warp_bin_dir = self.warp_cr["warp_bin_dir"]
        self.output_file = "output.csv"
        self.warp_dir = mkdtemp(prefix="warp-")
        self.pod_name_suffix = pod_name_suffix

        log.info(f"Warp initialized: namespace={self.namespace}, s3_host={self.host}")

    def create_resource_warp(self, multi_client=False, replicas=1):
        """
        Create resource for Warp S3 benchmark:
            * Create service account
            * Create PVC
            * Create warp pod for running workload

        """

        # create Warp service if multi client benchmarking
        self.ports = None
        if multi_client:
            service_data = templating.load_yaml(constants.WARP_SERVICE_YAML)
            self.service_obj = OCS(**service_data)
            self.service_obj.create()
            self.ports = [{"name": "http", "containerPort": constants.WARP_CLIENT_PORT}]

        # Create service account
        self.sa_name = helpers.create_serviceaccount(self.namespace)
        self.sa_name = self.sa_name.name
        helpers.add_scc_policy(sa_name=self.sa_name, namespace=self.namespace)

        # Create test pvc+pod
        log.info(f"Create Warp pod to generate S3 workload in {self.namespace}")
        pvc_size = "50Gi"
        # Use unique pod name if suffix provided
        if self.pod_name_suffix:
            self.pod_name = f"warppod-{self.pod_name_suffix}"
        else:
            self.pod_name = "warppod"
        self.pvc_obj = helpers.create_pvc(
            sc_name=constants.DEFAULT_STORAGECLASS_CEPHFS,
            namespace=self.namespace,
            size=pvc_size,
            access_mode=constants.ACCESS_MODE_RWX,
        )

        # create warp pods
        self.pod_obj = helpers.create_pod(
            constants.CEPHBLOCKPOOL,
            namespace=self.namespace,
            pod_name=self.pod_name,
            pvc_name=self.pvc_obj.name,
            sa_name=self.sa_name,
            pod_dict_path=self.pod_dic_path,
            deployment=True,
            replica_count=replicas,
            ports=self.ports,
        )

        all_warp_pods = [
            Pod(**pod_info)
            for pod_info in get_pods_having_label(
                label="app=warppod", namespace=self.pod_obj.namespace
            )
        ]

        for con in all_warp_pods:
            wait_for_container_status_ready(pod=con)

        if multi_client:
            self.client_pods = [
                pod_info
                for pod_info in all_warp_pods
                if pod_info.get("metadata").get("name") != self.pod_obj.name
            ]
            self.client_ips = {}
            for p in self.client_pods:
                ip = p.exec_cmd_on_pod(command="hostname -i")
                self.client_ips[p.name] = ip

    def _exec_warp_resilient(self, command, exec_timeout):
        """
        Run a warp-related shell command via oc exec; retry on transient container/exec errors.

        After OOM/restart, kubelet may not have the ``warp`` container attached yet,
        which surfaces as ``container not found`` or ``unable to upgrade connection``.

        Concurrent cluster load (e.g. volume snapshots, node I/O) can keep the pod or
        container unready longer than a short sleep; we wait for Ready before retrying.

        Args:
            command: Full command string passed to exec (e.g. blocking ``sh -c`` or
                ``nohup ... &`` for background).
            exec_timeout: ``oc exec`` timeout in seconds. Use the full benchmark
                timeout when running warp synchronously; use a short value when the
                command returns immediately (background start).
        """
        transient_markers = (
            "container not found",
            "unable to upgrade connection",
        )
        max_attempts = 24
        delay_seconds = 15
        wait_ready_timeout = 120
        wait_ready_interval = 5
        need_container_wait = False
        for attempt in range(1, max_attempts + 1):
            if need_container_wait:
                log.info(
                    "Waiting up to %ss for warp pod %s containers to be ready before retry",
                    wait_ready_timeout,
                    self.pod_obj.name,
                )
                wait_for_container_status_ready(
                    self.pod_obj,
                    timeout=wait_ready_timeout,
                    interval=wait_ready_interval,
                )
                time.sleep(delay_seconds)
            try:
                self.pod_obj.exec_cmd_on_pod(
                    command,
                    out_yaml_format=False,
                    timeout=exec_timeout,
                    container_name="warp",
                )
                if attempt > 1:
                    log.info("Warp exec succeeded after %s attempts", attempt)
                return
            except CommandFailed as exc:
                msg = str(exc).lower()
                retryable = any(marker in msg for marker in transient_markers)
                if retryable and attempt < max_attempts:
                    log.warning(
                        "Warp exec not ready (attempt %s/%s): %s; "
                        "next attempt will wait for container Ready (up to %ss) then retry",
                        attempt,
                        max_attempts,
                        exc,
                        wait_ready_timeout,
                    )
                    need_container_wait = True
                else:
                    raise

    def run_benchmark(
        self,
        bucket_name=None,
        access_key=None,
        secret_key=None,
        duration=None,
        concurrent=None,
        objects=None,
        obj_size=None,
        timeout=None,
        validate=True,
        multi_client=True,
        tls=False,
        insecure=False,
        debug=False,
        workload_type="put",
        s3_region=None,
        kwargs=None,
    ):
        """
         Running Warp S3 benchmark
         Usage detail can be found at: https://github.com/minio/warp

        Args:
            bucket_name (string): Name of bucket
            access_key (string): Access Key credential
            secret_key (string): Secret Key credential
            duration (string): Duration of the test
            concurrent (int): number of concurrent
            objects (int): number of objects
            obj_size (int): size of object
            timeout (int): timeout in seconds
            validate (Boolean): If True, run warp synchronously until completion then
                check ``output.csv``; if False, start warp in the background (returns
                quickly; no CSV validation in this method).
            multi_client (Boolean): If True, then run multi client benchmarking
            tls (Boolean): Use TLS (HTTPS) for transport
            insecure (Boolean): disable TLS certification verification
            debug (Boolean): Enable debug output
            workload_type (str): Type of workload to run (put, get, stat, mixed, etc.)
            s3_region (str): AWS SigV4 signing region. NooBaa expects ``us-east-1`` in
                most deployments; do not pass cloud ENV_DATA region (e.g. IBM) here.
            kwargs (dict): Additional keyword arguments to pass to the warp command
        """

        # Running warp S3 benchmark
        log.info("Running Minio Warp S3 benchmark")
        timeout = timeout if timeout else 3600
        self.access_key = access_key if access_key else self.access_key
        self.secret_key = secret_key if secret_key else self.secret_key
        self.bucket_name = bucket_name if bucket_name else self.bucket_name
        self.duration = duration if duration else self.duration["duration"]
        self.concurrent = concurrent if concurrent else self.concurrent["concurrent"]
        self.obj_size = obj_size if obj_size else self.obj_size["obj.size"]
        # Note: upstream minio/warp newer releases add --lookup; quay.io/ocsci/warp:latest
        # may be older and does not support that flag.
        warp_region = s3_region if s3_region is not None else "us-east-1"
        objects_opt = ""
        if objects is not None:
            objects_opt = f"--objects={int(objects)} "
        base_options = "".join(
            f"--duration={self.duration} "
            f"--host={self.host} "
            f"--region={warp_region} "
            f"--insecure={insecure} "
            f"--tls={tls} "
            f"--debug={debug} "
            f"--access-key={self.access_key} "
            f"--secret-key={self.secret_key} "
            f"--noclear --noprefix --concurrent={self.concurrent} "
            f"{objects_opt}"
            f"--obj.size={self.obj_size} "
            f"--bucket={self.bucket_name} "
            f"--analyze.out={self.output_file} "
        )

        # Setup warp clients on warp client pods
        self.client_str = ""
        multi_client_options = ""
        if multi_client:
            thread_exec = futures.ThreadPoolExecutor(max_workers=len(self.client_pods))
            for p in self.client_pods:
                command = f"{self.warp_bin_dir} client"
                thread_exec.submit(p.exec_cmd_on_pod, command=command, timeout=timeout)
            log.info("Wait for 5 seconds after the clients are started listening!")
            time.sleep(5)
            for client in self.client_ips:
                self.client_str += (
                    f"{self.client_ips[client]}:{constants.WARP_CLIENT_PORT},"
                )
            self.client_str = self.client_str.rstrip(",")
            multi_client_options = "".join(f"--warp-client={self.client_str} ")

        cmd = (
            f"{self.warp_bin_dir} {workload_type} "
            + base_options
            + multi_client_options
        )
        # Background: avoid bash -c '...' (extra fork + brittle quoting for secrets).
        # nohup + sh -c with shlex.quote handles special characters in keys; stdin
        # detached from /dev/null. Under Krkn chaos, "can't fork" is mitigated
        # slightly by one fewer shell layer than bash -c.
        inner = f"{cmd} > /tmp/warp.log 2>&1"
        if validate:
            # Synchronous run: block until warp finishes so output.csv exists for validation.
            cmd_blocking = f"sh -c {shlex.quote(inner)}"
            self._exec_warp_resilient(cmd_blocking, exec_timeout=timeout)
            log.info(
                "Warp benchmark finished (synchronous run; validation will follow)"
            )
        else:
            cmd_with_logging = f"nohup sh -c {shlex.quote(inner)} </dev/null &"
            self._exec_warp_resilient(cmd_with_logging, exec_timeout=10)
            log.info(
                "Warp benchmark started in background. Check logs with: "
                "oc exec %s -n %s -c warp -- tail -f /tmp/warp.log",
                self.pod_obj.name,
                self.namespace,
            )

        if validate:
            self.validate_warp_workload()

    def validate_warp_workload(self):
        """
        Validate if workload was running on the app-pod

        Raise:
            UnexpectedBehaviour: if output.csv file doesn't contain output data.
        """
        cmd = (
            f"cp {self.pod_obj.name}:/home/warp/{self.output_file} "
            f"{self.warp_dir}/{self.output_file}"
        )
        self.ocp_obj.exec_oc_cmd(
            command=cmd,
            out_yaml_format=False,
            timeout=180,
        )
        if os.path.getsize(f"{self.warp_dir}/{self.output_file}") != 0:
            log.info("Workload was running...")
        else:
            raise UnexpectedBehaviour(
                f"Output file {self.output_file} is empty, "
                "Warp workload doesn't run as expected..."
            )

    def get_warp_logs(self, lines=50):
        """
        Get Warp workload logs from the pod.

        Args:
            lines (int): Number of log lines to retrieve

        Returns:
            str: Warp log output
        """
        try:
            cmd = f"tail -n {lines} /tmp/warp.log 2>/dev/null || echo 'Warp log not available yet'"
            result = self.pod_obj.exec_cmd_on_pod(
                cmd, out_yaml_format=False, container_name="warp"
            )
            return result
        except Exception as e:
            log.warning(f"Could not retrieve Warp logs: {e}")
            return None

    def is_warp_running(self):
        """
        Check if Warp process is currently running in the pod.

        Returns:
            bool: True if Warp is running, False otherwise
        """
        try:
            cmd = "ps aux | grep '[w]arp mixed\\|[w]arp get\\|[w]arp put\\|[w]arp delete\\|[w]arp stat'"
            result = self.pod_obj.exec_cmd_on_pod(
                cmd, out_yaml_format=False, container_name="warp"
            )
            return bool(result and "warp" in result)
        except Exception as e:
            log.warning(f"Could not check Warp status: {e}")
            return False

    def cleanup(self, multi_client=False):
        """
        Clear all objects in the associated bucket
        Clean up deployment config, pvc, pod and test user

        """
        if multi_client:
            if self.service_obj:
                log.info(f"Deleting the service {self.service_obj.name}")
                self.service_obj.delete()
        log.info("Deleting pods and deployment config")
        if self.pod_obj:
            pod.delete_deployment_pods(self.pod_obj)
        if self.pvc_obj:
            self.pvc_obj.delete()

    def get_last_report(self):
        """
        Get the last report from the warp workload runner

        Returns:
            pd.DataFrame: The last report from the warp workload runner
        """
        try:
            output_csv = self.pod_obj.exec_cmd_on_pod(
                command=f"cat {self.output_file}", out_yaml_format=False, silent=True
            )
            df = pd.read_csv(StringIO(output_csv), sep="\t")
            return df
        except CommandFailed as e:
            log.warning(f"Failed to get last report: {e}")
            return None


class WarpWorkloadRunner:
    """
    Helper class to run a warp workload continuously in a background thread
    """

    def __init__(self, request, host, multi_client=False):
        """
        Initialize the workload runner

        Args:
            request (pytest.FixtureRequest): The request object
            host (str): The host to use for the warp workload
            multi_client (bool): Whether to use a multi-client benchmark
        """
        self.warp = Warp()
        self.request = request
        self.request.addfinalizer(self.warp.cleanup)
        self.warp.host = host
        self.warp.create_resource_warp()
        self.thread = None
        self.stop_event = None

    def start(
        self,
        access_key,
        secret_key,
        bucket_name,
        workload_type="put",
        concurrent=10,
        obj_size="1MiB",
        duration="30s",
        timeout=300,
    ):
        """
        Start a continuous warp workload in a background thread

        Args:
            access_key (str): S3 access key
            secret_key (str): S3 secret key
            bucket_name (str): Name of the bucket to use
            request (pytest.FixtureRequest): The pytest request object
            workload_type (str): Type of workload to run (put, get, stat, mixed, etc.)
            concurrent (int): number of concurrent
            obj_size (int): size of object
            duration (str): Duration for each warp iteration (default: "30s")
            timeout (int): Timeout for each warp iteration (default: 60 seconds)
        """
        if self.thread and self.thread.is_alive():
            log.warning("Warp workload is already running")
            return

        log.info("Starting warp workload in background thread")
        self.request.addfinalizer(self.stop)
        self.stop_event = threading.Event()

        def run_warp_workload():
            """Run warp benchmark in a loop until stop event is set"""
            while not self.stop_event.is_set():
                try:
                    log.info("Running warp workload")
                    self.warp.run_benchmark(
                        workload_type=workload_type,
                        bucket_name=bucket_name,
                        access_key=access_key,
                        secret_key=secret_key,
                        duration=duration,
                        concurrent=concurrent,
                        obj_size=obj_size,
                        timeout=timeout,
                        tls=True,
                        insecure=True,
                        validate=False,
                        multi_client=False,
                    )
                except Exception as e:
                    log.warning(f"Warp workload iteration failed: {e}")
                    if self.stop_event.is_set():
                        break

        self.thread = threading.Thread(target=run_warp_workload)
        self.thread.start()
        log.info("Warp workload thread started")

    def stop(self):
        """Stop the warp workload"""
        if not self.stop_event:
            log.warning("Stop event is not set, cannot stop warp workload")
            return

        if self.thread and self.thread.is_alive():
            self.stop_event.set()

            self.thread.join(timeout=120)
            if self.thread.is_alive():
                log.error("Warp workload thread is still alive after join")

            log.info("Warp workload thread stopped")
        else:
            log.warning("Warp workload thread is not running")
