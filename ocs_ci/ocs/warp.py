import logging
import os
import time

import concurrent.futures as futures
from ocs_ci.helpers import helpers
from ocs_ci.utility import templating
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.resources.pod import Pod, get_pods_having_label
from ocs_ci.ocs import constants
from ocs_ci.framework import config
from ocs_ci.ocs.resources import pod
from tempfile import mkdtemp
from ocs_ci.ocs.exceptions import UnexpectedBehaviour

log = logging.getLogger(__name__)


class Warp(object):
    """
    Warp - S3 benchmarking tool: https://github.com/minio/warp
    WARP is an open-source full-featured S3 performance assessment software
    built to conduct tests between WARP clients and object storage hosts.
    WARP measures GET and PUT performance from multiple clients against a MinIO cluster.

    """

    def __init__(self):
        """
        Initializer to create warp pod to running warp benchmark

        """
        self.pod_dic_path = constants.WARP_YAML
        self.namespace = config.ENV_DATA["cluster_namespace"]
        self.ocp_obj = OCP(namespace=self.namespace)
        self.bucket_name = None
        self.warp_cr = templating.load_yaml(constants.WARP_OBJ_YAML)
        self.host = self.warp_cr["host"]
        self.duration = self.warp_cr["duration"]
        self.concurrent = self.warp_cr["concurrent"]
        self.objects = self.warp_cr["objects"]
        self.obj_size = self.warp_cr["obj.size"]
        self.get_distrib = self.warp_cr["get-distrib"]
        self.put_distrib = self.warp_cr["put-distrib"]
        self.delete_distrib = self.warp_cr["delete-distrib"]
        self.stat_distrib = self.warp_cr["stat-distrib"]
        self.warp_bin_dir = self.warp_cr["warp_bin_dir"]
        self.output_file = "output.csv"
        self.warp_dir = mkdtemp(prefix="warp-")

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
            dc_deployment=True,
            deploy_pod_status=constants.STATUS_COMPLETED,
            replica_count=replicas,
            ports=self.ports,
        )

        if multi_client:
            self.client_pods = [
                Pod(**pod_info)
                for pod_info in get_pods_having_label(
                    label="app=warppod", namespace=self.pod_obj.namespace
                )
                if pod_info.get("metadata").get("name") != self.pod_obj.name
            ]
            self.client_ips = {}
            for p in self.client_pods:
                ip = p.exec_cmd_on_pod(command="hostname -i")
                self.client_ips[p.name] = ip

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
            validate (Boolean): Validates whether running workload is completed.
            multi_client (Boolean): If True, then run multi client benchmarking
            tls (Boolean): Use TLS (HTTPS) for transport
            insecure (Boolean): disable TLS certification verification
            debug (Boolean): Enable debug output

        """

        # Running warp S3 benchmark
        log.info("Running Minio Warp S3 benchmark")
        timeout = timeout if timeout else 3600
        self.access_key = access_key if access_key else self.access_key
        self.secret_key = secret_key if secret_key else self.secret_key
        self.bucket_name = bucket_name if bucket_name else self.bucket_name
        self.duration = duration if duration else self.duration["duration"]
        self.concurrent = concurrent if concurrent else self.concurrent["concurrent"]
        self.objects = objects if objects else self.objects["objects"]
        self.obj_size = obj_size if obj_size else self.obj_size["obj.size"]
        base_options = "".join(
            f"--duration={self.duration} "
            f"--host={self.host} "
            f"--insecure={insecure} "
            f"--tls={tls} "
            f"--debug={debug} "
            f"--access-key={self.access_key} "
            f"--secret-key={self.secret_key} "
            f"--noclear --noprefix --concurrent={self.concurrent} "
            f"--objects={self.objects} "
            f"--obj.size={self.obj_size} "
            f"--get-distrib={self.get_distrib} "
            f"--put-distrib={self.put_distrib} "
            f"--delete-distrib={self.delete_distrib} "
            f"--stat-distrib={self.stat_distrib} "
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

        cmd = f"{self.warp_bin_dir} mixed " + base_options + multi_client_options
        self.pod_obj.exec_cmd_on_pod(cmd, out_yaml_format=False, timeout=timeout)

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
        pod.delete_deploymentconfig_pods(self.pod_obj)
        self.pvc_obj.delete()
