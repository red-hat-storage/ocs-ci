import logging

from ocs_ci.helpers import helpers
from ocs_ci.utility import templating
from ocs_ci.utility.utils import run_cmd
from ocs_ci.ocs import constants
from ocs_ci.framework import config

log = logging.getLogger(__name__)


class Warp(object):
    """
    Warp S3 benchmark

    """

    def __init__(self):
        """
        Initializer to create warp pod to running warp benchmark

        """
        self.pod_dic_path = constants.WARP_YAML
        self.namespace = config.ENV_DATA["cluster_namespace"]
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

    def create_resource_warp(self):
        """
        Create resource for Warp S3 benchmark:
            * Create service account
            * Create PVC
            * Create warp pod for running workload

        """

        # Create service account
        self.sa_name = helpers.create_serviceaccount(self.namespace)
        self.sa_name = self.sa_name.name
        helpers.add_scc_policy(sa_name=self.sa_name, namespace=self.namespace)

        # Create test pvc+pod
        log.info(f"Create Warp pod to generate S3 workload in {self.namespace}")
        pvc_size = "50Gi"
        self.pod_name = "warp-pod"
        self.pvc_obj = helpers.create_pvc(
            sc_name=constants.DEFAULT_STORAGECLASS_RBD,
            namespace=self.namespace,
            size=pvc_size,
        )
        self.pod_obj = helpers.create_pod(
            constants.CEPHBLOCKPOOL,
            namespace=self.namespace,
            pod_name=self.pod_name,
            pvc_name=self.pvc_obj.name,
            sa_name=self.sa_name,
            pod_dict_path=self.pod_dic_path,
            dc_deployment=True,
            deploy_pod_status=constants.STATUS_COMPLETED,
        )

    def install_warp(self, timeout=1800):
        """
        Install minio warp S3 benchmark:
        https://github.com/minio/warp

        """
        # Install warp
        log.info(f"Installing warp S3 benchmark on testing pod {self.pod_obj.name}")
        warp_path = "https://github.com/minio/warp/releases/download/v0.5.5/warp_0.5.5_Linux_x86_64.tar.gz"
        self.pod_obj.exec_cmd_on_pod(f"wget {warp_path}", timeout=timeout)
        self.pod_obj.exec_cmd_on_pod(
            "tar -xzvf warp_0.5.5_Linux_x86_64.tar.gz", timeout=timeout
        )
        log.info("Successfully installing warp s3 benchmark")

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
        self.pod_obj.exec_cmd_on_pod(
            f"{self.warp_bin_dir} mixed "
            f"--duration={self.duration} "
            f"--host={self.host} "
            f"--access-key={self.access_key} "
            f"--secret-key={self.secret_key} "
            f"--noclear --noprefix --concurrent={self.concurrent} "
            f"--objects={self.objects} "
            f"--obj.size={self.obj_size} "
            f"--get-distrib={self.get_distrib} "
            f"--put-distrib={self.put_distrib} "
            f"--delete-distrib={self.delete_distrib} "
            f"--stat-distrib={self.stat_distrib} "
            f"--analyze.v --bucket={self.bucket_name}",
            out_yaml_format=False,
            timeout=timeout,
        )

    def cleanup(self):
        """
        Clear all objects in the associated bucket
        Clean up deployment config, pvc, pod and test user

        """
        log.info("Deleting pods and deployment config")
        run_cmd(f"oc delete deploymentconfig/{self.pod_name} -n {self.namespace}")
        self.pvc_obj.delete()
