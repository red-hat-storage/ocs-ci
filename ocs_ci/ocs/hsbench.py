import logging

from ocs_ci.helpers import helpers
from ocs_ci.utility import templating
from ocs_ci.utility.utils import run_cmd
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources import pod
from ocs_ci.framework import config
from ocs_ci.ocs.exceptions import CommandFailed

log = logging.getLogger(__name__)


class HsBench(object):
    """
    Hotsauce S3 benchmark

    """

    def __init__(self):
        """
        Initializer to create pvc and rgw pod to running hsbench benchmark

        """
        self.pod_dic_path = constants.GOLANG_YAML
        self.namespace = config.ENV_DATA["cluster_namespace"]
        self.hsbench_cr = templating.load_yaml(constants.HSBENCH_OBJ_YAML)
        self.object_size = self.hsbench_cr["object_size"]
        self.duration = self.hsbench_cr["duration"]
        self.num_threads = self.hsbench_cr["num_threads"]
        self.num_bucket = self.hsbench_cr["num_bucket"]
        self.bucket_prefix = self.hsbench_cr["bucket_prefix"]
        self.end_point = self.hsbench_cr["end_point"]
        self.hsbench_bin_dir = self.hsbench_cr["hsbench_bin_dir"]

    def create_resource_hsbench(self):
        """
        Create resource for hsbench mark test:
            Create service account
            Create PVC
            Create golang pod

        """

        # Create service account
        self.sa_name = helpers.create_serviceaccount(self.namespace)
        self.sa_name = self.sa_name.name
        helpers.add_scc_policy(sa_name=self.sa_name, namespace=self.namespace)

        # Create test pvc+pod
        log.info(f"Create Golang pod to generate S3 workload... {self.namespace}")
        pvc_size = "50Gi"
        self.pod_name = "hsbench-pod"
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

    def install_hsbench(self, timeout=2400):
        """
        Install HotSauce S3 benchmark:
        https://github.com/markhpc/hsbench

        """
        # Install hsbench
        log.info(f"Installing hsbench S3 benchmark on testing pod {self.pod_obj.name}")
        hsbench_path = "github.com/markhpc/hsbench"
        self.pod_obj.exec_cmd_on_pod(f"go get -v {hsbench_path}", timeout=timeout)
        log.info("Successfully installing hsbench benchmark")

    def create_test_user(self):
        """
        Create a radosgw test user for S3 access

        """
        # Create RWG test user
        self.toolbox = pod.get_ceph_tools_pod()
        self.uid = self.hsbench_cr["uid"]
        display_name = self.hsbench_cr["display_name"]
        email = self.hsbench_cr["email"]
        self.access_key = self.hsbench_cr["access_key"]
        self.secret_key = self.hsbench_cr["secret_key"]
        self.end_point = self.hsbench_cr["end_point"]

        log.info(f"Create RGW test user {self.uid}")
        self.toolbox.exec_cmd_on_pod(
            f"radosgw-admin user create --uid={self.uid} "
            f"--display-name={display_name} --email={email} "
            f"--access-key={self.access_key} --secret-key={self.secret_key}"
        )
        return

    def run_benchmark(
        self,
        num_obj=None,
        run_mode=None,
        timeout=None,
        access_key=None,
        secret_key=None,
        end_point=None,
    ):
        """
         Running Hotsauce S3 benchmark
         Usage detail can be found at: https://github.com/markhpc/hsbench

        Args:
            num_obj (int): Maximum number of objects
            run_mode (string): mode types
            timeout (int): timeout in seconds
            access_key (string): Access Key credential
            secret_key (string): Secret Key credential
            end_point (string): S3 end_point

        """
        # Create hsbench S3 benchmark
        log.info("Running hsbench benchmark")
        timeout = timeout if timeout else 3600
        self.timeout_clean = timeout * 3
        self.num_obj = num_obj if num_obj else self.hsbench_cr["num_obj"]
        self.run_mode = run_mode if run_mode else self.hsbench_cr["run_mode"]
        self.access_key = access_key if access_key else self.access_key
        self.secret_key = secret_key if secret_key else self.secret_key
        self.end_point = end_point if end_point else self.end_point
        self.pod_obj.exec_cmd_on_pod(
            f"{self.hsbench_bin_dir} -a {self.access_key} "
            f"-s {self.secret_key} "
            f"-u {self.end_point} "
            f"-z {self.object_size} "
            f"-d {self.duration} -t {self.num_threads} "
            f"-b {self.num_bucket} "
            f"-n {self.num_obj} -m {self.run_mode} "
            f"-bp {self.bucket_prefix}",
            timeout=timeout,
        )

    def validate_s3_objects(self):
        """
        Validate S3 objects created by hsbench on single bucket

        """
        self.bucket_name = self.bucket_prefix + "000000000000"
        num_objects = self.toolbox.exec_sh_cmd_on_pod(
            f"radosgw-admin bucket stats --bucket={self.bucket_name} | grep num_objects"
        )
        if num_objects is not self.num_obj:
            log.info(
                f"Number object is created by hsbench in {self.bucket_name} "
                f"is {num_objects}"
            )
        assert num_objects, (
            "Number objects in bucket don't match."
            f"Expecting {self.num_obj} but getting {num_objects}"
        )

    def validate_reshard_process(self):
        """
        Validate reshard process

        Raises:
            CommandFailed: If reshard process fails

        """
        log.info("Starting checking bucket limit and start reshard process")
        try:
            self.toolbox.exec_cmd_on_pod(
                f"radosgw-admin bucket limit check --uid={self.uid}"
            )
            self.toolbox.exec_cmd_on_pod("radosgw-admin reshard list")
            self.toolbox.exec_cmd_on_pod("radosgw-admin reshard process")
            self.toolbox.exec_cmd_on_pod(
                f"radosgw-admin reshard status --bucket={self.bucket_name}"
            )
        except CommandFailed as cf:
            log.error("Failed during reshard process")
            raise cf
        log.info("Reshard process has completed successfully.")

    def delete_test_user(self):
        """
        Delete RGW test user and bucket belong to test user

        """
        log.info(f"Deleting RGW test user: {self.uid}")
        self.toolbox.exec_cmd_on_pod(
            f"radosgw-admin user rm --uid={self.uid} --purge-data",
            timeout=self.timeout_clean,
        )

    def cleanup(self):
        """
        Clear all objects in the associated bucket
        Clean up deployment config, pvc, pod and test user

        """
        log.info("Deleting pods and deployment config")
        run_cmd(f"oc delete deploymentconfig/{self.pod_name} -n {self.namespace}")
        self.pod_obj.delete()
        self.pvc_obj.delete()
