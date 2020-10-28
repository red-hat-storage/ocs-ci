import logging

from tests import helpers
from ocs_ci.utility import templating
from ocs_ci.utility.utils import run_cmd
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources import pod
from ocs_ci.framework import config
from ocs_ci.ocs.exceptions import (
    CommandFailed, UnexpectedBehaviour
)

log = logging.getLogger(__name__)


class HsBench(object):
    """
    Hotsauce S3 benchmark

    """

    def __init__(self):
        """
        Initializer to create pvc and rgw pod to running hsbench benchmark

        Args:
            kind (str): Kind of service POD or DeploymentConfig
            pod_dict_path (yaml): Pod yaml
            node_selector (dict): Pods will be created in this node_selector

        """
        self.pod_dic_path = constants.FEDORA_DC_YAML
        self.namespace = config.ENV_DATA['cluster_namespace']
        self.hsbench_cr = templating.load_yaml(constants.HSBENCH_OBJ_YAML)

    def create_resource_hsbench(self):
        """
        Create resource for hsbench mark test:
            Create service account
            Create PVC
            Create Fedora DC pod

        """
        # Check for existing rgw pods on cluster
        self.rgw_pod = pod.get_rgw_pods()
        if self.rgw_pod:
            # Create service account
            self.sa_name = helpers.create_serviceaccount(self.namespace)
            self.sa_name = self.sa_name.name
            helpers.add_scc_policy(
                sa_name=self.sa_name, namespace=self.namespace
            )

            # Create test pvc+pod
            log.info(f"Create Fedora dc pod for testing in {self.namespace}")
            pvc_size = '50Gi'
            node_name = 'compute-0'
            self.pod_name = 'hsbench-pod'
            self.pvc_obj = helpers.create_pvc(sc_name=constants.DEFAULT_STORAGECLASS_RBD,
                                              namespace=self.namespace, size=pvc_size
                                              )
            self.pod_obj = helpers.create_pod(constants.CEPHBLOCKPOOL, namespace=self.namespace,
                                              pod_name=self.pod_name, pvc_name=self.pvc_obj.name,
                                              node_name=node_name, sa_name=self.sa_name,
                                              pod_dict_path=self.pod_dic_path, dc_deployment=True,
                                              deploy_pod_status=constants.STATUS_COMPLETED
                                              )
        else:
            raise UnexpectedBehaviour("This cluster doesn't have RGW pod(s) to perform hsbench")

    def install_hsbench(self, timeout=1800):
        """
        Install HotSauce S3 benchmark:
        https://github.com/markhpc/hsbench

        """
        # Install hsbench
        log.info(f"Installing hsbench S3 benchmark on testing pod {self.pod_obj.name}")
        hsbench_path = "github.com/markhpc/hsbench"
        self.pod_obj.exec_cmd_on_pod("yum update -y", timeout=timeout)
        self.pod_obj.exec_cmd_on_pod("yum install git -y", timeout=timeout)
        self.pod_obj.exec_cmd_on_pod("yum install golang-bin -y", timeout=timeout)
        self.pod_obj.exec_cmd_on_pod(f"go get -v {hsbench_path}", timeout=timeout)
        log.info("Successfully installing hsbench benchmark")

    def create_test_user(self):
        """
        Create a radosgw test user for S3 access

        """
        # Create RWG test user
        self.toolbox = pod.get_ceph_tools_pod()
        self.uid = self.hsbench_cr['uid']
        display_name = self.hsbench_cr['display_name']
        email = self.hsbench_cr['email']
        self.access_key = self.hsbench_cr['access_key']
        self.secret_key = self.hsbench_cr['secret_key']

        log.info(f"Create RGW test user {self.uid}")
        self.toolbox.exec_cmd_on_pod(
            f"radosgw-admin user create --uid={self.uid} "
            f"--display-name={display_name} --email={email} "
            f"--access-key={self.access_key} --secret-key={self.secret_key}"
        )
        return

    def run_hsbench(self, num_obj=None, timeout=None):
        """
         Running Hotsauce S3 benchmark
         Usage detail can be found at: https://github.com/markhpc/hsbench

        Args:
            num_obj (int): Maximum number of objects
            timeout (int): timeout in seconds

        """
        # Create hsbench S3 benchmark
        log.info("Running hsbench benchmark")
        object_size = self.hsbench_cr['object_size']
        duration = self.hsbench_cr['duration']
        num_threads = self.hsbench_cr['num_threads']
        num_bucket = self.hsbench_cr['num_bucket']
        run_mode = self.hsbench_cr['run_mode']
        self.bucket_prefix = self.hsbench_cr['bucket_prefix']
        end_point = self.hsbench_cr['end_point']
        end_point_port = self.hsbench_cr['end_point_port']
        hsbench_bin_dir = self.hsbench_cr['hsbench_bin_dir']
        self.num_obj = num_obj if num_obj else self.hsbench_cr['num_obj']
        timeout = timeout if timeout else 3600
        self.pod_obj.exec_cmd_on_pod(f"{hsbench_bin_dir} -a {self.access_key} "
                                     f"-s {self.secret_key} "
                                     f"-u {end_point}:{end_point_port} "
                                     f"-z {object_size} "
                                     f"-d {duration} -t {num_threads} "
                                     f"-b {num_bucket} "
                                     f"-n {self.num_obj} -m {run_mode} "
                                     f"-bp {self.bucket_prefix}",
                                     timeout=timeout
                                     )

    def validate_S3_objects(self):
        """
        Validate S3 objects created by hsbench on single bucket

        """
        self.bucket_name = self.bucket_prefix + '000000000000'
        num_objects = self.toolbox.exec_sh_cmd_on_pod(
            f"radosgw-admin bucket stats --bucket={self.bucket_name} | grep num_objects"
        )
        if num_objects is not self.num_obj:
            log.info(f"Number object is created by hsbench in {self.bucket_name} "
                     f"is {num_objects}")
        assert num_objects, "Number objects in bucket don't match." \
                            f"Expecting {self.num_obj} but getting {num_objects}"

    def validate_reshard_process(self):
        """
        Validate reshard process

        Raises:
            CommandFailed: If reshard process fails

        """
        log.info("Starting checking bucket limit and start reshard process")
        try:
            self.toolbox.exec_cmd_on_pod(f"radosgw-admin bucket limit check --uid={self.uid}")
            self.toolbox.exec_cmd_on_pod("radosgw-admin reshard list")
            self.toolbox.exec_cmd_on_pod("radosgw-admin reshard process")
            self.toolbox.exec_cmd_on_pod(f"radosgw-admin reshard status --bucket={self.bucket_name}")
        except CommandFailed as cf:
            log.error("Failed during reshard process")
            raise cf
        log.info("Reshard process has completed successfully.")

    def delete_test_user(self, timeout=3600):
        """
        Delete RGW test user and bucket belong to test user

        """
        log.info(f"Deleting RGW test user: {self.uid}")
        self.toolbox.exec_cmd_on_pod(f"radosgw-admin user rm --uid={self.uid} --purge-data", timeout=timeout)

    def cleanup(self):
        """
        Clean up deployment config, pvc, pod and test user

        """
        log.info("Deleting pods and deployment config")
        run_cmd(f"oc delete deploymentconfig/{self.pod_name}")
        self.pod_obj.delete()
        self.pvc_obj.delete()
        self.delete_test_user()
