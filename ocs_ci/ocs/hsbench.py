import logging
import os
import csv
import filecmp
import time

from ocs_ci.ocs.ocp import OCP
from ocs_ci.helpers import helpers
from ocs_ci.utility import templating
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources import pod
from ocs_ci.framework import config
from tempfile import mkdtemp
from ocs_ci.ocs.exceptions import CommandFailed, UnexpectedBehaviour

log = logging.getLogger(__name__)


class HsBench(object):
    """
    Hotsauce S3 benchmark

    """

    def __init__(self):
        """
        Initializer to create pvc and rgw pod to running hsbench benchmark

        """
        self.namespace = config.ENV_DATA["cluster_namespace"]
        self.ocp_obj = OCP(namespace=self.namespace)
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
        self.result = self.hsbench_cr["output_file"]
        self.hsbench_dir = mkdtemp(prefix="hsbench-")

    def _get_bucket_name(self, bucket_num):
        """
        Get bucket name from bucket number.

        Args:
            bucket_num (int): Number of bucket
        Returns:
            str : Name of bucket

        """

        bucket_postfix = str("{:d}".format(bucket_num).zfill(12))
        bucket_name = self.bucket_prefix + bucket_postfix
        return bucket_name

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

    def install_hsbench(self, timeout=4200):
        """
        Install HotSauce S3 benchmark:
        https://github.com/markhpc/hsbench

        """
        # Install hsbench
        log.info(f"Installing hsbench S3 benchmark on testing pod {self.pod_obj.name}")
        hsbench_path = "github.com/markhpc/hsbench"
        self.pod_obj.exec_cmd_on_pod(
            f"go get -v -insecure {hsbench_path}", timeout=timeout
        )
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
        num_bucket=None,
        object_size=None,
        bucket_prefix=None,
        result=None,
        validate=True,
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
            num_bucket (int): Number of bucket(s)
            object_size (int): Size of object
            bucket_prefix (str): Prefix for buckets
            result (str): Write CSV output to this file
            validate (Boolean): Validates whether running workload is completed.

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
        self.num_bucket = num_bucket if num_bucket else self.num_bucket
        self.object_size = object_size if object_size else self.object_size
        self.bucket_prefix = bucket_prefix if bucket_prefix else self.bucket_prefix
        self.result = result if result else self.result
        self.pod_obj.exec_cmd_on_pod(
            f"{self.hsbench_bin_dir} "
            f"-a {self.access_key} "
            f"-s {self.secret_key} "
            f"-u {self.end_point} "
            f"-z {self.object_size} "
            f"-d {self.duration} "
            f"-t {self.num_threads} "
            f"-b {self.num_bucket} "
            f"-n {self.num_obj} "
            f"-m {self.run_mode} "
            f"-bp {self.bucket_prefix} "
            f"-o {self.result}",
            timeout=timeout,
        )
        if validate:
            self.validate_hsbench_workload(result=self.result)

    def validate_hsbench_workload(self, result=None):
        """
        Validate if workload was running on the app-pod

        Raises:
            UnexpectedBehaviour: if result.csv file doesn't contain output data.

        """
        cmd = f"cp {self.pod_obj.name}:/go/{result} " f"{self.hsbench_dir}/{result}"
        self.ocp_obj.exec_oc_cmd(
            command=cmd,
            out_yaml_format=False,
            timeout=180,
        )
        if os.path.getsize(f"{self.hsbench_dir}/{result}") != 0:
            log.info("Workload was running...")
        else:
            raise UnexpectedBehaviour(
                f"Output file {result} is empty, "
                "Hsbench workload doesn't run as expected..."
            )

    def validate_s3_objects(self, upgrade=None):
        """
        Validate S3 objects using 'radosgw-admin' on single bucket
        Validate objects in buckets after completed upgrade

        Args:
            upgrade (str): Upgrade status
        Raises:
            UnexpectedBehaviour: If objects pre-upgrade and post-upgrade are not identical.

        """
        for i in range(self.num_bucket):
            bucket_name = self._get_bucket_name(i)
            num_objects = self.toolbox.exec_sh_cmd_on_pod(
                f"radosgw-admin bucket stats --bucket={bucket_name} | grep num_objects"
            )
            if num_objects is not self.num_obj:
                log.info(
                    f"Number object is created by hsbench in {bucket_name} "
                    f"is {num_objects}"
                )
            assert num_objects, (
                "Number objects in bucket don't match."
                f"Expecting {self.num_obj} but getting {num_objects}"
            )
            # Save objects to a file for validation
            file_path = f"{self.hsbench_dir}/obj_{upgrade}_{bucket_name}"
            object_list = self.toolbox.exec_sh_cmd_on_pod(
                f"radosgw-admin bi list --bucket={bucket_name}"
            )
            f = open(file_path, "w")
            f.write(object_list)

        # Validate objects in buckets for post upgrade
        if upgrade == "post_upgrade" and bucket_name != "new000000000000":
            for i in range(self.num_bucket):
                if os.path.exists(f"{self.hsbench_dir}/obj_{upgrade}_{bucket_name}"):
                    log.info(
                        f"Verifying objects in bucket {bucket_name} for post-upgrade..."
                    )
                    if filecmp.cmp(
                        f"{self.hsbench_dir}/obj_pre_upgrade_{bucket_name}",
                        f"{self.hsbench_dir}/obj_{upgrade}_{bucket_name}",
                    ):
                        log.info("Objects pre-upgrade and post-upgrade are identical.")
                    else:
                        raise UnexpectedBehaviour(
                            f"Objects in bucket {bucket_name} pre-upgrade and post-upgrade are not identical."
                        )
                else:
                    log.warning(f"No objects data to validate in bucket {bucket_name}")

    def validate_hsbench_put_get_list_objects(
        self, result=None, num_objs=None, put=None, get=None, list_obj=None
    ):
        """
        Validate PUT, GET, LIST objects from previous hsbench operation

        Args:
            result (str): Result file name
            num_objs (str): Number of objects to validate
            put (Boolean): Validate PUT operation
            get (Boolean): Validate GET operation
            list_obj (Boolean): Validate LIST operation

        """
        eval_data = {}
        with open(f"{self.hsbench_dir}/{result}", "r") as file, open(
            f"{self.hsbench_dir}/summary.csv", "w"
        ) as out:
            writer = csv.writer(out)
            for row in csv.reader(file):
                if row[1] == "TOTAL":
                    writer.writerow(row)

        with open(f"{self.hsbench_dir}/summary.csv", "r") as read_obj:
            reader = csv.reader(read_obj)
            for row in reader:
                eval_data[row[3]] = row[4]

        if put is True:
            put_val = eval_data["PUT"]
            log.info(f"Number of PUT objects is {put_val}")
            if put_val != num_objs:
                assert put_val, (
                    "Number of PUT objects don't match with number objects."
                    f"Expecting {num_objs} but getting {put_val}"
                )
        if get is True:
            get_val = eval_data["GET"]
            log.info(f"Number of GET objects is {get_val}")
            if get_val != num_objs:
                assert get_val, (
                    "Number of GET objects don't match with number objects."
                    f"Expecting {num_objs} but getting {get_val}"
                )
        if list_obj is True:
            list_val = eval_data["LIST"]
            log.info(f"Number of LIST objects is {list_val}")
            if list_val != 0:
                assert list_val, (
                    "LIST objects doesn't show correctly."
                    f"Expecting LIST objects is not zero but getting {list_val}"
                )

    def delete_objects_in_bucket(self, bucket_name=None):
        """
        Delete objects in a bucket

        Args:
            bucket_name (str): Name of bucket

        """
        log.info("Deleting objects in bucket...")
        self.toolbox.exec_sh_cmd_on_pod(
            f"radosgw-admin object rm --object=000000000000 --bucket={bucket_name}"
        )
        num_object = self.toolbox.exec_sh_cmd_on_pod(
            f"radosgw-admin bucket stats --bucket={bucket_name} | grep num_objects"
        )
        log.info(f"Number objects in {bucket_name} is {num_object}")
        if num_object == (self.num_obj - 1):
            log.info(f"Number objects in {bucket_name} is {num_object}")
        assert num_object, (
            f"Number object in bucket {bucket_name} is {num_object}"
            f"Expecting {self.num_obj - 1} in {bucket_name}"
        )

    def delete_bucket(self, bucket_name=None):
        """
        Delete bucket

        Args:
            bucket_name (str): Name of bucket

        """
        log.info("Deleting bucket with objects...")
        self.toolbox.exec_sh_cmd_on_pod(
            f"radosgw-admin bucket rm --bucket={bucket_name} --purge-objects"
        )
        num_bucket = self.toolbox.exec_sh_cmd_on_pod(
            f"radosgw-admin bucket list | grep {self.bucket_prefix} | wc -l"
        )
        log.info(f"Number buckets are {num_bucket}")
        log.info(f"Expecting buckets are {self.num_bucket - 1}")

        if num_bucket == (self.num_bucket - 1):
            log.info(f"Number buckets remaining {num_bucket}")
        assert num_bucket, (
            f"Number buckets are {num_bucket}."
            f"Expecting {self.num_bucket - 1} buckets remaining"
        )

    def validate_reshard_process(self):
        """
        Validate reshard process

        Raises:
            CommandFailed: If reshard process fails

        """
        bucket_name = self._get_bucket_name(bucket_num=0)
        log.info("Starting checking bucket limit and start reshard process")
        try:
            self.toolbox.exec_cmd_on_pod(
                f"radosgw-admin bucket limit check --uid={self.uid}"
            )
            self.toolbox.exec_cmd_on_pod("radosgw-admin reshard list")
            self.toolbox.exec_cmd_on_pod("radosgw-admin reshard process")
            self.toolbox.exec_cmd_on_pod(
                f"radosgw-admin reshard status --bucket={bucket_name}"
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

    def cleanup(self, timeout=600):
        """
        Clear all objects in the associated bucket
        Clean up deployment config, pvc, pod and test user

        """
        log.info("Deleting pods and deployment config")
        pod.delete_deploymentconfig_pods(self.pod_obj)
        self.pvc_obj.delete()
        time.sleep(timeout)
