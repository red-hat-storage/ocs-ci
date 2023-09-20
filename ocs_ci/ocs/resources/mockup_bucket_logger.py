from datetime import datetime
import logging
import uuid
from ocs_ci.helpers.helpers import setup_pod_directories

from ocs_ci.ocs import constants
from ocs_ci.ocs.bucket_utils import (
    craft_s3_command,
    list_objects_from_bucket,
    sync_object_directory,
)

logger = logging.getLogger(__name__)


class MockupBucketLogger:
    """
    This class facilitates S3 operations on an MCG bucket while writing
    corresponding mockup logs directly to a ULS bucket via a Namespacestore
    MCG bucket.

    The use of a Namespacestore MCG bucket for the logs enables storage provider
    agnosticity.

    """

    def __init__(self, awscli_pod, mcg_obj, bucket_factory, platform, region):
        """
        Args:
            awscli_pod(Pod): A pod running the AWS CLI
            mcg_obj(MCG): An MCG object
            bucket_factory: A bucket factory fixture
            platform(str): The platform of the uls bucket
            region(str): The region of the uls bucket

        """

        self.awscli_pod = awscli_pod
        self.mcg_obj = mcg_obj
        self.log_files_dir = setup_pod_directories(awscli_pod, ["bucket_logs_dir"])[0]

        logger.info("Creating the AWS logs bucket Namespacestore")

        bucketclass_dict = {
            "interface": "OC",
            "namespace_policy_dict": {
                "type": "Single",
                "namespacestore_dict": {platform: [(1, region)]},
            },
        }
        logs_bucket = bucket_factory(bucketclass=bucketclass_dict)[0]
        self.logs_bucket_mcg_name = logs_bucket.name
        self.logs_bucket_uls_name = logs_bucket.bucketclass.namespacestores[0].uls_name

        self._standard_test_obj_list = self.awscli_pod.exec_cmd_on_pod(
            f"ls -A1 {constants.AWSCLI_TEST_OBJ_DIR}"
        ).split(" ")

    @property
    def standard_test_obj_list(self):
        return self._standard_test_obj_list

    def upload_test_objs_and_log(self, bucket_name):
        """
        Uploads files from files_dir to the MCG bucket and write matching
        mockup logs to the logs bucket

        Args:
            files_dir(str): Full path to a directory on awscli_pod

        """

        logger.info(f"Uploading test objects to {bucket_name}")

        sync_object_directory(
            self.awscli_pod,
            constants.AWSCLI_TEST_OBJ_DIR,
            f"s3://{bucket_name}",
            self.mcg_obj,
        )

        self._upload_mockup_logs(
            bucket_name=bucket_name, obj_list=self._standard_test_obj_list, op="PUT"
        )

    def upload_arbitrary_object_and_log(self, bucket_name):
        """
        Uploads an arbitrary object to the MCG bucket and upload a matching mockup log

        """

        logger.info(f"Uploading an arbitrary object to {bucket_name}")

        obj_name = self._standard_test_obj_list[0]
        cmd = f"cp {constants.AWSCLI_TEST_OBJ_DIR}{obj_name} s3://{bucket_name}/{obj_name}"

        self.awscli_pod.exec_cmd_on_pod(
            craft_s3_command(cmd, mcg_obj=self.mcg_obj),
            out_yaml_format=False,
        )

        self._upload_mockup_logs(bucket_name, [obj_name], "PUT")

    def delete_objs_and_log(self, bucket_name, objs):
        """
        Delete list of objects from the MCG bucket and write
        matching mockup logs

        Args:
            bucket_name(str): Name of the MCG bucket
            objs(list): List of the objects to delete

        """
        logger.info(f"Deleting the {objs} from the bucket")
        obj_list = list_objects_from_bucket(
            self.awscli_pod,
            f"s3://{bucket_name}",
            s3_obj=self.mcg_obj,
        )
        if set(objs).issubset(set(obj_list)):
            for i in range(len(objs)):
                s3cmd = craft_s3_command(
                    f"rm s3://{bucket_name}/{objs[i]}", self.mcg_obj
                )
                self.awscli_pod.exec_cmd_on_pod(s3cmd)
            self._upload_mockup_logs(bucket_name, objs, "DELETE")

    def delete_all_objects_and_log(self, bucket_name):
        """
        Deletes all objects from the MCG bucket and write matching mockup logs

        """

        logger.info(f"Deleting all objects from {bucket_name}")

        obj_list = list_objects_from_bucket(
            self.awscli_pod,
            f"s3://{bucket_name}",
            s3_obj=self.mcg_obj,
        )

        s3cmd = craft_s3_command(f"rm s3://{bucket_name} --recursive", self.mcg_obj)
        self.awscli_pod.exec_cmd_on_pod(s3cmd)

        self._upload_mockup_logs(bucket_name, obj_list, "DELETE")

    def _upload_mockup_logs(self, bucket_name, obj_list, op):
        """
        Uploads a mockup log for each object in obj_list to the logs bucket based on the given operation

        Args:
            bucket_name(str): Name of the MCG bucket
            obj_list(list): List of object keys
            op(str): The operation to log. i.e "PUT", "DELETE", "GET"

        """

        logger.info(f"Logging {op} operations for {len(obj_list)} objects")

        # Build one command that creates all the log files on the awscli_pod
        command = "bash -c " + '"'
        for obj_name in obj_list:
            s3mockuplog = S3MockupLog(bucket_name, obj_name, op)
            command += (
                "echo "
                + f"'{s3mockuplog}'"
                + f"  > {self.log_files_dir}/{s3mockuplog.file_name};"
            )
        command += '"'
        self.awscli_pod.exec_cmd_on_pod(command)

        sync_object_directory(
            self.awscli_pod,
            f"{self.log_files_dir}",
            f"s3://{self.logs_bucket_mcg_name}",
            self.mcg_obj,
        )

        self.awscli_pod.exec_cmd_on_pod(f"rm -rf {self.log_files_dir}/*")


class S3MockupLog:
    """
    This class represents a mockup log file in the AWS log file format

    """

    OP_CODES = {
        "PUT": 200,
        "DELETE": 204,
        "GET": 206,
    }

    def __init__(self, bucket_name, object_key, operation):
        self._bucket_name = bucket_name
        self._object_key = object_key
        self._operation = operation = str.upper(operation)
        self._op_code = S3MockupLog.OP_CODES[operation]
        self._creation_time = datetime.utcnow()
        self._file_name = self._generate_unique_log_file_name()
        self.format = self._read_log_format()

    def _read_log_format(self):
        """
        Reads the log format from the template file.

        The template file is derived from an AWS bucket log
        that was automatically generated by AWS during
        bucket operations.

        Returns:
            str: The log format obtained from the AWS-generated
                log file.

        """

        with open(constants.AWS_BUCKET_LOG_TEMPLATE, "r") as f:
            return f.read()

    def _generate_unique_log_file_name(self):
        """
        Generates a name for an AWS log file

        Returns:
            str: A unique name in the AWS bucket log file name format

        """
        time = self._creation_time.strftime("%Y-%m-%d-%H-%M-%S")
        unique_id = str(uuid.uuid4().hex)[:16].upper()
        return time + unique_id

    @property
    def bucket_name(self):
        return self._bucket_name

    @property
    def object_key(self):
        return self._object_key

    @property
    def operation(self):
        return self._operation

    @property
    def file_name(self):
        return self._file_name

    @property
    def time(self):
        """
        Get the time in the AWS logs format.
        i.e "[06/Feb/2019:00:00:38 +0000]"

        Returns:
            str: The current time in the AWS bucket log format

        """
        format_pattern = "[%d/%b/%Y:%H:%M:%S +0000]"
        return self._creation_time.strftime(format_pattern)

    def __str__(self):
        raw_log = self.format.format(
            bucket=self.bucket_name,
            time=self.time,
            object_key=self.object_key,
            op=self.operation,
            op_code=self._op_code,
        )

        # Adjust for python parsing
        adjusted_log = raw_log.replace('"', '\\"')
        return adjusted_log
