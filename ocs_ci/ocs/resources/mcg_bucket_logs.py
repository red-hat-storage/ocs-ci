import json
import logging
from dataclasses import dataclass
from datetime import datetime

from ocs_ci.helpers.helpers import craft_s3_command
from ocs_ci.ocs.bucket_utils import list_objects_from_bucket

logger = logging.getLogger(__name__)


@dataclass
class MCGBucketLog:
    timestamp: datetime
    operation: str
    source_bucket: str
    object_key: str

    @classmethod
    def from_raw_log(cls, raw_log):
        """
        Parse a raw log line into a McgBucketLog object

        Args:
            raw_log (str): The raw log line

        Returns:
            McgBucketLog: An instance of McgBucketLog

        """
        # Split the raw log into timestamp part and JSON part
        timestamp_part, json_part = raw_log.split("{", 1)

        # Parse timestamp part into a datetime object
        # The timestamp part is in the format: 'Dec 11 14:00:00'
        timestamp_str = " ".join(timestamp_part.split()[:3])
        timestamp = datetime.strptime(timestamp_str, "%b %d %H:%M:%S")

        # Load JSON part to extract relevant information
        json_data = json.loads("{" + json_part)
        operation = json_data.get("op")
        source_bucket = json_data.get("source_bucket")
        object_key = json_data.get("object_key")

        # Return a new instance of McgBucketLog with parsed information
        return cls(timestamp, operation, source_bucket, object_key)


class MCGBucketLoggingHandler:
    def __init__(self, mcg_obj, awscli_pod):
        """
        Args:
            mcg_obj (MCG): An MCG object to work with

        """
        self.mcg_obj = mcg_obj
        self.awscli_pod = awscli_pod

    def put_bucket_logging(self, bucket, logging_bucket, prefix=""):
        """
        Configure bucket logging on target_bucket to log to logging_bucket

        Args:
            bucket (str): The name of the bucket to log
            logging_bucket (str): The name of the bucket to log to

        Returns:
            json: The response from the mcg-cli command

        """
        req_params = {
            "name": f"{bucket}",
            "log_bucket": f'"{logging_bucket}"',
            "log_preifx": f'"{prefix}"',
        }
        return self.mcg_obj.exec_mcg_cmd(
            "bucket_api", "put_bucket_logging", req_params
        ).json()

    def get_bucket_logging(self, bucket):
        """
        Get the bucket logging configuration for a bucket

        Args:
            bucket (str): The name of the bucket to get the logging configuration for

        """
        req_params = {"name": f"{bucket}"}
        return self.mcg_obj.exec_mcg_cmd(
            "bucket_api", "get_bucket_logging", req_params
        ).json()

    def delete_bucket_logging(self, bucket):
        """
        Delete the bucket logging configuration for a bucket

        Args:
            bucket (str): The name of the bucket to delete the logging configuration for

        """
        req_params = {"name": f"{bucket}"}
        return self.mcg_obj.exec_mcg_cmd(
            "bucket_api", "delete_bucket_logging", req_params
        ).json()

    def get_bucket_logs(self, logs_bucket):
        """
        Parse logs from the specified S3 bucket and return a dictionary of operations to object keys.

        Args:
            logs_bucket (str): The name of the S3 bucket containing log files.

        Returns:
            logs (list): A list of McgBucketLog objects

        """
        log_objs = []
        log_files = list_objects_from_bucket(
            self.awscli_pod, f"s3://{logs_bucket}", s3_obj=self.mcg_obj
        )
        for log_file in log_files:
            log_file_content = self.awscli_pod.exec_cmd_on_pod(
                craft_s3_command(f"cat s3://{logs_bucket}/{log_file}", self.mcg_obj),
                out_yaml_format=False,
            )
            for raw_log in log_file_content.split("\n"):
                log_objs.append(MCGBucketLog.from_raw_log(raw_log))

        return log_objs
