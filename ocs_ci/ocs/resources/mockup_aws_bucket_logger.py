from datetime import datetime
import logging

from ocs_ci.ocs import constants

logger = logging.getLogger(__name__)


class MockupAwsBucketLogger:
    """
    Peforms S3 operations on an MCG bucket and write mathcing mockup logs
    directly to a bucket on AWS in the same region.
    """

    def __init__(self, awscli_pod, cloud_uls_factory, region) -> None:
        """
        Args:
            awscli_pod(Pod): A pod running the AWS CLI
            cloud_uls_factory: TODO
        """

        self.awscli_pod = awscli_pod

        logger.info("Creating the AWS logs bucket")
        self.logs_bucket_name = self._create_logs_bucket(cloud_uls_factory, region)

    def _create_logs_bucket(self, cloud_uls_factory, region) -> str:
        uls_dict = cloud_uls_factory({"aws": [(1, region)]})
        aws_buckets_set = uls_dict["aws"]
        bucket_name = next(iter(aws_buckets_set))
        return bucket_name

    def upload_from_dir_and_log(files_dir):
        """
        Uploads files from files_dir to the MCG bucket and write matching
        mockup logs to the logs bucket.

        Args:
            files_dir(str): Full path to a directory on awscli_pod
        """
        pass

    def delete_file_and_log(target_file):
        """
        Delete an object from the MCG bucket and write a matching mockup
        log to the logs bucket.
        """
        pass


class S3MockupLog:
    OP_CODES = {
        "PUT": 200,
        "DELETE": 204,
        "GET": 206,
    }

    def __init__(self, aws_bucket_name, object_key, operation):
        self.aws_bucket_name = aws_bucket_name
        self.object_key = object_key

        self.operation = operation = str.upper(operation)
        self.op_code = S3MockupLog.OP_CODES[operation]

        with open(constants.AWS_BUCKET_LOG_TEMPLATE, "r") as f:
            self.format = f.read()

    @property
    def time(self):
        """
        Get the current time as a string in the AWS logs format.
        i.e "[06/Feb/2019:00:00:38 +0000]"
        """
        format_pattern = "[%d/%b/%Y:%H:%M:%S +0000]"
        return datetime.utcnow().strftime(format_pattern)

    def __str__(self):
        return self.format.format(
            bucket=self.aws_bucket_name,
            time=self.time,
            object_key=self.object_key,
            op=self.operation,
            op_code=self.op_code,
        )


print(S3MockupLog("shirshfe-source", "cat.jpeg", "delete"))
