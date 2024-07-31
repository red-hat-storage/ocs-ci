import logging
import json

from ocs_ci.framework import config
from ocs_ci.helpers.helpers import craft_s3_command
from ocs_ci.ocs import constants, ocp


logger = logging.getLogger(__name__)


class BucketLoggingManager:
    """
    This class facilitates MCG bucket logs management
    """

    DEFAULT_BUCKET_LOGS_PVC = "noobaa-bucket-logging-pvc"
    cur_logs_pvc = DEFAULT_BUCKET_LOGS_PVC

    def __init__(self, mcg_obj, awscli_pod):
        """
        Args:
            mcg_obj(MCG): An MCG object containing required credentials
            awscli_pod(Pod): A pod for running AWS CLI commands
        """
        self.mcg_obj = mcg_obj
        self.awscli_pod = awscli_pod

    @property
    def nb_config_resource(self):
        """
        Return the NooBaa configuration resource
        Note that this might change in the future.

        Returns:
            OCP: OCP instance of the NooBaa configuration resource
        """
        return ocp.OCP(
            kind="noobaa",
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name="noobaa",
        )

    @property
    def log_conf_yaml_path(self):
        """
        Return the YAML path to the bucket logging configuration
        Note that this might change in the future.

        Returns:
            str: YAML path to the bucket logging configuration
        """
        return "/spec"

    def enable_bucket_logging_on_cr(self, logs_pvc=None):
        """
        Set the guaranteed bucket logs feature

        Args:
            logs_pvc(str|optional): Name of the bucket logs PVC
            Note:
                If not provided, a PVC will be automatically be created
                by MCG when first enabling the feature.
        """
        logger.info("Enabling guaranteed bucket logs")

        # Build a patch command to enable guaranteed bucket logs
        bucket_logging_dict = {"loggingType": "guaranteed"}

        # Add the bucketLoggingPVC field if provided
        if logs_pvc:
            bucket_logging_dict["bucketLoggingPVC"] = logs_pvc

        patch_params = [
            {
                "op": "add",
                "path": f"{self.log_conf_yaml_path}/bucketLogging",
                "value": bucket_logging_dict,
            }
        ]

        # Try patching via add, if it fails, try replacing
        try:
            self.nb_config_resource.patch(
                params=json.dumps(patch_params),
                format_type="json",
            )
        except Exception as e:
            if "already exists" in str(e):
                patch_params[0]["op"] = "replace"
                self.nb_config_resource.patch(
                    params=json.dumps(patch_params),
                    format_type="json",
                )
            else:
                logger.error(f"Failed to enable guaranteed bucket logs: {e}")
                raise e

        self.cur_logs_pvc = logs_pvc if logs_pvc else self.DEFAULT_BUCKET_LOGS_PVC
        logger.info("Guaranteed bucket logs have been enabled")

    def get_logging_config_from_cr(self):
        """
        Return the NooBaa bucket logging configuration

        Returns:
            dict: Bucket logging configuration
        """
        # Traverse the YAML path to get the bucket logging configuration
        yaml_path_keys = [k for k in self.log_conf_yaml_path.split("/") if k]
        d = self.nb_config_resource.get()
        for k in yaml_path_keys:
            d = d.get(k)

        return d.get("bucketLogging", {})

    def disable_bucket_logging_on_cr(self):
        """
        Unset the guaranteed bucket logs feature
        """

        # Remove the bucketLoggingPVC field if it exists
        logger.info("Disabling guaranteed bucket logs")

        try:
            patch_params = [
                {
                    "op": "replace",
                    "path": f"{self.log_conf_yaml_path}/bucketLogging",
                    "value": None,
                },
            ]
            self.nb_config_resource.patch(
                params=json.dumps(patch_params),
                format_type="json",
            )

        # TODO: Find the more specific exception
        except Exception as e:
            if "not found" in str(e):
                logger.info("The bucketLogging field was not found")
            else:
                logger.error(f"Failed to disable guaranteed bucket logs: {e}")
                raise e

        logger.info("Guaranteed bucket logs have been disabled")

    def check_if_nb_pods_mount_the_logs_pvc(self):
        """
        Check if the noobaa-core and noobaa-endpoint pods
        have the expected mounts to the logging PVC.

        Returns:
            bool: True if the pods have the expected mounts

        """
        log_vol_name = "noobaa-bucket-logging-volume"
        expected_pvc = self.cur_logs_pvc
        expected_mount_path = "/var/logs/bucket-logs"
        pod_dicts = []
        logger.info("Checking the NooBaa pods mounts to the logging PVC")

        # Get the noobaa-core pod YAML
        pod_dicts.extend(
            ocp.OCP(
                kind=constants.POD,
                namespace=config.ENV_DATA["cluster_namespace"],
                selector=constants.NOOBAA_CORE_POD_LABEL,
            ).get()["items"]
        )

        # Get the noobaa-endpoint pod YAMLS
        pod_dicts.extend(
            ocp.OCP(
                kind=constants.POD,
                namespace=config.ENV_DATA["cluster_namespace"],
                selector=constants.NOOBAA_ENDPOINT_POD_LABEL,
            ).get()["items"]
        )

        # Check the PVC and mount path for each pod
        for pod_dict in pod_dicts:
            pod_name = pod_dict["metadata"]["name"]

            # Check the pvc
            volumes = pod_dict["spec"]["volumes"]
            log_vol = next(v for v in volumes if v["name"] == log_vol_name)
            nb_core_pvc = log_vol["persistentVolumeClaim"]["claimName"]
            if nb_core_pvc != expected_pvc:
                logger.warn(
                    f"The {pod_name} pod does not have a mount to the {expected_pvc} PVC"
                )
                return False

            # Check the logging mount path
            vol_mnts = pod_dict["spec"]["containers"][0]["volumeMounts"]
            log_mnt = next(vm for vm in vol_mnts if vm["name"] == log_vol_name)
            if log_mnt["mountPath"] != expected_mount_path:
                logger.warn(
                    f"The {pod_name} pod does not have a mount to the {expected_mount_path} path"
                )
                return False

        logger.info("The NooBaa pods have the expected mounts to the logging PVC")
        return True

    def set_logging_config_on_bucket(self, bucket_name, logs_bucket_name, prefix=""):
        """
        Set the logs bucket on the source bucket using the AWS CLI

        Args:
            bucket_name(str): Name of the source bucket
            logs_bucket_name(str): Name of the logs bucket
            prefix (str): Prefix for the logs
        """
        logger.info(
            f"Setting the logs bucket {logs_bucket_name} on the source bucket {bucket_name}"
        )

        logging_status = {
            "LoggingEnabled": {
                "TargetBucket": logs_bucket_name,
                "TargetPrefix": prefix,
            }
        }
        cmd = f"put-bucket-logging --bucket {bucket_name} "
        cmd += f"--bucket-logging-status '{json.dumps(logging_status)}'"
        cmd = cmd.replace('"', '\\"')

        self.awscli_pod.exec_cmd_on_pod(
            craft_s3_command(cmd, mcg_obj=self.mcg_obj, api=True),
            out_yaml_format=False,
        )

        logger.info(
            f"The logs bucket {logs_bucket_name} has been set on the source bucket {bucket_name}"
        )

    def get_logging_config_from_bucket(self, bucket_name):
        """
        Get the logging configuration for a given bucket

        Args:
            bucket_name(str): Name of the bucket

        Returns:
            dict: Logging configuration for the bucket
        """
        cmd = f"get-bucket-logging --bucket {bucket_name}"
        json_str = self.awscli_pod.exec_cmd_on_pod(
            craft_s3_command(cmd, mcg_obj=self.mcg_obj, api=True),
            out_yaml_format=False,
        )
        return json.loads(json_str) if json_str else {}

    def remove_logging_config_from_bucket(self, bucket_name):
        """
        Remove the logging configuration from a bucket

        Args:
            bucket_name(str): Name of the bucket
        """
        logger.info(f"Removing the logging configuration from the bucket {bucket_name}")

        # TODO: revalidate
        cmd = f"put-bucket-logging --bucket {bucket_name} "
        cmd += "--bucket-logging-status '{}'"
        cmd = cmd.replace('"', '\\"')

        self.awscli_pod.exec_cmd_on_pod(
            craft_s3_command(cmd, mcg_obj=self.mcg_obj, api=True),
            out_yaml_format=False,
        )

        logger.info(
            f"The logging configuration has been removed from the bucket {bucket_name}"
        )
