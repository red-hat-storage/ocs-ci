import logging
import json
import time

from ocs_ci.framework import config
from ocs_ci.helpers.helpers import craft_s3_command
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.bucket_utils import list_objects_from_bucket
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.utility.utils import TimeoutSampler


logger = logging.getLogger(__name__)


INTERM_LOGS_PATH = "/var/logs/bucket-logs"
LOG_CONFG_YAML_PATH = "/spec"


class BucketLoggingManager:
    """
    This class facilitates MCG bucket logs management
    """

    cur_logs_pvc = constants.DEFAULT_MCG_BUCKET_LOGS_PVC

    def __init__(self, mcg_obj=None, awscli_pod=None):
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
            ocs_ci.ocs.ocp.OCP: OCP instance of the NooBaa configuration resource
        """
        return ocp.OCP(
            kind="noobaa",
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name="noobaa",
        )

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
                "path": f"{LOG_CONFG_YAML_PATH}/bucketLogging",
                "value": bucket_logging_dict,
            }
        ]

        # Try patching via add, and if it fails - replace instead
        try:
            self.nb_config_resource.patch(
                params=json.dumps(patch_params),
                format_type="json",
            )
        except CommandFailed as e:
            if "already exists" in str(e).lower():
                patch_params[0]["op"] = "replace"
                self.nb_config_resource.patch(
                    params=json.dumps(patch_params),
                    format_type="json",
                )
            else:
                logger.error(f"Failed to enable guaranteed bucket logs: {e}")
                raise e

        self.cur_logs_pvc = (
            logs_pvc if logs_pvc else constants.DEFAULT_MCG_BUCKET_LOGS_PVC
        )
        logger.info("Guaranteed bucket logs have been enabled")

    def get_logging_config_from_cr(self):
        """
        Return the NooBaa bucket logging configuration

        Returns:
            dict: Bucket logging configuration
        """
        # Traverse the YAML path to get the bucket logging configuration
        yaml_path_keys = [k for k in LOG_CONFG_YAML_PATH.split("/") if k]
        d = self.nb_config_resource.get()
        for k in yaml_path_keys:
            d = d.get(k)

        return d.get("bucketLogging", {})

    def disable_bucket_logging_on_cr(self):
        """
        Unset the guaranteed bucket logs feature
        """
        logger.info("Disabling guaranteed bucket logs")

        try:
            patch_params = [
                {
                    "op": "replace",
                    "path": f"{LOG_CONFG_YAML_PATH}/bucketLogging",
                    "value": None,
                },
            ]
            self.nb_config_resource.patch(
                params=json.dumps(patch_params),
                format_type="json",
            )

        except CommandFailed as e:
            if "not found" in str(e):
                logger.info("The bucketLogging field was not found")
            else:
                logger.error(f"Failed to disable guaranteed bucket logs: {e}")
                raise e

        logger.info("Guaranteed bucket logs have been disabled")

    def put_bucket_logging(self, bucket_name, logs_bucket_name, prefix="", verify=True):
        """
        Set the logs bucket on the source bucket using the AWS CLI

        Args:
            bucket_name(str): Name of the source bucket
            logs_bucket_name(str): Name of the logs bucket
            prefix (str): Prefix for the logs
            verify (bool): Whether to verify and wait for the config to propogate
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

        if verify:
            try:
                for logging_config in TimeoutSampler(
                    timeout=60,
                    sleep=10,
                    func=self.get_bucket_logging,
                    bucket_name=bucket_name,
                ):
                    if (
                        logging_config["LoggingEnabled"]["TargetBucket"]
                        == logs_bucket_name
                    ):
                        logger.info(
                            f"Confirmed logging config on {bucket_name} via get-bucket-logging"
                        )
                        break
            except TimeoutError:
                logger.error(
                    f"Failed to set guaranteed bucket logging on {bucket_name}"
                )
                raise
            logger.info(
                "Waiting an additional 60 seconds for put-bucket-logging to propogate"
            )
            time.sleep(60)

        logger.info(
            f"The logs bucket {logs_bucket_name} has been set on the source bucket {bucket_name}"
        )

    def get_bucket_logging(self, bucket_name):
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

    def remove_bucket_logging(self, bucket_name):
        """
        Remove the logging configuration from a bucket

        Args:
            bucket_name(str): Name of the bucket
        """
        logger.info(f"Removing the logging configuration from the bucket {bucket_name}")

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

    def _get_nb_pods_logs_pvc_mount_status(self):
        """
        Check if the noobaa-core and noobaa-endpoint pods
        are mounting the expected logging PVC, and get the respective
        answers for each pod.

        Returns:
            dict: A dictionary containing the pod names as keys and
                  the respective answers as values.

        """
        log_vol_name = "noobaa-bucket-logging-volume"
        expected_pvc = self.cur_logs_pvc
        expected_mount_path = "/var/logs/bucket-logs"
        pod_dicts = []

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
        answers_dict = {}
        for pod_dict in pod_dicts:
            pod_name = pod_dict["metadata"]["name"]

            # Check the pvc
            pvc_name_check = True
            volumes = pod_dict["spec"]["volumes"]
            try:
                log_vol = next(v for v in volumes if v["name"] == log_vol_name)
                nb_core_pvc = log_vol["persistentVolumeClaim"]["claimName"]
                pvc_name_check = nb_core_pvc == expected_pvc
            except StopIteration:
                pvc_name_check = False

            # Check the logging mount path
            mount_path_check = True
            vol_mnts = pod_dict["spec"]["containers"][0]["volumeMounts"]
            try:
                log_mnt = next(vm for vm in vol_mnts if vm["name"] == log_vol_name)
                mount_path_check = log_mnt["mountPath"] == expected_mount_path
            except StopIteration:
                mount_path_check = False

            answers_dict[pod_name] = pvc_name_check and mount_path_check

        return answers_dict

    def wait_for_logs_pvc_mount_status(self, mount_status_expected=True, timeout=300):
        """
        Wait for the noobaa-core and noobaa-endpoint pods to mount or unmount the bucket logs PVC.

        Args:
            mount_status_expected (bool): If True, wait for the pods to mount the PVC,
                                             otherwise wait for them to unmount it.
            timeout (int): The maximum time to wait for the pods to change the logs PVC status.

        Returns:
            bool: True if all the pods have mounted/unmounted the PVC, False otherwise.
        """
        logger.info("Waiting for the noobaa pods to mount/unmount the logs PVC")

        retry_msg = (
            "One of the noobaa pods still doesn't mount the logs PVC"
            if mount_status_expected
            else "One of the noobaa pods still mounts the logs PVC"
        )
        timeout_msg = (
            "One of the noobaa pods failed to mount the logs PVC in time"
            if mount_status_expected
            else "One of the noobaa pods failed to unmount the logs PVC in time"
        )
        success_msg = (
            "All noobaa pods have mounted the logs PVC"
            if mount_status_expected
            else "All noobaa pods have unmounted the logs PVC"
        )

        last_status = None
        try:
            for pods_to_mount_status_dict in TimeoutSampler(
                timeout=timeout, sleep=15, func=self._get_nb_pods_logs_pvc_mount_status
            ):
                if mount_status_expected and all(pods_to_mount_status_dict.values()):
                    break
                elif not mount_status_expected and not any(
                    pods_to_mount_status_dict.values()
                ):
                    break
                else:
                    last_status = pods_to_mount_status_dict
                    logger.warning(f"{retry_msg}: {pods_to_mount_status_dict}")
        except TimeoutError:
            logger.warning(f"{timeout_msg}: {last_status}")
            return False
        logger.info(success_msg)
        return True

    def get_interm_logs(self, source_bucket=None, logs_bucket=None):
        """
        Get the logs from the logging PVC via the noobaa-core pod

        Args:
            source_bucket(str|optional): Filter logs by source bucket
            logs_bucket(str|optional): Filter logs by logs bucket

        Returns:
            list: A list of dicts, deserialized from the JSON logs
        """
        logs = []

        log_files = self.mcg_obj.core_pod.exec_cmd_on_pod(
            command=f"ls {INTERM_LOGS_PATH}", out_yaml_format=False
        )
        for log_file in log_files.strip().split("\n"):
            if log_file:
                log_file_str = self.mcg_obj.core_pod.exec_cmd_on_pod(
                    command=f"cat {INTERM_LOGS_PATH}/{log_file}",
                    out_yaml_format=False,
                )
                logs.extend(self._parse_log_file_str(log_file_str))

        if source_bucket:
            logs = [log for log in logs if log["source_bucket"] == source_bucket]
        if logs_bucket:
            logs = [log for log in logs if log["log_bucket"] == logs_bucket]
        return logs

    def await_interm_logs_transfer(self, logs_bucket, timeout=600, sleep=10):
        """
        Wait for intermediate logs to be moved from the logging PVC
        to their final destination in a specified logs bucket

        Args:
            logs_bucket(str): Name of the logs bucket
            timeout(int): The maximum time to wait for the logs to be moved
            sleep(int): Time to sleep between each check

        Raises:
            TimeoutError: If the logs were not transferred in time
        """
        logger.info("Waiting for the intermediate logs to move to the logs bucket")
        inter_logs_found = False
        try:
            for sample_logs in TimeoutSampler(
                timeout=timeout,
                sleep=sleep,
                func=self.get_interm_logs,
                logs_bucket=logs_bucket,
            ):
                if not sample_logs and not inter_logs_found:
                    logger.info("No intermediate logs were found yet")
                elif sample_logs and not inter_logs_found:
                    inter_logs_found = True
                    logger.info(
                        "Some intermediate logs were found, waiting for them to be moved"
                    )
                elif sample_logs and inter_logs_found:
                    logger.info(
                        "Still waiting for the intermediate logs to be moved to the logs bucket"
                    )
                elif not sample_logs and inter_logs_found:
                    logger.info("Intermediate logs have been moved to the logs bucket")
                    break
        except TimeoutError:
            if not inter_logs_found:
                logger.error("Intermediate logs were not found in the logging PVC")
            else:
                logger.error(
                    (
                        "The intermediate logs were not transferred to"
                        f" the logs bucket {logs_bucket} in time"
                    )
                )
            raise

    def get_bucket_logs(self, logs_bucket, source_bucket=None):
        """
        Get the logs from a logs bucket

        Args:
            logs_bucket(str): Name of the logs bucket
            source_bucket(str|optional): Filter logs by source bucket

        Returns:
            list: A list of dicts, deserialized from the JSON logs
        """
        logs = []
        log_objs = list_objects_from_bucket(
            pod_obj=self.awscli_pod,
            target=logs_bucket,
            s3_obj=self.mcg_obj,
        )
        for log_obj in log_objs:
            log_file_str = self.awscli_pod.exec_cmd_on_pod(
                craft_s3_command(
                    f"cp s3://{logs_bucket}/{log_obj} -",
                    mcg_obj=self.mcg_obj,
                ),
                out_yaml_format=False,
            )
            logs.extend(self._parse_log_file_str(log_file_str))

        if source_bucket:
            logs = [log for log in logs if log["source_bucket"] == source_bucket]
        return logs

    def verify_logs_integrity(self, logs, expected_ops, check_intent=False):
        """
        Check whether all the expected operations are present in the logs,
        including intent logs if specified.

        Note that this implementation assumes that each operation was only
        made once.

        Args:
            logs (list): A list of dicts, deserialized from the JSON logs
            expected_ops (list): A list of tuples representing operations.
                                I.E [('PUT', 'object1'), ('GET', 'object2')]
            check_intent (bool): Whether to check for intent logs

        Returns:
            bool: True if all the expected operations are present, False otherwise
        """
        # Convert the input into sets of strings with
        # unified operation-object-success_code format
        expected_ops_set = set()
        for op, obj in expected_ops:
            success_code = "200" if op != "DELETE" else "204"
            expected_ops_set.add(f"{op}-{obj}-{success_code}")

        # The http_status field is 102 for intent logs
        if check_intent:
            for op, obj in expected_ops:
                expected_ops_set.add(f"{op}-{obj}-102")

        # Parse the logs into a set of strings with the same format
        logs_set = {
            f"{log['op']}-{log['object_key']}-{log['http_status']}" for log in logs
        }

        return expected_ops_set.issubset(logs_set)

    def _parse_log_file_str(self, log_file_str):
        """
        Process a string containing JSON logs

        Args:
            log_file_str(str): A string containing JSON logs

        Returns:
            list: A list of dicts, deserialized from the JSON logs
        """
        log_dicts = []
        for line in log_file_str.split("\n"):
            if line:
                try:
                    log_dicts.append(json.loads(line))
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to parse log line: {line}, error: {e}")
        return log_dicts
