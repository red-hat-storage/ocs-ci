import logging

from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.helpers.helpers import create_resource
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.resources.pod import wait_for_pods_by_label_count
from ocs_ci.ocs.ui.workload_ui import wait_for_container_status_ready
from ocs_ci.utility import templating

logger = logging.getLogger(__name__)


class Pushgateway:
    """
    A class to manage Pushgateway installation and configuration.

    Pushgateway allows pushing custom metrics to Prometheus.
    """

    def __init__(self, namespace):
        """
        Initialize Pushgateway instance.

        Args:
            namespace (str): Namespace where Pushgateway will be deployed
        """
        self.namespace = namespace
        self.ocp_obj = OCP(namespace=self.namespace)
        self._exec_pod = None

    def install(self):
        """
        Install Pushgateway from the template.

        Raises:
            AssertionError: If Pushgateway installation fails.
        """
        logger.info(f"Installing Pushgateway in namespace {self.namespace}")

        try:
            # Load and create pushgateway resources from template
            pushgateway_data = list(
                templating.load_yaml(constants.PUSHGATEWAY_YAML, multi_document=True)
            )

            for resource in pushgateway_data:
                if "metadata" in resource:
                    resource["metadata"]["namespace"] = self.namespace

            for resource in pushgateway_data:
                create_resource(**resource)

            # Wait for pushgateway pod to be ready
            wait_for_pods_by_label_count(
                label=constants.PUSHGATEWAY_APP_LABEL,
                expected_count=1,
                namespace=self.namespace,
            )
            pod_obj = pod.Pod(
                **pod.get_pods_having_label(
                    label=constants.PUSHGATEWAY_APP_LABEL, namespace=self.namespace
                )[0]
            )
            wait_for_container_status_ready(pod=pod_obj)

            # Resolve one Prometheus pod (has curl) for exec when sending metrics
            prometheus_pods = pod.get_pods_having_label(
                label=constants.PROMETHEUS_POD_LABEL,
                namespace=constants.OPENSHIFT_MONITORING_NAMESPACE,
            )
            if not prometheus_pods:
                raise Exception(
                    f"No Prometheus pod found in {constants.OPENSHIFT_MONITORING_NAMESPACE}"
                )
            self._exec_pod = pod.Pod(**prometheus_pods[0])

            logger.info(
                f"Pushgateway installed successfully in namespace {self.namespace}"
            )

        except Exception as e:
            logger.error(f"Failed to install Pushgateway: {e}")
            raise

    def send_custom_metric(self, metric_name, metric_value, job_name="test_job"):
        """
        Send a custom metric to Pushgateway via exec from a Prometheus pod (curl to Service).

        Args:
            metric_name (str): Name of the metric
            metric_value (str): Value of the metric
            job_name (str): Job name for the metric (default: "test_job")

        Raises:
            Exception: If sending metric fails
        """
        if not self._exec_pod:
            raise Exception(
                "Pushgateway install not complete (no exec pod). Ensure install() was called."
            )

        url = (
            f"http://pushgateway.{self.namespace}.svc.cluster.local:9091"
            f"/metrics/job/{job_name}"
        )
        payload = f"{metric_name} {metric_value}\n"
        # Escape single quotes for use inside single-quoted shell string
        escaped_payload = payload.replace("'", "'\"'\"'")
        curl_cmd = (
            f"curl -s -S -X POST -H 'Content-Type: text/plain' "
            f"--data-binary '{escaped_payload}' '{url}'"
        )

        logger.info(
            f"Sending metric to Pushgateway via pod exec: {url} = {metric_value}"
        )

        try:
            self._exec_pod.exec_cmd_on_pod(
                command=curl_cmd, out_yaml_format=False, timeout=15
            )
            logger.info(
                f"Successfully sent metric {metric_name}={metric_value} to Pushgateway"
            )
        except CommandFailed as e:
            logger.error(f"Failed to send metric to Pushgateway: {e}")
            raise

    def cleanup(self):
        """
        Cleanup Pushgateway resources.
        """
        logger.info(f"Cleaning up Pushgateway in namespace {self.namespace}")

        try:
            # Delete resources by label
            self.ocp_obj.exec_oc_cmd(
                f"delete all,service,route,ServiceMonitor -l {constants.PUSHGATEWAY_APP_LABEL}",
                timeout=120,
            )
            logger.info("Pushgateway resources deleted")
        except CommandFailed as e:
            logger.warning(f"Failed to delete some Pushgateway resources: {e}")

        # Wait for resources to be deleted
        try:
            OCP(kind=constants.NAMESPACE).wait_for_delete(self.namespace, timeout=60)
        except Exception as e:
            logger.warning(f"Namespace deletion check failed: {e}")

        logger.info("Pushgateway cleanup completed")
