import logging
import requests

from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.helpers.helpers import create_resource
from ocs_ci.ocs.resources.pod import wait_for_pods_by_label_count
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
        self.pod = None
        self.service_url = None

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

            # Wait for pushgateway pod to be running
            wait_for_pods_by_label_count(
                label=constants.PUSHGATEWAY_APP_LABEL,
                expected_count=1,
                namespace=self.namespace,
            )

            # Get service URL
            ocp_route = OCP(kind=constants.ROUTE, namespace=self.namespace)
            routes = ocp_route.get(selector=constants.PUSHGATEWAY_APP_LABEL)

            if not routes.get("items"):
                raise Exception(
                    f"Pushgateway route not found in namespace {self.namespace}"
                )

            host = routes["items"][0]["spec"]["host"]
            if not host:
                raise Exception("Pushgateway route does not have a host configured")

            self.service_url = f"http://{host}"

            logger.info(
                f"Pushgateway installed successfully in namespace {self.namespace}"
            )

        except Exception as e:
            logger.error(f"Failed to install Pushgateway: {e}")
            raise

    def send_custom_metric(self, metric_name, metric_value, job_name="test_job"):
        """
        Send a custom metric to Pushgateway.

        Args:
            metric_name (str): Name of the metric
            metric_value (str): Value of the metric
            job_name (str): Job name for the metric (default: "test_job")

        Raises:
            Exception: If sending metric fails
        """
        if not self.service_url:
            raise Exception(
                "Pushgateway service URL not available. Ensure install() was called."
            )

        url = f"{self.service_url}/metrics/job/{job_name}"
        metric_data = f"{metric_name} {metric_value}\n"

        logger.info(f"Sending metric to Pushgateway: {url} = {metric_value}")

        try:
            response = requests.post(
                url,
                data=metric_data.encode("utf-8"),
                headers={"Content-Type": "text/plain"},
                timeout=10,
            )
            response.raise_for_status()
            logger.info(
                f"Successfully sent metric {metric_name}={metric_value} to Pushgateway"
            )
        except requests.exceptions.RequestException as e:
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
