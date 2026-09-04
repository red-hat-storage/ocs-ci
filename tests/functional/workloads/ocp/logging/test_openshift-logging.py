"""
This file contains the testcases for openshift-logging
"""

import logging
import json
import pytest
import time

from ocs_ci.helpers import helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import LokiEmptyResultError
from ocs_ci.ocs.resources.pod import delete_deployment_pods
from ocs_ci.utility.retry import retry
from ocs_ci.framework.pytest_customization.marks import skipif_aws_i3, magenta_squad
from ocs_ci.framework.testlib import (
    E2ETest,
    tier1,
    ignore_leftovers,
)
from ocs_ci.framework.pytest_customization.marks import (
    skipif_managed_service,
    skipif_ms_provider_and_consumer,
)
from ocs_ci.utility.utils import exec_cmd

logger = logging.getLogger(__name__)


@skipif_aws_i3
@pytest.fixture()
def setup_fixture(install_logging):
    """
    Installs openshift-logging
    """
    logger.info("Testcases execution post deployment of openshift-logging")


@magenta_squad
@pytest.mark.usefixtures(setup_fixture.__name__)
@ignore_leftovers
class Testopenshiftloggingonocs(E2ETest):
    """
    The class contains tests to verify openshift-logging backed by OCS.
    """

    @pytest.fixture()
    def create_pvc_and_deployment_pod(self, request, pvc_factory, pod_factory):
        """Create PVC and deployment pod for logging test"""
        logger.info("Setting up PVC and deployment pod for logging test")

        def finalizer():
            delete_deployment_pods(pod_obj)

        request.addfinalizer(finalizer)

        pvc_obj = pvc_factory(size=10)
        logger.info(
            f"Created PVC: {pvc_obj.name}, size=10Gi, namespace={pvc_obj.project.namespace}"
        )

        sa_name = helpers.create_serviceaccount(pvc_obj.project.namespace)
        logger.info(f"Created service account: {sa_name.name}")

        helpers.add_scc_policy(
            sa_name=sa_name.name, namespace=pvc_obj.project.namespace
        )
        logger.info(f"Added SCC policy to service account: {sa_name.name}")

        logger.info("Creating deployment pod with FIO workload and log generation")
        pod_obj = helpers.create_pod(
            interface_type=constants.CEPHBLOCKPOOL,
            pvc_name=pvc_obj.name,
            namespace=pvc_obj.project.namespace,
            sa_name=sa_name.name,
            deployment=True,
            command=["/bin/bash"],
            command_args=[
                "-c",
                "fio --name=test --filename=/mnt/test --size=6G --runtime=300 & "
                'while true; do echo "$(date) - Application running"; sleep 5; done',
            ],
        )

        helpers.wait_for_resource_state(
            resource=pod_obj, state=constants.STATUS_RUNNING
        )
        logger.info(f"Deployment pod is running: {pod_obj.name}")
        return pod_obj, pvc_obj

    def setup_prerequisites(self, project):
        """
        assign necessary permissions (full access) to service account
        and generate token

        Args:
            project: test namespace to verify logging

        Returns:
            return values: lokistack_route, decoded token for success
        """
        logger.info(f"Setting up prerequisites for project: {project}")
        sa_name = "loki-reader2"
        sa_cmd = f"oc create sa {sa_name} -n {project}"
        exec_cmd(sa_cmd)
        logger.info(f"Created service account: {sa_name} in namespace: {project}")

        permission_cmd = (
            "oc adm policy add-cluster-role-to-user cluster-admin "
            f"system:serviceaccount:{project}:{sa_name}"
        )
        exec_cmd(permission_cmd)
        logger.info(f"Granted cluster-admin permissions to service account: {sa_name}")

        token_cmd = f"oc create token {sa_name} -n {project}"
        token = exec_cmd(token_cmd)
        logger.info(f"Generated JWT token for service account: {sa_name}")

        result = exec_cmd("oc get route logging-loki -n openshift-logging -o json")
        decoded_output = result.stdout.decode("utf-8")
        lokistack_route1 = json.loads(decoded_output)
        lokistack_route = lokistack_route1["spec"]["host"]
        logger.info(f"Retrieved Loki route: {lokistack_route}")
        return lokistack_route, token.stdout.decode("utf-8")

    @retry(LokiEmptyResultError, tries=5, delay=200, backoff=1)
    def validate_project_exists_in_logs(self, project):
        """
        This function checks whether the new project exists in the
        lokistack stack by fetching the project logs

        Args:
            project (str): The project

        Raises:
            LokiEmptyResultError: If Loki returns no results (transient, retried)
            AssertionError: If curl command fails, returns an error, or log_type is wrong

        """
        logger.info(f"Validating project exists in Loki logs: {project}")
        route, TOKEN = self.setup_prerequisites(project)

        logger.debug("Waiting 40 seconds for log ingestion")
        time.sleep(40)

        curl_command = (
            f"curl -k "
            f'-H  "Authorization: Bearer {TOKEN}" '
            f" https://{route}/api/logs/v1/application/loki/api/v1/query_range?"
            f"query=%7Bk8s_namespace_name%3D%22{project}%22%7D&limit=30&direction=BACKWARD"
        )
        logger.debug(f"Querying Loki for namespace: {project}")

        try:
            curl_output_str = exec_cmd(curl_command).stdout.decode("utf-8")
            logger.info(
                f"Loki query raw response (first 500 chars): {curl_output_str[:500]}"
            )
        except Exception as e:
            logger.exception(f"Failed to fetch logs from Loki for project: {project}")
            raise AssertionError(f"Curl command failed to fetch logs: {e}")

        try:
            curl_output = json.loads(curl_output_str)
        except json.JSONDecodeError as e:
            logger.exception("Failed to parse JSON output from Loki query")
            raise AssertionError(f"Invalid JSON response from curl command: {e}")

        if "error" in curl_output:
            error_msg = curl_output.get("error", "Unknown error")
            error_type = curl_output.get("errorType", "Unknown type")
            logger.error(f"Error in curl response: {error_msg} (Type: {error_type})")
            raise AssertionError(
                f"Curl query returned error: {error_msg} (Type: {error_type}). "
                f"Full response: {curl_output_str}"
            )

        # Check if any results were returned — transient, safe to retry
        result = curl_output.get("data", {}).get("result")
        if not result:
            raise LokiEmptyResultError(
                f"No logs found for namespace {project} in LokiStack. "
                f"Query may need more time for log collection/indexing. "
                f"Full response: {curl_output_str}"
            )

        logger.info(f"Loki returned {len(result)} stream(s) for namespace: {project}")
        log_type = result[0]["stream"]["openshift_log_type"]
        logger.info(
            f"Log type validation: project={project}, expected='application', actual='{log_type}'"
        )
        assert log_type == "application", "not able to access project in logs"
        logger.info(f"Successfully validated project logs in Loki: {project}")

    @pytest.mark.polarion_id("OCS-6912")
    @tier1
    @skipif_managed_service
    @skipif_ms_provider_and_consumer
    def test_create_new_project_to_verify_logging(self, create_pvc_and_deployment_pod):
        """
        This function creates new project to verify logging in  lokistack
        1. Creates new project
        2. Creates PVC
        3. Creates Deployment pod in the new_project and run-io on the app pod
        4. verify if apllication logs are present in lokistack
        """
        logger.test_step("Setup test environment with PVC and deployment pod")
        pod_obj, pvc_obj = create_pvc_and_deployment_pod
        project = pvc_obj.project.namespace
        logger.info(f"Test project: {project}, pod: {pod_obj.name}")

        logger.test_step("Wait for logs to be collected by Loki")
        logger.info("Waiting 300 seconds for logs to be collected and ingested")
        time.sleep(300)

        logger.test_step("Validate project logs are present in Loki")
        self.validate_project_exists_in_logs(project)
        logger.info("Test completed successfully: project logs verified in Loki")
