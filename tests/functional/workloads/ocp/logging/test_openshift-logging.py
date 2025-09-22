"""
This file contains the testcases for openshift-logging
"""

import logging
import json
import pytest


from ocs_ci.helpers import helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import delete_deployment_pods
from ocs_ci.utility.retry import retry
from ocs_ci.ocs.exceptions import CommandFailed
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
    def create_pvc_and_deploymentconfig_pod(self, request, pvc_factory):
        """"""

        def finalizer():
            delete_deployment_pods(pod_obj)

        request.addfinalizer(finalizer)

        # Create pvc
        pvc_obj = pvc_factory()

        # Create service_account to get privilege for deployment pods
        sa_name = helpers.create_serviceaccount(pvc_obj.project.namespace)

        helpers.add_scc_policy(
            sa_name=sa_name.name, namespace=pvc_obj.project.namespace
        )

        pod_obj = helpers.create_pod(
            interface_type=constants.CEPHBLOCKPOOL,
            pvc_name=pvc_obj.name,
            namespace=pvc_obj.project.namespace,
            sa_name=sa_name.name,
            deployment=True,
        )
        helpers.wait_for_resource_state(
            resource=pod_obj, state=constants.STATUS_RUNNING
        )
        return pod_obj, pvc_obj

    def setup_prerequisites(self, project):
        """
        assign necessary permissions (full access) to service account
        and generate token

        Args:
            project: test namespace to verify logging

        Returns:
               lokistack_route
               decoded token
        """
        sa_name = "loki-reader2"
        sa_cmd = f"oc create sa {sa_name} -n {project}"
        exec_cmd(sa_cmd)

        # grants permission to service account
        permission_cmd = f"oc adm policy add-cluster-role-to-user cluster-admin -z {sa_name} -n {project}"
        exec_cmd(permission_cmd)

        # generate a valid JWT token from the ServiceAccount
        token_cmd = f"oc create token {sa_name} -n {project}"
        token = exec_cmd(token_cmd)

        # gets lokistack route
        result = exec_cmd("oc get route logging-loki -n openshift-logging -o json")
        decoded_output = result.stdout.decode("utf-8")
        lokistack_route1 = json.loads(decoded_output)
        lokistack_route = lokistack_route1["spec"]["host"]
        return lokistack_route, token.stdout.decode("utf-8")

    @retry(ModuleNotFoundError, tries=5, delay=200, backoff=1)
    def validate_project_exists_in_logs(self, project):
        """
        This function checks whether the new project exists in the
        lokistack stack by fetching the project logs

        Args:
            project (str): The project

        """
        route, TOKEN = self.setup_prerequisites(project)
        curl_command = (
            f"curl -k "
            f'-H  "Authorization: Bearer {TOKEN}" '
            f" https://{route}/api/logs/v1/application/loki/api/v1/query_range?"
            f"query=%7Bk8s_namespace_name%3D%22{project}%22%7D&limit=30&direction=BACKWARD"
        )
        try:
            curl_output = exec_cmd(curl_command).stdout.decode("utf-8")
            logger.info(curl_output)
        except CommandFailed:
            logger.error("failed to fetch logs")
        return False

        assert (
            curl_output["data"]["result"][0]["stream"]["openshift_log_type"]
            == "application"
        ), "not able to access project in logs"

    @pytest.mark.polarion_id("OCS-657")
    @tier1
    @skipif_managed_service
    @skipif_ms_provider_and_consumer
    def test_create_new_project_to_verify_logging(
        self, create_pvc_and_deploymentconfig_pod
    ):
        """
        This function creates new project to verify logging in  lokistack
        1. Creates new project
        2. Creates PVC
        3. Creates Deployment pod in the new_project and run-io on the app pod
        4. verify if apllication logs are present in lokistack
        """

        pod_obj, pvc_obj = create_pvc_and_deploymentconfig_pod

        # Running IO on the app_pod
        pod_obj.run_io(storage_type="fs", size=6000)

        # Validating if the project exists in lokistack
        project = pvc_obj.project.namespace
        self.validate_project_exists(project)
