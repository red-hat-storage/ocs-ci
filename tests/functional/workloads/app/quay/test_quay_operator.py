import logging
from time import sleep

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    skipif_ocs_version,
    magenta_squad,
)
from ocs_ci.framework.testlib import E2ETest, workloads
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.quay_operator import (
    QuayOperator,
    create_quay_repository,
    get_super_user_token,
    delete_quay_repository,
    create_quay_org,
    check_super_user,
    quay_super_user_login,
)
from ocs_ci.ocs.resources import pod
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import exec_cmd
from ocs_ci.ocs.exceptions import (
    CommandFailed,
)

logger = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def quay_operator(request):

    quay_operator = QuayOperator()

    def teardown():
        quay_operator.teardown()

    request.addfinalizer(teardown)
    return quay_operator


@retry(CommandFailed, tries=10, delay=10, backoff=1)
def _exec_cmd(cmd):
    exec_cmd(cmd)


@magenta_squad
@workloads
class TestQuayWorkload(E2ETest):
    """
    Tests Quay operator
    """

    @pytest.mark.polarion_id("OCS-2596")
    @skipif_ocs_version("<4.6")
    def test_quay(self, quay_operator, mcg_obj):
        """
        Test verifies quay operator deployment and
        whether single OB/OBC are created/bound.
        """
        logger.test_step("Deploy Quay operator")
        quay_operator.setup_quay_operator()
        logger.info("Quay operator deployed successfully")

        logger.test_step("Create Quay registry")
        quay_operator.create_quay_registry()
        logger.info(f"Quay registry created: {quay_operator.quay_registry_name}")

        logger.test_step(
            "Verify Quay registry OBC is bound and only one bucket is created"
        )
        count = 0
        for bucket in mcg_obj.s3_resource.buckets.all():
            if bucket.name.startswith("quay-datastore"):
                count += 1

        logger.assertion(f"Quay datastore bucket count: expected=1, actual={count}")
        assert count == 1, "More than one quay datastore buckets are created"
        logger.info("Verified exactly 1 quay-datastore bucket exists")

        obc_phase = OCP(
            kind="obc",
            namespace=quay_operator.namespace,
            resource_name=f"{quay_operator.quay_registry_name}-quay-datastore",
        ).get()["status"]["phase"]

        logger.assertion(
            f"OBC status: expected='Bound', actual='{obc_phase}', "
            f"namespace={quay_operator.namespace}"
        )
        assert obc_phase == "Bound", f"OBC status is {obc_phase}, expected Bound"
        logger.info("Verified OBC is in Bound state")

    @pytest.mark.polarion_id("OCS-2758")
    @skipif_ocs_version("<4.6")
    def test_quay_with_failures(self, quay_operator):
        """
        Test quay operations with Noobaa core failure

        1. Creates quay operator and registry on ODF.
        2. Initializes Quay super user to access the API's.
        3. Gets the super user token.
        4. Creates a new repo
        5. Creates a new org
        6. Pushes the image to the new repo
        7. Pulls the image locally from the quay repo
        8. Re-spins noobaa core
        9. Pulls the image again
        10. Deletes the repo
        """
        logger.test_step("Deploy Quay operator and create registry")
        quay_operator.setup_quay_operator()
        logger.info("Quay operator deployed successfully")

        quay_operator.create_quay_registry()
        logger.info(f"Quay registry created: {quay_operator.quay_registry_name}")

        logger.info("Waiting for quay endpoint to start serving")
        sleep(180)
        endpoint = quay_operator.get_quay_endpoint()
        logger.info(f"Quay endpoint: {endpoint}")

        logger.test_step("Setup Quay authentication and validate super user")
        logger.info(f"Pulling test image: {constants.COSBENCH_IMAGE}")
        _exec_cmd(f"podman pull {constants.COSBENCH_IMAGE}")

        token = get_super_user_token(endpoint)
        logger.info("Retrieved super user token")

        check_super_user(endpoint, token)
        logger.info("Super user validated successfully")

        podman_url = endpoint.replace("https://", "")
        logger.info(f"Logging into quay endpoint: {podman_url}")
        quay_super_user_login(podman_url)

        logger.test_step("Create Quay repository and organization")
        repo_name = "test_repo"
        test_image = f"{constants.QUAY_SUPERUSER}/{repo_name}:latest"
        create_quay_repository(
            endpoint, token, org_name=constants.QUAY_SUPERUSER, repo_name=repo_name
        )
        logger.info(f"Created repository: {repo_name}")

        org_name = "test_org"
        create_quay_org(endpoint, token, org_name)
        logger.info(f"Created organization: {org_name}")

        logger.test_step("Push test image to Quay repository")
        logger.info(f"Tagging image as: {podman_url}/{test_image}")
        _exec_cmd(f"podman tag {constants.COSBENCH_IMAGE} {podman_url}/{test_image}")

        logger.info(f"Pushing image to repository: {repo_name}")
        _exec_cmd(f"podman push {podman_url}/{test_image} --tls-verify=false")

        logger.test_step("Verify image can be pulled from Quay")
        logger.info(f"Pulling image from repository: {repo_name}")
        _exec_cmd(f"podman pull {podman_url}/{test_image} --tls-verify=false")

        logger.test_step("Trigger Noobaa core pod restart and verify recovery")
        pod_obj = pod.Pod(
            **pod.get_pods_having_label(
                label=constants.NOOBAA_CORE_POD_LABEL,
                namespace=config.ENV_DATA["cluster_namespace"],
            )[0]
        )
        logger.info(f"Deleting Noobaa core pod: {pod_obj.name}")
        pod_obj.delete(force=True)

        logger.info("Waiting for Noobaa core pod to recover")
        pod_recovered = pod_obj.ocp.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            selector=constants.NOOBAA_CORE_POD_LABEL,
            resource_count=1,
            timeout=800,
            sleep=60,
        )
        logger.assertion(f"Noobaa core recovery: expected=True, actual={pod_recovered}")
        assert pod_recovered, "Noobaa core pod did not recover"

        logger.test_step("Verify image pull after Noobaa core failure")
        logger.info("Pulling image again post Noobaa core restart")
        _exec_cmd(f"podman pull {podman_url}/{test_image} --tls-verify=false")
        logger.info("Image pull successful after Noobaa core recovery")

        logger.test_step("Cleanup: Delete Quay repository")
        delete_quay_repository(
            endpoint, token, org_name=constants.QUAY_SUPERUSER, repo_name=repo_name
        )
        logger.info(f"Deleted repository: {repo_name}")
