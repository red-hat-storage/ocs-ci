import logging
from time import sleep

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import bugzilla, skipif_ocs_version
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

log = logging.getLogger(__name__)


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


@workloads
@pytest.mark.skip(
    reason="Skipped because of issue https://github.com/red-hat-storage/ocs-ci/issues/7419"
)
class TestQuayWorkload(E2ETest):
    """
    Tests Quay operator
    """

    @bugzilla("1947796")
    @bugzilla("1959331")
    @bugzilla("1959333")
    @pytest.mark.polarion_id("OCS-2596")
    @skipif_ocs_version("<4.6")
    def test_quay(self, quay_operator, mcg_obj):
        """
        Test verifies quay operator deployment and
        whether single OB/OBC are created/bound.
        """
        # Deploy quay operator
        quay_operator.setup_quay_operator()

        # Create quay registry
        quay_operator.create_quay_registry()

        # Verify quay registry OBC is bound and only one bucket is created
        count = 0
        for bucket in mcg_obj.s3_resource.buckets.all():
            if bucket.name.startswith("quay-datastore"):
                count += 1
        assert count == 1, "More than one quay datastore buckets are created"
        assert (
            OCP(
                kind="obc",
                namespace=quay_operator.namespace,
                resource_name=f"{quay_operator.quay_registry_name}-quay-datastore",
            ).get()["status"]["phase"]
            == "Bound"
        )

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
        # Deploy quay operator
        quay_operator.setup_quay_operator()

        # Create quay registry
        quay_operator.create_quay_registry()
        log.info("Waiting for quay endpoint to start serving")
        sleep(120)
        endpoint = quay_operator.get_quay_endpoint()

        log.info("Pulling test image")
        _exec_cmd(f"podman pull {constants.COSBENCH_IMAGE}")
        log.info("Getting the Super user token")
        token = get_super_user_token(endpoint)

        log.info("Validating super_user using token")
        check_super_user(endpoint, token)

        podman_url = endpoint.replace("https://", "")
        log.info(f"Logging into quay endpoint: {podman_url}")
        quay_super_user_login(podman_url)

        repo_name = "test_repo"
        test_image = f"{constants.QUAY_SUPERUSER}/{repo_name}:latest"
        create_quay_repository(
            endpoint, token, org_name=constants.QUAY_SUPERUSER, repo_name=repo_name
        )
        org_name = "test_org"
        log.info(f"Creating a new organization name: {org_name}")
        create_quay_org(endpoint, token, org_name)

        log.info("Tagging a test image")
        _exec_cmd(f"podman tag {constants.COSBENCH_IMAGE} {podman_url}/{test_image}")
        log.info(f"Pushing the test image to quay repo: {repo_name}")
        _exec_cmd(f"podman push {podman_url}/{test_image} --tls-verify=false")

        log.info(f"Validating whether the image can be pull from quay: {repo_name}")
        _exec_cmd(f"podman pull {podman_url}/{test_image} --tls-verify=false")

        # TODO: Trigger build
        pod_obj = pod.Pod(
            **pod.get_pods_having_label(
                label=constants.NOOBAA_CORE_POD_LABEL,
                namespace=config.ENV_DATA["cluster_namespace"],
            )[0]
        )
        pod_obj.delete(force=True)
        assert pod_obj.ocp.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            selector=constants.NOOBAA_CORE_POD_LABEL,
            resource_count=1,
            timeout=800,
            sleep=60,
        )
        log.info("Pulling the image again from quay, post noobaa core failure")
        _exec_cmd(f"podman pull {podman_url}/{test_image} --tls-verify=false")

        log.info(f"Deleting the repository: {repo_name}")
        delete_quay_repository(
            endpoint, token, org_name=constants.QUAY_SUPERUSER, repo_name=repo_name
        )
