import logging
from time import sleep

import pytest

from ocs_ci.framework.pytest_customization.marks import bugzilla, skipif_ocs_version
from ocs_ci.framework.testlib import E2ETest, workloads, config
from ocs_ci.ocs import defaults, constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.quay_operator import (
    QuayOperator,
    create_quay_repository,
    get_super_user_token,
    delete_quay_repository,
    create_quay_org,
    check_super_user, quay_super_user_login,
)
from ocs_ci.ocs.resources import pod
from ocs_ci.utility.utils import exec_cmd

log = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def quay_operator(request):

    quay_operator = QuayOperator()

    def teardown():
        quay_operator.teardown()

    request.addfinalizer(teardown)
    return quay_operator


@workloads
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

    @pytest.mark.polarion_id("OCS-2596")
    @skipif_ocs_version("<4.6")
    def test_quay_with_failures(self, quay_operator):
        """
        Test quay operations with Noobaa core failure

        """
        # Deploy quay operator
        quay_operator.setup_quay_operator()

        # Create quay registry
        quay_operator.create_quay_registry()

        endpoint = quay_operator.get_quay_endpoint()
        log.info(endpoint)
        sleep(60)
        log.info("Pulling test image")
        exec_cmd("podman pull quay.io/ocsci/cosbench:latest")

        log.info("Getting Super user token")
        token = get_super_user_token(endpoint)
        log.info(token)

        log.info("Validating super user using token")
        check_super_user(endpoint, token)

        podman_url = endpoint.replace('https://', '')
        log.info("Logging into quay endpoint")
        quay_super_user_login(podman_url)

        repo_name = "test_repo"
        test_image = f"quayadmin/{repo_name}:latest"
        create_quay_repository(endpoint, token, org_name="quayadmin", repo_name=repo_name)

        log.info("Tagging test image")
        exec_cmd(
            f"podman tag quay.io/ocsci/cosbench:latest {podman_url}/{test_image}"
        )

        log.info("Pushing")
        exec_cmd(
            f"podman push {podman_url}/{test_image} --tls-verify=false"
        )
        log.info("Pulling")
        exec_cmd(
            f"podman pull {podman_url}/{test_image} --tls-verify=false"
        )

        pod_obj = pod.Pod(
            **pod.get_pods_having_label(
                label="noobaa_core",
                namespace=defaults.ROOK_CLUSTER_NAMESPACE,
            )[0]
        )
        pod_obj.delete(force=True)
        assert pod_obj.ocp.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            selector=self.labels_map["noobaa_core"],
            resource_count=1,
            timeout=800,
            sleep=60,
        )
        log.info("Pulling again")
        exec_cmd(
            f"podman pull {podman_url}/{test_image} --tls-verify=false"
        )

        log.info("Deleting repo")
        delete_quay_repository(endpoint, token, org="quayadmin", repo=repo_name)

        org_name = "test"
        log.info("Creating new org")
        create_quay_org(endpoint, token, org_name)
