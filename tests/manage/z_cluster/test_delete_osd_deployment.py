import logging
import pytest
from ocs_ci.framework.testlib import (
    ManageTest,
    tier4c,
    skipif_ocs_version,
    ignore_leftover_label,
)
from ocs_ci.framework import config
from ocs_ci.ocs.resources.pod import get_osd_deployments
from ocs_ci.ocs.resources.storage_cluster import osd_encryption_verification
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility.utils import ceph_health_check

logger = logging.getLogger(__name__)


@tier4c
@skipif_ocs_version("<4.10")
@ignore_leftover_label(constants.OSD_APP_LABEL)
@pytest.mark.polarion_id("OCS-3731")
@pytest.mark.bugzilla("2032656")
class TestDeleteOSDDeployment(ManageTest):
    """
    This test case deletes all the OSD deployments one after the other.
    The expected result is that once the OSD deployment is deleted, a new OSD
    deployment and pod should be created in its place.

    """

    def test_delete_rook_ceph_osd_deployment(self):
        osd_deployments = get_osd_deployments()
        deployment_obj = OCP(
            kind=constants.DEPLOYMENT, namespace=config.ENV_DATA["cluster_namespace"]
        )
        pod_obj = OCP(
            kind=constants.POD, namespace=config.ENV_DATA["cluster_namespace"]
        )
        for osd_deployment in osd_deployments:
            # Get rook-ceph-osd pod name associated with the deployment
            osd_deployment_name = osd_deployment.name
            old_osd_pod = get_pod_name_by_pattern(
                pattern=osd_deployment_name,
                namespace=config.ENV_DATA["cluster_namespace"],
            )[0]

            logger.info(f"Deleting OSD deployment: {osd_deployment_name}")
            try:
                deployment_obj.delete(resource_name=osd_deployment_name)
                deployment_obj.wait_for_resource(
                    condition="0/1", resource_name=osd_deployment_name, column="READY"
                )
            except CommandFailed as err:
                if "NotFound" not in str(err):
                    raise

            # Wait for new OSD deployment to be Ready
            deployment_obj.wait_for_resource(
                condition="1/1",
                resource_name=osd_deployment_name,
                column="READY",
                timeout=120,
            )

            # Check if a new OSD pod is created
            new_osd_pod = get_pod_name_by_pattern(
                pattern=osd_deployment_name,
                namespace=config.ENV_DATA["cluster_namespace"],
            )[0]
            assert old_osd_pod != new_osd_pod, "New OSD pod not created"

            # Check if new OSD pod is up and running
            logger.info(
                "Waiting for a new OSD pod to get created and reach Running state"
            )
            assert pod_obj.wait_for_resource(
                condition=constants.STATUS_RUNNING,
                resource_name=new_osd_pod,
                column="STATUS",
            ), f"New OSD pod {new_osd_pod} is not in {constants.STATUS_RUNNING} state"

        # If clusterwide encryption is enabled, verify that the new OSDs are encrypted
        if config.ENV_DATA.get("encryption_at_rest"):
            osd_encryption_verification()

        assert ceph_health_check(delay=120, tries=50), "Ceph health check failed"
