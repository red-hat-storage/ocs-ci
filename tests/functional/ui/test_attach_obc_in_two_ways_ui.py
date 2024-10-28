import logging

from ocs_ci.framework.pytest_customization.marks import black_squad
from ocs_ci.ocs.ui.page_objects.object_bucket_claims_tab import ObjectBucketClaimsTab
from ocs_ci.framework.testlib import (
    ManageTest,
    ui,
    bugzilla,
    polarion_id,
    tier1,
    ignore_leftovers,
)


logger = logging.getLogger(__name__)


@ui
@tier1
@black_squad
@bugzilla("2302575")
@polarion_id("OCS-6267")
@ignore_leftovers
class TestAttachOBCTwoWaysUi(ManageTest):
    """
    1. Validate if the user is able to attach deployment to the existing OBC
    2. Validate if the user is able to attach OBC to the existing deployment

    """

    def test_attach_obcs_and_deployments(
        self, setup_ui_class, obc_deployment_factory_fixture, bucket_factory
    ):
        """
        Test Steps consists of 2 workflows to attach storage

        flow 1:
        1) click Storage → Object Storage → Object Bucket Claims.
        2) Click the Action menu (⋮) next to the OBC you created.
        3) From the drop-down menu, select Attach to Deployment.
        4) Select the desired deployment from the Deployment Name list,
        5) then click Attach.

        flow 2:
        1) Click Workloads -> Deployments ->
        2) Click for the Action menu (⋮) next to the Deployment you created
        3) From the drop-down menu, select Add storage.
        4) Select ObjectBucketClaim and use existing claim
        5) Click the desired OBC and click create

        """
        obc_obj = ObjectBucketClaimsTab()
        deployment = obc_deployment_factory_fixture(number_of_deployments=2)
        obcs = bucket_factory(
            amount=2,
            interface="OC",
        )
        assert obc_obj.attach_deployment_to_obc_ui(
            deployment=deployment[0]["metadata"]["name"], obc_name=obcs[0].name
        ), "Failed to attach deployment to obc"
        assert obc_obj.attach_obc_to_deployment_ui(
            deployment=deployment[1]["metadata"]["name"], obc_name=obcs[1].name
        ), "Failed to attach obc to the deployment"
