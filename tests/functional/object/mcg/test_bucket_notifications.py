import pytest

from ocs_ci.framework import config
from ocs_ci.framework.testlib import (
    MCGTest,
    mcg,
    polarion_id,
    red_squad,
    skipif_mcg_only,
    tier1,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.bucket_utils import write_random_test_objects_to_bucket
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.bucket_notifications_manager import BucketNotificationsManager
from ocs_ci.utility.utils import TimeoutSampler


@mcg
@red_squad
class TestBucketNotifications(MCGTest):
    """
    Test the MCG bucket notifications feature
    """

    @pytest.fixture(autouse=True, scope="class")
    def notif_manager(self, request):
        """
        TODO
        """
        notif_manager = BucketNotificationsManager()
        request.addfinalizer(notif_manager.cleanup)

        notif_manager.setup_kafka()
        return notif_manager

    @tier1
    @pytest.mark.parametrize(
        argnames=["use_provided_notifs_pvc"],
        argvalues=[
            # pytest.param(False, marks=[polarion_id("OCS-6242"), bugzilla("2302842")]),
            pytest.param(
                True,
                marks=[polarion_id("OCS-6243"), skipif_mcg_only],
            ),
        ],
        ids=[
            # "default-logs-pvc",
            "provided-logs-pvc",
        ],
    )
    def test_bucket_notifications(
        self,
        mcg_obj,
        awscli_pod,
        bucket_factory,
        pvc_factory,
        test_directory_setup,
        notif_manager,
        use_provided_notifs_pvc,
    ):
        """
        Test the MCG bucket notifications feature
        # TODO
        """
        # Enable bucket notifications on the NooBaa CR
        provided_notifs_pvc = None
        if use_provided_notifs_pvc:
            clstr_proj_obj = OCP(namespace=config.ENV_DATA["cluster_namespace"])
            provided_notifs_pvc = pvc_factory(
                interface=constants.CEPHFILESYSTEM,
                project=clstr_proj_obj,
                size=20,
                access_mode=constants.ACCESS_MODE_RWX,
            )
            notif_manager.enable_bucket_notifs_on_cr(notifs_pvc=provided_notifs_pvc)
        else:
            notif_manager.enable_bucket_notifs_on_cr()

        # Add a Kafka notif connection to the NooBaa CR
        topic = notif_manager.add_new_notif_conn()

        # Create a bucket and configure bucket notifs on it using the new connection
        bucket = bucket_factory()[0].name
        notif_manager.put_bucket_notification(
            events=["s3:ObjectCreated:*"],
            topic=topic,
        )

        # Verify the bucket notification configuration was set correctly
        resp = notif_manager.get_bucket_notification()
        assert resp["TopicConfigurations"]["Topic"] == topic

        # Verify that uploads are generating the expected events in Kafka
        obj_keys = write_random_test_objects_to_bucket(
            io_pod=awscli_pod,
            bucket_to_write=bucket,
            file_dir=test_directory_setup.origin_dir,
            amount=20,
            mcg_obj=mcg_obj,
        )

        # Wait for Kafka to process the notifications
        for events in TimeoutSampler(
            timeout=300,
            sleep=5,
            func=notif_manager.get_events,
            topic=topic,
        ):
            # TODO compare obj_keys with the received events
            for obj in obj_keys:
                assert obj in events

            pass
