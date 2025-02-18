import logging

import pytest

from ocs_ci.framework.testlib import (
    MCGTest,
    bugzilla,
    ignore_leftover_label,
    mcg,
    polarion_id,
    red_squad,
    skipif_disconnected_cluster,
    skipif_external_mode,
    skipif_mcg_only,
    skipif_noobaa_external_pgsql,
    skipif_proxy_cluster,
    tier1,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.bucket_utils import (
    write_random_test_objects_to_bucket,
)
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.ocs.resources.bucket_notifications_manager import BucketNotificationsManager
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)


@mcg
@red_squad
@skipif_disconnected_cluster
@skipif_noobaa_external_pgsql
@skipif_external_mode
@skipif_proxy_cluster
@ignore_leftover_label(constants.CUSTOM_MCG_LABEL)
class TestBucketNotifications(MCGTest):
    """
    Test the MCG bucket notifications feature
    """

    @pytest.fixture(autouse=True, scope="class")
    def notif_manager(self, request, pvc_factory_class):
        """
        Set up Kafka and the BucketNotificationsManager

        Note that the dependency on the pvc_factory_class fixture is necessary
        to guarantee the correct teardown order. Otherwise the pvc factory teardown
        might fail when deleting a PVC that the BucketNotificationsManager is still using.

        Returns:
            BucketNotificationsManager: An instance of the BucketNotificationsManager class
        """
        notif_manager = BucketNotificationsManager()
        notif_manager.pvc_factory = pvc_factory_class
        request.addfinalizer(notif_manager.cleanup)

        notif_manager.setup_kafka()
        return notif_manager

    @tier1
    @pytest.mark.parametrize(
        argnames=["use_provided_pvc"],
        argvalues=[
            pytest.param(False, marks=[polarion_id("OCS-6329"), bugzilla("2302842")]),
            pytest.param(
                True,
                marks=[polarion_id("OCS-6330"), skipif_mcg_only],
            ),
        ],
        ids=[
            "default-logs-pvc",
            "provided-logs-pvc",
        ],
    )
    def test_bucket_notifications(
        self,
        mcg_obj,
        awscli_pod,
        bucket_factory,
        test_directory_setup,
        notif_manager,
        use_provided_pvc,
    ):
        """
        Test the MCG bucket notifications feature

        1. Enable bucket notifications on the NooBaa CR
        2. Create a Kafka topic and add a Kafka notification connection to the NooBaa CR
        3. Create a bucket and configure bucket notifications
        on it using the new connection
        4. Verify that the bucket notification configuration was set correctly
        5. Write some objects to the bucket
        6. Verify that the expected events were received by Kafka
        """
        # 1. Enable bucket notifications on the NooBaa CR
        notif_manager.enable_bucket_notifs_on_cr(use_provided_pvc=use_provided_pvc)

        # 2. Add a Kafka topic connection to the NooBaa CR
        topic = notif_manager.create_kafka_topic()
        secret, conn_config_path = notif_manager.create_kafka_conn_secret(topic)
        notif_manager.add_notif_conn_to_noobaa_cr(secret)

        # 3. Create a bucket and configure bucket notifs on it using the new connection
        bucket = bucket_factory()[0].name
        notif_manager.put_bucket_notification(
            awscli_pod=awscli_pod,
            mcg_obj=mcg_obj,
            bucket=bucket,
            events=["s3:ObjectCreated:*"],
            conn_config_path=conn_config_path,
        )

        # 4. Verify the bucket notification configuration was set correctly
        resp = notif_manager.get_bucket_notification_configuration(
            awscli_pod, mcg_obj, bucket
        )
        assert resp["TopicConfigurations"][0]["TopicArn"] == conn_config_path

        # 5. Write some objects to the bucket
        obj_keys = write_random_test_objects_to_bucket(
            io_pod=awscli_pod,
            bucket_to_write=bucket,
            file_dir=test_directory_setup.origin_dir,
            amount=20,
            mcg_obj=mcg_obj,
        )
        obj_keys_set = set(obj_keys)

        # 6. Verify that the expected events were received by Kafka
        delta = set()
        try:
            for events in TimeoutSampler(
                timeout=120,
                sleep=5,
                func=notif_manager.get_events,
                topic=topic,
            ):
                keys_in_notifs = set(event["s3"]["object"]["key"] for event in events)
                delta = obj_keys_set.difference(keys_in_notifs)
                if not delta:
                    logger.info("All expected events were received by Kafka")
                    break
        except TimeoutExpiredError as e:
            raise TimeoutExpiredError(
                e,
                f"Some PutObject events were not received by Kafka: {delta}",
            )
