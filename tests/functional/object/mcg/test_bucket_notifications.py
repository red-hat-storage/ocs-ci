import logging
import os
from itertools import combinations
from time import sleep

import pytest

from ocs_ci.framework.testlib import (
    MCGTest,
    bugzilla,
    jira,
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
    tier2,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.bucket_utils import (
    delete_object_tags,
    expire_objects_in_bucket,
    put_bucket_versioning_via_awscli,
    rm_object_recursive,
    tag_objects,
    write_random_test_objects_to_bucket,
)
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.ocs.resources.bucket_notifications_manager import BucketNotificationsManager
from ocs_ci.ocs.resources.mcg_lifecycle_policies import ExpirationRule, LifecyclePolicy
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
        secret, conn_file_name = notif_manager.create_kafka_conn_secret(topic)
        notif_manager.add_notif_conn_to_noobaa_cr(secret)

        # 3. Create a bucket and configure bucket notifs on it using the new connection
        bucket = bucket_factory()[0].name
        notif_manager.put_bucket_notification(
            awscli_pod=awscli_pod,
            mcg_obj=mcg_obj,
            bucket=bucket,
            events=["s3:ObjectCreated:*"],
            conn_file=conn_file_name,
        )

        # 4. Verify the bucket notification configuration was set correctly
        resp = notif_manager.get_bucket_notification(awscli_pod, mcg_obj, bucket)
        assert resp["TopicConfiguration"]["Topic"] == conn_file_name

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

    @pytest.fixture()
    def reduce_expiration_interval(self, add_env_vars_to_noobaa_core_class):
        """
        Reduce the interval in which the lifecycle background worker is running

        """
        new_interval_in_miliseconds = 60 * 1000
        add_env_vars_to_noobaa_core_class(
            [(constants.LIFECYCLE_INTERVAL_PARAM, new_interval_in_miliseconds)]
        )

    @tier2
    @skipif_noobaa_external_pgsql
    @polarion_id("OCS-6331")
    @pytest.mark.usefixtures(reduce_expiration_interval.__name__)
    def test_multi_notif_event_types(
        self,
        mcg_obj,
        awscli_pod,
        bucket_factory,
        test_directory_setup,
        notif_manager,
        jira_issue,
    ):
        """
        Test that various bucket notification events are received by Kafka

        1. Enable bucket notifications on the NooBaa CR
        2. Add a Kafka topic connection to the NooBaa CR
        3 Create a bucket and configure bucket notifications on it using the new connection
        4. Write objects to the bucket
        5. Delete some objects
        6. Expire some objects
        7. Tag some objects
        8. Delete some tags from objects
        9. Put versioning on the bucket then delete and expire objects
        10. Verify that the expected events were received by Kafka
        """
        # 1. Enable bucket notifications on the NooBaa CR
        notif_manager.enable_bucket_notifs_on_cr()

        # 2. Add a Kafka topic connection to the NooBaa CR
        topic = notif_manager.create_kafka_topic()
        secret, conn_file_name = notif_manager.create_kafka_conn_secret(topic)
        notif_manager.add_notif_conn_to_noobaa_cr(secret)

        # 3. Create a bucket and configure bucket notifs on it using the new connection
        config_events = [
            "ObjectRemoved:Delete",
            "ObjectRemoved:DeleteMarkerCreated",
            "LifecycleExpiration:Delete",
            "LifecycleExpiration:DeleteMarkerCreated",
            "ObjectTagging:Put",
            "ObjectTagging:Delete",
        ]
        bucket = bucket_factory()[0].name
        notif_manager.put_bucket_notification(
            awscli_pod=awscli_pod,
            mcg_obj=mcg_obj,
            bucket=bucket,
            events=[f"s3:{event}" for event in config_events],
            conn_file=conn_file_name,
        )

        # 4. Write objects to the bucket
        prefix_to_obj = dict()
        for prefix in [
            "deleted",
            "versioned_deleted",
            "expired",
            "versioned_expired",
            "tagged",
            "untagged",
        ]:
            prefix_to_obj[prefix] = write_random_test_objects_to_bucket(
                io_pod=awscli_pod,
                bucket_to_write=bucket,
                file_dir=os.path.join(test_directory_setup.origin_dir, prefix),
                amount=5,
                mcg_obj=mcg_obj,
                pattern=f"{prefix}-",
                prefix=prefix,
            )

        # 5. Delete some objects
        rm_object_recursive(awscli_pod, f"{bucket}/deleted", mcg_obj)

        # 6. Expire some objects
        mcg_obj.s3_client.put_bucket_lifecycle_configuration(
            Bucket=bucket,
            LifecycleConfiguration=LifecyclePolicy(ExpirationRule(days=1)).as_dict(),
        )
        expire_objects_in_bucket(bucket, prefix_to_obj["expired"], prefix="expired")

        # 7. Tag some objects
        tag = {"tag": "value"}
        for prefix in ["tagged", "untagged"]:
            tag_objects(
                io_pod=awscli_pod,
                mcg_obj=mcg_obj,
                bucket=bucket,
                object_keys=prefix_to_obj[prefix],
                tags=tag,
                prefix=prefix,
            )

        # 8. Delete some tags from objects
        delete_object_tags(
            io_pod=awscli_pod,
            mcg_obj=mcg_obj,
            bucket=bucket,
            object_keys=prefix_to_obj["untagged"],
            prefix="untagged",
        )

        # 9. Put versioning on the bucket then delete and expire objects
        put_bucket_versioning_via_awscli(
            mcg_obj,
            awscli_pod,
            bucket,
        )
        wait_time = 30
        logger.info(
            f"Sleeping for {wait_time} seconds to allow versioning to take effect"
        )
        sleep(wait_time)
        rm_object_recursive(awscli_pod, f"{bucket}/versioned_deleted", mcg_obj)
        expire_objects_in_bucket(
            bucket,
            prefix_to_obj["versioned_expired"],
            prefix="versioned_expired",
        )

        # 10. Verify that the expected events were received by Kafka
        expected_events = set()
        for event, prefix in zip(config_events, prefix_to_obj.keys()):
            for obj_key in prefix_to_obj[prefix]:
                expected_events.add((event, os.path.join(prefix, obj_key)))

        if jira_issue("DFBUGS-1468"):
            logger.warning(
                (
                    "Not testing the LifecycleExpiration:DeleteMarkerCreated"
                    " event due to DFBUGS-1468"
                )
            )
            expected_events = {
                event
                for event in expected_events
                if "LifecycleExpiration:DeleteMarkerCreated" not in event
            }

        delta = set()
        try:
            for raw_received_events in TimeoutSampler(
                timeout=120,
                sleep=5,
                func=notif_manager.get_events,
                topic=topic,
            ):
                received_events = set()
                for event in raw_received_events:
                    received_events.add(
                        (event["eventName"], event["s3"]["object"]["key"])
                    )
                delta = expected_events.difference(received_events)
                if not delta:
                    logger.info("All expected events were received by Kafka")
                    break
                logger.warning(f"Some expected events were not received: {delta}")
        except TimeoutExpiredError as e:
            raise TimeoutExpiredError(
                e, f"Some expected events were not received by Kafka: {delta}"
            )

    @tier2
    @jira("DFBUGS-1481")
    @polarion_id("OCS-6332")
    def test_multiple_bucket_notifs_setups(
        self,
        mcg_obj,
        awscli_pod,
        bucket_factory,
        test_directory_setup,
        notif_manager,
    ):
        """
        Test multiple bucket notifications setups: one bucket to one topic

        1. Enable bucket notifications on the NooBaa CR
        2. Configure multiple bucket notification setups
        3. Write some objects to each bucket
        4. Verify that the expected events were received by Kafka
        5. Check that no topic received events it shouldn't
        """
        # Constants and variables
        SETUP_NUM = 3
        buckets_to_topics = dict()
        buckets_to_written_objs = dict()

        # 1. Enable bucket notifications on the NooBaa CR
        notif_manager.enable_bucket_notifs_on_cr()

        # 2. Configure multiple bucket notification setups
        # Create the Kafka topics and the secrets that define the connections
        kafka_conn_resources = []
        for i in range(SETUP_NUM):
            topic = notif_manager.create_kafka_topic()
            secret, conn_file_name = notif_manager.create_kafka_conn_secret(topic)
            kafka_conn_resources.append((secret, conn_file_name))

            notif_manager.add_notif_conn_to_noobaa_cr(
                secret=secret,
                # Only wait on the last iteration to avoid waiting multiple times
                wait_for_ready_status=True if i == SETUP_NUM - 1 else False,
            )

        # Create the buckets and configure the bucket notifications
        for i in range(SETUP_NUM):
            bucket = bucket_factory()[0].name
            _, conn_file_name = kafka_conn_resources[i]
            notif_manager.put_bucket_notification(
                awscli_pod=awscli_pod,
                mcg_obj=mcg_obj,
                bucket=bucket,
                events=["s3:ObjectCreated:*"],
                conn_file=conn_file_name,
                # Only wait on the last iteration to avoid waiting multiple times
                wait=True if i == SETUP_NUM - 1 else False,
            )
            buckets_to_topics[bucket] = topic

        # 3. Write some objects to each bucket
        for bucket in buckets_to_topics.keys():
            objs = write_random_test_objects_to_bucket(
                io_pod=awscli_pod,
                bucket_to_write=bucket,
                file_dir=test_directory_setup.origin_dir,
                amount=5,
                pattern=f"{bucket}-",
                mcg_obj=mcg_obj,
            )
            buckets_to_written_objs[bucket] = set(objs)

        # 4. Verify that the expected events were received by Kafka
        def _get_events_by_topic():
            events_by_topic = dict()
            for topic in buckets_to_topics.values():
                parsed_events_set = set(
                    event["s3"]["object"]["key"]
                    for event in notif_manager.get_events(topic)
                )
                events_by_topic[topic] = parsed_events_set
            return events_by_topic

        buckets_to_received_events = dict()
        accumulated_delta = set()
        try:
            for events in TimeoutSampler(
                timeout=120, sleep=5, func=_get_events_by_topic
            ):
                for bucket, expected_objs_set in buckets_to_written_objs.items():
                    received_objs_set = events[buckets_to_topics[bucket]]
                    buckets_to_received_events[bucket] = received_objs_set
                    accumulated_delta.update(
                        expected_objs_set.difference(received_objs_set)
                    )

                accumulated_delta = (
                    delta for delta in buckets_to_received_events.values() if delta
                )
                if any(accumulated_delta):
                    logger.warning(
                        f"Some expected events were not received: {accumulated_delta}"
                    )
                    continue
                logger.info("Every topic received all its expected events")
        except TimeoutExpiredError as e:
            raise TimeoutExpiredError(
                e,
                f"Some expected events were not received by Kafka: {accumulated_delta}",
            )

        # 5. Check that no topic received events it shouldn't
        for set_a, set_b in combinations(buckets_to_received_events.values(), 2):
            assert not set_a.intersection(
                set_b
            ), f"Two different topics received the same event/s: {set_a.intersection(set_b)}"

    @tier2
    @polarion_id("OCS-6333")
    def test_bucket_notifs_shared_topic(
        self,
        mcg_obj,
        awscli_pod,
        bucket_factory,
        test_directory_setup,
        notif_manager,
    ):
        """
        Test setting multiple buckets to send notifications to the same Kafka topic

        1. Enable bucket notifications on the NooBaa CR
        2. Setup two buckets with bucket notifications to the same topic
        3. Write some objects to each bucket
        4. Verify that the topic received all the expected events
        """
        # Constants and variables
        SETUP_NUM = 3
        all_objs = set()

        # 1. Enable bucket notifications on the NooBaa CR
        notif_manager.enable_bucket_notifs_on_cr()

        # 2. Setup two buckets with bucket notifications to the same topic
        topic = notif_manager.create_kafka_topic()
        secret, conn_file_name = notif_manager.create_kafka_conn_secret(topic)
        notif_manager.add_notif_conn_to_noobaa_cr(secret)

        buckets = [bucket.name for bucket in bucket_factory(SETUP_NUM)]
        for bucket in buckets:
            notif_manager.put_bucket_notification(
                awscli_pod=awscli_pod,
                mcg_obj=mcg_obj,
                bucket=bucket,
                events=["s3:ObjectCreated:*"],
                conn_file=conn_file_name,
            )

        # 3. Write some objects to each bucket
        for bucket in buckets:
            objs = write_random_test_objects_to_bucket(
                io_pod=awscli_pod,
                bucket_to_write=bucket,
                file_dir=test_directory_setup.origin_dir,
                amount=5,
                pattern=f"{bucket}-",
                mcg_obj=mcg_obj,
            )
            all_objs.update(objs)

        # 4. Verify that the topic received all the expected events
        delta = set()
        try:
            for events in TimeoutSampler(
                timeout=120,
                sleep=5,
                func=notif_manager.get_events,
                topic=topic,
            ):
                keys_in_notifs = set(event["s3"]["object"]["key"] for event in events)
                delta = all_objs.difference(keys_in_notifs)
                if not delta:
                    logger.info("All expected events were received by Kafka")
                    break
        except TimeoutExpiredError as e:
            raise TimeoutExpiredError(
                e,
                f"Some PutObject events were not received by Kafka: {delta}",
            )
