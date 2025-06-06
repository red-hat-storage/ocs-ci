import pytest

from concurrent.futures import ThreadPoolExecutor, as_completed

from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import (
    ignore_leftover_label,
    polarion_id,
    mcg,
    magenta_squad,
    system_test,
    skipif_disconnected_cluster,
    skipif_proxy_cluster,
    skipif_noobaa_external_pgsql,
    skipif_external_mode,
)
from ocs_ci.ocs.bucket_utils import (
    write_random_test_objects_to_bucket,
    s3_delete_object,
    s3_put_object_tagging,
    expire_objects_in_bucket,
)
from ocs_ci.ocs.resources.bucket_notifications_manager import (
    BucketNotificationsManager,
    logger,
)
from ocs_ci.ocs.resources.mcg_lifecycle_policies import (
    LifecyclePolicy,
    ExpirationRule,
    LifecycleFilter,
)
from ocs_ci.ocs.resources.pod import (
    get_pod_node,
    get_pods_having_label,
    Pod,
    get_noobaa_core_pod,
    get_noobaa_db_pod,
    get_noobaa_endpoint_pods,
)
from ocs_ci.utility.utils import TimeoutSampler


@mcg
@system_test
@skipif_disconnected_cluster
@skipif_proxy_cluster
@skipif_noobaa_external_pgsql
@skipif_external_mode
@magenta_squad
@ignore_leftover_label(constants.CUSTOM_MCG_LABEL)
class TestBucketNotificationSystemTest:

    @pytest.fixture(autouse=True, scope="class")
    def notify_manager(self, request, pvc_factory_class):
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

    def verify_events(
        self, notify_manager, topic, bucket_names, event_name, timeout=120, sleep=5
    ):

        for events in TimeoutSampler(
            timeout=timeout,
            sleep=sleep,
            func=notify_manager.get_events,
            topic=topic,
        ):

            for event in events:
                logger.info(f"Event: {event}")
                if (
                    event.get("eventName") == event_name
                    and event.get("s3").get("bucket").get("name") in bucket_names
                ):
                    logger.info(
                        f'{event_name} event found for the bucket {event["s3"]["bucket"]["name"]}'
                    )
                    bucket_names.remove(event["s3"]["bucket"]["name"])

            if len(bucket_names) == 0:
                logger.info(f"Verified {event_name} for all the buckets.")
                break

    @pytest.fixture(autouse=True)
    def teardown(self, request, nodes):
        """
        Make sure all nodes are up again

        """

        def finalizer():
            nodes.restart_nodes_by_stop_and_start_teardown()

        request.addfinalizer(finalizer)

    @polarion_id("OCS-6406")
    def test_bucket_notification_system_test(
        self,
        nodes,
        notify_manager,
        bucket_factory,
        reduce_expiration_interval,
        awscli_pod,
        test_directory_setup,
        mcg_obj,
        noobaa_db_backup_and_recovery_locally,
        setup_mcg_bg_features,
        validate_mcg_bg_features,
    ):
        """
        This is to test MCG bucket notification feature on multiple MCG buckets
        with bucket notification enabled for multiple s3 operation. We also perform some
        Noobaa specific disruptive operations to verify notification is not affected

        Steps:
        1. Enable bucket notifications on the NooBaa CR
        2. Create a Kafka topic and add a Kafka notification connection to the NooBaa CR
        3. Create 5 buckets and configure bucket notification for each of the s3 operation
        4. Perform Object upload and verify object upload events are received for all the buckets
        5. Tag object from the bucket and restart noobaa pod nodes.
           Verify ObjectTagging:Put events has occurred for all the buckets.
        6. Remove object from the bucket and restart kafka pods. Verify ObjectRemoved:Delete event has occurred
        7. Enable object expiration for 1 day and expire the objects manually.
           Perform Noobaa db backup and recovery.
           Make sure notifications are updated for LifecycleExpiration:Delete event

        """

        NUM_OF_BUCKETS = 5
        events_to_notify = [
            "s3:ObjectCreated:*",
            "s3:ObjectRemoved:*",
            "s3:LifecycleExpiration:*",
            "s3:ObjectRestore:*",
            "s3:ObjectTagging:*",
        ]

        feature_setup_map = setup_mcg_bg_features(
            num_of_buckets=5,
            object_amount=5,
            is_disruptive=True,
            skip_any_features=["nsfs", "rgw kafka", "caching"],
        )

        # 1. Enable bucket notification on Noobaa CR
        notify_manager.enable_bucket_notifs_on_cr(use_provided_pvc=True)

        # 2. Create kafka topic and add it to the Noobaa CR
        topic = notify_manager.create_kafka_topic()
        secret, conn_config_path = notify_manager.create_kafka_conn_secret(topic)
        notify_manager.add_notif_conn_to_noobaa_cr(secret)

        # 3. Create the five buckets that is needed for the testing
        buckets_created = []
        for _ in range(NUM_OF_BUCKETS):
            bucket = bucket_factory()[0]
            logger.info(f"Enabling bucket notification for the bucket {bucket.name}")
            notify_manager.put_bucket_notification(
                awscli_pod,
                mcg_obj,
                bucket.name,
                events=events_to_notify,
                conn_config_path=conn_config_path,
            )
            buckets_created.append(bucket.name)

        # 4. Write some object to all the buckets and verify the events
        # has occurred for all the buckets
        obj_written = []
        for bucket in buckets_created:
            obj_written = write_random_test_objects_to_bucket(
                awscli_pod,
                bucket,
                file_dir=test_directory_setup.origin_dir,
                amount=2,
                mcg_obj=mcg_obj,
            )
        self.verify_events(
            notify_manager,
            topic,
            bucket_names=buckets_created[:],
            event_name="ObjectCreated:Put",
        )

        with ThreadPoolExecutor(max_workers=5) as executor:

            # 5. Tag object from the bucket and stop/start noobaa pod nodes
            # Verify ObjectTagging:Put events has occurred for all the buckets
            noobaa_pods = [
                get_noobaa_core_pod(),
                get_noobaa_db_pod(),
            ] + get_noobaa_endpoint_pods()
            noobaa_pod_nodes = [get_pod_node(pod_obj) for pod_obj in noobaa_pods]
            aws_cli_pod_node = get_pod_node(awscli_pod).name
            for node_obj in noobaa_pod_nodes:
                if node_obj.name != aws_cli_pod_node:
                    node_to_shutdown = [node_obj]
                    break

            future_objs = []
            for bucket in buckets_created:
                logger.info(f"Tagging object {obj_written[1]} from the bucket {bucket}")
                future_objs.append(
                    executor.submit(
                        s3_put_object_tagging,
                        mcg_obj,
                        bucket,
                        obj_written[1],
                        [{"Key": "type", "Value": "media"}],
                    )
                )

            logger.info(f"Stopping Noobaa pod node {node_to_shutdown}")
            nodes.stop_nodes(nodes=node_to_shutdown)

            for future in as_completed(future_objs):
                future.result()

            logger.info(
                "Verifying if ObjectTagging:Put events has occurred for all the buckets"
            )
            self.verify_events(
                notify_manager,
                topic,
                bucket_names=buckets_created[:],
                event_name="ObjectTagging:Put",
                timeout=1200,
                sleep=30,
            )

            logger.info("Starting Noobaa pod nodes")
            nodes.start_nodes(nodes=node_to_shutdown)

            # 6. Remove object from the bucket and restart kafka pods
            # Verify ObjectRemoved event has occurred
            kafka_pods = [
                Pod(**pod_info)
                for pod_info in get_pods_having_label(
                    label=constants.KAFKA_KIND_LABEL, namespace=constants.AMQ_NAMESPACE
                )
            ]
            future_objs = []
            for bucket in buckets_created:
                logger.info(f"Deleting object {obj_written[0]} from bucket {bucket}")
                future_objs.append(
                    executor.submit(
                        s3_delete_object,
                        mcg_obj,
                        bucket,
                        obj_written[1],
                    )
                )

            logger.info("Restarting Kafka pods")
            for pod in kafka_pods:
                logger.info(f"Deleting pod {pod.name}")
                pod.delete()
            for future in as_completed(future_objs):
                future.result()

            logger.info(
                "Verifying if ObjectRemoved:Delete events has occurred for all buckets"
            )
            self.verify_events(
                notify_manager,
                topic,
                bucket_names=buckets_created[:],
                event_name="ObjectRemoved:Delete",
                timeout=600,
                sleep=30,
            )

        # 7. Enable object expiration for 1 day and expire the objects manually
        # Perform noobaa db backup and recovery. Make sure notifications are updated
        # for LifecycleExpiration:Delete event

        for bucket in buckets_created:
            logger.info(f"Setting object expiration on bucket {bucket}")
            lifecycle_policy = LifecyclePolicy(
                ExpirationRule(days=1, filter=LifecycleFilter())
            )
            mcg_obj.s3_client.put_bucket_lifecycle_configuration(
                Bucket=bucket, LifecycleConfiguration=lifecycle_policy.as_dict()
            )
            expire_objects_in_bucket(bucket)

        reduce_expiration_interval(interval=5)
        logger.info("Reduced the object expiration interval to 5 minutes")

        # Backup noobaa db and recovery
        noobaa_db_backup_and_recovery_locally()
        logger.info("Noobaa db backup and recovery is completed")

        logger.info(
            "Verifying if LifecycleExpiration:Delete events has occurred for all the buckets"
        )
        self.verify_events(
            notify_manager,
            topic,
            bucket_names=buckets_created[:],
            event_name="LifecycleExpiration:Delete",
            timeout=600,
            sleep=30,
        )

        validate_mcg_bg_features(
            feature_setup_map,
            run_in_bg=False,
            skip_any_features=["nsfs", "rgw kafka", "caching"],
            object_amount=5,
        )
        logger.info("No issues seen with the MCG bg feature validation")
