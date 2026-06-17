import boto3
import logging
import pytest
import re

from datetime import datetime
from semantic_version import Version

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import magenta_squad
from ocs_ci.framework.testlib import (
    E2ETest,
    tier1,
    on_prem_platform_required,
    skipif_external_mode,
    skipif_disconnected_cluster,
    rgw,
)
from ocs_ci.helpers.helpers import default_storage_class
from ocs_ci.ocs.amq import AMQ
from ocs_ci.ocs.bucket_utils import retrieve_verification_mode
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.objectbucket import OBC
from ocs_ci.ocs.resources.rgw import RGW
from ocs_ci.ocs.resources.pod import (
    Pod,
    get_pod_logs,
    get_pods_having_label,
    get_rgw_pods,
    get_pod_obj,
)
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import exec_cmd, run_cmd, clone_notify

logger = logging.getLogger(__name__)


@retry(AssertionError, tries=5, delay=10)
def check_kafka_messages(kafkadrop_host, kafka_topic_name):
    """Check if messages are received in Kafka topic"""
    logger.debug(f"Checking Kafka messages for topic: {kafka_topic_name}")
    curl_command = (
        f"curl -X GET {kafkadrop_host}/topic/{kafka_topic_name} "
        "-H 'content-type: application/vnd.kafka.json.v2+json'"
    )
    logger.debug(f"Curl command: {curl_command}")
    json_output = run_cmd(cmd=curl_command)
    new_string = json_output.split()
    messages = new_string[new_string.index("messages</td>") + 1]

    if messages.find("1") == -1:
        logger.error(
            f"No messages found in Kafka topic '{kafka_topic_name}'. "
            "RGW bucket notification may not be working."
        )
        raise AssertionError(
            "Error: Messages are not received from Kafka side. "
            "RGW bucket notification is not working as expected."
        )
    logger.info(f"Messages successfully received in Kafka topic: {kafka_topic_name}")


@rgw
@magenta_squad
@tier1
@on_prem_platform_required
@skipif_external_mode
@skipif_disconnected_cluster
@pytest.mark.polarion_id("OCS-2514")
class TestRGWAndKafkaNotifications(E2ETest):
    """
    Test to verify rgw kafka notifications

    """

    @pytest.fixture(autouse=True)
    def test_fixture_amq(self, request):
        self.amq = AMQ()

        self.kafka_topic = self.kafkadrop_pod = self.kafkadrop_svc = (
            self.kafkadrop_route
        ) = None

        def teardown():
            if self.kafka_topic:
                self.kafka_topic.delete()
            if self.kafkadrop_pod:
                self.kafkadrop_pod.delete()
            if self.kafkadrop_svc:
                self.kafkadrop_svc.delete()
            if self.kafkadrop_route:
                self.kafkadrop_route.delete()

            self.amq.cleanup()

        request.addfinalizer(teardown)
        return self.amq

    def test_rgw_kafka_notifications(self, rgw_bucket_factory):
        """
        Test to verify rgw kafka notifications

        """
        logger.test_step("Setup AMQ cluster with storage class")
        sc = default_storage_class(interface_type=constants.CEPHBLOCKPOOL)
        logger.info(f"Using storage class: {sc.name}")

        self.amq.setup_amq_cluster(sc.name)
        logger.info("AMQ cluster deployed successfully")

        logger.test_step("Create Kafka topic for RGW notifications")
        topic_name = (
            f"test-rgw-kafka-notifications-{datetime.now().strftime('%Y%m%d%H%M%S')}"
        )
        self.kafka_topic = self.amq.create_kafka_topic(name=topic_name)
        logger.info(f"Created Kafka topic: {topic_name}")

        logger.test_step("Create Kafkadrop pod and route")
        (
            self.kafkadrop_pod,
            self.kafkadrop_pod,
            self.kafkadrop_route,
        ) = self.amq.create_kafkadrop()

        kafkadrop_host = self.kafkadrop_route.get().get("spec").get("host")
        logger.info(f"Kafkadrop host: {kafkadrop_host}")

        logger.test_step("Create RGW bucket and setup S3 client")
        bucketname = rgw_bucket_factory(amount=1, interface="RGW-OC")[0].name
        logger.info(f"Created RGW bucket: {bucketname}")

        rgw_obj = RGW()
        rgw_endpoint, access_key, secret_key = rgw_obj.get_credentials()
        logger.info(f"Retrieved RGW credentials, endpoint: {rgw_endpoint}")

        logger.test_step("Clone notify repository and setup notification command")
        notify_path = clone_notify()
        logger.info(f"Cloned notify repository to: {notify_path}")

        data = "A random string data to write on created rgw bucket"
        obc_obj = OBC(bucketname)
        s3_resource = boto3.resource(
            "s3",
            verify=retrieve_verification_mode(),
            endpoint_url=rgw_endpoint,
            aws_access_key_id=obc_obj.access_key_id,
            aws_secret_access_key=obc_obj.access_key,
        )
        s3_client = s3_resource.meta.client
        logger.info("S3 client initialized")

        notify_cmd = (
            f"python {notify_path} -e {rgw_endpoint} -a {obc_obj.access_key_id} "
            f"-s {obc_obj.access_key} -b {bucketname} -ke {constants.KAFKA_ENDPOINT} -t {self.kafka_topic.name}"
        )
        logger.info(f"Notification command: {notify_cmd}")

        logger.test_step("Put objects to bucket and configure notifications")
        put_result_1 = s3_client.put_object(Bucket=bucketname, Key="key-1", Body=data)
        logger.assertion(
            f"Put object 'key-1': bucket={bucketname}, success={bool(put_result_1)}"
        )
        assert put_result_1, "Failed: Put object: key-1"
        logger.info("Successfully put object 'key-1' to bucket")

        exec_cmd(notify_cmd)
        logger.info("Executed notification command for key-1")

        logger.test_step("Validate RGW logs show no notification errors")
        pattern = "ERROR: failed to create push endpoint"
        rgw_pod_obj = get_rgw_pods()
        rgw_log = get_pod_logs(pod_name=rgw_pod_obj[0].name, container="rgw")

        error_found = re.search(pattern=pattern, string=rgw_log) is not None
        logger.assertion(
            f"RGW logs check: pattern='{pattern}', error_found={error_found}, "
            f"pod={rgw_pod_obj[0].name}"
        )
        assert not error_found, (
            f"Error: {pattern} msg found in the rgw logs. "
            f"Validate {pattern} found on rgw logs and also "
            f"rgw bucket notification is working correctly"
        )
        logger.info("RGW logs validated: no notification errors found")

        put_result_2 = s3_client.put_object(Bucket=bucketname, Key="key-2", Body=data)
        logger.assertion(
            f"Put object 'key-2': bucket={bucketname}, success={bool(put_result_2)}"
        )
        assert put_result_2, "Failed: Put object: key-2"
        logger.info("Successfully put object 'key-2' to bucket")

        exec_cmd(notify_cmd)
        logger.info("Executed notification command for key-2")

        logger.test_step("Validate messages received in Kafka")
        check_kafka_messages(
            kafkadrop_host=kafkadrop_host, kafka_topic_name=self.kafka_topic.name
        )

        logger.test_step("Validate event timestamps (OCS 4.8+)")
        ocs_version = config.ENV_DATA["ocs_version"]
        logger.info(f"Current OCS version: {ocs_version}")

        if Version.coerce(ocs_version) >= Version.coerce("4.8"):
            cmd = (
                f"bin/kafka-console-consumer.sh --bootstrap-server {constants.KAFKA_ENDPOINT} "
                f"--topic {self.kafka_topic.name} --from-beginning --timeout-ms 20000"
            )
            kafka_pods_list = [
                Pod(**pod_info)
                for pod_info in get_pods_having_label(
                    namespace=constants.AMQ_NAMESPACE, label=constants.KAFKA_PODS_LABEL
                )
            ]
            kafka_pod_obj = get_pod_obj(
                name=kafka_pods_list[0].name, namespace=constants.AMQ_NAMESPACE
            )
            logger.info(
                f"Executing Kafka console consumer on pod: {kafka_pod_obj.name}"
            )

            event_obj = kafka_pod_obj.exec_cmd_on_pod(command=cmd)
            logger.debug(f"Event object: {event_obj}")

            event_time = event_obj.get("Records")[0].get("eventTime")
            format_string = "%Y-%m-%dT%H:%M:%S.%fZ"

            try:
                datetime.strptime(event_time, format_string)
                logger.assertion(
                    f"Timestamp validation: event_time='{event_time}', "
                    f"format='{format_string}', valid=True"
                )
                logger.info(
                    f"Timestamp event {event_time} matches the pattern {format_string}"
                )
            except ValueError:
                logger.assertion(
                    f"Timestamp validation: event_time='{event_time}', "
                    f"format='{format_string}', valid=False"
                )
                logger.exception(
                    f"Timestamp event {event_time} doesn't match the pattern {format_string}"
                )
                raise
        else:
            logger.info(
                f"Skipping timestamp validation for OCS version {ocs_version} (< 4.8)"
            )

        logger.info("RGW Kafka notifications test completed successfully")
