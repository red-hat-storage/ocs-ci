import boto3
import logging
import pytest
import re

from datetime import datetime
from semantic_version import Version

from ocs_ci.framework import config
from ocs_ci.framework.testlib import (
    E2ETest,
    tier1,
    on_prem_platform_required,
    bugzilla,
    skipif_external_mode,
    rgw,
)
from ocs_ci.helpers.helpers import default_storage_class
from ocs_ci.ocs.amq import AMQ
from ocs_ci.ocs.bucket_utils import retrieve_verification_mode
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.objectbucket import OBC
from ocs_ci.ocs.resources.rgw import RGW
from ocs_ci.ocs.resources.pod import get_pod_logs, get_rgw_pods, get_pod_obj
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.utility.utils import exec_cmd, run_cmd, clone_notify


log = logging.getLogger(__name__)


@rgw
@tier1
@bugzilla("1937187")
@bugzilla("1958818")
@on_prem_platform_required
@skipif_external_mode
@pytest.mark.polarion_id("OCS-2514")
class TestRGWAndKafkaNotifications(E2ETest):
    """
    Test to verify rgw kafka notifications

    """

    @pytest.fixture(autouse=True)
    def test_fixture_amq(self, request):
        self.amq = AMQ()

        self.kafka_topic = (
            self.kafkadrop_pod
        ) = self.kafkadrop_svc = self.kafkadrop_route = None

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

    def test_rgw_kafka_notifications(self, bucket_factory):
        """
        Test to verify rgw kafka notifications

        """
        # Get sc
        sc = default_storage_class(interface_type=constants.CEPHBLOCKPOOL)

        # Deploy amq cluster
        self.amq.setup_amq_cluster(sc.name)

        # Create topic
        self.kafka_topic = self.amq.create_kafka_topic()

        # Create Kafkadrop pod
        (
            self.kafkadrop_pod,
            self.kafkadrop_pod,
            self.kafkadrop_route,
        ) = self.amq.create_kafkadrop()

        # Get the kafkadrop route
        kafkadrop_host = self.kafkadrop_route.get().get("spec").get("host")

        # Create bucket
        bucketname = bucket_factory(amount=1, interface="RGW-OC")[0].name

        # Get RGW credentials
        rgw_obj = RGW()
        rgw_endpoint, access_key, secret_key = rgw_obj.get_credentials()

        # Clone notify repo
        notify_path = clone_notify()

        # Initialise to put objects
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

        # Initialize notify command to run
        notify_cmd = (
            f"python {notify_path} -e {rgw_endpoint} -a {obc_obj.access_key_id} "
            f"-s {obc_obj.access_key} -b {bucketname} -ke {constants.KAFKA_ENDPOINT} -t {self.kafka_topic.name}"
        )
        log.info(f"Running cmd {notify_cmd}")

        # Put objects to bucket
        assert s3_client.put_object(
            Bucket=bucketname, Key="key-1", Body=data
        ), "Failed: Put object: key-1"
        exec_cmd(notify_cmd)

        # Validate rgw logs notification are sent
        # No errors are seen
        pattern = "ERROR: failed to create push endpoint"
        rgw_pod_obj = get_rgw_pods()
        rgw_log = get_pod_logs(pod_name=rgw_pod_obj[0].name, container="rgw")
        assert re.search(pattern=pattern, string=rgw_log) is None, (
            f"Error: {pattern} msg found in the rgw logs."
            f"Validate {pattern} found on rgw logs and also "
            f"rgw bucket notification is working correctly"
        )
        assert s3_client.put_object(
            Bucket=bucketname, Key="key-2", Body=data
        ), "Failed: Put object: key-2"
        exec_cmd(notify_cmd)

        # Validate message are received Kafka side using curl command
        # A temporary way to check from Kafka side, need to check from UI
        curl_command = (
            f"curl -X GET {kafkadrop_host}/topic/{self.kafka_topic.name} "
            "-H 'content-type: application/vnd.kafka.json.v2+json'"
        )
        json_output = run_cmd(cmd=curl_command)
        new_string = json_output.split()
        messages = new_string[new_string.index("messages</td>") + 1]
        if messages.find("1") == -1:
            raise Exception(
                "Error: Messages are not recieved from Kafka side."
                "RGW bucket notification is not working as expected."
            )

        # Validate the timestamp events
        ocs_version = config.ENV_DATA["ocs_version"]
        if Version.coerce(ocs_version) >= Version.coerce("4.8"):
            cmd = (
                f"bin/kafka-console-consumer.sh --bootstrap-server {constants.KAFKA_ENDPOINT} "
                f"--topic {self.kafka_topic.name} --from-beginning --timeout-ms 20000"
            )
            pod_list = get_pod_name_by_pattern(
                pattern="my-cluster-zookeeper", namespace=constants.AMQ_NAMESPACE
            )
            zookeeper_obj = get_pod_obj(
                name=pod_list[0], namespace=constants.AMQ_NAMESPACE
            )
            event_obj = zookeeper_obj.exec_cmd_on_pod(command=cmd)
            log.info(f"Event obj: {event_obj}")
            event_time = event_obj.get("Records")[0].get("eventTime")
            format_string = "%Y-%m-%dT%H:%M:%S.%fZ"
            try:
                datetime.strptime(event_time, format_string)
            except ValueError as ef:
                log.error(
                    f"Timestamp event {event_time} doesnt match the pattern {format_string}"
                )
                raise ef

            log.info(
                f"Timestamp event {event_time} matches the pattern {format_string}"
            )

        # ToDo: To check from KafkaUI the messages are viewed
