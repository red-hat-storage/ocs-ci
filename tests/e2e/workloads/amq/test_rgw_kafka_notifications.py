import boto3
import logging
import pytest
import re
import tempfile

from subprocess import CalledProcessError, run

from ocs_ci.framework.testlib import E2ETest, tier1, vsphere_platform_required, bugzilla
from ocs_ci.helpers.helpers import default_storage_class
from ocs_ci.ocs.amq import AMQ
from ocs_ci.ocs.bucket_utils import retrieve_verification_mode
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.resources.objectbucket import OBC
from ocs_ci.ocs.resources.rgw import RGW
from ocs_ci.ocs.resources.pod import get_pod_logs
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.utility import templating
from ocs_ci.utility.utils import run_cmd


log = logging.getLogger(__name__)


def clone_notify():
    """
    Returns notify path
    """
    notify_dir = tempfile.mkdtemp(prefix="notify_")
    log.info(f"cloning repo notify in {notify_dir}")
    git_clone_cmd = f"git clone {constants.RGW_KAFKA_NOTIFY}"
    run(git_clone_cmd, shell=True, cwd=notify_dir, check=True)
    notify_path = f"{notify_dir}/notify/notify.py"
    return notify_path


@tier1
@bugzilla("1937187")
@vsphere_platform_required
@pytest.mark.parametrize(
    argnames="interface",
    argvalues=[
        pytest.param(*["RGW-OC"], marks=[pytest.mark.polarion_id("OCS-2514")]),
    ],
)
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

    def test_rgw_kafka_notifications(self, interface, bucket_factory):
        """
        Test to verify rgw kafka notifications

        """
        # Get sc
        sc = default_storage_class(interface_type=constants.CEPHBLOCKPOOL)

        # Deploy amq cluster
        self.amq.setup_amq_cluster(sc.name)

        # Create topic
        self.kafka_topic = self.amq.create_kafka_topic()

        # Create kafkadrop pod
        try:
            kafkadrop = list(
                templating.load_yaml(constants.KAFKADROP_YAML, multi_document=True)
            )
            self.kafkadrop_pod = OCS(**kafkadrop[0])
            self.kafkadrop_svc = OCS(**kafkadrop[1])
            self.kafkadrop_route = OCS(**kafkadrop[2])
            self.kafkadrop_pod.create()
            self.kafkadrop_svc.create()
            self.kafkadrop_route.create()
        except (CommandFailed, CalledProcessError) as cf:
            log.error("Failed during creation of kafkadrop which kafka UI")
            raise cf

        # Validate kafkadrop pod running
        ocp_obj = OCP(kind=constants.POD, namespace=constants.AMQ_NAMESPACE)
        kafdrop_pod_name = ocp_obj.get(selector="app=kafdrop")["items"][0]["metadata"][
            "name"
        ]
        ocp_obj.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            resource_name=kafdrop_pod_name,
            timeout=120,
            sleep=5,
        )

        # Get the kafkadrop route
        kafkadrop_host = self.kafkadrop_route.get().get("spec").get("host")

        # # Get the kafkaendpoint
        kafka_endpoint = f"my-cluster-kafka-bootstrap.{constants.AMQ_NAMESPACE}.svc.cluster.local:9092"

        # Create bucket
        bucketname = bucket_factory(amount=1, interface=interface)[0].name

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
            f"-s {obc_obj.access_key} -b {bucketname} -ke {kafka_endpoint} -t {self.kafka_topic.name}"
        )
        log.info(f"Running cmd {notify_cmd}")

        # Put objects to bucket
        assert s3_client.put_object(
            Bucket=bucketname, Key="key-1", Body=data
        ), "Failed: Put object: key-1"
        run_cmd(notify_cmd)

        # Validate rgw logs notification are sent
        # No errors are seen
        pattern = "ERROR: failed to create push endpoint"
        rgw_pod_name = get_pod_name_by_pattern(
            pattern="rgw", namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        rgw_log = get_pod_logs(pod_name=rgw_pod_name[0], container="rgw")
        assert (
            re.search(pattern=pattern, string=rgw_log) is None
        ), f"{pattern} msg found in the rgw logs"
        assert s3_client.put_object(
            Bucket=bucketname, Key="key-2", Body=data
        ), "Failed: Put object: key-2"
        run_cmd(notify_cmd)

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
            raise Exception("Messages are not recieved from Kafka side")

        # ToDo: To check from KafkaUI the messages are viewed
