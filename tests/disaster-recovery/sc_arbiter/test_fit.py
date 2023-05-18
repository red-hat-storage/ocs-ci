import pytest
import logging
import boto3
import time

from ocs_ci.ocs.bucket_utils import patch_replication_policy_to_bucket

# from ocs_ci.ocs import constants

# from concurrent.futures import ThreadPoolExecutor, wait
from ocs_ci.helpers.helpers import default_storage_class
from ocs_ci.ocs.amq import AMQ
from ocs_ci.ocs.bucket_utils import retrieve_verification_mode
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.objectbucket import OBC
from ocs_ci.ocs.resources.rgw import RGW
from ocs_ci.helpers.sc_utils import start_mcg_bi_replication

# from ocs_ci.ocs.resources.pod import get_pod_logs, get_rgw_pods, get_pod_obj
# from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.utility.utils import clone_notify  # exec_cmd, run_cmd, clone_notify

logger = logging.getLogger(__name__)


@pytest.fixture()
def setup_mcg_bucket_replication(request, bucket_factory):

    first_bucket_class_dict = {
        "interface": "cli",
        "backingstore_dict": {"rgw": [(1, None)]},
    }

    second_bucket_class_dict = {
        "interface": "OC",
        "backingstore_dict": {"aws": [(1, "eu-central-1")]},
    }
    first_bucket = bucket_factory(bucketclass=first_bucket_class_dict)[0].name
    replication_policy = ("basic-replication-rule", first_bucket, None)
    second_bucket = bucket_factory(
        1,
        bucketclass=second_bucket_class_dict,
        replication_policy=replication_policy,
    )[0].name
    patch_replication_policy_to_bucket(
        first_bucket, "basic-replication-rule-2", second_bucket
    )

    return first_bucket, second_bucket


@pytest.fixture
def setup_noobaa_caching(request, bucket_factory):
    ttl = 300000  # 300 seconds
    cache_bucketclass = {
        "interface": "OC",
        "namespace_policy_dict": {
            "type": "Cache",
            "ttl": ttl,
            "namespacestore_dict": {
                "rgw": [(1, "eu-central-1")],
            },
        },
        "placement_policy": {
            "tiers": [{"backingStores": [constants.DEFAULT_NOOBAA_BACKINGSTORE]}]
        },
    }

    cached_bucket_obj = bucket_factory(bucketclass=cache_bucketclass)[0]
    cached_bucket = cached_bucket_obj.name
    hub_bucket = cached_bucket_obj.bucketclass.namespacestores[0].uls_name

    return cached_bucket, hub_bucket


class TestFITonSC:
    @pytest.fixture()
    def setup_kafka(self, request, bucket_factory):

        self.amq = AMQ()

        self.kafka_topic = (
            self.kafkadrop_pod
        ) = self.kafkadrop_svc = self.kafkadrop_route = None

        # Get sc
        sc = default_storage_class(interface_type=constants.CEPHBLOCKPOOL)

        # Deploy amq cluster
        self.amq.setup_amq_cluster(sc.name)

        # Create topic
        self.kafka_topic = self.amq.create_kafka_topic()

        # Create Kafkadrop pod
        (
            self.kafkadrop_pod,
            self.kafkadrop_svc,
            self.kafkadrop_route,
        ) = self.amq.create_kafkadrop()

        # Get the kafkadrop route
        self.kafkadrop_host = self.kafkadrop_route.get().get("spec").get("host")

        # Create bucket
        self.bucketname = bucket_factory(amount=1, interface="RGW-OC")[0].name

        # Get RGW credentials
        rgw_obj = RGW()
        self.rgw_endpoint, self.access_key, self.secret_key = rgw_obj.get_credentials()

        # Clone notify repo
        self.notify_path = clone_notify()

        # Initialise to put objects
        self.data = "A random string data to write on created rgw bucket"
        self.obc_obj = OBC(self.bucketname)
        s3_resource = boto3.resource(
            "s3",
            verify=retrieve_verification_mode(),
            endpoint_url=self.rgw_endpoint,
            aws_access_key_id=self.obc_obj.access_key_id,
            aws_secret_access_key=self.obc_obj.access_key,
        )
        self.s3_client = s3_resource.meta.client

        # Initialize notify command to run
        self.notify_cmd = (
            f"python {self.notify_path} -e {self.rgw_endpoint} -a {self.obc_obj.access_key_id} "
            f"-s {self.obc_obj.access_key} -b {self.bucketname} "
            f"-ke {constants.KAFKA_ENDPOINT} -t {self.kafka_topic.name}"
        )

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

    def test_fit_on_sc(
        self, setup_mcg_bucket_replication, setup_noobaa_caching, setup_kafka
    ):

        # MCG bucket replication on RGW bucket and any other cloud provider. Both uni-directional & bi-directional
        first_bucket, second_bucket = setup_mcg_bucket_replication
        logger.info(f"First bucket: {first_bucket} Second bucket: {second_bucket}")

        # Noobaa caching
        cached_bucket, hub_bucket = setup_noobaa_caching
        logger.info(f"Cached bucket: {cached_bucket} Hub bucket: {hub_bucket}")

        # MCG NSFS

        # RGW kafka notification

        start_mcg_bi_replication(first_bucket, second_bucket, duration=2)

    def random_func(self):
        time.sleep(30)
        logger.info("Inside this function")
        assert False, "Here failed!"

    def test_sample(self):
        start_mcg_bi_replication("test1", "test2", duration=2)
