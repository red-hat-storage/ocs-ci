import pytest
import logging
import boto3

# import time

from ocs_ci.ocs.bucket_utils import patch_replication_policy_to_bucket

# from ocs_ci.ocs import constants

from concurrent.futures import ThreadPoolExecutor
from ocs_ci.helpers.helpers import default_storage_class
from ocs_ci.ocs.amq import AMQ
from ocs_ci.ocs.bucket_utils import retrieve_verification_mode
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.objectbucket import OBC
from ocs_ci.ocs.resources.rgw import RGW
from ocs_ci.ocs.longevity import Longevity

# from ocs_ci.helpers.sc_utils import start_mcg_bi_replication

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
        "backingstore_dict": {"rgw": [(1, None)]},
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
    pass


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
        self,
        project_factory,
        multi_pvc_pod_lifecycle_factory,
        multi_obc_lifecycle_factory,
        pod_factory,
        multi_pvc_clone_factory,
        multi_snapshot_factory,
        snapshot_restore_factory,
        teardown_factory,
    ):

        # first_bucket_class_dict = {
        #     "interface": "cli",
        #     "backingstore_dict": {"rgw": [(1, None)]},
        # }
        #
        # second_bucket_class_dict = {
        #     "interface": "OC",
        #     "backingstore_dict": {"rgw": [(1, None)]},
        # }
        #
        # executor_1 = ThreadPoolExecutor(max_workers=1)
        # thread_1 = executor_1.submit(start_mcg_bucket_replication,
        # first_bucket_class_dict, second_bucket_class_dict, 5)

        # ttl = 300000  # 300 seconds
        # cache_bucketclass = {
        #     "interface": "OC",
        #     "namespace_policy_dict": {
        #         "type": "Cache",
        #         "ttl": ttl,
        #         "namespacestore_dict": {
        #             "rgw": [(1, None)],
        #         },
        #     },
        #     "placement_policy": {
        #         "tiers": [{"backingStores": [constants.DEFAULT_NOOBAA_BACKINGSTORE]}]
        #     },
        # }
        #
        # executor_2 = ThreadPoolExecutor(max_workers=1)
        # thread_2 = executor_2.submit(start_noobaa_cache_io, cache_bucketclass, 3)
        # #
        # result = thread_2.result()
        # logger.info(f"Result: {result}")

        threads = []

        # RUN STAGE 2
        executor = ThreadPoolExecutor(max_workers=2)
        thread_1 = executor.submit(
            Longevity().stage_2,
            project_factory,
            multi_pvc_pod_lifecycle_factory,
            multi_obc_lifecycle_factory,
            5,
            1,
            5,
            15,
            False,
            30,
        )
        threads.append(thread_1)

        # # RUN STAGE 3
        # thread_2 = executor.submit(
        #     Longevity().stage_3,
        #     project_factory,
        #     5,
        #     5,
        #     1,
        #     30,
        #     15
        # )
        # threads.append(thread_2)
        #
        # # RUN STAGE 4
        # thread_3 = executor.submit(
        #     Longevity().stage_4,
        #     project_factory,
        #     multi_pvc_pod_lifecycle_factory,
        #     pod_factory,
        #     multi_pvc_clone_factory,
        #     multi_snapshot_factory,
        #     snapshot_restore_factory,
        #     teardown_factory,
        #     5,
        #     25,
        #     1,
        #     15,
        #     2
        # )
        # threads.append(thread_3)

        for thread in threads:
            thread.result()
