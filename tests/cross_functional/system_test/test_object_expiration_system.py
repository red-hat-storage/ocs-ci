import logging
import uuid
from copy import deepcopy
from time import sleep

import pytest

from ocs_ci.helpers.e2e_helpers import create_muliple_types_provider_obcs
from botocore.exceptions import ClientError

from ocs_ci.ocs.resources.mcg_lifecycle_policies import LifecyclePolicy, ExpirationRule
from ocs_ci.utility.retry import retry
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import (
    system_test,
    magenta_squad,
)
from ocs_ci.framework.testlib import version, skipif_ocs_version
from ocs_ci.ocs.bucket_utils import (
    s3_put_object,
    upload_bulk_buckets,
    expire_objects_in_bucket,
    s3_list_objects_v2,
    bulk_s3_put_bucket_lifecycle_config,
)
from ocs_ci.ocs.resources.pod import (
    get_noobaa_core_pod,
    get_noobaa_db_pod,
    wait_for_noobaa_pods_running,
)
from ocs_ci.ocs.node import (
    drain_nodes,
    wait_for_nodes_status,
    schedule_nodes,
    get_node_objs,
)
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)


class TestObjectExpirationSystemTest:
    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance

        """
        self.sanity_helpers = Sanity()

    def check_if_objects_expired(self, mcg_obj, bucket_name, prefix=""):
        response = s3_list_objects_v2(
            mcg_obj, bucketname=bucket_name, prefix=prefix, delimiter="/"
        )
        return response["KeyCount"] == 0

    @system_test
    @skipif_ocs_version("<4.11")
    @pytest.mark.polarion_id("OCS-4852")
    @magenta_squad
    def test_object_expiration(
        self, mcg_obj, bucket_factory, reduce_expiration_interval
    ):
        """
        Test object expiration, see if the object is deleted within the expiration + 8 hours buffer time

        """
        reduce_expiration_interval(interval=2)

        # Creating S3 bucket
        bucket = retry(ClientError, tries=3, delay=10)(bucket_factory)()[0].name
        object_key = "ObjKey-" + str(uuid.uuid4().hex)
        obj_data = "Random data" + str(uuid.uuid4().hex)
        expiration_days = 1

        expire_rule_4_10 = {
            "Rules": [
                {
                    "Expiration": {
                        "Days": expiration_days,
                        "ExpiredObjectDeleteMarker": False,
                    },
                    "ID": "data-expire",
                    "Prefix": "",
                    "Status": "Enabled",
                }
            ]
        }

        lifecycle_policy = LifecyclePolicy(ExpirationRule(days=1))

        logger.info(f"Setting object expiration on bucket: {bucket}")
        if version.get_semantic_ocs_version_from_config() < version.VERSION_4_11:
            mcg_obj.s3_client.put_bucket_lifecycle_configuration(
                Bucket=bucket, LifecycleConfiguration=expire_rule_4_10
            )
        else:
            mcg_obj.s3_client.put_bucket_lifecycle_configuration(
                Bucket=bucket, LifecycleConfiguration=lifecycle_policy.as_dict()
            )

        PROP_SLEEP_TIME = 10
        logger.info(
            f"Sleeping for {PROP_SLEEP_TIME} seconds to let the policy propagate"
        )
        sleep(PROP_SLEEP_TIME)

        logger.info(f"Getting object expiration configuration from bucket: {bucket}")
        logger.info(
            f"Got configuration: {mcg_obj.s3_client.get_bucket_lifecycle_configuration(Bucket=bucket)}"
        )

        logger.info(f"Writing {object_key} to bucket: {bucket}")
        assert s3_put_object(
            s3_obj=mcg_obj, bucketname=bucket, object_key=object_key, data=obj_data
        ), "Failed: Put Object"

        expire_objects_in_bucket(bucket)

        sampler = TimeoutSampler(
            timeout=600,
            sleep=10,
            func=self.check_if_objects_expired,
            mcg_obj=mcg_obj,
            bucket_name=bucket,
        )
        assert sampler.wait_for_func_status(
            result=True
        ), f"Objects in the bucket {bucket} are not expired"
        logger.info("Objects in the bucket are expired as expected")

    def create_obcs_apply_expire_rule(
        self,
        number_of_buckets,
        cloud_providers,
        bucket_types,
        expiration_rule,
        mcg_obj,
        bucket_factory,
    ):
        """
        This method will create the obcs and then apply the expire rule
        for each obcs created

        Args:
            number_of_buckets (int): Number of buckets
            cloud_providers (Dict): Dict representing cloudprovider config
            bucket_types (Dict): Dict representing bucket type and respective
                                config
            expiration_rule (Dict): Lifecycle expiry rule
            mcg_obj (MCG): MCG object
            bucket_factory (Fixture): Bucket factory fixture object

        Returns:
            List: of buckets

        """
        all_buckets = create_muliple_types_provider_obcs(
            number_of_buckets, bucket_types, cloud_providers, bucket_factory
        )

        bulk_s3_put_bucket_lifecycle_config(mcg_obj, all_buckets, expiration_rule)

        return all_buckets

    @system_test
    @magenta_squad
    def test_object_expiration_with_disruptions(
        self,
        mcg_obj,
        scale_noobaa_resources_session,
        setup_mcg_bg_features,
        validate_mcg_bg_features,
        awscli_pod_session,
        nodes,
        bucket_factory,
        noobaa_db_backup_and_recovery_locally,
        node_drain_teardown,
        node_restart_teardown,
    ):
        """
        Test object expiration feature when there are some sort of disruption to the noobaa
        like node drain, node restart, nb db recovery etc

        """
        feature_setup_map = setup_mcg_bg_features(
            num_of_buckets=5,
            object_amount=5,
            is_disruptive=True,
            skip_any_features=["nsfs", "rgw kafka", "caching"],
        )

        lifecycle_policy = LifecyclePolicy(ExpirationRule(days=1))

        cloud_providers = {
            "aws": (1, "eu-central-1"),
            "azure": (1, None),
            "pv": (
                1,
                constants.MIN_PV_BACKINGSTORE_SIZE_IN_GB,
                "ocs-storagecluster-ceph-rbd",
            ),
        }

        bucket_types = {
            "data": {
                "interface": "OC",
                "backingstore_dict": {},
            }
        }

        expire_rule_prefix = deepcopy(lifecycle_policy.as_dict())
        number_of_buckets = 50

        # Create bulk buckets with expiry rule and no prefix set
        logger.info(
            f"Creating first set of {number_of_buckets} buckets with no-prefix expiry rule"
        )

        buckets_without_prefix = self.create_obcs_apply_expire_rule(
            number_of_buckets=number_of_buckets,
            cloud_providers=cloud_providers,
            bucket_types=bucket_types,
            expiration_rule=lifecycle_policy.as_dict(),
            mcg_obj=mcg_obj,
            bucket_factory=bucket_factory,
        )

        # Create another set of bulk buckets with expiry rule and prefix set
        logger.info(
            f"Create second set of {number_of_buckets} buckets with prefix 'others' expiry rule"
        )
        expire_rule_prefix["Rules"][0]["Filter"]["Prefix"] = "others"
        buckets_with_prefix = self.create_obcs_apply_expire_rule(
            number_of_buckets=number_of_buckets,
            cloud_providers=cloud_providers,
            bucket_types=bucket_types,
            expiration_rule=expire_rule_prefix,
            mcg_obj=mcg_obj,
            bucket_factory=bucket_factory,
        )

        @retry(ClientError, tries=5, delay=5)
        def upload_objects_and_expire():

            # upload objects with prefix 'tmp'
            logger.info("Uploading objects with prefix 'tmp'")
            upload_bulk_buckets(
                mcg_obj,
                buckets_without_prefix,
                amount=50,
                object_key="tmp-obj",
                prefix="tmp",
            )

            # Manually expire objects in bucket
            logger.info("For each bucket, change the creation time of the objects")
            for bucket in buckets_without_prefix:
                expire_objects_in_bucket(bucket_name=bucket.name)

            # Upload objects with same prefix 'others'
            logger.info("upload objects under 'others' prefix")
            upload_bulk_buckets(
                mcg_obj,
                buckets_with_prefix,
                amount=50,
                object_key="other-obj",
                prefix="others",
            )

            # Upload objects with different prefix 'perm'
            logger.info("upload objects under 'perm' prefix")
            upload_bulk_buckets(
                mcg_obj,
                buckets_with_prefix,
                amount=50,
                object_key="perm-obj",
                prefix="perm",
            )

            # Manually expire objects in bucket
            logger.info(
                "For each of the prefixed buckets in the second set, change the creation time of the objects"
            )
            for bucket in buckets_with_prefix:
                expire_objects_in_bucket(bucket_name=bucket.name)

        def sample_if_objects_expired():
            def check_if_objects_expired(mcg_obj, bucket_name, prefix=""):
                response = s3_list_objects_v2(
                    mcg_obj, bucketname=bucket_name, prefix=prefix, delimiter="/"
                )
                return response["KeyCount"] == 0

            logger.info(
                "All the objects in the first set of buckets should be deleted irrespective of the prefix"
            )
            for bucket in buckets_without_prefix:
                sampler = TimeoutSampler(
                    timeout=900,
                    sleep=10,
                    func=check_if_objects_expired,
                    mcg_obj=mcg_obj,
                    bucket_name=bucket.name,
                )
                if not sampler.wait_for_func_status(result=True):
                    logger.error(
                        f"[{bucket.name}] Objects in first set of buckets without prefix rule are not expired"
                    )
                else:
                    logger.info(
                        f"[{bucket.name}] Objects in first set of buckets without prefix rule are expired"
                    )

            logger.info(
                "Objects with prefix 'others' should expire but not with the prefix 'perm'"
            )
            for bucket in buckets_with_prefix:
                sampler = TimeoutSampler(
                    timeout=600,
                    sleep=10,
                    func=check_if_objects_expired,
                    mcg_obj=mcg_obj,
                    bucket_name=bucket.name,
                    prefix="others",
                )
                if not sampler.wait_for_func_status(result=True):
                    logger.error(
                        f'[{bucket.name}] Objects in second set of buckets with prefix "others" are not expired'
                    )
                else:
                    logger.info(
                        f'[{bucket.name}] Objects in first set of buckets with prefix "others" are expired'
                    )

                sampler = TimeoutSampler(
                    timeout=600,
                    sleep=10,
                    func=check_if_objects_expired,
                    mcg_obj=mcg_obj,
                    bucket_name=bucket.name,
                    prefix="perm",
                )
                if sampler.wait_for_func_status(result=False):
                    logger.info(
                        f'[{bucket.name}] Objects in first set of buckets with prefix "perm" are not expired'
                    )
                else:
                    logger.error(
                        f'[{bucket.name}] Objects in second set of buckets with prefix "perm" expired'
                    )

        upload_objects_and_expire()

        # Drain the node where noobaa core pod is running
        logger.info("Drain noobaa core pod node")
        nb_core_pod_node = get_noobaa_core_pod().get_node()
        drain_nodes([nb_core_pod_node], timeout=300)

        # Shutdown the node where noobaa db pod is running
        logger.info("Shutdown noobaa db pod node")
        nb_db_pod = get_noobaa_db_pod()
        nb_db_pod_node = nb_db_pod.get_node()
        nodes.stop_nodes(nodes=get_node_objs([nb_db_pod_node]))
        wait_for_nodes_status(
            node_names=[nb_db_pod_node],
            status=constants.NODE_NOT_READY,
            timeout=300,
        )

        # Schedule back the noobaa core pod node
        logger.info("Schedule back the noobaa core pod node")
        schedule_nodes([nb_core_pod_node])

        # Turn on the noobaa db pod node
        logger.info("Turn on the noobaa db pod node")
        nodes.start_nodes(nodes=get_node_objs([nb_db_pod_node]))
        wait_for_nodes_status(
            node_names=[nb_db_pod_node],
            status=constants.NODE_READY,
            timeout=300,
        )
        wait_for_noobaa_pods_running(timeout=1200)

        # check if the objects are expired
        sample_if_objects_expired()

        # upload objects again and expire
        upload_objects_and_expire()

        # Perform noobaa db backup and recovery
        noobaa_db_backup_and_recovery_locally()
        wait_for_noobaa_pods_running(timeout=1200)

        sample_if_objects_expired()

        # validate mcg entry criteria post test
        retry(Exception, tries=5, delay=10)(validate_mcg_bg_features)(
            feature_setup_map,
            run_in_bg=False,
            skip_any_features=["nsfs", "rgw kafka", "caching"],
            object_amount=5,
        )

        logger.info("No issues seen with the MCG bg feature validation")
