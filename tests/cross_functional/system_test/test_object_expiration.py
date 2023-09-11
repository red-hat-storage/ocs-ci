import logging
import uuid
from time import sleep
from copy import deepcopy

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    bugzilla,
    system_test,
    magenta_squad,
)

from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import bugzilla, system_test
from ocs_ci.framework.testlib import version, skipif_ocs_version
from ocs_ci.ocs.bucket_utils import (
    s3_put_object,
    s3_get_object,
    upload_bulk_buckets,
    expire_objects_in_bucket,
    s3_list_objects_v2,
)
from ocs_ci.ocs.resources.pod import (
    get_noobaa_core_pod,
    get_noobaa_db_pod,
    wait_for_storage_pods,
)
from ocs_ci.ocs.node import (
    drain_nodes,
    wait_for_nodes_status,
    schedule_nodes,
    get_node_objs,
)
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)


class TestObjectExpiration:
    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance

        """
        self.sanity_helpers = Sanity()

    @system_test
    @bugzilla("2039309")
    @skipif_ocs_version("<4.11")
    @pytest.mark.polarion_id("OCS-4852")
    def test_object_expiration(self, mcg_obj, bucket_factory):
        """
        Test object expiration, see if the object is deleted within the expiration + 8 hours buffer time

        """
        # Creating S3 bucket
        bucket = bucket_factory()[0].name
        object_key = "ObjKey-" + str(uuid.uuid4().hex)
        obj_data = "Random data" + str(uuid.uuid4().hex)
        expiration_days = 1
        buffer_time_in_hours = 8

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
        expire_rule = {
            "Rules": [
                {
                    "Expiration": {
                        "Days": expiration_days,
                        "ExpiredObjectDeleteMarker": False,
                    },
                    "Filter": {"Prefix": ""},
                    "ID": "data-expire",
                    "Status": "Enabled",
                }
            ]
        }

        logger.info(f"Setting object expiration on bucket: {bucket}")
        if version.get_semantic_ocs_version_from_config() < version.VERSION_4_11:
            mcg_obj.s3_client.put_bucket_lifecycle_configuration(
                Bucket=bucket, LifecycleConfiguration=expire_rule_4_10
            )
        else:
            mcg_obj.s3_client.put_bucket_lifecycle_configuration(
                Bucket=bucket, LifecycleConfiguration=expire_rule
            )

        logger.info(f"Getting object expiration configuration from bucket: {bucket}")
        logger.info(
            f"Got configuration: {mcg_obj.s3_client.get_bucket_lifecycle_configuration(Bucket=bucket)}"
        )

        logger.info(f"Writing {object_key} to bucket: {bucket}")
        assert s3_put_object(
            s3_obj=mcg_obj, bucketname=bucket, object_key=object_key, data=obj_data
        ), "Failed: Put Object"

        logger.info("Waiting for 1 day + 8 hours buffer time")
        sleep(((expiration_days * 24) + buffer_time_in_hours) * 60 * 60)

        logger.info(f"Getting {object_key} from bucket: {bucket} after 1 day + 8 hours")
        try:
            s3_get_object(s3_obj=mcg_obj, bucketname=bucket, object_key=object_key)
        except Exception:
            logger.info(
                f"Test passed, object {object_key} got deleted after expiration + buffer time"
            )
        else:
            assert (
                False
            ), f"Test failed, object {object_key} didn't get deleted after expiration + buffer time"

    @system_test
    def test_object_expiration_with_disruptions(
        self,
        mcg_obj,
        multi_obc_setup_factory,
        awscli_pod_session,
        nodes,
        snapshot_factory,
        setup_mcg_bg_features,
        validate_mcg_bg_feature,
        noobaa_db_backup_and_recovery,
        noobaa_db_backup_and_recovery_locally,
        change_noobaa_lifecycle_interval,
        node_drain_teardown,
    ):

        """
        Test object expiration feature when there are some sort of disruption to the noobaa
        like node drain, node restart, nb db recovery etc

        """
        change_noobaa_lifecycle_interval(interval=2)
        expiration_days = 1
        expire_rule = {
            "Rules": [
                {
                    "Expiration": {
                        "Days": expiration_days,
                        "ExpiredObjectDeleteMarker": False,
                    },
                    "Filter": {"Prefix": ""},
                    "ID": "data-expire",
                    "Status": "Enabled",
                }
            ]
        }

        expire_rule_prefix = deepcopy(expire_rule)
        number_of_buckets = 50

        # Entry criteria
        mcg_sys_dict, kafka_rgw_dict = setup_mcg_bg_features()

        # Create bulk buckets with expiry rule and no prefix set
        logger.info(
            f"Creating first set of {number_of_buckets} buckets with no-prefix expiry rule"
        )
        buckets_without_prefix = multi_obc_setup_factory(
            num_obcs=number_of_buckets,
            expiration_rule=expire_rule,
            type_of_bucket=["data"],
        )

        # Create another set of bulk buckets with expiry rule and prefix set
        logger.info(
            f"Create second set of {number_of_buckets} buckets with prefix 'others' expiry rule"
        )
        expire_rule_prefix["Rules"][0]["Filter"]["Prefix"] = "others"
        buckets_with_prefix = multi_obc_setup_factory(
            num_obcs=number_of_buckets,
            expiration_rule=expire_rule_prefix,
            type_of_bucket=["data"],
        )

        from botocore.exceptions import ClientError
        from ocs_ci.utility.retry import retry

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
            logger.info("For each buckets, change the creation time of the objects")
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
                "For each second set of buckets, change the creation time of the objects"
            )
            for bucket in buckets_with_prefix:
                expire_objects_in_bucket(bucket_name=bucket.name)

        def sample_if_objects_expired():
            def check_if_objects_expired(mcg_obj, bucket_name, prefix=""):
                response = s3_list_objects_v2(
                    mcg_obj, bucketname=bucket_name, prefix=prefix, delimiter="/"
                )
                if response["KeyCount"] != 0:
                    return False
                return True

            logger.info(
                "All the objects in the first set of buckets should be deleted irrespective of the prefix"
            )
            for bucket in buckets_without_prefix:
                sampler = TimeoutSampler(
                    timeout=600,
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
        wait_for_storage_pods()

        # check if the objects are expired
        sample_if_objects_expired()

        # upload obejcts again
        upload_objects_and_expire()

        # Perform noobaa db backup and recovery
        noobaa_db_backup_and_recovery(snapshot_factory=snapshot_factory)
        wait_for_storage_pods()
        self.sanity_helpers.health_check(tries=120)

        sample_if_objects_expired()

        upload_objects_and_expire()

        # Perform noobaa db recovery locally
        noobaa_db_backup_and_recovery_locally()
        wait_for_storage_pods()

        sample_if_objects_expired()

        # validate mcg entry criteria post test
        validate_mcg_bg_feature(mcg_sys_dict, kafka_rgw_dict)
