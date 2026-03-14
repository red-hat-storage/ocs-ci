import logging
import uuid
from datetime import datetime, timedelta
from time import sleep

import pytest

from ocs_ci.helpers.e2e_helpers import create_muliple_types_provider_obcs

from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.ocs.resources.mcg_lifecycle_policies import (
    LifecyclePolicy,
    ExpirationRule,
    NoncurrentVersionExpirationRule,
)
from ocs_ci.ocs.resources.mcg_replication_policy import ReplicationPolicyWithVersioning
from ocs_ci.utility.retry import retry
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import (
    system_test,
    magenta_squad,
)
from ocs_ci.ocs.bucket_utils import (
    s3_put_object,
    expire_objects_in_bucket,
    s3_list_objects_v2,
    bulk_s3_put_bucket_lifecycle_config,
    write_random_test_objects_to_bucket,
    compare_bucket_object_list,
    update_replication_policy,
    upload_obj_versions,
    get_obj_versions,
    wait_for_object_versions_match,
    sample_if_objects_expired,
    change_versions_creation_date_in_noobaa_db,
)
from ocs_ci.ocs.resources.pod import (
    wait_for_noobaa_pods_running,
)
from ocs_ci.ocs.node import (
    wait_for_nodes_status,
    get_node_objs,
)
from ocs_ci.utility.utils import TimeoutSampler, get_primary_nb_db_pod

logger = logging.getLogger(__name__)


class TestMCGReplicationLifecycleWithDisruptions:
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
    def test_mcg_replication_lifecycle_with_disruptions(
        self,
        mcg_obj,
        scale_noobaa_resources_session,
        setup_mcg_bg_features,
        validate_mcg_bg_features,
        awscli_pod_session,
        nodes,
        bucket_factory,
        noobaa_db_backup_and_recovery_locally,
        validate_noobaa_rebuild_system,
        bucket_factory_session,
        node_drain_teardown,
        node_restart_teardown,
        mcg_obj_session,
        cld_mgr,
        source_bucketclass,
        target_bucketclass,
        test_directory_setup,
        awscli_pod,
        make_buckets_with_versioning,
        bucket_pairs_amount,
        reduce_expiration_interval,
    ):
        """
        Test object expiration feature when there are some sort of disruption to the noobaa
        like node drain, node restart, nb db recovery etc

        """
        # 1.  entry criteria setup
        feature_setup_map = setup_mcg_bg_features(
            num_of_buckets=5,
            object_amount=5,
            is_disruptive=True,
            skip_any_features=["nsfs", "rgw kafka", "caching"],
        )

        # 2. Create two namespace buckets for replication verification
        # [Enable uni-directional bucket replication with no deletion sync] test_replication_with_disruptions
        # check uni bucket replication from multi (aws+azure) namespace bucket to s3-compatible namespace bucket
        prefix_site_1 = "site1"
        rep_target_bucket_name_1 = bucket_factory(bucketclass=target_bucketclass)[
            0
        ].name
        replication_policy = (
            "basic-replication-rule",
            rep_target_bucket_name_1,
            prefix_site_1,
        )
        rep_source_bucket_name_1 = bucket_factory(
            bucketclass=source_bucketclass, replication_policy=replication_policy
        )[0].name
        written_random_objects = write_random_test_objects_to_bucket(
            awscli_pod_session,
            rep_source_bucket_name_1,
            test_directory_setup.origin_dir,
            mcg_obj=mcg_obj_session,
            amount=5,
            pattern="first-write-",
            prefix=prefix_site_1,
        )
        logger.info(f"Written objects: {written_random_objects}")

        assert compare_bucket_object_list(
            mcg_obj_session, rep_source_bucket_name_1, rep_target_bucket_name_1
        )
        logger.info("Uni-directional bucket replication working as expected")

        # 3. Create a data bucket for expiration verification, test_object_expiration------
        reduce_expiration_interval(interval=2)

        # Creating S3 bucket
        exp_bucket_1 = bucket_factory()[0].name
        object_key = "ObjKey-" + str(uuid.uuid4().hex)
        obj_data = "Random data" + str(uuid.uuid4().hex)
        lifecycle_policy = LifecyclePolicy(ExpirationRule(days=1))
        mcg_obj.s3_client.put_bucket_lifecycle_configuration(
            Bucket=exp_bucket_1, LifecycleConfiguration=lifecycle_policy.as_dict()
        )

        PROP_SLEEP_TIME = 10
        logger.info(
            f"Sleeping for {PROP_SLEEP_TIME} seconds to let the policy propagate"
        )
        sleep(PROP_SLEEP_TIME)

        logger.info(
            f"Getting object expiration configuration from bucket: {exp_bucket_1}"
        )
        logger.info(
            f"Got configuration: {mcg_obj.s3_client.get_bucket_lifecycle_configuration(Bucket=exp_bucket_1)}"
        )

        logger.info(f"Writing {object_key} to bucket: {exp_bucket_1}")
        assert s3_put_object(
            s3_obj=mcg_obj,
            bucketname=exp_bucket_1,
            object_key=object_key,
            data=obj_data,
        ), "Failed: Put Object"

        expire_objects_in_bucket(exp_bucket_1)

        sampler = TimeoutSampler(
            timeout=600,
            sleep=10,
            func=self.check_if_objects_expired,
            mcg_obj=mcg_obj,
            bucket_name=exp_bucket_1,
        )
        assert sampler.wait_for_func_status(
            result=True
        ), f"Objects in the bucket {exp_bucket_1} are not expired"
        logger.info("Objects in the bucket are expired as expected")

        # 4. Create a data bucket for versioning verification. test_bucket_replication_with_versioning-------
        # Create source and target buckets with versioning enabled
        obj_key = "test_obj_" + str(uuid.uuid4())[:4]
        versions_amount = 5
        bucket_pairs = []
        for _ in range(bucket_pairs_amount):
            ver_source_bucket_1, ver_target_bucket_1 = make_buckets_with_versioning(2)
            bucket_pairs.append((ver_source_bucket_1, ver_target_bucket_1))

        # Set bucket replication policies with versioning enabled on the source buckets
        for ver_source_bucket_1, ver_target_bucket_1 in bucket_pairs:
            replication_policy = ReplicationPolicyWithVersioning(
                target_bucket=ver_target_bucket_1.name
            )
            update_replication_policy(
                ver_source_bucket_1.name, replication_policy.to_dict()
            )

        # Write some versions to the source buckets
        for ver_source_bucket_1, _ in bucket_pairs:
            upload_obj_versions(
                mcg_obj,
                awscli_pod,
                ver_source_bucket_1.name,
                obj_key=obj_key,
                amount=versions_amount,
            )
            source_etags = get_obj_versions(
                mcg_obj, awscli_pod, ver_source_bucket_1.name, obj_key
            )
            logger.info(f"Uploaded versions with etags: {source_etags}")

        # Verify the versions were replicated to the target buckets in the same order
        for ver_source_bucket_1, ver_target_bucket_1 in bucket_pairs:
            wait_for_object_versions_match(
                mcg_obj,
                awscli_pod,
                ver_source_bucket_1.name,
                ver_target_bucket_1.name,
                obj_key,
            )
        logger.info("All the versions were replicated successfully")

        # 5. Perform noobaa db backup and recovery locally
        noobaa_db_backup_and_recovery_locally()
        wait_for_noobaa_pods_running(timeout=1200)

        # Verify replication works
        assert compare_bucket_object_list(
            mcg_obj_session, rep_source_bucket_name_1, rep_target_bucket_name_1
        )
        logger.info("Objects sync works even when the cluster is rebooted")

        # verify the expiration works
        sample_if_objects_expired(
            mcg_obj_session, exp_bucket_1.name, timeout=36000, sleep=60
        )

        # verify the object versions
        # Upload new versions under a different object key to the source bucket
        obj_key = "test_obj_" + str(uuid.uuid4())[:4]
        for ver_source_bucket_1, _ in bucket_pairs:
            upload_obj_versions(
                mcg_obj,
                awscli_pod,
                ver_source_bucket_1.name,
                obj_key=obj_key,
                amount=versions_amount,
            )
            source_etags = get_obj_versions(
                mcg_obj, awscli_pod, ver_source_bucket_1.name, obj_key
            )
            logger.info(f"Uploaded versions with etags: {source_etags}")

        # Verify the versions were replicated to the target buckets in the same order
        for ver_source_bucket_1, ver_target_bucket_1 in bucket_pairs:
            wait_for_object_versions_match(
                mcg_obj,
                awscli_pod,
                ver_source_bucket_1.name,
                ver_target_bucket_1.name,
                obj_key,
            )
        logger.info("All the versions were replicated successfully")

        # 9. Do a complete noobaa rebuild
        validate_noobaa_rebuild_system(bucket_factory_session, mcg_obj_session)

        # 10. Redo the steps 3 for creating buckets.
        # 2. Create two namespace buckets for replication verification
        # [Enable uni-directional bucket replication with no deletion sync] test_replication_with_disruptions
        # check uni bucket replication from multi (aws+azure) namespace bucket to s3-compatible namespace bucket
        prefix_site_1 = "site1"
        rep_target_bucket_name_2 = bucket_factory(bucketclass=target_bucketclass)[
            0
        ].name
        replication_policy = (
            "basic-replication-rule",
            rep_target_bucket_name_2,
            prefix_site_1,
        )
        rep_source_bucket_name_2 = bucket_factory(
            bucketclass=source_bucketclass, replication_policy=replication_policy
        )[0].name
        written_random_objects = write_random_test_objects_to_bucket(
            awscli_pod_session,
            rep_source_bucket_name_2,
            test_directory_setup.origin_dir,
            mcg_obj=mcg_obj_session,
            amount=5,
            pattern="first-write-",
            prefix=prefix_site_1,
        )
        logger.info(f"Written objects: {written_random_objects}")

        assert compare_bucket_object_list(
            mcg_obj_session, rep_source_bucket_name_2, rep_target_bucket_name_2
        )
        logger.info("Uni-directional bucket replication working as expected")

        # 3. Create a data bucket for expiration verification, test_object_expiration------
        reduce_expiration_interval(interval=2)
        key = "test_obj"
        older_versions_amount = 5
        newer_versions_amount = 7

        # Creating S3 bucket
        exp_bucket_2 = bucket_factory()[0].name
        lifecycle_policy = LifecyclePolicy(
            NoncurrentVersionExpirationRule(
                non_current_days=older_versions_amount,
                newer_non_current_versions=newer_versions_amount,
            )
        )
        mcg_obj.s3_client.put_bucket_lifecycle_configuration(
            Bucket=exp_bucket_2, LifecycleConfiguration=lifecycle_policy.as_dict()
        )
        PROP_SLEEP_TIME = 10
        logger.info(
            f"Sleeping for {PROP_SLEEP_TIME} seconds to let the policy propagate"
        )
        sleep(PROP_SLEEP_TIME)

        # Upload versions
        # older versions + newer versions + the current version
        amount = 2 * older_versions_amount + 1
        upload_obj_versions(
            mcg_obj,
            awscli_pod,
            exp_bucket_2,
            key,
            amount=amount,
        )

        #  Manually set the age of each version to be one day older than its successor
        uploaded_versions = get_obj_versions(mcg_obj, awscli_pod, exp_bucket_2, key)
        version_ids = [version["VersionId"] for version in uploaded_versions]

        # Parse the timestamp from the first version
        mongodb_style_time = uploaded_versions[0]["LastModified"]
        iso_timestamp = mongodb_style_time.replace("Z", "+00:00")
        latest_version_creation_date = datetime.fromisoformat(iso_timestamp)

        for i, version_id in enumerate(version_ids):
            change_versions_creation_date_in_noobaa_db(
                bucket_name=exp_bucket_2,
                object_key=key,
                version_ids=[version_id],
                new_creation_time=(
                    latest_version_creation_date - timedelta(days=i)
                ).timestamp(),
            )

        # Wait for versions to expire
        # While older_versions_amount versions qualify for deletion due to
        # NoncurrentDays, the lifecycle policy should keep the NewerNoncurrentVersions
        # amount of versions.
        expected_remaining = set(
            version_ids[: newer_versions_amount + 1]
        )  # +1 for the current version

        for versions in TimeoutSampler(
            timeout=600,
            sleep=30,
            func=get_obj_versions,
            mcg_obj=mcg_obj,
            awscli_pod=awscli_pod,
            bucket_name=exp_bucket_2,
            obj_key=key,
        ):
            remaining = {v["VersionId"] for v in versions}
            # Expected end result
            if remaining == expected_remaining:
                logger.info("Only the expected versions remained")
                break
            # Newer versions were deleted
            elif not (expected_remaining <= remaining):
                raise UnexpectedBehaviour(
                    (
                        "Some versions were deleted when they shouldn't have!"
                        f"Versions that were deleted: {expected_remaining - remaining}"
                    )
                )
            # Some older versions are yet to be deleted
            else:
                logger.warning(
                    (
                        "Some older versions have not expired yet:\n"
                        f"Remaining: {remaining}\n"
                        f"Versions yet to expire: {remaining - expected_remaining}"
                    )
                )

        # 4. Create a data bucket for versioning verification. test_bucket_replication_with_versioning-------
        # Create source and target buckets with versioning enabled
        obj_key = "test_obj_" + str(uuid.uuid4())[:4]
        versions_amount = 3
        bucket_pairs = []
        for _ in range(bucket_pairs_amount):
            ver_source_bucket_2, ver_target_bucket_2 = make_buckets_with_versioning(2)
            bucket_pairs.append((ver_source_bucket_2, ver_target_bucket_2))

        # Set bucket replication policies with versioning enabled on the source buckets
        for ver_source_bucket_2, ver_target_bucket_2 in bucket_pairs:
            replication_policy = ReplicationPolicyWithVersioning(
                target_bucket=ver_target_bucket_2.name
            )
            update_replication_policy(
                ver_source_bucket_2.name, replication_policy.to_dict()
            )

        # Write some versions to the source buckets
        for ver_source_bucket_2, _ in bucket_pairs:
            upload_obj_versions(
                mcg_obj,
                awscli_pod,
                ver_source_bucket_2.name,
                obj_key=obj_key,
                amount=versions_amount,
            )
            source_etags = get_obj_versions(
                mcg_obj, awscli_pod, ver_source_bucket_2.name, obj_key
            )
            logger.info(f"Uploaded versions with etags: {source_etags}")

        # Verify the versions were replicated to the target buckets in the same order
        for ver_source_bucket_2, ver_target_bucket_2 in bucket_pairs:
            wait_for_object_versions_match(
                mcg_obj,
                awscli_pod,
                ver_source_bucket_2.name,
                ver_target_bucket_2.name,
                obj_key,
            )
        logger.info("All the versions were replicated successfully")

        # 15. Shutdown the node where primary noobaa db pod is running
        logger.info("Shutdown noobaa db pod node")
        nb_db_pod = get_primary_nb_db_pod()()
        nb_db_pod_node = nb_db_pod.get_node()
        nodes.stop_nodes(nodes=get_node_objs([nb_db_pod_node]))
        wait_for_nodes_status(
            node_names=[nb_db_pod_node],
            status=constants.NODE_NOT_READY,
            timeout=300,
        )
        # Turn on the primary noobaa db pod node
        logger.info("Turn on the noobaa db pod node")
        nodes.start_nodes(nodes=get_node_objs([nb_db_pod_node]))
        wait_for_nodes_status(
            node_names=[nb_db_pod_node],
            status=constants.NODE_READY,
            timeout=300,
        )
        wait_for_noobaa_pods_running(timeout=1200)

        # Verify replication works
        assert compare_bucket_object_list(
            mcg_obj_session, rep_source_bucket_name_2, rep_target_bucket_name_2
        )
        logger.info("Objects sync works even when the cluster is rebooted")

        # verify the expiration works
        sample_if_objects_expired(
            mcg_obj_session, exp_bucket_2.name, timeout=36000, sleep=60
        )

        # verify the object versions
        # Upload new versions under a different object key to the source bucket
        obj_key = "test_obj_" + str(uuid.uuid4())[:4]
        for ver_source_bucket_2, _ in bucket_pairs:
            upload_obj_versions(
                mcg_obj,
                awscli_pod,
                ver_source_bucket_2.name,
                obj_key=obj_key,
                amount=versions_amount,
            )
            source_etags = get_obj_versions(
                mcg_obj, awscli_pod, ver_source_bucket_2.name, obj_key
            )
            logger.info(f"Uploaded versions with etags: {source_etags}")

        # Verify the versions were replicated to the target buckets in the same order
        for ver_source_bucket_2, ver_target_bucket_2 in bucket_pairs:
            wait_for_object_versions_match(
                mcg_obj,
                awscli_pod,
                ver_source_bucket_2.name,
                ver_target_bucket_2.name,
                obj_key,
            )
        logger.info("All the versions were replicated successfully")

        # To do: step 18 to 25

        # Verify IO's for the background feature setup
        # validate mcg entry criteria post test
        retry(Exception, tries=5, delay=10)(validate_mcg_bg_features)(
            feature_setup_map,
            run_in_bg=False,
            skip_any_features=["nsfs", "rgw kafka", "caching"],
            object_amount=5,
        )

        logger.info("No issues seen with the MCG bg feature validation")
