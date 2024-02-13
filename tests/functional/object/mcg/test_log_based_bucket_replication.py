import logging

import pytest

from ocs_ci.framework.pytest_customization.marks import mcg, red_squad
from ocs_ci.framework.testlib import (
    MCGTest,
    ignore_leftover_label,
    polarion_id,
    runs_on_provider,
    skipif_aws_creds_are_missing,
    skipif_disconnected_cluster,
    skipif_vsphere_ipi,
    tier1,
    tier2,
    tier3,
    tier4b,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.bucket_utils import (
    compare_bucket_object_list,
    update_replication_policy,
)
from ocs_ci.ocs.resources.mcg_replication_policy import AwsLogBasedReplicationPolicy
from ocs_ci.ocs.resources.mockup_bucket_logger import MockupBucketLogger
from ocs_ci.ocs.resources.pod import get_noobaa_pods, get_pod_node
from ocs_ci.ocs.scale_noobaa_lib import noobaa_running_node_restart

logger = logging.getLogger(__name__)


@mcg
@red_squad
@runs_on_provider
@ignore_leftover_label(constants.MON_APP_LABEL)  # tier4b test requirement
@skipif_aws_creds_are_missing
@skipif_disconnected_cluster
class TestLogBasedBucketReplication(MCGTest):
    """
    Test log-based replication with deletion sync.

    Log-based replication requires reading AWS bucket logs from an AWS bucket in the same region as the source bucket.
    As these logs may take several hours to become available, this test suite utilizes MockupBucketLogger to upload
    mockup logs for each I/O operation performed on the source bucket to a dedicated log bucket on AWS.

    """

    DEFAULT_AWS_REGION = "us-east-2"
    DEFAULT_TIMEOUT = 10 * 60

    @pytest.fixture(scope="class", autouse=True)
    def reduce_replication_delay_setup(self, add_env_vars_to_noobaa_core_class):
        """
        A fixture to reduce the replication delay to one minute.

        Args:
            new_delay_in_miliseconds (function): A function to add env vars to the noobaa-core pod

        """
        new_delay_in_miliseconds = 60 * 1000
        new_env_var_touples = [
            (constants.BUCKET_REPLICATOR_DELAY_PARAM, new_delay_in_miliseconds),
            (constants.BUCKET_LOG_REPLICATOR_DELAY_PARAM, new_delay_in_miliseconds),
        ]
        add_env_vars_to_noobaa_core_class(new_env_var_touples)

    @pytest.fixture()
    def log_based_replication_setup(
        self, awscli_pod_session, mcg_obj_session, bucket_factory
    ):
        """
        A fixture to set up standard log-based replication with deletion sync.

        Args:
            awscli_pod_session(Pod): A pod running the AWS CLI
            mcg_obj_session(MCG): An MCG object
            bucket_factory: A bucket factory fixture

        Returns:
            MockupBucketLogger: A MockupBucketLogger object
            Bucket: The source bucket
            Bucket: The target bucket
        """

        logger.info("Starting log-based replication setup")

        bucketclass_dict = {
            "interface": "OC",
            "namespace_policy_dict": {
                "type": "Single",
                "namespacestore_dict": {
                    constants.AWS_PLATFORM: [(1, self.DEFAULT_AWS_REGION)]
                },
            },
        }
        target_bucket = bucket_factory(bucketclass=bucketclass_dict)[0]

        mockup_logger = MockupBucketLogger(
            awscli_pod=awscli_pod_session,
            mcg_obj=mcg_obj_session,
            bucket_factory=bucket_factory,
            platform=constants.AWS_PLATFORM,
            region=self.DEFAULT_AWS_REGION,
        )
        replication_policy = AwsLogBasedReplicationPolicy(
            destination_bucket=target_bucket.name,
            sync_deletions=True,
            logs_bucket=mockup_logger.logs_bucket_uls_name,
        )

        source_bucket = bucket_factory(
            1, bucketclass=bucketclass_dict, replication_policy=replication_policy
        )[0]

        logger.info("log-based replication setup complete")

        return mockup_logger, source_bucket, target_bucket

    @tier1
    @polarion_id("OCS-4936")
    def test_deletion_sync(self, mcg_obj_session, log_based_replication_setup):
        """
        Test log-based replication with deletion sync.

        1. Upload a set of objects to the source bucket
        2. Wait for the objects to be replicated to the target bucket
        3. Delete all objects from the source bucket
        4. Wait for the objects to be deleted from the target bucket

        """
        mockup_logger, source_bucket, target_bucket = log_based_replication_setup

        upload_test_objects_to_source_and_wait_for_replication(
            mcg_obj_session,
            source_bucket,
            target_bucket,
            mockup_logger,
            self.DEFAULT_TIMEOUT,
        )

        delete_objects_from_source_and_wait_for_deletion_sync(
            mcg_obj_session,
            source_bucket,
            target_bucket,
            mockup_logger,
            self.DEFAULT_TIMEOUT,
        )

    @tier1
    @polarion_id("OCS-4937")
    def test_deletion_sync_opt_out(self, mcg_obj_session, log_based_replication_setup):
        """
        Test that deletion sync can be disabled.

        1. Upload a set of objects to the source bucket
        2. Wait for the objects to be replicated to the target bucket
        3. Disable deletion sync
        4. Delete all objects from the source bucket
        5. Verify that the objects are not deleted from the target bucket

        """
        mockup_logger, source_bucket, target_bucket = log_based_replication_setup

        upload_test_objects_to_source_and_wait_for_replication(
            mcg_obj_session,
            source_bucket,
            target_bucket,
            mockup_logger,
            self.DEFAULT_TIMEOUT,
        )

        logger.info("Disabling the deletion sync")
        disabled_del_sync_policy = source_bucket.replication_policy
        disabled_del_sync_policy["rules"][0]["sync_deletions"] = False
        update_replication_policy(source_bucket.name, disabled_del_sync_policy)

        logger.info("Deleting source objects and verifying they remain on target")
        mockup_logger.delete_all_objects_and_log(source_bucket.name)
        assert not compare_bucket_object_list(
            mcg_obj_session,
            source_bucket.name,
            target_bucket.name,
            timeout=self.DEFAULT_TIMEOUT,
        ), "Deletion sync completed even though the policy was disabled!"

    @tier2
    @polarion_id("OCS-4941")
    def test_patch_deletion_sync_to_existing_bucket(
        self, awscli_pod_session, mcg_obj_session, bucket_factory
    ):
        """
        Test patching deletion sync onto an existing bucket.

        1. Create a source bucket
        2. Create a target bucket
        3. Patch the source bucket with a replication policy that includes deletion sync
        4. Upload a set of objects to the source bucket
        5. Wait for the objects to be replicated to the target bucket
        6. Delete all objects from the source bucket
        7. Wait for the objects to be deleted from the target bucket

        """

        logger.info("Creating source and target buckets")
        bucketclass_dict = {
            "interface": "OC",
            "namespace_policy_dict": {
                "type": "Single",
                "namespacestore_dict": {
                    constants.AWS_PLATFORM: [(1, self.DEFAULT_AWS_REGION)]
                },
            },
        }
        target_bucket = bucket_factory(bucketclass=bucketclass_dict)[0]
        source_bucket = bucket_factory(bucketclass=bucketclass_dict)[0]

        logger.info("Patching the policy to the source bucket")
        mockup_logger = MockupBucketLogger(
            awscli_pod=awscli_pod_session,
            mcg_obj=mcg_obj_session,
            bucket_factory=bucket_factory,
            platform=constants.AWS_PLATFORM,
            region=self.DEFAULT_AWS_REGION,
        )
        replication_policy = AwsLogBasedReplicationPolicy(
            destination_bucket=target_bucket.name,
            sync_deletions=True,
            logs_bucket=mockup_logger.logs_bucket_uls_name,
        )
        update_replication_policy(source_bucket.name, replication_policy.to_dict())

        upload_test_objects_to_source_and_wait_for_replication(
            mcg_obj_session,
            source_bucket,
            target_bucket,
            mockup_logger,
            self.DEFAULT_TIMEOUT,
        )

        # Deletion sync has shown to take longer in this scenario, so we double the timeout
        delete_objects_from_source_and_wait_for_deletion_sync(
            mcg_obj_session,
            source_bucket,
            target_bucket,
            mockup_logger,
            self.DEFAULT_TIMEOUT * 2,
        )

    @tier3
    @polarion_id("OCS-4940")
    def test_deletion_sync_after_instant_deletion(
        self, mcg_obj_session, log_based_replication_setup
    ):
        """
        Test deletion sync behavior when an object is immediately deleted after being uploaded to the source bucket.

        1. Upload an object to the source bucket
        2. Delete the object from the source bucket
        3. Upload a set of objects to the source bucket
        4. Wait for the objects to be replicated to the target bucket
        5. Delete all objects from the source bucket
        6. Wait for the objects to be deleted from the target bucket

        """
        mockup_logger, source_bucket, target_bucket = log_based_replication_setup

        logger.info(
            "Uploading an object to the source bucket then immediately deleting it"
        )
        mockup_logger.upload_arbitrary_object_and_log(source_bucket.name)
        mockup_logger.delete_all_objects_and_log(source_bucket.name)

        upload_test_objects_to_source_and_wait_for_replication(
            mcg_obj_session,
            source_bucket,
            target_bucket,
            mockup_logger,
            self.DEFAULT_TIMEOUT,
        )

        delete_objects_from_source_and_wait_for_deletion_sync(
            mcg_obj_session,
            source_bucket,
            target_bucket,
            mockup_logger,
            self.DEFAULT_TIMEOUT,
        )

    _nodes_tested = []

    @tier4b
    @skipif_vsphere_ipi
    @pytest.mark.parametrize(
        argnames=["target_pod_name"],
        argvalues=[
            pytest.param(
                "noobaa-db",
                marks=pytest.mark.polarion_id("OCS-4938"),
            ),
            pytest.param(
                "noobaa-core",
                marks=pytest.mark.polarion_id("OCS-4939"),
            ),
        ],
    )
    def test_deletion_sync_after_node_restart(
        self, mcg_obj_session, log_based_replication_setup, target_pod_name
    ):
        """
        Test deletion sync behavior after a node restart.

        1. Check if the node that the target pod is located on has already passed this test with a previous param
            1.1 If it has, skip the rest of the test and pass
        2. Upload a set of objects to the source bucket
        3. Wait for the objects to be replicated to the target bucket
        4. Restart the node that the source bucket is located on
        5. Delete all objects from the source bucket
        6. Verify that the objects are deleted from the target bucket

        """
        mockup_logger, source_bucket, target_bucket = log_based_replication_setup

        # Skip the rest of the test and pass if the target pod's node
        # was already reset with a previous passing parametrization of this test
        logger.info(
            f"Checking if {target_pod_name}'s node has already passed this test with a previous param"
        )
        target_pod = [
            pods for pods in get_noobaa_pods() if target_pod_name in pods.name
        ][0]
        target_node_name = get_pod_node(target_pod).name
        if target_node_name in self._nodes_tested:
            logger.info(
                f"Skipping the rest of the test because {target_pod_name}'s node has already passed this test"
            )
            return
        else:
            logger.info(f"{target_pod_name}'s node has not passed this test yet")

        upload_test_objects_to_source_and_wait_for_replication(
            mcg_obj_session,
            source_bucket,
            target_bucket,
            mockup_logger,
            self.DEFAULT_TIMEOUT,
        )

        logger.info(f"Restarting {target_pod_name}'s node")
        noobaa_running_node_restart(pod_name=target_pod_name)

        delete_objects_from_source_and_wait_for_deletion_sync(
            mcg_obj_session,
            source_bucket,
            target_bucket,
            mockup_logger,
            self.DEFAULT_TIMEOUT,
        )

        # Keep track of the target node to prevent its redundant testing in this scenario
        self._nodes_tested.append(target_node_name)


def upload_test_objects_to_source_and_wait_for_replication(
    mcg_obj, source_bucket, target_bucket, mockup_logger, timeout
):
    """
    Upload a set of objects to the source bucket, logs the operations and wait for the replication to complete.

    """
    logger.info("Uploading test objects and waiting for replication to complete")
    mockup_logger.upload_test_objs_and_log(source_bucket.name)

    logger.info(
        "Resetting the noobaa-core pod to trigger the replication background worker"
    )

    assert compare_bucket_object_list(
        mcg_obj,
        source_bucket.name,
        target_bucket.name,
        timeout=timeout,
    ), f"Standard replication failed to complete in {timeout} seconds"


def delete_objects_from_source_and_wait_for_deletion_sync(
    mcg_obj, source_bucket, target_bucket, mockup_logger, timeout
):
    """
    Delete all objects from the source bucket,logs the operations and wait for the deletion sync to complete.

    """
    logger.info("Deleting source objects and waiting for deletion sync with target")
    mockup_logger.delete_all_objects_and_log(source_bucket.name)

    logger.info(
        "Resetting the noobaa-core pod to trigger the replication background worker"
    )

    assert compare_bucket_object_list(
        mcg_obj,
        source_bucket.name,
        target_bucket.name,
        timeout=timeout,
    ), f"Deletion sync failed to complete in {timeout} seconds"
