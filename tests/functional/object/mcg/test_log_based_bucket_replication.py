import logging

from ocs_ci.helpers.helpers import craft_s3_command
import pytest

from ocs_ci.framework.pytest_customization.marks import mcg, red_squad, bugzilla
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

    @pytest.mark.parametrize(
        argnames=["platform"],
        argvalues=[
            pytest.param(
                constants.AWS_PLATFORM,
                marks=[tier1, polarion_id("OCS-4936")],
            ),
        ],
    )
    def test_deletion_sync(self, platform, log_based_replication_handler_factory):
        """
        Test log-based replication with deletion sync:

        1. Setup log-based replication with deletion sync between two buckets
        2. Upload a set of objects to the source bucket and wait for the replication to complete
        3. Delete all objects from the source bucket and wait for the deletion sync to complete
        """
        replication_handler = log_based_replication_handler_factory(platform)

        replication_handler.upload_random_objects_to_source(amount=10)
        assert replication_handler.wait_for_sync(
            timeout=self.DEFAULT_TIMEOUT
        ), f"Replication failed to complete in {self.DEFAULT_TIMEOUT} seconds"

        replication_handler.delete_recursively_from_source()
        assert replication_handler.wait_for_sync(
            timeout=self.DEFAULT_TIMEOUT
        ), f"Deletion sync failed to complete in {self.DEFAULT_TIMEOUT} seconds"

    @pytest.mark.parametrize(
        argnames=["platform"],
        argvalues=[
            pytest.param(
                constants.AWS_PLATFORM,
                marks=[tier1, polarion_id("OCS-4937")],
            ),
        ],
    )
    def test_deletion_sync_opt_out(
        self, platform, log_based_replication_handler_factory
    ):
        """
        Test that deletion sync can be disabled:

        1. Setup log-based replication with deletion sync between two buckets
        2. Upload a set of objects to the source bucket
        3. Wait for the objects to be replicated to the target bucket
        4. Disable deletion sync by patching the replication policy
        5. Delete all objects from the source bucket
        6. Verify that the objects are not deleted from the target bucket
        """
        replication_handler = log_based_replication_handler_factory(platform)

        replication_handler.upload_random_objects_to_source(amount=10)
        assert replication_handler.wait_for_sync(
            timeout=self.DEFAULT_TIMEOUT
        ), f"Replication failed to complete in {self.DEFAULT_TIMEOUT} seconds"

        logger.info("Disabling the deletion sync")
        replication_handler.deletion_sync_enabled = False
        replication_handler.delete_recursively_from_source()

        assert not replication_handler.wait_for_sync(
            timeout=180
        ), "Deletion sync completed even though the policy was disabled!"

    @bugzilla("2281729")
    @pytest.mark.parametrize(
        argnames=["platform"],
        argvalues=[
            pytest.param(
                constants.AWS_PLATFORM,
                marks=[tier2],
            ),
        ],
    )
    def test_deletion_sync_with_prefix(
        self, platform, log_based_replication_handler_factory, test_directory_setup
    ):
        """
        Test deletion sync with a prefix:

        1. Setup log-based replication with deletion sync between two buckets
        2. Patch the policy to sync only a specific prefix
        3. Upload objects to two prefixes - one that should be synced and one that shouldn't
        4. Wait for the objects under the synced prefix to be replicated
        5. Make sure that the objects under the other prefix were not replicated
        6. Copy the objects that were not deleted to the same prefix on the target bucket
        7. Delete all the objects on both prefixes from the source bucket
        8. Wait for the objects with the prefix to be deleted from the target bucket
        9. Make sure the objects without the prefix were not deleted from the target bucket
        """
        # 1. Setup log-based replication with deletion sync between two buckets
        replication_handler = log_based_replication_handler_factory(platform)
        # 2. Patch the policy to sync only a specific prefix
        replication_handler.policy_prefix_filter = "synced_prefix"

        # 3. Upload objects to two prefixs - one that should be synced and one that shouldn't
        replication_handler.upload_random_objects_to_source(
            amount=10, prefix="synced_prefix"
        )
        replication_handler.upload_random_objects_to_source(
            amount=10, prefix="other_prefix"
        )

        # 4. Wait for the objects under the synced prefix to be replicated
        assert replication_handler.wait_for_sync(
            timeout=self.DEFAULT_TIMEOUT, prefix="synced_prefix"
        ), f"Replication failed to complete in {self.DEFAULT_TIMEOUT} seconds"

        # 5. Make sure that the objects under the other prefix were not replicated
        assert not replication_handler.wait_for_sync(
            timeout=120, prefix="other_prefix"
        ), "Replication has completed for the wrong prefix"

        # 6. Copy the objects that were not deleted to the same prefix on the target bucket
        cp_cmd = f"cp s3://{replication_handler.source_bucket}/other_prefix"
        cp_cmd += f" s3://{replication_handler.target_bucket}/synced_prefix --recursive"
        replication_handler.awscli_pod.exec_cmd_on_pod(craft_s3_command(cp_cmd))

        # 7. Delete all the objects on both prefixes from the source bucket
        replication_handler.delete_recursively_from_source()

        # 8. Wait for the objects with the prefix to be deleted
        # form the target bucket
        assert replication_handler.wait_for_sync(
            timeout=self.DEFAULT_TIMEOUT, prefix="synced_prefix"
        ), f"Deletion sync failed to complete in {self.DEFAULT_TIMEOUT} seconds"

        # 9. Make sure the objects without the prefix were not deleted
        # from the target bucket
        assert not replication_handler.wait_for_sync(
            timeout=120, prefix="other_prefix"
        ), "Deletion sync also deleted objects it shouldn't have"

    @pytest.mark.parametrize(
        argnames=["platform"],
        argvalues=[
            pytest.param(
                constants.AWS_PLATFORM,
                marks=[tier2, polarion_id("OCS-4941")],
            ),
        ],
    )
    def test_patch_deletion_sync_to_existing_bucket(
        self, platform, log_based_replication_handler_factory
    ):
        """
        Test patching deletion sync onto an existing bucket:

        1. Create a source bucket and target bucket
        2. Patch the source bucket with a log-based replication policy that includes deletion sync
        3. Upload a set of objects to the source bucket
        4. Wait for the objects to be replicated to the target bucket
        """
        replication_handler = log_based_replication_handler_factory(
            platform, patch_to_existing_bucket=True
        )

        replication_handler.upload_random_objects_to_source(amount=10)
        assert replication_handler.wait_for_sync(
            timeout=self.DEFAULT_TIMEOUT
        ), f"Replication failed to complete in {self.DEFAULT_TIMEOUT} seconds"

        # Deletion sync has shown to take longer in this scenario, so we double the timeout
        replication_handler.delete_recursively_from_source()
        assert replication_handler.wait_for_sync(
            timeout=self.DEFAULT_TIMEOUT * 2
        ), f"Deletion sync failed to complete in {self.DEFAULT_TIMEOUT} seconds"

    @pytest.mark.parametrize(
        argnames=["platform"],
        argvalues=[
            pytest.param(
                constants.AWS_PLATFORM,
                marks=[tier3, polarion_id("OCS-4937")],
            ),
        ],
    )
    def test_deletion_sync_after_instant_deletion(
        self,
        platform,
        log_based_replication_handler_factory,
    ):
        """
        Test deletion sync behavior when an object is immediately
        deleted after being uploaded:

        1. Setup log-based replication with deletion sync between two buckets
        2. Upload an object to the source bucket and then immediately delete it
        3. Upload more objects to the source bucket
        4. Wait for the two buckets to sync
        5. Delete all objects from the source bucket
        6. Check that the objects are deleted from the target bucket
        """

        replication_handler = log_based_replication_handler_factory(platform)

        logger.info(
            "Uploading an object to the source bucket then immediately deleting it"
        )

        replication_handler.upload_random_objects_to_source(amount=1)
        replication_handler.delete_recursively_from_source()

        replication_handler.upload_random_objects_to_source(amount=10)
        assert replication_handler.wait_for_sync(
            timeout=self.DEFAULT_TIMEOUT
        ), f"Replication failed to complete in {self.DEFAULT_TIMEOUT} seconds"

        replication_handler.delete_recursively_from_source()
        assert replication_handler.wait_for_sync(
            timeout=self.DEFAULT_TIMEOUT
        ), f"Deletion sync failed to complete in {self.DEFAULT_TIMEOUT} seconds"

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
        self, platform, log_based_replication_handler_factory, target_pod_name
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
        replication_handler = log_based_replication_handler_factory(platform)

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

        replication_handler.upload_random_objects_to_source(amount=10)
        assert replication_handler.wait_for_sync(
            timeout=self.DEFAULT_TIMEOUT
        ), f"Replication failed to complete in {self.DEFAULT_TIMEOUT} seconds"

        logger.info(f"Restarting {target_pod_name}'s node")
        noobaa_running_node_restart(pod_name=target_pod_name)

        replication_handler.delete_recursively_from_source()
        assert replication_handler.wait_for_sync(
            timeout=self.DEFAULT_TIMEOUT
        ), f"Deletion sync failed to complete in {self.DEFAULT_TIMEOUT} seconds"

        # Keep track of the target node to prevent its redundant testing in this scenario
        self._nodes_tested.append(target_node_name)
