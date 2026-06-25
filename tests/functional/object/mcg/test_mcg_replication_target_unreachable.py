import logging

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    mcg,
    red_squad,
    skipif_aws_creds_are_missing,
    skipif_disconnected_cluster,
)
from ocs_ci.framework.testlib import MCGTest, polarion_id, tier2
from ocs_ci.ocs import constants
from ocs_ci.ocs.bucket_utils import (
    compare_bucket_object_list,
    write_random_test_objects_to_bucket,
)
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.utility.prometheus import PrometheusAPI
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)


def _wait_for_replication_alert(
    threading_lock,
    source_bucket_name,
    target_bucket_name=None,
    timeout=600,
    sleep=30,
    cleared=False,
):
    """
    Wait for the NooBaaReplicationTargetUnreachable alert to fire or clear
    for a specific source bucket and optionally a specific target bucket.

    Args:
        threading_lock: Lock for Prometheus API access
        source_bucket_name (str): Source bucket name to filter by
        target_bucket_name (str, optional): Target bucket name to filter by
        timeout (int): Seconds to wait before timing out
        sleep (int): Seconds between polls
        cleared (bool): If True, wait for the alert to clear instead of appear

    Returns:
        list: The matching alerts (empty list when cleared=True).

    Raises:
        TimeoutExpiredError: If the alert does not reach the expected
            state within the timeout.
    """
    alert_name = constants.ALERT_NOOBAA_REPLICATION_TARGET_UNREACHABLE
    api = PrometheusAPI(threading_lock=threading_lock)
    labels = {"source_bucket": source_bucket_name}
    if target_bucket_name:
        labels["target_bucket"] = target_bucket_name
    for alerts in TimeoutSampler(
        timeout=timeout,
        sleep=sleep,
        func=api.get_alerts_by_labels,
        alert_name=alert_name,
        labels_dict=labels,
    ):
        found = bool(alerts)
        if (cleared and not found) or (not cleared and found):
            state = "cleared" if cleared else "firing"
            logger.info(f"Alert {alert_name} {state} for {source_bucket_name}")
            return alerts


@mcg
@red_squad
class TestMCGReplicationTargetUnreachableAlert(MCGTest):
    """
    Test suite for MCG replication target unreachable alert (RHSTOR-8110)
    """

    @pytest.fixture(autouse=True)
    def reduce_replication_delay(self, add_env_vars_to_noobaa_core):
        """
        Reduce the replication delay to one minute

        Args:
            add_env_vars_to_noobaa_core (function): A function to add env vars to the noobaa-core pod
        """
        new_delay_in_milliseconds = 60 * 1000
        new_env_var_tuples = [
            (constants.BUCKET_REPLICATOR_DELAY_PARAM, new_delay_in_milliseconds),
        ]
        add_env_vars_to_noobaa_core(new_env_var_tuples)

    @tier2
    @polarion_id("OCS-7917")
    def test_noobaa_replication_target_unreachable(
        self,
        bucket_factory,
        mcg_obj,
        awscli_pod_session,
        test_directory_setup,
        threading_lock,
        jira_issue,
    ):
        """
        1. Create source and target buckets with a replication policy
        2. Write test objects and verify replication works
        3. Delete the target bucket and wait for alert to fire
        4. Verify alert properties and bucket names (not hash IDs - DFBUGS-6380)
        5. Delete the source bucket
        6. Verify alert clears after source bucket deletion (blocked by DFBUGS-6398)
        """
        # 1. Create source and target buckets with a replication policy
        target_bucket = bucket_factory(1, "OC")[0]
        replication_policy = ("basic-replication-rule", target_bucket.name, None)
        source_bucket = bucket_factory(1, "OC", replication_policy=replication_policy)[
            0
        ]
        logger.info(
            f"Replication configured: {source_bucket.name} -> {target_bucket.name}"
        )

        # 2. Write test objects and verify replication works
        write_random_test_objects_to_bucket(
            awscli_pod_session,
            source_bucket.name,
            test_directory_setup.origin_dir,
            amount=5,
            mcg_obj=mcg_obj,
        )
        assert compare_bucket_object_list(
            mcg_obj, source_bucket.name, target_bucket.name, timeout=600
        ), "Replication did not work before target deletion"
        logger.info("Replication verified successfully")

        # 3. Delete the target bucket and wait for alert to fire
        target_bucket.delete()
        logger.info(f"Target bucket deleted: {target_bucket.name}")

        alerts = _wait_for_replication_alert(
            threading_lock, source_bucket.name, timeout=60 * 8
        )

        # 4. Verify alert properties and bucket names (not hash IDs - DFBUGS-6380)
        alert = alerts[0]
        assert (
            alert["annotations"]["message"]
            == "A NooBaa Replication Target Is Unreachable"
        )
        assert alert["annotations"]["severity_level"] == "warning"

        description = alert.get("annotations", {}).get("description", "")
        logger.info(f"Alert description: {description}")
        assert (
            f"from bucket {source_bucket.name} to bucket {target_bucket.name}"
            in description
        ), f"Expected bucket names in description, got: {description}"

        # 5. Delete the source bucket
        try:
            source_bucket.delete(verify=True)
        except CommandFailed as e:
            pytest.fail(f"Failed to delete source bucket: {e}")

        # 6. Verify alert clears after source bucket deletion (blocked by DFBUGS-6398)
        if jira_issue("DFBUGS-6398"):
            pytest.skip("DFBUGS-6398: alert persists after source bucket deletion")

        _wait_for_replication_alert(
            threading_lock, source_bucket.name, timeout=60 * 5, cleared=True
        )

    @skipif_aws_creds_are_missing
    @skipif_disconnected_cluster
    @tier2
    @polarion_id("OCS-7918")
    def test_noobaa_replication_target_unreachable_iam_key_revocation(
        self,
        aws_backingstore_with_toggleable_creds,
        bucket_factory,
        bucket_class_factory,
        mcg_obj,
        awscli_pod_session,
        test_directory_setup,
        threading_lock,
    ):
        """
        1. Create a target OBC backed by the toggleable-creds backingstore
        2. Create a source OBC with a replication policy targeting it
        3. Write test objects and verify replication works
        4. Disable the IAM access key
        5. Upload new objects to trigger failing replication
        6. Wait for the NooBaaReplicationTargetUnreachable alert to fire
        7. Re-enable the IAM access key
        8. Write test objects and verify replication works again
        9. Wait for the alert to clear
        """

        # 1. Create a target OBC backed by the toggleable-creds backingstore
        bs_obj = aws_backingstore_with_toggleable_creds["backingstore"]
        bc_obj = bucket_class_factory({"interface": "CLI", "backingstores": [bs_obj]})
        target_bucket = bucket_factory(1, "OC", bucketclass=bc_obj)[0]
        target_obc_name = target_bucket.name

        # 2. Create a source OBC with a replication policy
        replication_policy = ("repl-alert-rule", target_obc_name, None)
        source_bucket = bucket_factory(1, "OC", replication_policy=replication_policy)[
            0
        ]
        logger.info(
            f"Replication configured: {source_bucket.name} -> {target_obc_name}"
        )

        # 3. Write test objects and verify replication works
        pre_disrupt_dir = f"{test_directory_setup.origin_dir}/pre"
        write_random_test_objects_to_bucket(
            awscli_pod_session,
            source_bucket.name,
            pre_disrupt_dir,
            amount=3,
            pattern="pre-disrupt-",
            mcg_obj=mcg_obj,
        )
        assert compare_bucket_object_list(
            mcg_obj, source_bucket.name, target_obc_name, timeout=600
        ), "Replication did not work before IAM key revocation"
        logger.info("Initial replication verified successfully")

        # 4. Disable the IAM access key
        aws_backingstore_with_toggleable_creds["disable"]()

        # 5. Upload new objects to trigger failing replication
        post_disrupt_dir = f"{test_directory_setup.origin_dir}/post_disrupt"
        write_random_test_objects_to_bucket(
            awscli_pod_session,
            source_bucket.name,
            post_disrupt_dir,
            amount=3,
            pattern="post-disrupt-",
            mcg_obj=mcg_obj,
        )

        # 6. Wait for the NooBaaReplicationTargetUnreachable alert to fire
        _wait_for_replication_alert(
            threading_lock, source_bucket.name, timeout=600, sleep=10
        )

        # 7. Re-enable the IAM access key
        aws_backingstore_with_toggleable_creds["enable"]()

        # 8. Write test objects and verify replication works again
        post_recovery_dir = f"{test_directory_setup.origin_dir}/post_recovery"
        write_random_test_objects_to_bucket(
            awscli_pod_session,
            source_bucket.name,
            post_recovery_dir,
            amount=3,
            pattern="post-recovery-",
            mcg_obj=mcg_obj,
        )
        assert compare_bucket_object_list(
            mcg_obj, source_bucket.name, target_obc_name, timeout=600
        ), "Replication did not work after IAM key recovery"
        logger.info("Post-recovery replication verified successfully")

        # 9. Wait for the alert to clear
        _wait_for_replication_alert(
            threading_lock, source_bucket.name, timeout=600, sleep=10, cleared=True
        )

    @tier2
    @pytest.mark.parametrize(
        "store_type",
        [
            pytest.param(
                "namespacestore", id="namespacestore", marks=polarion_id("OCS-8000")
            ),
            pytest.param(
                "backingstore", id="backingstore", marks=polarion_id("OCS-7999")
            ),
        ],
    )
    def test_noobaa_replication_target_unreachable_underlying_bucket_deleted(
        self,
        store_type,
        bucket_factory,
        mcg_obj,
        awscli_pod_session,
        test_directory_setup,
        threading_lock,
    ):
        """
        1. Create a target OBC backed by a self-ref MCG store (an
           S3-compatible store backed by an MCG's own bucket on the
           same cluster)
        2. Create a source OBC with a replication policy targeting it
        3. Write test objects and verify replication works
        4. Delete the underlying MCG bucket of the target's store
        5. Upload new objects to trigger failing replication
        6. Wait for the NooBaaReplicationTargetUnreachable alert to fire
        7. Recreate the underlying MCG bucket
        8. Write test objects and verify replication works again
        9. Wait for the alert to clear
        """

        # 1. Create a target OBC backed by a self-ref MCG store
        if store_type == "namespacestore":
            bucketclass_dict = {
                "interface": "CLI",
                "namespace_policy_dict": {
                    "type": "Single",
                    "namespacestore_dict": {"self-ref-mcg": [(1, None)]},
                },
            }
        else:
            bucketclass_dict = {
                "interface": "CLI",
                "backingstore_dict": {"self-ref-mcg": [(1, None)]},
            }
        target_bucket = bucket_factory(1, "OC", bucketclass=bucketclass_dict)[0]
        bc_obj = target_bucket.bucketclass
        if store_type == "namespacestore":
            underlying_bucket_name = bc_obj.namespacestores[0].uls_name
        else:
            underlying_bucket_name = bc_obj.backingstores[0].uls_name
        logger.info(
            f"Target OBC created: {target_bucket.name}, "
            f"underlying MCG bucket: {underlying_bucket_name}"
        )

        # 2. Create a source OBC with a replication policy
        replication_policy = ("repl-alert-rule", target_bucket.name, None)
        source_bucket = bucket_factory(1, "OC", replication_policy=replication_policy)[
            0
        ]
        logger.info(
            f"Replication configured: {source_bucket.name} -> {target_bucket.name}"
        )

        # 3. Write test objects and verify replication works
        pre_disrupt_dir = f"{test_directory_setup.origin_dir}/pre"
        write_random_test_objects_to_bucket(
            awscli_pod_session,
            source_bucket.name,
            pre_disrupt_dir,
            amount=3,
            pattern="pre-disrupt-",
            mcg_obj=mcg_obj,
        )
        assert compare_bucket_object_list(
            mcg_obj, source_bucket.name, target_bucket.name, timeout=600
        ), "Replication did not work before underlying bucket deletion"
        logger.info("Initial replication verified successfully")

        # 4. Delete the underlying MCG bucket of the target's store
        mcg_obj.s3_resource.Bucket(underlying_bucket_name).objects.all().delete()
        mcg_obj.s3_resource.Bucket(underlying_bucket_name).delete()
        logger.info(f"Underlying MCG bucket deleted: {underlying_bucket_name}")

        # 5. Upload new objects to trigger failing replication
        post_disrupt_dir = f"{test_directory_setup.origin_dir}/post_disrupt"
        write_random_test_objects_to_bucket(
            awscli_pod_session,
            source_bucket.name,
            post_disrupt_dir,
            amount=3,
            pattern="post-disrupt-",
            mcg_obj=mcg_obj,
        )

        # 6. Wait for the NooBaaReplicationTargetUnreachable alert to fire
        _wait_for_replication_alert(
            threading_lock, source_bucket.name, timeout=600, sleep=10
        )

        # 7. Recreate the underlying MCG bucket (reuses the same name
        # that cloud_uls_factory tracks, so its finalizer handles cleanup)
        mcg_obj.s3_resource.create_bucket(Bucket=underlying_bucket_name)
        logger.info(f"Underlying MCG bucket recreated: {underlying_bucket_name}")

        # 8. Write test objects and verify replication works again
        post_recovery_dir = f"{test_directory_setup.origin_dir}/post_recovery"
        write_random_test_objects_to_bucket(
            awscli_pod_session,
            source_bucket.name,
            post_recovery_dir,
            amount=3,
            pattern="post-recovery-",
            mcg_obj=mcg_obj,
        )
        assert compare_bucket_object_list(
            mcg_obj, source_bucket.name, target_bucket.name, timeout=600
        ), "Replication did not work after underlying bucket recreation"
        logger.info("Post-recovery replication verified successfully")

        # 9. Wait for the alert to clear
        _wait_for_replication_alert(
            threading_lock, source_bucket.name, timeout=600, sleep=10, cleared=True
        )
