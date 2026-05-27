import logging

import pytest

from ocs_ci.framework.testlib import MCGTest, tier2, polarion_id
from ocs_ci.framework.pytest_customization.marks import mcg, red_squad
from ocs_ci.utility.prometheus import PrometheusAPI
from ocs_ci.ocs import constants
from ocs_ci.ocs.bucket_utils import (
    write_random_test_objects_to_bucket,
    compare_bucket_object_list,
)
from ocs_ci.ocs.exceptions import CommandFailed

logger = logging.getLogger(__name__)


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

        api = PrometheusAPI(threading_lock=threading_lock)
        alerts = api.wait_for_alert(
            name=constants.ALERT_NOOBAA_REPLICATION_TARGET_UNREACHABLE,
            state="firing",
            timeout=60 * 8,
            sleep=30,
        )
        # 4. Verify alert properties and bucket names (not hash IDs - DFBUGS-6380)
        alert = next(
            (
                a
                for a in alerts
                if a.get("labels", {}).get("source_bucket") == source_bucket.name
            ),
            None,
        )
        assert (
            alert is not None
        ), "NooBaaReplicationTargetUnreachable alert not found for source bucket"
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

        cleared = api.wait_for_alert(
            name=constants.ALERT_NOOBAA_REPLICATION_TARGET_UNREACHABLE,
            state=None,
            timeout=60 * 5,
            sleep=30,
        )
        assert (
            len(cleared) == 0
        ), f"{constants.ALERT_NOOBAA_REPLICATION_TARGET_UNREACHABLE} alerts were not cleared"
