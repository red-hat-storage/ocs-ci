import base64
import json
import logging
import time

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    mcg,
    red_squad,
    skipif_aws_creds_are_missing,
    skipif_disconnected_cluster,
)
from ocs_ci.framework.testlib import MCGTest, polarion_id, tier2
from ocs_ci.helpers.helpers import create_unique_resource_name
from ocs_ci.ocs import constants
from ocs_ci.ocs.bucket_utils import (
    compare_bucket_object_list,
    write_random_test_objects_to_bucket,
)
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.utility import templating
from ocs_ci.utility.aws import AWS
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.utility.prometheus import PrometheusAPI

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

    @pytest.fixture()
    def replication_target_with_aws_toggleable_creds(self, request, cld_mgr, mcg_obj):
        """
        Create an MCG replication target OBC backed by an AWS S3
        backingstore whose IAM credentials can be toggled on and off.

        Creates the full chain: S3 bucket, dedicated IAM user with
        a bucket-scoped policy, access key, K8s secret, backingstore,
        bucketclass, and target OBC.

        Cleanup handles partial failures — any resource that was
        successfully created will be torn down regardless of where
        the fixture failed.

        Returns:
            dict: target_obc_name, disable (callable) to revoke
                the IAM key, enable (callable) to restore it.

        """
        aws_obj = AWS()
        namespace = config.ENV_DATA["cluster_namespace"]
        region = cld_mgr.aws_client.region
        created = {}

        def _cleanup():
            if "target_obc" in created:
                try:
                    OCP(kind="obc", namespace=namespace).delete(
                        resource_name=created["target_obc"]
                    )
                except Exception:
                    logger.warning(f"Failed to delete OBC {created['target_obc']}")
            if "bucketclass" in created:
                try:
                    OCP(kind="bucketclass", namespace=namespace).delete(
                        resource_name=created["bucketclass"]
                    )
                except Exception:
                    logger.warning(
                        f"Failed to delete bucketclass {created['bucketclass']}"
                    )
            if "backingstore" in created:
                bs_ocp = OCP(kind="backingstore", namespace=namespace)
                for attempt in range(6):
                    try:
                        bs_ocp.delete(resource_name=created["backingstore"])
                        break
                    except Exception:
                        if attempt < 5:
                            logger.info(
                                f"Backingstore {created['backingstore']} not ready "
                                f"for deletion, retrying in 10s ({attempt + 1}/6)"
                            )
                            time.sleep(10)
                        else:
                            logger.warning(
                                f"Failed to delete backingstore "
                                f"{created['backingstore']} after 6 attempts"
                            )
            if "secret" in created:
                try:
                    created["secret"].delete()
                except Exception:
                    logger.warning("Failed to delete K8s secret")
            if "access_key_id" in created:
                try:
                    aws_obj.update_access_key_status(
                        created["username"],
                        created["access_key_id"],
                        status="Active",
                    )
                except Exception:
                    logger.warning("Failed to re-enable IAM key")
                try:
                    aws_obj.delete_access_key(
                        created["username"], created["access_key_id"]
                    )
                except Exception:
                    logger.warning("Failed to delete IAM key")
            if "policy_name" in created:
                try:
                    aws_obj.delete_user_policy(
                        created["username"], created["policy_name"]
                    )
                except Exception:
                    logger.warning("Failed to delete IAM policy")
            if "username" in created:
                try:
                    aws_obj.delete_iam_user(created["username"])
                except Exception:
                    logger.warning(f"Failed to delete IAM user {created['username']}")
            if "bucket_name" in created:
                try:
                    cld_mgr.aws_client.internal_delete_uls(created["bucket_name"])
                except Exception:
                    logger.warning(
                        f"Failed to delete S3 bucket {created['bucket_name']}"
                    )

        request.addfinalizer(_cleanup)

        # 1. Create the S3 bucket
        bucket_name = create_unique_resource_name("repl-alert", "aws")
        cld_mgr.aws_client.internal_create_uls(bucket_name, region=region)
        created["bucket_name"] = bucket_name
        logger.info(f"Created S3 bucket {bucket_name}")

        # 2. Create IAM user with a bucket-scoped inline policy
        username = create_unique_resource_name("ocs-ci-s3", "iam")
        aws_obj.create_iam_user(username)
        created["username"] = username

        # Scope the user to only this bucket — no access to any other S3 resource
        # NooBaa also requires s3:ListAllMyBuckets for CheckExternalConnection
        policy_name = f"{username}-s3"
        policy_document = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": "s3:*",
                        "Resource": [
                            f"arn:aws:s3:::{bucket_name}",
                            f"arn:aws:s3:::{bucket_name}/*",
                        ],
                    },
                    {
                        "Effect": "Allow",
                        "Action": "s3:ListAllMyBuckets",
                        "Resource": "arn:aws:s3:::*",
                    },
                ],
            }
        )
        aws_obj.put_user_policy(username, policy_name, policy_document)
        created["policy_name"] = policy_name

        # 3. Create access key
        response = aws_obj.create_access_key(username)
        key_data = response["AccessKey"]
        access_key_id = key_data["AccessKeyId"]
        created["access_key_id"] = access_key_id

        # Wait for IAM credentials to propagate through AWS
        logger.info("Sleeping 60s for IAM credential propagation")
        time.sleep(60)

        # 4. Create K8s secret with the IAM credentials
        bs_secret_data = templating.load_yaml(constants.MCG_BACKINGSTORE_SECRET_YAML)
        secret_name = create_unique_resource_name("repl-alert", "secret")
        bs_secret_data["metadata"]["name"] = secret_name
        bs_secret_data["metadata"]["namespace"] = namespace
        bs_secret_data["data"]["AWS_ACCESS_KEY_ID"] = base64.urlsafe_b64encode(
            access_key_id.encode()
        ).decode()
        bs_secret_data["data"]["AWS_SECRET_ACCESS_KEY"] = base64.urlsafe_b64encode(
            key_data["SecretAccessKey"].encode()
        ).decode()
        secret_obj = OCS(**bs_secret_data)
        secret_obj.create()
        created["secret"] = secret_obj
        logger.info(f"Created K8s secret {secret_name}")

        # 5. Create backingstore
        bs_name = create_unique_resource_name("repl-alert", "bs")
        mcg_obj.exec_mcg_cmd(
            f"backingstore create aws-s3 {bs_name} "
            f"--secret-name {secret_name} "
            f"--target-bucket {bucket_name} "
            f"--region {region}",
            use_yes=True,
        )
        created["backingstore"] = bs_name
        logger.info(f"Created backingstore {bs_name}")

        # 6. Create bucketclass
        bc_name = create_unique_resource_name("repl-alert", "bc")
        mcg_obj.exec_mcg_cmd(
            f"bucketclass create placement-bucketclass {bc_name} "
            f"--backingstores {bs_name}",
            use_yes=True,
        )
        created["bucketclass"] = bc_name
        logger.info(f"Created bucketclass {bc_name}")

        # 7. Create target OBC
        target_obc_name = create_unique_resource_name("repl-alert-tgt", "obc")
        mcg_obj.exec_mcg_cmd(
            f"obc create {target_obc_name} --bucketclass {bc_name} --exact",
            use_yes=True,
        )
        created["target_obc"] = target_obc_name
        OCP(kind="obc", namespace=namespace).wait_for_resource(
            condition="Bound",
            resource_name=target_obc_name,
            column="PHASE",
        )
        for obc_healthy in TimeoutSampler(
            timeout=300,
            sleep=10,
            func=lambda: all(
                mark
                in mcg_obj.exec_mcg_cmd(f"obc status {target_obc_name}").stdout.replace(
                    " ", ""
                )
                for mark in [
                    constants.HEALTHY_OBC_CLI_PHASE,
                    constants.HEALTHY_OB_CLI_MODE,
                ]
            ),
        ):
            if obc_healthy:
                break
        logger.info(f"Target OBC {target_obc_name} is Bound and healthy")

        return {
            "target_obc_name": target_obc_name,
            "disable": lambda: aws_obj.update_access_key_status(
                username, access_key_id, status="Inactive"
            ),
            "enable": lambda: aws_obj.update_access_key_status(
                username, access_key_id, status="Active"
            ),
        }

    @skipif_aws_creds_are_missing
    @skipif_disconnected_cluster
    @tier2
    @polarion_id("OCS-7918")
    def test_noobaa_replication_target_unreachable_iam_key_revocation(
        self,
        bucket_factory,
        mcg_obj,
        awscli_pod_session,
        test_directory_setup,
        threading_lock,
        replication_target_with_aws_toggleable_creds,
    ):
        """
        1. Create a source OBC with a replication policy targeting
           the AWS-backed target OBC
        2. Write test objects and verify replication works
        3. Disable the IAM access key
        4. Upload new objects to trigger failing replication
        5. Wait for the NooBaaReplicationTargetUnreachable alert to fire
        6. Re-enable the IAM access key
        7. Wait for the alert to clear
        """

        target_obc_name = replication_target_with_aws_toggleable_creds[
            "target_obc_name"
        ]

        # 1. Create a source OBC with a replication policy
        replication_policy = ("repl-alert-rule", target_obc_name, None)
        source_bucket = bucket_factory(1, "OC", replication_policy=replication_policy)[
            0
        ]
        logger.info(
            f"Replication configured: {source_bucket.name} -> {target_obc_name}"
        )

        # 2. Write test objects and verify replication works
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

        # 3. Disable the IAM access key
        replication_target_with_aws_toggleable_creds["disable"]()

        # 4. Upload new objects to trigger failing replication
        post_disrupt_dir = f"{test_directory_setup.origin_dir}/post"
        write_random_test_objects_to_bucket(
            awscli_pod_session,
            source_bucket.name,
            post_disrupt_dir,
            amount=3,
            pattern="post-disrupt-",
            mcg_obj=mcg_obj,
        )

        # 5. Wait for the NooBaaReplicationTargetUnreachable alert to fire
        alert_name = constants.ALERT_NOOBAA_REPLICATION_TARGET_UNREACHABLE
        api = PrometheusAPI(threading_lock=threading_lock)
        for alert_found in TimeoutSampler(
            timeout=600,
            sleep=10,
            func=api.get_alerts_for_bucket,
            alert_name=alert_name,
            source_bucket=source_bucket.name,
        ):
            if alert_found:
                logger.info(f"Alert {alert_name} is firing for {source_bucket.name}")
                break

        # 6. Re-enable the IAM access key
        replication_target_with_aws_toggleable_creds["enable"]()

        # 7. Wait for the alert to clear
        for alert_cleared in TimeoutSampler(
            timeout=600,
            sleep=10,
            func=api.get_alerts_for_bucket,
            alert_name=alert_name,
            source_bucket=source_bucket.name,
        ):
            if not alert_cleared:
                logger.info(f"Alert {alert_name} cleared for {source_bucket.name}")
                break
