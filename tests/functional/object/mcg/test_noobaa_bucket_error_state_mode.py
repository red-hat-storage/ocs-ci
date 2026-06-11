import logging

from ocs_ci.framework.pytest_customization.marks import (
    post_upgrade,
    red_squad,
    skipif_ocs_version,
    skipif_mcg_only,
)
from ocs_ci.framework.testlib import (
    runs_on_provider,
    skipif_aws_creds_are_missing,
    skipif_disconnected_cluster,
    skipif_managed_service,
    tier2,
    mcg,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.bucket_utils import (
    copy_objects,
    get_bucket_mode,
    rm_object_recursive,
    wait_for_bucket_mode,
    write_random_test_objects_to_bucket,
)
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.resources.objectbucket import MCGCLIBucket
from ocs_ci.helpers.helpers import create_unique_resource_name
from ocs_ci.utility.prometheus import PrometheusAPI, wait_for_alert_firing

logger = logging.getLogger(__name__)


@mcg
@red_squad
@tier2
@post_upgrade
@runs_on_provider
@skipif_disconnected_cluster
@skipif_ocs_version("<4.22")
class TestNooBaaBucketErrorStateMode:
    """
    Tests for RHSTOR-7732: verify that NooBaa buckets enter the
    expected error modes (bucket_mode) when specific failure
    conditions are triggered.

    Each test triggers an error condition on a bucket, then verifies
    via NooBaa's get_bucket_info that the bucket enters the expected
    error mode.

    Covers three error categories:
    - Quota-related (data buckets): EXCEEDING_QUOTA
    - Resource-related (data and namespace buckets):
      NOT_ENOUGH_HEALTHY_RESOURCES
    - Capacity-related (data buckets): capacity exhaustion modes

    Also verifies that the NooBaaBucketErrorState Prometheus alert
    fires with the correct bucket_mode label.

    Note: Namespace buckets are only tested for resource errors,
    as they act as proxies and do not enforce quota or track
    capacity internally.
    """

    # ------------------------------------------------------------------
    # Quota error mode tests (EXCEEDING_QUOTA)
    # ------------------------------------------------------------------

    # TODO: Replace with actual Polarion ID
    # @polarion_id("OCS-XXXXX")
    def test_data_bucket_quota_error_mode(self, mcg_obj, awscli_pod):
        """
        Trigger a quota-related bucket mode on a data bucket and verify
        that the bucket enters EXCEEDING_QUOTA mode.

        Steps:
            1. Create a data bucket with a 2Gi quota
            2. Upload ~2.5GB to exceed the quota
            3. Wait for bucket mode to propagate
            4. Verify bucket mode is EXCEEDING_QUOTA via NooBaa
            5. Clean up

        Args:
            mcg_obj (MCG): MCG object with S3 connection credentials
            awscli_pod (Pod): Pod running the AWSCLI tools
        """
        bucket_name = create_unique_resource_name(
            resource_description="bucket", resource_type="quota"
        )
        bucket = MCGCLIBucket(bucket_name, mcg=mcg_obj, quota="2Gi")
        logger.info(f"Created bucket {bucket_name} with 2Gi quota")

        try:
            try:
                write_random_test_objects_to_bucket(
                    io_pod=awscli_pod,
                    bucket_to_write=bucket_name,
                    file_dir=f"/tmp/{bucket_name}",
                    amount=5,
                    bs="500M",
                    mcg_obj=mcg_obj,
                )
            except CommandFailed:
                logger.warning("Some uploads failed as expected when exceeding quota")
            logger.info(
                f"Uploaded data to bucket {bucket_name} "
                f"(quota 2Gi) to trigger EXCEEDING_QUOTA"
            )

            wait_for_bucket_mode(mcg_obj, bucket_name, "EXCEEDING_QUOTA")
        finally:
            try:
                rm_object_recursive(awscli_pod, bucket_name, mcg_obj)
            except CommandFailed:
                logger.warning(f"Cleanup of bucket {bucket_name} objects failed")
            bucket.delete()

    # ------------------------------------------------------------------
    # Resource error mode tests (NOT_ENOUGH_HEALTHY_RESOURCES)
    # ------------------------------------------------------------------

    @skipif_managed_service
    @skipif_aws_creds_are_missing
    # TODO: Replace with actual Polarion ID
    # @polarion_id("OCS-XXXXX")
    def test_data_bucket_resource_error_mode(
        self, mcg_obj, awscli_pod, bucket_factory, cld_mgr
    ):
        """
        Trigger a resource-related bucket mode on a data bucket and
        verify the bucket enters NOT_ENOUGH_HEALTHY_RESOURCES mode.

        Uses an AWS cloud backing store and deletes its target bucket
        to simulate a resource failure.

        Steps:
            1. Create a data bucket with an AWS cloud backing store
            2. Upload data so NooBaa tracks objects against the store
            3. Delete the target bucket of the backing store
            4. Wait for bucket to detect the resource error
            5. Verify bucket mode is NOT_ENOUGH_HEALTHY_RESOURCES
            6. Clean up

        Args:
            mcg_obj (MCG): MCG object with S3 connection credentials
            awscli_pod (Pod): Pod running the AWSCLI tools
            bucket_factory (func): Factory for creating MCG buckets
            cld_mgr (CloudManager): Cloud manager for cloud operations
        """
        bucketclass_dict = {
            "interface": "OC",
            "backingstore_dict": {"aws": [(1, "us-east-2")]},
        }
        bucket = bucket_factory(
            amount=1,
            interface=bucketclass_dict["interface"],
            bucketclass=bucketclass_dict,
        )[0]
        backingstore = bucket.bucketclass.backingstores[0]

        logger.info(
            f"Created data bucket {bucket.name} with "
            f"AWS backing store {backingstore.name}"
        )

        write_random_test_objects_to_bucket(
            io_pod=awscli_pod,
            bucket_to_write=bucket.name,
            file_dir=f"/tmp/{bucket.name}",
            amount=2,
            bs="50M",
            mcg_obj=mcg_obj,
        )
        logger.info(
            f"Uploaded ~100MB to bucket {bucket.name} "
            f"so NooBaa tracks objects against the backing store"
        )

        target_uls_name = backingstore.uls_name
        logger.info(
            f"Deleting target bucket {target_uls_name} of "
            f"backing store {backingstore.name}"
        )
        cld_mgr.aws_client.delete_uls(target_uls_name)

        wait_for_bucket_mode(mcg_obj, bucket.name, "NOT_ENOUGH_HEALTHY_RESOURCES")

    @skipif_managed_service
    @skipif_aws_creds_are_missing
    # TODO: Replace with actual Polarion ID
    # @polarion_id("OCS-XXXXX")
    def test_ns_bucket_resource_error_mode(
        self,
        mcg_obj,
        awscli_pod,
        bucket_factory,
        namespace_store_factory,
        cld_mgr,
    ):
        """
        Trigger a resource-related bucket mode on a namespace bucket
        and verify the bucket enters NOT_ENOUGH_HEALTHY_RESOURCES mode.

        Steps:
            1. Create 2 namespace stores backed by AWS target buckets
            2. Create a namespace bucket using Multi policy
            3. Upload data so NooBaa tracks objects against the stores
            4. Delete the target bucket of one namespace store
            5. Wait for bucket to detect the resource error
            6. Verify bucket mode is NOT_ENOUGH_HEALTHY_RESOURCES
            7. Clean up

        Args:
            mcg_obj (MCG): MCG object with S3 connection credentials
            awscli_pod (Pod): Pod running the AWSCLI tools
            bucket_factory (func): Factory for creating MCG buckets
            namespace_store_factory (func): Factory for creating namespace stores
            cld_mgr (CloudManager): Cloud manager for cloud operations
        """
        nss_tup = ("oc", {"aws": [(2, "us-east-2")]})
        ns_stores = namespace_store_factory(*nss_tup)

        bucketclass_dict = {
            "interface": "OC",
            "namespace_policy_dict": {
                "type": "Multi",
                "namespacestores": ns_stores,
            },
        }
        ns_bucket = bucket_factory(
            amount=1,
            interface=bucketclass_dict["interface"],
            bucketclass=bucketclass_dict,
        )[0]

        logger.info(f"Created namespace stores: {[ns.name for ns in ns_stores]}")
        logger.info(f"Created namespace bucket {ns_bucket.name} with Multi policy")

        write_random_test_objects_to_bucket(
            io_pod=awscli_pod,
            bucket_to_write=ns_bucket.name,
            file_dir=f"/tmp/{ns_bucket.name}",
            amount=2,
            bs="50M",
            mcg_obj=mcg_obj,
        )
        logger.info(
            f"Uploaded ~100MB to namespace bucket {ns_bucket.name} "
            f"so NooBaa tracks objects against the namespace stores"
        )

        target_uls_name = ns_stores[0].uls_name
        logger.info(
            f"Deleting target bucket {target_uls_name} of "
            f"namespace store {ns_stores[0].name}"
        )
        cld_mgr.aws_client.delete_uls(target_uls_name)

        wait_for_bucket_mode(mcg_obj, ns_bucket.name, "NOT_ENOUGH_HEALTHY_RESOURCES")

    # ------------------------------------------------------------------
    # Capacity error mode tests
    # ------------------------------------------------------------------

    @skipif_mcg_only
    # TODO: Replace with actual Polarion ID
    # @polarion_id("OCS-XXXXX")
    def test_data_bucket_capacity_error_mode(self, mcg_obj, awscli_pod, bucket_factory):
        """
        Gradually fill a data bucket's PV pool backing store and track
        bucket mode transitions through capacity error states.

        Uploads data in 4GB increments, checking the bucket mode after
        each upload to capture intermediate modes (e.g. LOW_CAPACITY,
        NO_CAPACITY) before the final NOT_ENOUGH_HEALTHY_RESOURCES.

        Steps:
            1. Create a data bucket backed by a small PV pool (17Gi)
            2. Upload data in 4GB increments, checking mode after each
            3. Log all observed mode transitions
            4. Verify bucket reaches a capacity error mode
            5. Clean up

        Args:
            mcg_obj (MCG): MCG object with S3 connection credentials
            awscli_pod (Pod): Pod running the AWSCLI tools
            bucket_factory (func): Factory for creating MCG buckets
        """
        capacity_error_modes = {
            "LOW_CAPACITY",
            "TIER_LOW_CAPACITY",
            "NO_CAPACITY",
            "TIER_NO_CAPACITY",
            "NOT_ENOUGH_HEALTHY_RESOURCES",
            "TIER_NOT_ENOUGH_HEALTHY_RESOURCES",
            "NOT_ENOUGH_RESOURCES",
            "TIER_NOT_ENOUGH_RESOURCES",
            "NO_RESOURCES",
        }

        bucketclass_dict = {
            "interface": "OC",
            "backingstore_dict": {"pv": [(1, 17, constants.DEFAULT_STORAGECLASS_RBD)]},
        }
        bucket = bucket_factory(
            amount=1,
            interface=bucketclass_dict["interface"],
            bucketclass=bucketclass_dict,
        )[0]
        backingstore = bucket.bucketclass.backingstores[0]

        logger.info(
            f"Created data bucket {bucket.name} with "
            f"17Gi PV pool backing store {backingstore.name}"
        )

        observed_modes = []
        file_num = 0
        final_mode = None

        awscli_pod.exec_cmd_on_pod("dd if=/dev/zero of=/tmp/testfile bs=1M count=4000")
        logger.info("Created 4GB test file for gradual upload")

        for chunk in range(1, 7):
            file_num += 1
            logger.info(
                f"Uploading chunk {chunk}/6 (~4GB) to bucket {bucket.name} "
                f"(total so far: ~{chunk * 4}GB, PV pool: 17Gi)"
            )
            try:
                copy_objects(
                    awscli_pod,
                    "/tmp/testfile",
                    f"s3://{bucket.name}/testfile{file_num}",
                    s3_obj=mcg_obj,
                )
            except CommandFailed:
                logger.warning(
                    f"Upload of chunk {chunk} failed "
                    f"(expected when exceeding capacity)"
                )

            mode = get_bucket_mode(mcg_obj, bucket.name)
            logger.info(
                f"After chunk {chunk}: bucket mode = {mode} "
                f"(uploaded ~{chunk * 4}GB / 17Gi)"
            )

            if mode != "OPTIMAL" and mode not in observed_modes:
                observed_modes.append(mode)
                logger.info(f"New non-OPTIMAL mode observed: {mode}")

            if mode in capacity_error_modes:
                final_mode = mode
                logger.info(f"Bucket {bucket.name} entered capacity error mode: {mode}")
                break

        if not final_mode:
            logger.info(
                "Bucket still OPTIMAL after all uploads, "
                "waiting for mode to propagate"
            )
            final_mode = wait_for_bucket_mode(
                mcg_obj, bucket.name, "NOT_ENOUGH_HEALTHY_RESOURCES"
            )
            if final_mode and final_mode not in observed_modes:
                observed_modes.append(final_mode)

        logger.info(
            f"Capacity test complete. "
            f"Observed mode transitions: {observed_modes}. "
            f"Final mode: {final_mode}"
        )
        assert (
            final_mode in capacity_error_modes
        ), f"Expected one of {capacity_error_modes}, got {final_mode}"

    # ------------------------------------------------------------------
    # Alert verification test
    # ------------------------------------------------------------------

    # TODO: Replace with actual Polarion ID
    # @polarion_id("OCS-XXXXX")
    def test_bucket_error_state_alert(self, mcg_obj, awscli_pod, threading_lock):
        """
        Verify that the NooBaaBucketErrorState Prometheus alert fires
        with the correct bucket_mode label when a bucket enters an
        error state.

        Uses a quota-exceeded scenario (fastest to trigger) and waits
        for the alert to fire after 5+ minutes.

        Steps:
            1. Create a data bucket with a 2Gi quota
            2. Upload ~2.5GB to exceed the quota
            3. Verify bucket enters EXCEEDING_QUOTA mode
            4. Wait for NooBaaBucketErrorState alert to fire (~5 min)
            5. Verify alert contains bucket_mode=EXCEEDING_QUOTA
            6. Clean up

        Args:
            mcg_obj (MCG): MCG object with S3 connection credentials
            awscli_pod (Pod): Pod running the AWSCLI tools
            threading_lock: Threading lock for Prometheus API
        """
        bucket_name = create_unique_resource_name(
            resource_description="bucket", resource_type="alert-quota"
        )
        bucket = MCGCLIBucket(bucket_name, mcg=mcg_obj, quota="2Gi")
        logger.info(f"Created bucket {bucket_name} with 2Gi quota")

        try:
            try:
                write_random_test_objects_to_bucket(
                    io_pod=awscli_pod,
                    bucket_to_write=bucket_name,
                    file_dir=f"/tmp/{bucket_name}",
                    amount=5,
                    bs="500M",
                    mcg_obj=mcg_obj,
                )
            except CommandFailed:
                logger.warning("Some uploads failed as expected when exceeding quota")
            logger.info(
                f"Uploaded data to bucket {bucket_name} "
                f"(quota 2Gi) to trigger EXCEEDING_QUOTA"
            )

            wait_for_bucket_mode(mcg_obj, bucket_name, "EXCEEDING_QUOTA")
            logger.info(
                f"Bucket {bucket_name} is in EXCEEDING_QUOTA mode, "
                f"waiting for Prometheus alert to fire (5+ minutes)"
            )

            prometheus_api = PrometheusAPI(threading_lock=threading_lock)
            alert_name = "NooBaaBucketErrorState"
            alerts = wait_for_alert_firing(
                api=prometheus_api,
                alert_name=alert_name,
                timeout=600,
                expected_message_substr="EXCEEDING_QUOTA",
            )

            alert_labels = alerts[0]["labels"]
            logger.info(f"Alert {alert_name} fired with labels: {alert_labels}")
            assert alert_labels.get("bucket_mode") == "EXCEEDING_QUOTA", (
                f"Expected bucket_mode=EXCEEDING_QUOTA in alert labels, "
                f"got bucket_mode={alert_labels.get('bucket_mode')}"
            )
            assert alert_labels.get("bucket_name") == bucket_name, (
                f"Expected bucket_name={bucket_name} in alert labels, "
                f"got bucket_name={alert_labels.get('bucket_name')}"
            )
            logger.info(
                f"Alert {alert_name} verified: "
                f"bucket_name={alert_labels.get('bucket_name')}, "
                f"bucket_mode={alert_labels.get('bucket_mode')}"
            )
        finally:
            try:
                rm_object_recursive(awscli_pod, bucket_name, mcg_obj)
            except CommandFailed:
                logger.warning(f"Cleanup of bucket {bucket_name} objects failed")
            bucket.delete()
