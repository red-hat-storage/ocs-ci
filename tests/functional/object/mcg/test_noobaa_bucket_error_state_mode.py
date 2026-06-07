import logging

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
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
from ocs_ci.ocs.bucket_utils import craft_s3_command
from ocs_ci.ocs.resources.objectbucket import MCGCLIBucket
from ocs_ci.ocs.resources.pod import get_noobaa_pvpool_pods
from ocs_ci.helpers.helpers import create_unique_resource_name
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.utility.utils import TimeoutSampler

log = logging.getLogger(__name__)

BUCKET_MODE_TIMEOUT = 60 * 10
BUCKET_MODE_POLL_INTERVAL = 15


def _wait_for_bucket_mode(mcg_obj, bucket_name, expected_mode, timeout=None):
    """
    Poll NooBaa until the bucket enters the expected mode or timeout.

    Args:
        mcg_obj: MCG object
        bucket_name (str): Name of the bucket
        expected_mode (str): Expected bucket mode (e.g. "EXCEEDING_QUOTA")
            or "!OPTIMAL" to wait for any non-OPTIMAL mode
            (UNKNOWN is excluded to avoid false positives on lookup failures)
        timeout (int): Timeout in seconds (default: BUCKET_MODE_TIMEOUT)

    Returns:
        str: The actual bucket mode once matched

    Raises:
        AssertionError: If the bucket does not reach the expected mode
            within the timeout
    """
    if timeout is None:
        timeout = BUCKET_MODE_TIMEOUT
    check_not_optimal = expected_mode == "!OPTIMAL"
    last_mode = "UNKNOWN"

    try:
        for mode in TimeoutSampler(
            timeout=timeout,
            sleep=BUCKET_MODE_POLL_INTERVAL,
            func=_get_bucket_mode,
            mcg_obj=mcg_obj,
            bucket_name=bucket_name,
        ):
            last_mode = mode
            if check_not_optimal and mode not in ("OPTIMAL", "UNKNOWN"):
                log.info(f"Bucket {bucket_name} entered non-OPTIMAL mode: {mode}")
                return mode
            elif not check_not_optimal and mode == expected_mode:
                log.info(f"Bucket {bucket_name} entered expected mode: {mode}")
                return mode
    except TimeoutExpiredError:
        if check_not_optimal:
            assert False, (
                f"Bucket {bucket_name} is still OPTIMAL after "
                f"{timeout}s (last mode: {last_mode})"
            )
        else:
            assert False, (
                f"Expected bucket mode {expected_mode} for {bucket_name}, "
                f"got {last_mode} after {timeout}s"
            )


def _get_bucket_mode(mcg_obj, bucket_name):
    """
    Get the current mode of a bucket from NooBaa.

    Args:
        mcg_obj: MCG object
        bucket_name (str): Name of the bucket

    Returns:
        str: The bucket mode (e.g. "OPTIMAL", "EXCEEDING_QUOTA")
    """
    bucket_info = mcg_obj.get_bucket_info(bucket_name)
    if bucket_info:
        log.info(f"Full bucket info for {bucket_name}: {bucket_info}")
        return bucket_info.get("mode", "UNKNOWN")
    log.warning(f"Bucket {bucket_name} not found in NooBaa system")
    return "UNKNOWN"


def _upload_data_to_bucket(awscli_pod, bucket_name, mcg_obj, file_count, file_size_mb):
    """
    Upload data files to a bucket via S3.

    Args:
        awscli_pod: Pod with awscli tools
        bucket_name (str): Target bucket name
        mcg_obj: MCG object with S3 credentials
        file_count (int): Number of files to upload
        file_size_mb (int): Size of each file in MB
    """
    awscli_pod.exec_cmd_on_pod(
        f"dd if=/dev/zero of=/tmp/testfile bs=1M count={file_size_mb}"
    )
    for i in range(1, file_count + 1):
        try:
            awscli_pod.exec_cmd_on_pod(
                craft_s3_command(
                    f"cp /tmp/testfile s3://{bucket_name}/testfile{i}",
                    mcg_obj,
                ),
                out_yaml_format=False,
                secrets=[
                    mcg_obj.access_key_id,
                    mcg_obj.access_key,
                    mcg_obj.s3_endpoint,
                ],
            )
        except Exception:
            log.warning(
                f"Upload of testfile{i} to {bucket_name} failed "
                f"(expected when exceeding capacity/quota)"
            )


def _delete_data_from_bucket(awscli_pod, bucket_name, mcg_obj, file_count):
    """
    Delete data files from a bucket via S3.

    Args:
        awscli_pod: Pod with awscli tools
        bucket_name (str): Target bucket name
        mcg_obj: MCG object with S3 credentials
        file_count (int): Number of files to delete
    """
    for i in range(1, file_count + 1):
        try:
            awscli_pod.exec_cmd_on_pod(
                craft_s3_command(f"rm s3://{bucket_name}/testfile{i}", mcg_obj),
                out_yaml_format=False,
                secrets=[
                    mcg_obj.access_key_id,
                    mcg_obj.access_key,
                    mcg_obj.s3_endpoint,
                ],
            )
        except Exception:
            log.warning(f"Failed to delete testfile{i} from {bucket_name}")


@mcg
@red_squad
@tier2
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

    Covers three error categories for both data and namespace buckets:
    - Quota-related: EXCEEDING_QUOTA
    - Resource-related: NOT_ENOUGH_HEALTHY_RESOURCES
    - Capacity-related: NOT_ENOUGH_HEALTHY_RESOURCES
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
        log.info(f"Created bucket {bucket_name} with 2Gi quota")

        try:
            _upload_data_to_bucket(awscli_pod, bucket_name, mcg_obj, 5, 500)
            log.info(
                f"Uploaded ~2.5GB to bucket {bucket_name} "
                f"(quota 2Gi) to trigger EXCEEDING_QUOTA"
            )

            _wait_for_bucket_mode(mcg_obj, bucket_name, "EXCEEDING_QUOTA")
        finally:
            _delete_data_from_bucket(awscli_pod, bucket_name, mcg_obj, 5)
            bucket.delete()

    @pytest.mark.skip(
        reason="NS buckets don't keep metadata and act as a proxy — "
        "quota is not supported for namespace buckets. "
        "Confirmed by Eran Tamir on RHSTOR-7732."
    )
    @skipif_managed_service
    @skipif_aws_creds_are_missing
    # TODO: Replace with actual Polarion ID
    # @polarion_id("OCS-XXXXX")
    def test_ns_bucket_quota_error_mode(
        self,
        mcg_obj,
        awscli_pod,
        bucket_factory,
        namespace_store_factory,
        request,
    ):
        """
        Trigger a quota-related bucket mode on a namespace bucket and
        verify the bucket enters EXCEEDING_QUOTA mode.

        Steps:
            1. Create an AWS-backed namespace store
            2. Create a namespace bucket (Single policy) using the
               namespace store
            3. Set a 1Gi quota on the namespace bucket
            4. Upload data exceeding the quota
            5. Wait for bucket mode to propagate
            6. Verify bucket mode is EXCEEDING_QUOTA via NooBaa
            7. Clean up

        Args:
            mcg_obj (MCG): MCG object with S3 connection credentials
            awscli_pod (Pod): Pod running the AWSCLI tools
            bucket_factory (func): Factory for creating MCG buckets
            namespace_store_factory (func): Factory for creating namespace stores
            request: Pytest request object for finalizer registration
        """
        nss_tup = ("oc", {"aws": [(1, "us-east-2")]})
        ns_stores = namespace_store_factory(*nss_tup)

        bucketclass_dict = {
            "interface": "OC",
            "namespace_policy_dict": {
                "type": "Single",
                "namespacestores": ns_stores,
            },
        }
        ns_bucket = bucket_factory(
            amount=1,
            interface=bucketclass_dict["interface"],
            bucketclass=bucketclass_dict,
        )[0]

        def cleanup():
            ns_bucket.delete()
            ns_bucket.bucketclass.delete()
            ns_stores[0].delete()

        request.addfinalizer(cleanup)

        log.info(f"Created namespace store: {ns_stores[0].name}")
        log.info(f"Created namespace bucket {ns_bucket.name} (Single policy)")

        mcg_obj.exec_mcg_cmd(
            cmd=f"bucket update --max-size=1Gi {ns_bucket.name}",
            namespace=config.ENV_DATA["cluster_namespace"],
            use_yes=True,
        )
        log.info(f"Set 1Gi quota on namespace bucket {ns_bucket.name}")

        _upload_data_to_bucket(awscli_pod, ns_bucket.name, mcg_obj, 3, 500)
        log.info(
            f"Uploaded ~1.5GB to namespace bucket {ns_bucket.name} "
            f"(quota 1Gi) to trigger EXCEEDING_QUOTA"
        )

        _wait_for_bucket_mode(mcg_obj, ns_bucket.name, "EXCEEDING_QUOTA")

    # ------------------------------------------------------------------
    # Resource error mode tests (NOT_ENOUGH_HEALTHY_RESOURCES)
    # ------------------------------------------------------------------

    @skipif_mcg_only
    # TODO: Replace with actual Polarion ID
    # @polarion_id("OCS-XXXXX")
    def test_data_bucket_resource_error_mode(
        self, mcg_obj, awscli_pod, bucket_factory, request
    ):
        """
        Trigger a resource-related bucket mode on a data bucket and
        verify the bucket enters NOT_ENOUGH_HEALTHY_RESOURCES mode.

        Uses a PV pool backing store and deletes its pool pod
        to simulate a resource failure.

        Steps:
            1. Create a data bucket with a PV pool backing store (17Gi)
            2. Upload data so NooBaa tracks objects against the store
            3. Delete the PV pool pod to trigger resource error
            4. Wait for bucket to detect the resource error
            5. Verify bucket mode is NOT_ENOUGH_HEALTHY_RESOURCES
            6. Clean up

        Args:
            mcg_obj (MCG): MCG object with S3 connection credentials
            awscli_pod (Pod): Pod running the AWSCLI tools
            bucket_factory (func): Factory for creating MCG buckets
            request: Pytest request object for finalizer registration
        """
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

        def cleanup():
            bucket.delete()
            bucket.bucketclass.delete()
            backingstore.delete()

        request.addfinalizer(cleanup)

        log.info(
            f"Created data bucket {bucket.name} with "
            f"PV pool backing store {backingstore.name}"
        )

        _upload_data_to_bucket(awscli_pod, bucket.name, mcg_obj, 2, 50)
        log.info(
            f"Uploaded ~100MB to bucket {bucket.name} "
            f"so NooBaa tracks objects against the backing store"
        )

        pool_pods = get_noobaa_pvpool_pods(backingstore.name)
        assert pool_pods, f"No pool pods found for backing store {backingstore.name}"
        for pod in pool_pods:
            log.info(
                f"Deleting pool pod {pod.name} of backing store "
                f"{backingstore.name} to trigger resource error"
            )
            pod.delete(force=True)

        _wait_for_bucket_mode(mcg_obj, bucket.name, "NOT_ENOUGH_HEALTHY_RESOURCES")

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
        request,
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
            request: Pytest request object for finalizer registration
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

        def cleanup():
            ns_bucket.delete()
            ns_bucket.bucketclass.delete()
            for ns_store in ns_stores:
                ns_store.delete()

        request.addfinalizer(cleanup)

        log.info(f"Created namespace stores: {[ns.name for ns in ns_stores]}")
        log.info(f"Created namespace bucket {ns_bucket.name} with Multi policy")

        _upload_data_to_bucket(awscli_pod, ns_bucket.name, mcg_obj, 2, 50)
        log.info(
            f"Uploaded ~100MB to namespace bucket {ns_bucket.name} "
            f"so NooBaa tracks objects against the namespace stores"
        )

        target_uls_name = ns_stores[0].uls_name
        log.info(
            f"Deleting target bucket {target_uls_name} of "
            f"namespace store {ns_stores[0].name}"
        )
        cld_mgr.aws_client.delete_uls(target_uls_name)

        _wait_for_bucket_mode(mcg_obj, ns_bucket.name, "NOT_ENOUGH_HEALTHY_RESOURCES")

    # ------------------------------------------------------------------
    # Capacity error mode tests
    # ------------------------------------------------------------------

    @skipif_mcg_only
    # TODO: Replace with actual Polarion ID
    # @polarion_id("OCS-XXXXX")
    def test_data_bucket_capacity_error_mode(
        self, mcg_obj, awscli_pod, bucket_factory, request
    ):
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
            request: Pytest request object for finalizer registration
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

        def cleanup():
            bucket.delete()
            bucket.bucketclass.delete()
            backingstore.delete()

        request.addfinalizer(cleanup)

        log.info(
            f"Created data bucket {bucket.name} with "
            f"17Gi PV pool backing store {backingstore.name}"
        )

        observed_modes = []
        file_num = 0
        final_mode = None

        awscli_pod.exec_cmd_on_pod("dd if=/dev/zero of=/tmp/testfile bs=1M count=4000")
        log.info("Created 4GB test file for gradual upload")

        for chunk in range(1, 7):
            file_num += 1
            log.info(
                f"Uploading chunk {chunk}/6 (~4GB) to bucket {bucket.name} "
                f"(total so far: ~{chunk * 4}GB, PV pool: 17Gi)"
            )
            try:
                awscli_pod.exec_cmd_on_pod(
                    craft_s3_command(
                        f"cp /tmp/testfile s3://{bucket.name}/testfile{file_num}",
                        mcg_obj,
                    ),
                    out_yaml_format=False,
                    secrets=[
                        mcg_obj.access_key_id,
                        mcg_obj.access_key,
                        mcg_obj.s3_endpoint,
                    ],
                )
            except Exception:
                log.warning(
                    f"Upload of chunk {chunk} failed "
                    f"(expected when exceeding capacity)"
                )

            mode = _get_bucket_mode(mcg_obj, bucket.name)
            log.info(
                f"After chunk {chunk}: bucket mode = {mode} "
                f"(uploaded ~{chunk * 4}GB / 17Gi)"
            )

            if mode != "OPTIMAL" and mode not in observed_modes:
                observed_modes.append(mode)
                log.info(f"New non-OPTIMAL mode observed: {mode}")

            if mode in capacity_error_modes:
                final_mode = mode
                log.info(f"Bucket {bucket.name} entered capacity error mode: {mode}")
                break

        if not final_mode:
            log.info(
                "Bucket still OPTIMAL after all uploads, "
                "waiting for mode to propagate"
            )
            final_mode = _wait_for_bucket_mode(
                mcg_obj, bucket.name, "NOT_ENOUGH_HEALTHY_RESOURCES"
            )
            if final_mode and final_mode not in observed_modes:
                observed_modes.append(final_mode)

        log.info(
            f"Capacity test complete. "
            f"Observed mode transitions: {observed_modes}. "
            f"Final mode: {final_mode}"
        )
        assert (
            final_mode in capacity_error_modes
        ), f"Expected one of {capacity_error_modes}, got {final_mode}"

    @pytest.mark.skip(
        reason="NS buckets don't keep metadata and act as a proxy — "
        "capacity tracking is not supported for namespace buckets. "
        "Confirmed by Eran Tamir on RHSTOR-7732."
    )
    @skipif_mcg_only
    @skipif_managed_service
    @skipif_aws_creds_are_missing
    # TODO: Replace with actual Polarion ID
    # @polarion_id("OCS-XXXXX")
    def test_ns_bucket_capacity_error_mode(
        self,
        mcg_obj,
        awscli_pod,
        bucket_factory,
        backingstore_factory,
        namespace_store_factory,
        request,
    ):
        """
        Trigger a capacity-related bucket mode on a namespace bucket
        and verify the bucket enters NOT_ENOUGH_HEALTHY_RESOURCES mode.

        Uses a Cache namespace bucket where the cache layer is a small
        PV pool backing store (17Gi). Filling the cache beyond its
        capacity triggers a capacity error mode.

        Steps:
            1. Create a PV pool backing store (17Gi) for the cache layer
            2. Create an AWS-backed namespace store for the hub
            3. Create a Cache namespace bucket with the AWS hub and
               PV pool cache
            4. Upload data exceeding PV pool capacity
            5. Wait for bucket to detect the capacity exhaustion
            6. Verify bucket mode is NOT_ENOUGH_HEALTHY_RESOURCES
            7. Clean up

        Args:
            mcg_obj (MCG): MCG object with S3 connection credentials
            awscli_pod (Pod): Pod running the AWSCLI tools
            bucket_factory (func): Factory for creating MCG buckets
            backingstore_factory (func): Factory for creating backing stores
            namespace_store_factory (func): Factory for creating namespace stores
            request: Pytest request object for finalizer registration
        """
        pv_backingstore = backingstore_factory(
            "oc", {"pv": [(1, 17, constants.DEFAULT_STORAGECLASS_RBD)]}
        )[0]

        nss_tup = ("oc", {"aws": [(1, "us-east-2")]})
        ns_stores = namespace_store_factory(*nss_tup)

        cache_bucketclass = {
            "interface": "OC",
            "namespace_policy_dict": {
                "type": "Cache",
                "ttl": 300000,
                "namespacestores": ns_stores,
            },
            "placement_policy": {"tiers": [{"backingStores": [pv_backingstore.name]}]},
        }
        ns_bucket = bucket_factory(
            amount=1,
            interface=cache_bucketclass["interface"],
            bucketclass=cache_bucketclass,
        )[0]

        def cleanup():
            ns_bucket.delete()
            ns_bucket.bucketclass.delete()
            pv_backingstore.delete()
            ns_stores[0].delete()

        request.addfinalizer(cleanup)

        log.info(
            f"Created PV pool backing store {pv_backingstore.name} "
            f"(17Gi) for cache layer"
        )
        log.info(f"Created namespace store (hub): {ns_stores[0].name}")
        log.info(f"Created Cache namespace bucket {ns_bucket.name} with PV pool cache")

        _upload_data_to_bucket(awscli_pod, ns_bucket.name, mcg_obj, 4, 5000)
        log.info(
            f"Uploaded ~20GB to cache namespace bucket {ns_bucket.name} "
            f"(cache PV pool 17Gi) to trigger capacity error"
        )

        _wait_for_bucket_mode(mcg_obj, ns_bucket.name, "NOT_ENOUGH_HEALTHY_RESOURCES")
