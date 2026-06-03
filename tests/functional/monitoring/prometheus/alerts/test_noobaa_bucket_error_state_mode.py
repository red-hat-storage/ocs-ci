import logging
import time

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    red_squad,
    skipif_ocs_version,
    skipif_mcg_only,
)
from ocs_ci.framework.testlib import (
    polarion_id,
    runs_on_provider,
    skipif_aws_creds_are_missing,
    skipif_disconnected_cluster,
    skipif_managed_service,
    tier2,
    mcg,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.bucket_utils import craft_s3_command
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.objectbucket import MCGCLIBucket
from ocs_ci.helpers.helpers import create_unique_resource_name

log = logging.getLogger(__name__)


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
@skipif_managed_service
@skipif_disconnected_cluster
@skipif_aws_creds_are_missing
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
    - Quota-related modes (e.g. EXCEEDING_QUOTA)
    - Resource-related modes (e.g. NO_RESOURCES, ALL_TIERS_HAVE_ISSUES)
    - Capacity-related modes (e.g. NO_CAPACITY, LOW_CAPACITY)
    """

    # ------------------------------------------------------------------
    # Data bucket tests
    # ------------------------------------------------------------------

    @polarion_id("OCS-XXXXX")
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

            run_time = 60 * 7
            log.info(f"Waiting {run_time}s for bucket mode to propagate")
            time.sleep(run_time)

            expected_mode = "EXCEEDING_QUOTA"
            bucket_mode = _get_bucket_mode(mcg_obj, bucket_name)
            assert bucket_mode == expected_mode, (
                f"Expected bucket mode {expected_mode}, " f"got {bucket_mode}"
            )
        finally:
            _delete_data_from_bucket(awscli_pod, bucket_name, mcg_obj, 5)
            bucket.delete()

    @polarion_id("OCS-XXXXX")
    def test_data_bucket_resource_error_mode(
        self, mcg_obj, bucket_factory, cld_mgr, request
    ):
        """
        Trigger a resource-related bucket mode on a data bucket and
        verify the bucket enters a non-OPTIMAL error mode.

        Steps:
            1. Create a data bucket with an AWS-backed backing store
            2. Delete the AWS target bucket of the backing store
            3. Wait for bucket to detect the resource error
            4. Verify bucket mode is not OPTIMAL via NooBaa
            5. Clean up

        Args:
            mcg_obj (MCG): MCG object with S3 connection credentials
            bucket_factory (func): Factory for creating MCG buckets
            cld_mgr (CloudManager): Cloud manager for cloud operations
            request: Pytest request object for finalizer registration
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

        def cleanup():
            bucket.delete()
            bucket.bucketclass.delete()
            backingstore.delete()

        request.addfinalizer(cleanup)

        log.info(f"Created data bucket {bucket.name} with AWS backing store")

        target_uls_name = backingstore.uls_name
        log.info(
            f"Backing store {backingstore.name} uses "
            f"target bucket {target_uls_name}"
        )

        cld_mgr.aws_client.delete_uls(target_uls_name)
        log.info(
            f"Deleted target bucket {target_uls_name} " f"to trigger resource error"
        )

        run_time = 60 * 7
        log.info(f"Waiting {run_time}s for bucket to enter error state")
        time.sleep(run_time)

        bucket_mode = _get_bucket_mode(mcg_obj, bucket.name)
        assert bucket_mode != "OPTIMAL", (
            f"Bucket {bucket.name} is still OPTIMAL after "
            f"deleting backing store target"
        )

    @skipif_mcg_only
    @polarion_id("OCS-XXXXX")
    def test_data_bucket_capacity_error_mode(
        self, mcg_obj, awscli_pod, bucket_factory, request
    ):
        """
        Trigger a capacity-related bucket mode on a data bucket and
        verify the bucket enters a non-OPTIMAL error mode.

        Steps:
            1. Create a data bucket backed by a small PV pool (2Gi)
            2. Upload data exceeding PV pool capacity
            3. Wait for bucket to detect the capacity exhaustion
            4. Verify bucket mode is not OPTIMAL via NooBaa
               (e.g. NO_CAPACITY, LOW_CAPACITY)
            5. Clean up

        Args:
            mcg_obj (MCG): MCG object with S3 connection credentials
            awscli_pod (Pod): Pod running the AWSCLI tools
            bucket_factory (func): Factory for creating MCG buckets
            request: Pytest request object for finalizer registration
        """
        bucketclass_dict = {
            "interface": "OC",
            "backingstore_dict": {"pv": [(1, 2, constants.DEFAULT_STORAGECLASS_RBD)]},
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
            f"2Gi PV pool backing store {backingstore.name}"
        )

        _upload_data_to_bucket(awscli_pod, bucket.name, mcg_obj, 4, 700)
        log.info(
            f"Uploaded ~2.8GB to bucket {bucket.name} "
            f"(PV pool capacity 2Gi) to trigger capacity error"
        )

        run_time = 60 * 7
        log.info(f"Waiting {run_time}s for bucket to enter capacity error state")
        time.sleep(run_time)

        bucket_mode = _get_bucket_mode(mcg_obj, bucket.name)
        assert bucket_mode != "OPTIMAL", (
            f"Bucket {bucket.name} is still OPTIMAL after "
            f"exceeding PV pool capacity"
        )

    # ------------------------------------------------------------------
    # Namespace bucket tests
    # ------------------------------------------------------------------

    @polarion_id("OCS-XXXXX")
    def test_ns_bucket_resource_error_mode(
        self,
        mcg_obj,
        bucket_factory,
        namespace_store_factory,
        cld_mgr,
        request,
    ):
        """
        Trigger a resource-related bucket mode on a namespace bucket
        and verify the bucket enters a non-OPTIMAL error mode.

        Steps:
            1. Create 2 namespace stores backed by AWS target buckets
            2. Create a namespace bucket using Multi policy
            3. Delete the target bucket of one namespace store
            4. Wait for bucket to detect the resource error
            5. Verify bucket mode is not OPTIMAL via NooBaa
            6. Clean up

        Args:
            mcg_obj (MCG): MCG object with S3 connection credentials
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

        target_uls_name = ns_stores[0].uls_name
        log.info(
            f"Deleting target bucket {target_uls_name} of "
            f"namespace store {ns_stores[0].name}"
        )
        cld_mgr.aws_client.delete_uls(target_uls_name)

        run_time = 60 * 7
        log.info(f"Waiting {run_time}s for namespace bucket to enter error state")
        time.sleep(run_time)

        bucket_mode = _get_bucket_mode(mcg_obj, ns_bucket.name)
        assert bucket_mode != "OPTIMAL", (
            f"Namespace bucket {ns_bucket.name} is still OPTIMAL "
            f"after deleting namespace store target"
        )

    @polarion_id("OCS-XXXXX")
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
            3. Set a 100Mi quota on the namespace bucket
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
            cmd=f"bucket update --max-size=100Mi {ns_bucket.name}",
            namespace=config.ENV_DATA["cluster_namespace"],
            use_yes=True,
        )
        log.info(f"Set 100Mi quota on namespace bucket {ns_bucket.name}")

        _upload_data_to_bucket(awscli_pod, ns_bucket.name, mcg_obj, 3, 50)
        log.info(
            f"Uploaded ~150MB to namespace bucket {ns_bucket.name} "
            f"(quota 100Mi) to trigger EXCEEDING_QUOTA"
        )

        run_time = 60 * 7
        log.info(f"Waiting {run_time}s for bucket mode to propagate")
        time.sleep(run_time)

        expected_mode = "EXCEEDING_QUOTA"
        bucket_mode = _get_bucket_mode(mcg_obj, ns_bucket.name)
        assert (
            bucket_mode == expected_mode
        ), f"Expected bucket mode {expected_mode}, got {bucket_mode}"

    @skipif_mcg_only
    @polarion_id("OCS-XXXXX")
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
        and verify the bucket enters a non-OPTIMAL error mode.

        Uses a Cache namespace bucket where the cache layer is a small
        PV pool backing store (2Gi). Filling the cache beyond its
        capacity triggers a capacity error mode.

        Steps:
            1. Create a PV pool backing store (2Gi) for the cache layer
            2. Create an AWS-backed namespace store for the hub
            3. Create a Cache namespace bucket with the AWS hub and
               PV pool cache
            4. Upload data exceeding PV pool capacity
            5. Wait for bucket to detect the capacity exhaustion
            6. Verify bucket mode is not OPTIMAL via NooBaa
               (e.g. NO_CAPACITY, LOW_CAPACITY)
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
            "oc", {"pv": [(1, 2, constants.DEFAULT_STORAGECLASS_RBD)]}
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
            f"(2Gi) for cache layer"
        )
        log.info(f"Created namespace store (hub): {ns_stores[0].name}")
        log.info(f"Created Cache namespace bucket {ns_bucket.name} with PV pool cache")

        _upload_data_to_bucket(awscli_pod, ns_bucket.name, mcg_obj, 4, 700)
        log.info(
            f"Uploaded ~2.8GB to cache namespace bucket {ns_bucket.name} "
            f"(cache PV pool 2Gi) to trigger capacity error"
        )

        run_time = 60 * 7
        log.info(f"Waiting {run_time}s for namespace bucket to enter error state")
        time.sleep(run_time)

        bucket_mode = _get_bucket_mode(mcg_obj, ns_bucket.name)
        assert bucket_mode != "OPTIMAL", (
            f"Namespace bucket {ns_bucket.name} is still OPTIMAL "
            f"after exceeding cache PV pool capacity"
        )


def setup_module(module):
    ocs_obj = OCP()
    module.original_user = ocs_obj.get_user_name()


def teardown_module(module):
    if hasattr(module, "original_user"):
        ocs_obj = OCP()
        ocs_obj.login_as_user(module.original_user)
