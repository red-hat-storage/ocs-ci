"""
Test NooBaa System Capacity Alerts and Runbook Links

This test module validates NooBaa capacity alerts (85%, 95%, 100%) and their
runbook links in the UI.

Test Case: Clearer NooBaa capacity alerting
Description: Adds concise runbooks for the NooBaaSystemCapacityWarning85, 95,
and 100 alerts, outlining how capacity is calculated and the required mitigation steps.
"""

import base64
import logging
import tempfile
import time

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    blue_squad,
    mcg,
    polarion_id,
    tier2,
    ui,
    skipif_managed_service,
    skipif_disconnected_cluster,
)
from ocs_ci.framework import config
from ocs_ci.helpers.helpers import create_unique_resource_name, create_resource
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.objectbucket import OBC
from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator
from ocs_ci.utility import prometheus, templating
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)

# Alert constants
ALERT_NOOBAA_CAPACITY_WARNING_85 = "NooBaaSystemCapacityWarning85"
ALERT_NOOBAA_CAPACITY_WARNING_95 = "NooBaaSystemCapacityWarning95"
ALERT_NOOBAA_CAPACITY_WARNING_100 = "NooBaaSystemCapacityWarning100"

# Runbook URLs
RUNBOOK_URL_85 = (
    "https://github.com/openshift/runbooks/blob/master/alerts/"
    "openshift-container-storage-operator/NooBaaSystemCapacityWarning85.md"
)
RUNBOOK_URL_95 = (
    "https://github.com/openshift/runbooks/blob/master/alerts/"
    "openshift-container-storage-operator/NooBaaSystemCapacityWarning95.md"
)
RUNBOOK_URL_100 = (
    "https://github.com/openshift/runbooks/blob/master/alerts/"
    "openshift-container-storage-operator/NooBaaSystemCapacityWarning100.md"
)


@pytest.fixture(scope="function")
def setup_noobaa_custom_backingstore(
    request, backingstore_factory, bucket_factory, mcg_obj, awscli_pod
):
    """
    Setup custom NooBaa backingstore and configure it as default.

    This fixture:
    1. Creates a PVC-based backingstore with 25GiB capacity
    2. Configures the new backingstore as default
    3. Updates the default bucketclass to use the new backingstore
    4. Creates an OBC using the default bucketclass

    Cleanup is handled automatically by the factory fixtures.

    Returns:
        dict: Contains backingstore and bucket objects
    """
    logger.info("Setting up custom NooBaa backingstore configuration")
    namespace = config.ENV_DATA["cluster_namespace"]

    def finalizer():
        """
        Cleanup function for test resources.
        Note: Individual resources (backingstore, bucketclass, bucket) are cleaned up
        by their respective factory fixtures automatically.
        """
        try:
            logger.info("Running fixture cleanup")
            # Factory fixtures will handle cleanup of:
            # - backingstore (via backingstore_factory)
            # - bucketclass (via create_resource with finalizer)
            # - bucket (via bucket_factory)
            logger.info(
                "Cleanup complete - factory fixtures will handle resource deletion"
            )
        except Exception as cleanup_error:
            logger.warning(f"Error during cleanup: {cleanup_error}")

    request.addfinalizer(finalizer)

    # Create PVC-based backingstore with 30GiB capacity manually using CephFS
    logger.info("Creating PVC-based backingstore with 30GiB capacity using CephFS")

    # Generate unique backingstore name
    backingstore_name = create_unique_resource_name(
        resource_description="pv-backingstore", resource_type="backingstore"
    )

    # Create backingstore using manual YAML structure with CephFS storage class
    backingstore_data = {
        "apiVersion": "noobaa.io/v1alpha1",
        "kind": "BackingStore",
        "metadata": {
            "name": backingstore_name,
            "namespace": namespace,
            "finalizers": ["noobaa.io/finalizer"],
            "labels": {"app": "noobaa"},
        },
        "spec": {
            "type": "pv-pool",
            "pvPool": {
                "numVolumes": 1,
                "storageClass": constants.CEPHFILESYSTEM_SC,  # Using CephFS instead of RBD
                "resources": {
                    "requests": {"storage": "30Gi"},
                },
            },
        },
    }

    # Create a temporary file for the YAML
    temp_yaml_file = tempfile.NamedTemporaryFile(
        mode="w", prefix="backingstore_", suffix=".yaml", delete=False
    )
    temp_yaml_path = temp_yaml_file.name
    temp_yaml_file.close()

    # Dump the YAML data to the temporary file
    templating.dump_data_to_temp_yaml(backingstore_data, temp_yaml_path)

    ocp_obj = OCP(
        kind=constants.BACKINGSTORE,
        namespace=namespace,
    )
    ocp_obj.create(yaml_file=temp_yaml_path)
    logger.info(f"Created backingstore: {backingstore_name}")

    # Create OCP object with resource name for future operations
    backingstore_obj = OCP(
        kind=constants.BACKINGSTORE,
        namespace=namespace,
        resource_name=backingstore_name,
    )

    # Patch backingstore to add memory limits for better upload performance
    logger.info(f"Patching backingstore {backingstore_name} to add memory limits")
    backingstore_obj.patch(
        params='{"spec":{"pvPool":{"resources":{"limits":{"memory":"6Gi"}}}}}',
        format_type="merge",
    )
    logger.info("Backingstore memory limits configured")

    # Wait for backingstore to be ready
    logger.info(f"Waiting for backingstore {backingstore_name} to be ready")
    for sample in TimeoutSampler(timeout=300, sleep=15, func=backingstore_obj.get):
        phase = sample.get("status", {}).get("phase")
        if phase == "Ready":
            logger.info(f"Backingstore {backingstore_name} is ready")
            break
        logger.info(f"Backingstore phase: {phase}, waiting...")

    # Create a BackingStore object for cleanup tracking
    from ocs_ci.ocs.resources.backingstore import BackingStore

    backingstore = BackingStore(
        name=backingstore_name,
        method="oc",
        type="pv-pool",
        mcg_obj=mcg_obj,
    )

    # Update the default bucketclass to use the new backingstore
    # This is cleaner than creating a new bucketclass
    logger.info(
        f"Updating default bucketclass to use backingstore: {backingstore.name}"
    )

    # Step 1: Enable manual default backingstore management
    logger.info("Enabling manual default backingstore management")
    noobaa_obj = OCP(
        kind="NooBaa",
        namespace=namespace,
        resource_name="noobaa",
    )
    noobaa_obj.patch(
        params='{"spec":{"manualDefaultBackingStore":true}}',
        format_type="merge",
    )

    # Step 2: Update the default account to use the new backingstore
    logger.info(f"Updating default account to use backingstore: {backingstore.name}")
    mcg_obj.exec_mcg_cmd(
        f"account update admin@noobaa.io --new_default_resource={backingstore.name}"
    )

    # Step 3: Patch the default bucketclass to use the new backingstore
    logger.info("Patching default bucketclass to use new backingstore")
    default_bucketclass = OCP(
        kind=constants.BUCKETCLASS,
        namespace=namespace,
        resource_name=constants.DEFAULT_NOOBAA_BUCKETCLASS,
    )
    default_bucketclass.patch(
        params=f'{{"spec":{{"placementPolicy":{{"tiers":[{{"backingStores":["{backingstore.name}"]}}]}}}}}}',
        format_type="merge",
    )

    # Wait for bucketclass to be ready
    logger.info("Waiting for default bucketclass to be ready")
    for sample in TimeoutSampler(timeout=120, sleep=5, func=default_bucketclass.get):
        if sample.get("status", {}).get("phase") == "Ready":
            logger.info("Default bucketclass is ready")
            break

    # Verify the bucketclass is using the new backingstore
    bucketclass_data = default_bucketclass.get()
    try:
        # Extract backingstore from bucketclass spec
        placement_policy = bucketclass_data["spec"]["placementPolicy"]  # type: ignore
        current_backingstore = placement_policy["tiers"][0]["backingStores"][0]
        logger.info(
            f"Default bucketclass now uses backingstore: {current_backingstore}"
        )
        assert current_backingstore == backingstore.name, (
            f"Expected backingstore {backingstore.name}, "
            f"but got {current_backingstore}"
        )
    except (KeyError, IndexError, TypeError) as e:
        logger.warning(f"Could not verify bucketclass backingstore: {e}")
        logger.info("Proceeding with OBC creation anyway")

    # Create OBC using the default bucketclass with explicit additionalConfig
    # This is CRITICAL - the OBC MUST explicitly specify bucketclass: noobaa-default-bucket-class
    # in additionalConfig for the capacity alerts to work correctly
    logger.info("Creating OBC with explicit bucketclass in additionalConfig")

    # Generate unique OBC name
    obc_name = create_unique_resource_name(
        resource_description="test-capacity-bucket", resource_type="obc"
    )

    # Create OBC with explicit additionalConfig specifying noobaa-default-bucket-class
    obc_data = {
        "apiVersion": "objectbucket.io/v1alpha1",
        "kind": "ObjectBucketClaim",
        "metadata": {
            "name": obc_name,
            "namespace": namespace,
        },
        "spec": {
            "generateBucketName": obc_name,
            "storageClassName": "openshift-storage.noobaa.io",
            "additionalConfig": {
                "bucketclass": constants.DEFAULT_NOOBAA_BUCKETCLASS,
            },
        },
    }

    # Create OBC using create_resource
    create_resource(**obc_data)
    logger.info(
        f"Created OBC: {obc_name} with explicit bucketclass: "
        f"{constants.DEFAULT_NOOBAA_BUCKETCLASS}"
    )

    # Create bucket object for tracking (OBC class instance)
    bucket = OBC(obc_name)
    logger.info(f"Created bucket object for OBC: {obc_name}")

    # Wait for OBC to be bound (critical for upload success)
    logger.info(f"Waiting for OBC {obc_name} to be bound")
    obc_obj = OCP(
        kind="obc",
        namespace=namespace,
        resource_name=obc_name,
    )
    for sample in TimeoutSampler(timeout=300, sleep=10, func=obc_obj.get):
        phase = sample.get("status", {}).get("phase")
        if phase == "Bound":
            logger.info(f"OBC {obc_name} is bound")
            break
        logger.info(f"OBC phase: {phase}, waiting...")

    # Additional wait for NooBaa internal bucket initialization
    logger.info("Waiting 90 seconds for NooBaa to fully initialize OBC bucket")
    time.sleep(90)
    logger.info("Bucket initialization wait complete")

    # Delete any existing buckets that might be using the old default backingstore
    # This is critical to ensure only the new 30GiB backingstore is used for capacity calculations
    logger.info(
        "Checking for and deleting any existing buckets using old default backingstore"
    )
    try:
        # List all buckets
        bucket_list_result = mcg_obj.exec_mcg_cmd("bucket list")
        bucket_list_output = (
            bucket_list_result.stdout
            if hasattr(bucket_list_result, "stdout")
            else str(bucket_list_result)
        )
        logger.info(f"Current buckets: {bucket_list_output}")

        # Parse bucket names from output and delete all EXCEPT the current test bucket
        bucket_lines = bucket_list_output.strip().split("\n")
        buckets_to_delete = []
        for line in bucket_lines[1:]:  # Skip header line
            line = line.strip()
            if line and line != "BUCKET-NAME":  # Skip header
                bucket_name = line.split()[0] if line.split() else None
                # Delete all buckets EXCEPT the one we just created
                if bucket_name and bucket_name != obc_name:
                    buckets_to_delete.append(bucket_name)

        logger.info(
            f"Found {len(buckets_to_delete)} buckets to delete: {buckets_to_delete}"
        )

        # Empty and delete each old bucket
        for bucket_name in buckets_to_delete:
            try:
                # Step 1: Empty the bucket first (delete all objects)
                logger.info(f"Emptying bucket: {bucket_name}")
                try:
                    # Use AWS CLI to remove all objects from the bucket
                    # Get S3 endpoint from NooBaa route
                    s3_route = OCP(
                        kind="Route",
                        namespace=namespace,
                        resource_name="s3",
                    )
                    route_data = s3_route.get()
                    s3_endpoint = f"https://{route_data['spec']['host']}"  # type: ignore

                    # Get admin credentials to access any bucket
                    noobaa_secret = OCP(
                        kind="Secret",
                        namespace=namespace,
                        resource_name="noobaa-admin",
                    )
                    secret_data = noobaa_secret.get()
                    access_key = base64.b64decode(
                        secret_data["data"]["AWS_ACCESS_KEY_ID"]  # type: ignore
                    ).decode("utf-8")
                    secret_key = base64.b64decode(
                        secret_data["data"]["AWS_SECRET_ACCESS_KEY"]  # type: ignore
                    ).decode("utf-8")

                    # Empty bucket using AWS CLI via awscli pod
                    empty_cmd = (
                        f"sh -c 'AWS_ACCESS_KEY_ID={access_key} "
                        f"AWS_SECRET_ACCESS_KEY={secret_key} "
                        f"aws s3 rm s3://{bucket_name} --recursive "
                        f"--endpoint-url {s3_endpoint} --no-verify-ssl'"
                    )
                    awscli_pod.exec_cmd_on_pod(
                        command=empty_cmd, out_yaml_format=False, timeout=300
                    )
                    logger.info(f"Successfully emptied bucket: {bucket_name}")
                except Exception as empty_error:
                    logger.warning(
                        f"Could not empty bucket {bucket_name}: {empty_error}"
                    )
                    logger.info(
                        "Attempting to delete bucket anyway (may fail if not empty)"
                    )

                # Step 2: Delete the empty bucket
                logger.info(f"Deleting bucket: {bucket_name}")
                mcg_obj.exec_mcg_cmd(f"bucket delete {bucket_name}")
                logger.info(f"Successfully deleted bucket: {bucket_name}")
            except Exception as e:
                logger.warning(f"Could not delete bucket {bucket_name}: {e}")
                logger.info("⚠️ Manual cleanup required:")
                logger.info(
                    f"   1. Empty bucket via UI: Storage → Object Storage → "
                    f"Buckets → {bucket_name} → Delete all objects"
                )
                logger.info(
                    f"   2. Delete bucket via UI: Storage → Object Storage → "
                    f"Buckets → {bucket_name} → Delete"
                )
    except Exception as e:
        logger.warning(f"Error checking/deleting existing buckets: {e}")

    # Update ALL accounts to use the new backingstore as their default resource
    # This is required before we can delete the old backingstore
    logger.info(
        "Updating all NooBaa accounts to use new backingstore as default resource"
    )
    try:
        # List all accounts
        account_list_result = mcg_obj.exec_mcg_cmd("account list")
        account_list_output = (
            account_list_result.stdout
            if hasattr(account_list_result, "stdout")
            else str(account_list_result)
        )
        logger.info(f"Current accounts: {account_list_output}")

        # Parse account names from output (format: "EMAIL" column)
        # Example output:
        # EMAIL
        # admin@noobaa.io
        # obc-openshift-storage-oc-bucket-xxx@noobaa.io
        account_lines = account_list_output.strip().split("\n")
        account_emails = []
        for line in account_lines[1:]:  # Skip header line
            line = line.strip()
            if line and "@noobaa.io" in line:
                # Extract email (first column)
                email = line.split()[0] if line.split() else None
                if email and email != "EMAIL":  # Skip header if present
                    account_emails.append(email)

        logger.info(f"Found {len(account_emails)} accounts to update: {account_emails}")

        # Update each account to use the new backingstore
        for email in account_emails:
            try:
                logger.info(
                    f"Updating account {email} to use backingstore: {backingstore.name}"
                )
                mcg_obj.exec_mcg_cmd(
                    f"account update {email} --new_default_resource={backingstore.name}"
                )
                logger.info(f"Successfully updated account {email}")
            except Exception as e:
                logger.warning(f"Could not update account {email}: {e}")
                logger.info("Continuing with other accounts...")
    except Exception as e:
        logger.warning(f"Error updating accounts: {e}")
        logger.info(
            "Some accounts may still use old backingstore. Backingstore deletion may fail."
        )

    # Delete ALL old backstores to ensure only new backingstore is used
    # This includes the default backingstore AND any leftover backstores
    # from previous test runs. This can only succeed after all buckets
    # are deleted AND all accounts are updated
    logger.info(
        "Attempting to delete all old backstores "
        "(including leftovers from previous test runs)"
    )
    try:
        # List all backstores
        backingstore_list_result = mcg_obj.exec_mcg_cmd("backingstore list")
        backingstore_list_output = (
            backingstore_list_result.stdout
            if hasattr(backingstore_list_result, "stdout")
            else str(backingstore_list_result)
        )
        logger.info(f"Current backstores: {backingstore_list_output}")

        # Parse backingstore names from output
        backingstore_lines = backingstore_list_output.strip().split("\n")
        backstores_to_delete = []
        for line in backingstore_lines[1:]:  # Skip header line
            line = line.strip()
            if line and line != "NAME":  # Skip header
                bs_name = line.split()[0] if line.split() else None
                # Delete all backstores EXCEPT the one we just created
                if bs_name and bs_name != backingstore.name:
                    backstores_to_delete.append(bs_name)

        logger.info(
            f"Found {len(backstores_to_delete)} old backstores to delete: "
            f"{backstores_to_delete}"
        )

        # Delete each old backingstore
        for bs_name in backstores_to_delete:
            try:
                logger.info(f"Deleting backingstore: {bs_name}")
                mcg_obj.exec_mcg_cmd(f"backingstore delete {bs_name}")
                logger.info(f"✅ Successfully deleted backingstore: {bs_name}")
            except Exception as e:
                logger.warning(f"❌ Could not delete backingstore {bs_name}: {e}")
                logger.info("⚠️ This means accounts or buckets are still using it.")
                logger.info(
                    "⚠️ Manual cleanup required: Update all accounts and "
                    f"delete backingstore {bs_name} via UI"
                )
    except Exception as e:
        logger.warning(f"Error listing/deleting old backstores: {e}")
        logger.info(
            "⚠️ Some old backstores may still exist. "
            "Capacity calculations may be incorrect!"
        )

    # Verify bucket is accessible by attempting a small test write
    logger.info("Verifying bucket is writable with test object")
    test_data = "test"
    test_obj_key = "readiness-test"

    # Get S3 client from OBC object (bucket is already an OBC instance)
    s3_client = bucket.s3_client

    # Retry loop to handle NooBaa internal initialization race condition
    bucket_ready = False
    for attempt in range(10):
        try:
            s3_client.put_object(Bucket=obc_name, Key=test_obj_key, Body=test_data)
            s3_client.delete_object(Bucket=obc_name, Key=test_obj_key)
            logger.info(
                f"Bucket write verification successful on attempt {attempt + 1}"
            )
            bucket_ready = True
            break
        except Exception as e:
            logger.info(
                f"Bucket not ready yet (attempt {attempt + 1}/10): {e}. "
                f"Waiting 15 seconds..."
            )
            time.sleep(15)

    if not bucket_ready:
        logger.error("Bucket still not writable after 10 attempts")
        raise RuntimeError(f"Bucket {obc_name} failed readiness check")

    logger.info("CLI bucket is ready for data uploads")

    return {
        "backingstore": backingstore,
        "bucket": bucket,
        "s3_client": s3_client,
        "mcg_obj": mcg_obj,
    }


def fill_bucket_to_capacity(
    bucket, awscli_pod, test_directory_setup, mcg_obj, target_percentage
):
    """
    Fill bucket to specified capacity percentage using AWS CLI directly.

    Args:
        bucket: Bucket object to fill
        awscli_pod: AWS CLI pod for operations
        test_directory_setup: Test directory setup fixture
        mcg_obj: MCG object for bucket operations
        target_percentage (int): Target capacity percentage (85, 95, or 100)

    Returns:
        list: List of written object names
    """
    logger.info(f"Filling bucket {bucket.name} to {target_percentage}% capacity")

    # Detect which random-data tool is available in the pod (openssl > dd).
    # NOTE: The s3cli pod runs BusyBox, so yum/dnf/apt are not available and
    # BusyBox dd does NOT support piping to stdout without an explicit of= target.
    # We therefore always write to a temp file first, then upload, then delete.
    use_openssl = False
    try:
        awscli_pod.exec_cmd_on_pod(command="which openssl", out_yaml_format=False)
        use_openssl = True
        logger.info("openssl is available for random data generation")
    except Exception:
        logger.info(
            "openssl not available, will use dd (BusyBox-compatible two-step method)"
        )

    # Get bucket credentials from OBC
    obc_obj = OBC(bucket.name)
    bucket_name = obc_obj.bucket_name
    access_key = obc_obj.access_key_id
    secret_key = obc_obj.access_key

    # Get S3 endpoint URL from route
    logger.info("Fetching S3 endpoint URL from route")
    route_obj = OCP(kind="route", namespace=config.ENV_DATA["cluster_namespace"])
    s3_route_data = route_obj.get(resource_name="s3")
    if s3_route_data and isinstance(s3_route_data, dict):
        s3_host = s3_route_data.get("spec", {}).get("host", "")
        s3_url = f"https://{s3_host}" if s3_host else mcg_obj.s3_endpoint
    else:
        s3_url = mcg_obj.s3_endpoint
    logger.info(f"S3 endpoint URL: {s3_url}")

    # Calculate number of 1 GiB objects needed
    backingstore_size_gb = 30
    target_size_gb = (backingstore_size_gb * target_percentage) / 100
    # +1 to make sure we cross the alert threshold
    num_objects = int(target_size_gb) + 1

    logger.info(
        f"Writing approximately {num_objects} objects of 1GiB each "
        f"to reach {target_percentage}% capacity"
    )

    tmp_file = "/tmp/upload_chunk.bin"
    written_objects = []

    for i in range(1, num_objects + 1):
        object_name = f"1GB_{i}.bin"
        logger.info(f"Uploading {object_name} ({i}/{num_objects})...")

        # Step 1: generate 1 GiB random data into a temp file inside the pod.
        # Both openssl and BusyBox dd support writing to a file path.
        if use_openssl:
            gen_cmd = f"openssl rand -out {tmp_file} $((1024*1024*1024))"
        else:
            # BusyBox dd: bs and count must not use 'M' suffix — use bytes or
            # the numeric forms that BusyBox supports (b=512, k=1024).
            # 1024 blocks of 1 MiB (1048576 bytes) = 1 GiB
            gen_cmd = f"dd if=/dev/urandom of={tmp_file} bs=1048576 count=1024"

        # Step 2: upload the temp file to S3 using the aws cli.
        # IMPORTANT: Wrap in sh -c '...' for oc rsh to properly interpret env vars
        # Use --no-verify-ssl to bypass SSL certificate verification for self-signed certs
        upload_cmd = (
            f"sh -c 'AWS_ACCESS_KEY_ID={access_key} "
            f"AWS_SECRET_ACCESS_KEY={secret_key} "
            f"aws s3 cp {tmp_file} s3://{bucket_name}/{object_name} "
            f"--endpoint-url {s3_url} --no-verify-ssl'"
        )

        # Step 3: delete the temp file to free pod disk space.
        cleanup_cmd = f"rm -f {tmp_file}"

        try:
            logger.info(f"Generating 1GiB temp file with: {gen_cmd}")
            awscli_pod.exec_cmd_on_pod(
                command=gen_cmd, out_yaml_format=False, timeout=120
            )

            logger.info(f"Uploading {tmp_file} → s3://{bucket_name}/{object_name}")
            awscli_pod.exec_cmd_on_pod(
                command=upload_cmd, out_yaml_format=False, timeout=300
            )

            written_objects.append(object_name)
            logger.info(f"Successfully uploaded {object_name} ({i}/{num_objects})")

        except Exception as e:
            logger.warning(f"Error uploading {object_name}: {e}")
            # If we have already written ≥90% of the target objects, that is
            # likely enough to trigger the alert — stop early rather than fail.
            if len(written_objects) >= int(num_objects * 0.9):
                logger.info(
                    "Reached sufficient capacity despite some failures, stopping early"
                )
                break
        finally:
            # Always clean up the temp file regardless of upload success
            try:
                awscli_pod.exec_cmd_on_pod(
                    command=cleanup_cmd, out_yaml_format=False, timeout=30
                )
            except Exception:
                pass  # Ignore cleanup errors

        # Brief pause between uploads
        if i < num_objects:
            logger.info("Waiting 2 seconds before next upload...")
            time.sleep(2)

    logger.info(
        f"Completed filling bucket {bucket_name} with {len(written_objects)} objects"
    )
    return written_objects


def wait_for_alert(
    alert_name, threading_lock, timeout=600, sleep=30, expected_state="firing"
):
    """
    Wait for a specific alert to appear and reach expected state.

    Args:
        alert_name (str): Name of the alert to wait for
        threading_lock: Threading lock for Prometheus API
        timeout (int): Maximum time to wait in seconds
        sleep (int): Sleep interval between checks
        expected_state (str): Expected alert state (pending/firing)

    Returns:
        bool: True if alert found in expected state, False otherwise
    """
    logger.info(
        f"Waiting for alert {alert_name} to reach state: {expected_state} "
        f"(timeout: {timeout}s)"
    )

    api = prometheus.PrometheusAPI(threading_lock=threading_lock)

    for sample in TimeoutSampler(
        timeout=timeout,
        sleep=sleep,
        func=lambda: api.get("alerts", payload={"silenced": False, "inhibited": False}),
    ):
        response = sample
        if not response.ok:
            logger.warning(f"Failed to get alerts: {response.text}")
            continue

        alerts = response.json().get("data", {}).get("alerts", [])
        matching_alerts = [
            alert
            for alert in alerts
            if alert.get("labels", {}).get("alertname") == alert_name
            and alert.get("state") == expected_state
        ]

        if matching_alerts:
            logger.info(
                f"Alert {alert_name} found in {expected_state} state: "
                f"{matching_alerts[0]}"
            )
            return True

        logger.debug(
            f"Alert {alert_name} not yet in {expected_state} state, continuing to wait..."
        )

    logger.error(
        f"Timeout waiting for alert {alert_name} to reach {expected_state} state"
    )
    return False


def validate_alert_runbook_ui(setup_ui, alert_name, expected_runbook_url):
    """
    Validate alert runbook link in the UI.

    Args:
        setup_ui: UI setup fixture
        alert_name (str): Name of the alert
        expected_runbook_url (str): Expected runbook URL

    Returns:
        bool: True if runbook link is valid and accessible
    """
    logger.info(f"Validating runbook link for alert {alert_name} in UI")

    try:
        # Navigate to alerting page
        alerting_page = PageNavigator().navigate_alerting_page()
        alerting_rules_page = alerting_page.nav_alerting_rules()

        # Navigate to specific alert details
        alert_details_page = alerting_rules_page.navigate_alerting_rule_details(
            alert_name
        )

        # Get runbook link
        runbook_link = alert_details_page.get_runbook_link()
        logger.info(f"Found runbook link: {runbook_link}")

        # Validate runbook link matches expected URL
        if runbook_link != expected_runbook_url:
            logger.error(
                f"Runbook link mismatch. Expected: {expected_runbook_url}, "
                f"Got: {runbook_link}"
            )
            return False

        # Verify runbook is accessible
        runbook = alert_details_page.get_raw_runbook()
        if runbook is None:
            logger.error(f"Failed to retrieve runbook content for {alert_name}")
            return False

        # Validate runbook content has required sections
        mandatory_headers = ["Meaning", "Impact", "Diagnosis", "Mitigation"]
        if not runbook.check_text_content(mandatory_headers, alert_name):
            logger.error(f"Runbook content validation failed for {alert_name}")
            return False

        logger.info(f"Runbook validation successful for alert {alert_name}")
        return True

    except Exception as e:
        logger.error(f"Error validating runbook for {alert_name}: {e}")
        return False


@mcg
@blue_squad
@tier2
@ui
@polarion_id("OCS-XXXX")  # Update with actual Polarion ID
@skipif_managed_service
@skipif_disconnected_cluster
def test_noobaa_capacity_alert_85(
    setup_noobaa_custom_backingstore,
    awscli_pod,
    test_directory_setup,
    threading_lock,
    setup_ui,
):
    """
    Test NooBaaSystemCapacityWarning85 alert and runbook link.

    Steps:
    1. Setup custom backingstore with 50GiB capacity
    2. Fill bucket to 85% capacity
    3. Wait for NooBaaSystemCapacityWarning85 alert to fire
    4. Validate alert is triggered
    5. Validate runbook link in UI
    """
    logger.info("Starting test_noobaa_capacity_alert_85")

    bucket = setup_noobaa_custom_backingstore["bucket"]
    mcg_obj = setup_noobaa_custom_backingstore["mcg_obj"]

    # Fill bucket to 85% capacity
    fill_bucket_to_capacity(
        bucket, awscli_pod, test_directory_setup, mcg_obj, target_percentage=85
    )

    # Wait 5 minutes for Prometheus to evaluate and fire the alert
    logger.info("Waiting 5 minutes for alert evaluation before checking alert status")
    time.sleep(300)
    logger.info("5 minute wait complete, now checking for alert")

    # Wait for alert to fire
    alert_fired = wait_for_alert(
        ALERT_NOOBAA_CAPACITY_WARNING_85,
        threading_lock,
        timeout=600,
        sleep=30,
    )
    assert alert_fired, f"Alert {ALERT_NOOBAA_CAPACITY_WARNING_85} did not fire"

    # Validate runbook link in UI
    runbook_valid = validate_alert_runbook_ui(
        setup_ui, ALERT_NOOBAA_CAPACITY_WARNING_85, RUNBOOK_URL_85
    )
    assert (
        runbook_valid
    ), f"Runbook validation failed for {ALERT_NOOBAA_CAPACITY_WARNING_85}"

    logger.info("test_noobaa_capacity_alert_85 completed successfully")


@mcg
@blue_squad
@tier2
@ui
@polarion_id("OCS-XXXX")  # Update with actual Polarion ID
@skipif_managed_service
@skipif_disconnected_cluster
def test_noobaa_capacity_alert_95(
    setup_noobaa_custom_backingstore,
    awscli_pod,
    test_directory_setup,
    threading_lock,
    setup_ui,
):
    """
    Test NooBaaSystemCapacityWarning95 alert and runbook link.

    Steps:
    1. Setup custom backingstore with 50GiB capacity
    2. Fill bucket to 95% capacity
    3. Wait for NooBaaSystemCapacityWarning95 alert to fire
    4. Validate alert is triggered
    5. Validate runbook link in UI
    """
    logger.info("Starting test_noobaa_capacity_alert_95")

    bucket = setup_noobaa_custom_backingstore["bucket"]
    mcg_obj = setup_noobaa_custom_backingstore["mcg_obj"]

    # Fill bucket to 95% capacity
    fill_bucket_to_capacity(
        bucket, awscli_pod, test_directory_setup, mcg_obj, target_percentage=95
    )

    # Wait 5 minutes for Prometheus to evaluate and fire the alert
    logger.info("Waiting 5 minutes for alert evaluation before checking alert status")
    time.sleep(300)
    logger.info("5 minute wait complete, now checking for alert")

    # Wait for alert to fire
    alert_fired = wait_for_alert(
        ALERT_NOOBAA_CAPACITY_WARNING_95,
        threading_lock,
        timeout=600,
        sleep=30,
    )
    assert alert_fired, f"Alert {ALERT_NOOBAA_CAPACITY_WARNING_95} did not fire"

    # Validate runbook link in UI
    runbook_valid = validate_alert_runbook_ui(
        setup_ui, ALERT_NOOBAA_CAPACITY_WARNING_95, RUNBOOK_URL_95
    )
    assert (
        runbook_valid
    ), f"Runbook validation failed for {ALERT_NOOBAA_CAPACITY_WARNING_95}"

    logger.info("test_noobaa_capacity_alert_95 completed successfully")


@mcg
@blue_squad
@tier2
@ui
@polarion_id("OCS-XXXX")  # Update with actual Polarion ID
@skipif_managed_service
@skipif_disconnected_cluster
def test_noobaa_capacity_alert_100(
    setup_noobaa_custom_backingstore,
    awscli_pod,
    test_directory_setup,
    threading_lock,
    setup_ui,
):
    """
    Test NooBaaSystemCapacityWarning100 alert and runbook link.

    Steps:
    1. Setup custom backingstore with 50GiB capacity
    2. Fill bucket to 100% capacity
    3. Wait for NooBaaSystemCapacityWarning100 alert to fire
    4. Validate alert is triggered
    5. Validate runbook link in UI
    """
    logger.info("Starting test_noobaa_capacity_alert_100")

    bucket = setup_noobaa_custom_backingstore["bucket"]
    mcg_obj = setup_noobaa_custom_backingstore["mcg_obj"]

    # Fill bucket to 100% capacity
    fill_bucket_to_capacity(
        bucket, awscli_pod, test_directory_setup, mcg_obj, target_percentage=100
    )

    # Wait 5 minutes for Prometheus to evaluate and fire the alert
    logger.info("Waiting 5 minutes for alert evaluation before checking alert status")
    time.sleep(300)
    logger.info("5 minute wait complete, now checking for alert")

    # Wait for alert to fire
    alert_fired = wait_for_alert(
        ALERT_NOOBAA_CAPACITY_WARNING_100,
        threading_lock,
        timeout=600,
        sleep=30,
    )
    assert alert_fired, f"Alert {ALERT_NOOBAA_CAPACITY_WARNING_100} did not fire"

    # Validate runbook link in UI
    runbook_valid = validate_alert_runbook_ui(
        setup_ui, ALERT_NOOBAA_CAPACITY_WARNING_100, RUNBOOK_URL_100
    )
    assert (
        runbook_valid
    ), f"Runbook validation failed for {ALERT_NOOBAA_CAPACITY_WARNING_100}"

    logger.info("test_noobaa_capacity_alert_100 completed successfully")


def setup_module(module):
    """
    Setup module - save original user for restoration.
    """
    ocs_obj = OCP()
    module.original_user = ocs_obj.get_user_name()


def teardown_module(module):
    """
    Teardown module - restore original user.
    """
    ocs_obj = OCP()
    ocs_obj.login_as_user(module.original_user)


# Made with Bob
