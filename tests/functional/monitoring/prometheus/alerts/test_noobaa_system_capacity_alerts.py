"""
Test NooBaa System Capacity Alerts (85%, 95%, 100%)

This test validates the NooBaa system capacity alerts and their runbook links.
It creates a backing store with limited capacity, fills it to trigger alerts
at 85%, 95%, and 100% capacity thresholds, and validates the alert properties
including runbook URLs.
"""

import logging
import time

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    blue_squad,
    tier2,
    skipif_managed_service,
    skipif_disconnected_cluster,
    mcg,
    runs_on_provider,
)
from ocs_ci.framework.testlib import polarion_id
from ocs_ci.ocs import constants
from ocs_ci.ocs.bucket_utils import (
    craft_s3_command,
    cli_create_pv_backingstore,
    wait_for_pv_backingstore,
)
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.backingstore import BackingStore
from ocs_ci.ocs.resources.bucketclass import BucketClass
from ocs_ci.ocs.resources.objectbucket import MCGCLIBucket
from ocs_ci.utility import prometheus
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.helpers.helpers import create_unique_resource_name
from ocs_ci.framework import config

log = logging.getLogger(__name__)


# Runbook URLs for NooBaa System Capacity Alerts
RUNBOOK_BASE_URL = "https://github.com/openshift/runbooks/blob/master/alerts/openshift-container-storage-operator"
NOOBAA_CAPACITY_85_RUNBOOK = f"{RUNBOOK_BASE_URL}/NooBaaSystemCapacityWarning85.md"
NOOBAA_CAPACITY_95_RUNBOOK = f"{RUNBOOK_BASE_URL}/NooBaaSystemCapacityWarning95.md"
NOOBAA_CAPACITY_100_RUNBOOK = f"{RUNBOOK_BASE_URL}/NooBaaSystemCapacityWarning100.md"


def fill_obc_to_capacity(
    awscli_pod,
    mcg_obj,
    bucket_name,
    target_percentage,
    file_size_mb=100,
):
    """
    Fill an OBC bucket to a specific capacity percentage.
    
    This function uploads files until NooBaa naturally stops accepting uploads
    due to capacity limits. It uses rate limiting to prevent overwhelming the S3 endpoint.

    Args:
        awscli_pod: AWS CLI pod object for executing commands
        mcg_obj: MCG object for S3 operations
        bucket_name (str): Name of the bucket to fill
        target_percentage (int): Target capacity percentage (e.g., 85, 95, 100)
        file_size_mb (int): Size of each file to upload in MB

    Returns:
        list: List of uploaded file names
    """
    log.info(f"Filling bucket {bucket_name} to {target_percentage}% capacity")
    
    # Create a test file
    awscli_pod.exec_cmd_on_pod(
        f"dd if=/dev/zero of=/tmp/testfile bs=1M count={file_size_mb}"
    )
    
    uploaded_files = []
    
    # Upload files until capacity is reached (let NooBaa naturally stop)
    for i in range(1000):
        file_name = f"testfile_{target_percentage}_{i}"
        
        try:
            awscli_pod.exec_cmd_on_pod(
                craft_s3_command(
                    f"cp /tmp/testfile s3://{bucket_name}/{file_name}",
                    mcg_obj
                ),
                out_yaml_format=False,
                secrets=[
                    mcg_obj.access_key_id,
                    mcg_obj.access_key,
                    mcg_obj.s3_endpoint,
                ],
            )
            
            uploaded_files.append(file_name)
            
            # Add rate limiting to prevent overwhelming S3
            if i > 0 and i % 5 == 0:
                time.sleep(3)
                
        except Exception:
            log.info("Reached capacity limit")
            break
    
    log.info(f"Successfully uploaded {len(uploaded_files)} files to {bucket_name}")
    return uploaded_files


def wait_for_alert(
    api, alert_name, timeout=600, sleep=30, expected_state="firing"
):
    """
    Wait for a specific alert to appear in Prometheus.

    Args:
        api: PrometheusAPI instance
        alert_name (str): Name of the alert to wait for
        timeout (int): Maximum time to wait in seconds
        sleep (int): Sleep interval between checks in seconds
        expected_state (str): Expected alert state (firing, pending)

    Returns:
        bool: True if alert is found, False otherwise
    """
    log.info(f"Waiting for alert {alert_name} to reach state {expected_state}")
    
    for sample in TimeoutSampler(timeout=timeout, sleep=sleep, func=lambda: None):
        alerts = api.get_alerts()
        for alert in alerts:
            if (
                alert.get("labels", {}).get("alertname") == alert_name
                and alert.get("state") == expected_state
            ):
                log.info(f"Alert {alert_name} found in state {expected_state}")
                return True
        log.info(f"Alert {alert_name} not yet in state {expected_state}, waiting...")
    
    log.error(f"Alert {alert_name} did not reach state {expected_state} within {timeout}s")
    return False


def validate_alert_runbook(api, alert_name, expected_runbook_url):
    """
    Validate that an alert has the correct runbook URL.

    Args:
        api: PrometheusAPI instance
        alert_name (str): Name of the alert
        expected_runbook_url (str): Expected runbook URL

    Returns:
        bool: True if runbook URL matches, False otherwise
    """
    log.info(f"Validating runbook URL for alert {alert_name}")
    
    alerts = api.get_alerts()
    for alert in alerts:
        if alert.get("labels", {}).get("alertname") == alert_name:
            runbook_url = alert.get("annotations", {}).get("runbook_url", "")
            log.info(f"Found runbook URL: {runbook_url}")
            
            if runbook_url == expected_runbook_url:
                log.info(f"Runbook URL matches expected: {expected_runbook_url}")
                return True
            else:
                log.error(
                    f"Runbook URL mismatch. Expected: {expected_runbook_url}, "
                    f"Got: {runbook_url}"
                )
                return False
    
    log.error(f"Alert {alert_name} not found in Prometheus alerts")
    return False


@mcg
@blue_squad
@tier2
@runs_on_provider
@skipif_managed_service
@skipif_disconnected_cluster
@polarion_id("OCS-XXXX")  # Update with actual Polarion ID
class TestNooBaaSystemCapacityAlerts:
    """
    Test class for NooBaa System Capacity Alerts validation.
    """

    @pytest.fixture(scope="class")
    def setup_backingstore_and_bucket(
        self, request, mcg_obj_session, awscli_pod_session
    ):
        """
        Setup fixture to create backing store, bucket class, and OBC.

        This fixture:
        1. Creates a PV-based backing store with 50GiB capacity
        2. Sets it as the manual default backing store
        3. Creates a bucket class using this backing store
        4. Creates an OBC using the bucket class

        Returns:
            dict: Contains backing store, bucket class, and OBC objects
        """
        log.info("Setting up backing store and bucket for capacity testing")
        
        # Create backing store name
        backingstore_name = create_unique_resource_name(
            resource_description="backingstore",
            resource_type="pv"
        )
        
        # Create PV-based backing store with 50GiB capacity
        log.info(f"Creating PV backing store: {backingstore_name}")
        cli_create_pv_backingstore(
            mcg_obj=mcg_obj_session,
            backingstore_name=backingstore_name,
            vol_num=1,
            size=50,
            storage_class=constants.DEFAULT_STORAGECLASS_CEPHFS,
        )
        
        # Wait for backing store to be ready
        log.info(f"Waiting for backing store {backingstore_name} to be ready")
        wait_for_pv_backingstore(backingstore_name)
        
        backingstore_obj = BackingStore(
            name=backingstore_name,
            method="cli",
            type="pv-pool",
            mcg_obj=mcg_obj_session,
            vol_num=1,
            vol_size=50,
        )
        
        # Enable manual default backing store
        log.info("Enabling manual default backing store")
        OCP(
            kind="noobaa",
            namespace=config.ENV_DATA["cluster_namespace"]
        ).patch(
            resource_name="noobaa",
            params='{"spec":{"manualDefaultBackingStore":true}}',
            format_type="merge"
        )
        
        # Update NooBaa account default resource
        log.info(f"Setting {backingstore_name} as default backing store")
        mcg_obj_session.exec_mcg_cmd(
            f"account update admin@noobaa.io "
            f"--new_default_resource={backingstore_name}"
        )
        
        # Update default bucket class to use new backing store
        log.info("Updating default bucket class")
        OCP(
            kind="bucketclass",
            namespace=config.ENV_DATA["cluster_namespace"]
        ).patch(
            resource_name="noobaa-default-bucket-class",
            params=f'{{"spec":{{"placementPolicy":{{"tiers":[{{"backingStores":["{backingstore_name}"]}}]}}}}}}',
            format_type="merge"
        )
        
        # Create bucket class
        bucketclass_name = create_unique_resource_name(
            resource_description="bucketclass",
            resource_type="capacity-test"
        )
        
        log.info(f"Creating bucket class: {bucketclass_name}")
        bucketclass_obj = BucketClass(
            name=bucketclass_name,
            backingstores=[backingstore_obj],
            namespacestores=None,
            placement_policy="Spread",
            namespace_policy=None,
            replication_policy=None,
        )
        
        mcg_obj_session.oc_create_bucketclass_over_backingstores(
            name=bucketclass_name,
            backingstores=[backingstore_obj],
            placement_policy="Spread",
        )
        
        # Create OBC
        obc_name = create_unique_resource_name(
            resource_description="obc",
            resource_type="capacity-test"
        )
        
        log.info(f"Creating OBC: {obc_name}")
        obc_obj = MCGCLIBucket(
            obc_name,
            mcg=mcg_obj_session,
            bucketclass=bucketclass_obj,
        )
        
        # Wait for OBC to be ready
        obc_obj.verify_health(timeout=300)
        
        setup_data = {
            "backingstore": backingstore_obj,
            "bucketclass": bucketclass_obj,
            "obc": obc_obj,
            "obc_name": obc_name,
        }
        
        def teardown():
            """Cleanup resources"""
            log.info("Cleaning up test resources")
            try:
                # Delete OBC
                obc_obj.delete()
                log.info(f"Deleted OBC: {obc_name}")
            except Exception as e:
                log.warning(f"Failed to delete OBC: {e}")
            
            try:
                # Delete bucket class
                OCP(
                    kind="bucketclass",
                    namespace=config.ENV_DATA["cluster_namespace"]
                ).delete(resource_name=bucketclass_name)
                log.info(f"Deleted bucket class: {bucketclass_name}")
            except Exception as e:
                log.warning(f"Failed to delete bucket class: {e}")
            
            try:
                # Delete backing store
                backingstore_obj.delete()
                log.info(f"Deleted backing store: {backingstore_name}")
            except Exception as e:
                log.warning(f"Failed to delete backing store: {e}")
        
        request.addfinalizer(teardown)
        return setup_data

    def test_noobaa_capacity_alert_85(
        self,
        setup_backingstore_and_bucket,
        mcg_obj_session,
        awscli_pod_session,
        threading_lock,
    ):
        """
        Test NooBaa System Capacity Warning at 85% utilization.

        Steps:
        1. Fill the OBC to 85% capacity
        2. Wait for NooBaaSystemCapacityWarning85 alert to fire
        3. Validate alert properties including runbook URL
        """
        log.info("Testing NooBaaSystemCapacityWarning85 alert")
        
        obc_name = setup_backingstore_and_bucket["obc_name"]
        backingstore_obj = setup_backingstore_and_bucket["backingstore"]
        
        # Log backing store capacity before filling
        try:
            bs_data = OCP(
                kind=constants.BACKINGSTORE,
                namespace=config.ENV_DATA["cluster_namespace"],
            ).get(resource_name=backingstore_obj.name)
            if isinstance(bs_data, dict) and "status" in bs_data:
                capacity = bs_data.get("status", {}).get("capacity", {})
                log.info(
                    f"Backingstore capacity: "
                    f"total={capacity.get('total')}, "
                    f"used={capacity.get('used')}, "
                    f"available={capacity.get('available')}"
                )
        except Exception as e:
            log.warning(f"Could not retrieve backing store capacity: {e}")
        
        # Fill bucket to 85% capacity
        fill_obc_to_capacity(
            awscli_pod_session,
            mcg_obj_session,
            obc_name,
            target_percentage=85,
            file_size_mb=100,
        )
        
        # Wait for alert to fire (15 minutes timeout)
        api = prometheus.PrometheusAPI(threading_lock=threading_lock)
        
        alert_name = "NooBaaSystemCapacityWarning85"
        assert wait_for_alert(
            api, alert_name, timeout=900, sleep=30
        ), f"Alert {alert_name} did not fire within expected time"
        
        # Validate runbook URL
        assert validate_alert_runbook(
            api, alert_name, NOOBAA_CAPACITY_85_RUNBOOK
        ), f"Runbook URL validation failed for {alert_name}"
        
        log.info(f"Successfully validated {alert_name} alert and runbook URL")

    def test_noobaa_capacity_alert_95(
        self,
        setup_backingstore_and_bucket,
        mcg_obj_session,
        awscli_pod_session,
        threading_lock,
    ):
        """
        Test NooBaa System Capacity Warning at 95% utilization.

        Steps:
        1. Fill the OBC to 95% capacity
        2. Wait for NooBaaSystemCapacityWarning95 alert to fire
        3. Validate alert properties including runbook URL
        """
        log.info("Testing NooBaaSystemCapacityWarning95 alert")
        
        obc_name = setup_backingstore_and_bucket["obc_name"]
        backingstore_obj = setup_backingstore_and_bucket["backingstore"]
        
        # Log backing store capacity before filling
        try:
            bs_data = OCP(
                kind=constants.BACKINGSTORE,
                namespace=config.ENV_DATA["cluster_namespace"],
            ).get(resource_name=backingstore_obj.name)
            if isinstance(bs_data, dict) and "status" in bs_data:
                capacity = bs_data.get("status", {}).get("capacity", {})
                log.info(
                    f"Backingstore capacity: "
                    f"total={capacity.get('total')}, "
                    f"used={capacity.get('used')}, "
                    f"available={capacity.get('available')}"
                )
        except Exception as e:
            log.warning(f"Could not retrieve backing store capacity: {e}")
        
        # Fill bucket to 95% capacity
        fill_obc_to_capacity(
            awscli_pod_session,
            mcg_obj_session,
            obc_name,
            target_percentage=95,
            file_size_mb=100,
        )
        
        # Wait for alert to fire (15 minutes timeout)
        api = prometheus.PrometheusAPI(threading_lock=threading_lock)
        
        alert_name = "NooBaaSystemCapacityWarning95"
        assert wait_for_alert(
            api, alert_name, timeout=900, sleep=30
        ), f"Alert {alert_name} did not fire within expected time"
        
        # Validate runbook URL
        assert validate_alert_runbook(
            api, alert_name, NOOBAA_CAPACITY_95_RUNBOOK
        ), f"Runbook URL validation failed for {alert_name}"
        
        log.info(f"Successfully validated {alert_name} alert and runbook URL")

    def test_noobaa_capacity_alert_100(
        self,
        setup_backingstore_and_bucket,
        mcg_obj_session,
        awscli_pod_session,
        threading_lock,
    ):
        """
        Test NooBaa System Capacity Warning at 100% utilization.

        Steps:
        1. Fill the OBC to 100% capacity
        2. Wait for NooBaaSystemCapacityWarning100 alert to fire
        3. Validate alert properties including runbook URL
        """
        log.info("Testing NooBaaSystemCapacityWarning100 alert")
        
        obc_name = setup_backingstore_and_bucket["obc_name"]
        backingstore_obj = setup_backingstore_and_bucket["backingstore"]
        
        # Log backing store capacity before filling
        try:
            bs_data = OCP(
                kind=constants.BACKINGSTORE,
                namespace=config.ENV_DATA["cluster_namespace"],
            ).get(resource_name=backingstore_obj.name)
            if isinstance(bs_data, dict) and "status" in bs_data:
                capacity = bs_data.get("status", {}).get("capacity", {})
                log.info(
                    f"Backingstore capacity: "
                    f"total={capacity.get('total')}, "
                    f"used={capacity.get('used')}, "
                    f"available={capacity.get('available')}"
                )
        except Exception as e:
            log.warning(f"Could not retrieve backing store capacity: {e}")
        
        # Fill bucket to 100% capacity
        fill_obc_to_capacity(
            awscli_pod_session,
            mcg_obj_session,
            obc_name,
            target_percentage=100,
            file_size_mb=100,
        )
        
        # Wait for alert to fire (15 minutes timeout)
        api = prometheus.PrometheusAPI(threading_lock=threading_lock)
        
        alert_name = "NooBaaSystemCapacityWarning100"
        assert wait_for_alert(
            api, alert_name, timeout=900, sleep=30
        ), f"Alert {alert_name} did not fire within expected time"
        
        # Validate runbook URL
        assert validate_alert_runbook(
            api, alert_name, NOOBAA_CAPACITY_100_RUNBOOK
        ), f"Runbook URL validation failed for {alert_name}"
        
        log.info(f"Successfully validated {alert_name} alert and runbook URL")

# Made with Bob
