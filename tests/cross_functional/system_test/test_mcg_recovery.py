import logging

import pytest
import time

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    system_test,
    ignore_leftovers,
    polarion_id,
    skipif_ocs_version,
    magenta_squad,
)
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.ocs import constants
from ocs_ci.ocs.bucket_utils import list_objects_from_bucket
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pvc import get_pvc_objs, get_pvc_size
from ocs_ci.utility.prometheus import PrometheusAPI, check_alert_list

log = logging.getLogger(__name__)


@magenta_squad
@system_test
@ignore_leftovers
@polarion_id("OCS-2716")
@skipif_ocs_version("<4.9")
class TestMCGRecovery(E2ETest):
    """
    Test MCG system recovery with dual NooBaa DB instances

    """

    @pytest.mark.parametrize(
        argnames=["bucket_amount", "object_amount"],
        argvalues=[pytest.param(2, 2)],
    )
    def test_mcg_db_backup_recovery(
        self,
        setup_mcg_bg_features,
        bucket_amount,
        object_amount,
        noobaa_db_backup_and_recovery_locally,
        validate_mcg_bg_features,
        md_blow_factory,
        bucket_factory_session,
        scale_noobaa_db_pod_pv_size,
        mcg_obj_session,
        awscli_pod_session,
        threading_lock,
    ):
        """
        Test MCG DB backup and recovery with noobaa db fill alerts.

        Steps:
        1. Run MCG background features and IOs
        2. Setup 2 instances of Noobaa DB:
           - Primary instance: 56GB PVC
           - Secondary instance: 50GB PVC
        3. Fill primary instance DB to ~80% of 56GB and verify 80% warning alert
        4. Perform noobaa DB backup
        5. Verify alerts for both instances:
           - Primary: 80% warning alert
           - Secondary: 90% critical alert
        6. Backup the DB
        7. Verify alert behavior post-backup
        """
        # Step 1: Setup MCG background features
        log.info("Setting up MCG background features")
        feature_setup_map = setup_mcg_bg_features(
            num_of_buckets=bucket_amount,
            object_amount=object_amount,
            is_disruptive=True,
            skip_any_features=["nsfs", "rgw kafka", "caching"],
        )

        # Initialize Prometheus API
        api = PrometheusAPI(threading_lock=threading_lock)

        # Step 2: Setup primary instance with 56GB PVC
        noobaa_db_pvc_obj = get_pvc_objs(pvc_names=[constants.NOOBAA_DB_PVC_NAME])[0]
        pvc_capacity = get_pvc_size(noobaa_db_pvc_obj)
        log.info(f"Current PVC capacity is {pvc_capacity} GB")

        increase_factor = (
            0.9 / 0.8
        )  # 80% of the increased PVC equals 90% of the original PVC
        primary_pvc_size = round(pvc_capacity * increase_factor)

        # Change db-noobaa-db-pg-0 PVC size to new PVC size and ensure PVC size is changed to new size
        scale_noobaa_db_pod_pv_size(pv_size=primary_pvc_size)
        log.info(f"new_pvc_size is {primary_pvc_size} ")

        # Verify PVC size is set correctly
        noobaa_db_pvc_obj = get_pvc_objs(pvc_names=[constants.NOOBAA_DB_PVC_NAME])[0]
        current_pvc_capacity = get_pvc_size(noobaa_db_pvc_obj)
        assert (
            current_pvc_capacity == primary_pvc_size
        ), f"Failed to set primary PVC size to {primary_pvc_size}GB, current size: {current_pvc_capacity}GB"
        log.info(f"Primary instance PVC size set to {primary_pvc_size}GB")

        # Step 3: Fill primary instance DB to ~80% of 56GB
        log.info("Filling primary instance DB to ~80% capacity")
        primary_bucket = bucket_factory_session(1)[0].name
        md_blow_factory.upload_obj_using_md_blow(primary_bucket, threshold_pct=79)

        # Count objects before backup
        obj_count_pre_backup = len(
            list_objects_from_bucket(
                pod_obj=awscli_pod_session,
                s3_obj=mcg_obj_session,
                target=primary_bucket,
                recursive=True,
            )
        )
        log.info(f"Object count before backup: {obj_count_pre_backup}")

        # Step 3: Verify 80% warning alert for primary instance
        log.info("Verifying 80% warning alert for primary instance")
        noobaa_capacity_alert = "NooBaaDatabaseReachingCapacity"
        primary_pod = "noobaa-db-pg-cluster-1"

        alert_list = api.wait_for_alert(name=noobaa_capacity_alert, state="firing")
        description = f"The NooBaa database on pod {primary_pod} is using 80% of its PVC requested size."
        message = (
            f"The NooBaa database on pod {primary_pod} is using 80% of its PVC capacity. "
            f"Plan to increase the PVC size soon to prevent service impact."
        )

        check_alert_list(
            label=noobaa_capacity_alert,
            msg=message,
            description=description,
            states=["firing"],
            severity="warning",
            alerts=alert_list,
        )
        log.info("Primary instance 80% warning alert verified successfully")

        # Step 4: Perform NooBaa DB backup
        log.info("Performing NooBaa DB backup")
        noobaa_db_backup_and_recovery_locally()

        # Verify object count after backup
        obj_count_post_backup = len(
            list_objects_from_bucket(
                pod_obj=awscli_pod_session,
                s3_obj=mcg_obj_session,
                target=primary_bucket,
                recursive=True,
            )
        )
        assert (
            obj_count_pre_backup == obj_count_post_backup
        ), f"Object count mismatch: before={obj_count_pre_backup}, after={obj_count_post_backup}"
        log.info(
            f"Backup completed successfully. Object count: {obj_count_post_backup}"
        )

        # Step 5: Verify 90% critical alert for secondary instance
        log.info("Verifying 90% critical alert for secondary instance")
        noobaa_capacity_full_alert = "NooBaaDatabaseStorageFull"
        secondary_pod = "noobaa-db-pg-cluster-2"

        alert_list = api.wait_for_alert(name=noobaa_capacity_full_alert, state="firing")
        description = f"The NooBaa database on pod {secondary_pod} is using 90% of its PVC requested size."
        message = (
            f"The NooBaa database on pod {secondary_pod} is using 90% of its PVC capacity. "
            f"Expand the PVC size now to avoid imminent service disruption."
        )

        check_alert_list(
            label=noobaa_capacity_full_alert,
            msg=message,
            description=description,
            states=["firing"],
            severity="critical",
            alerts=alert_list,
        )
        log.info("Secondary instance 90% critical alert verified successfully")

        # Step 6: Backup the DB again
        log.info("Performing second DB backup")
        noobaa_db_backup_and_recovery_locally()

        # Step 7: Verify alert behavior post-backup
        log.info("Verifying alert behavior after backup")

        # Verify primary instance 80% warning alert persists
        log.info("Verifying primary instance 80% warning alert still firing")
        alert_list = api.wait_for_alert(name=noobaa_capacity_alert, state="firing")
        primary_alerts = [
            alert
            for alert in alert_list
            if primary_pod in alert.get("labels", {}).get("pod", "")
        ]
        assert (
            primary_alerts
        ), "Primary instance 80% warning alert should still be firing"
        log.info("Primary instance 80% warning alert persists as expected")

        # Verify secondary instance 90% critical alert disappears
        log.info("Verifying secondary instance 90% critical alert has cleared")
        time.sleep(60)  # Wait for alert state to update
        alerts_response = api.get(
            "alerts",
            payload={
                "silenced": False,
                "inhibited": False,
            },
        )
        alert_list = alerts_response.json().get("data", {}).get("alerts", [])
        secondary_critical_alerts = [
            alert
            for alert in alert_list
            if alert.get("labels", {}).get("alertname") == noobaa_capacity_full_alert
            and secondary_pod in alert.get("labels", {}).get("pod", "")
            and alert.get("state") == "firing"
        ]
        assert (
            not secondary_critical_alerts
        ), "Secondary instance 90% critical alert should have cleared"
        log.info("Secondary instance 90% critical alert cleared as expected")

        # Verify secondary instance 80% warning alert appears
        log.info("Verifying secondary instance 80% warning alert is now firing")
        alert_list = api.wait_for_alert(name=noobaa_capacity_alert, state="firing")
        secondary_warning_alerts = [
            alert
            for alert in alert_list
            if secondary_pod in alert.get("labels", {}).get("pod", "")
        ]
        assert (
            secondary_warning_alerts
        ), "Secondary instance 80% warning alert should be firing"
        log.info("Secondary instance 80% warning alert is firing as expected")

        # Verify default backingstore is in ready state
        log.info("Verifying default backingstore status")
        default_bs = OCP(
            kind=constants.BACKINGSTORE, namespace=config.ENV_DATA["cluster_namespace"]
        ).get(resource_name=constants.DEFAULT_NOOBAA_BACKINGSTORE)
        assert (
            default_bs["status"]["phase"] == constants.STATUS_READY
        ), "Default backingstore is not in ready state"
        log.info("Default backingstore is in ready state")

        # Validation of MCG background features
        log.info("Performing final validation of MCG background features")
        time.sleep(60)  # Wait for complete stabilization

        validate_mcg_bg_features(
            feature_setup_map,
            run_in_bg=False,
            skip_any_features=["nsfs", "rgw kafka", "caching"],
            object_amount=object_amount,
        )
        log.info("MCG background feature validation completed successfully")
        log.info("Test completed: MCG DB backup and recovery with dual instance setup")
