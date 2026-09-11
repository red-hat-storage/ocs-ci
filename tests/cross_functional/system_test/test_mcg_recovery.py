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
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)

DB_CAPACITY_WARNING_THRESHOLD = 80
DB_CAPACITY_CRITICAL_THRESHOLD = 90
DB_FILL_WARNING_THRESHOLD = 80
PRIMARY_DB_POD = constants.NOOBAA_DB_PVC_NAME
SECONDARY_DB_POD = constants.NOOBAA_DB_SECONDARY_PVC_NAME
NOOBAA_PODS_RUNNING_TIMEOUT_AFTER_DB_FILL = 1800


def verify_noobaa_db_capacity_alert(
    api,
    pod_name,
    alert_name,
    threshold_pct,
    severity,
    impact_message,
    state="firing",
):
    """
    Verify a NooBaa DB capacity alert for a specific DB pod instance.

    Args:
        api (PrometheusAPI): Prometheus API instance
        pod_name (str): NooBaa DB pod name
        alert_name (str): Alert name to verify
        threshold_pct (int): Expected PVC usage threshold in the alert text
        severity (str): Expected alert severity
        impact_message (str): Expected trailing impact message in the alert body
        state (str): Expected alert state
    """
    alert_list = api.wait_for_alert(name=alert_name, state=state)
    description = (
        f"The NooBaa database on pod {pod_name} is using {threshold_pct}% "
        f"of its PVC requested size."
    )
    message = (
        f"The NooBaa database on pod {pod_name} is using {threshold_pct}% "
        f"of its PVC capacity. {impact_message}"
    )
    check_alert_list(
        label=alert_name,
        msg=message,
        description=description,
        states=[state],
        severity=severity,
        alerts=alert_list,
    )


def get_firing_alerts_for_pod(api, alert_name, pod_name):
    """
    Return firing alerts for a specific NooBaa DB pod.

    Args:
        api (PrometheusAPI): Prometheus API instance
        alert_name (str): Alert name to filter on
        pod_name (str): NooBaa DB pod name

    Returns:
        list: Matching firing alerts
    """
    alerts_response = api.get(
        "alerts",
        payload={
            "silenced": False,
            "inhibited": False,
        },
    )
    alert_list = alerts_response.json().get("data", {}).get("alerts", [])
    return [
        alert
        for alert in alert_list
        if alert.get("labels", {}).get("alertname") == alert_name
        and pod_name in alert.get("labels", {}).get("pod", "")
        and alert.get("state") == "firing"
    ]


@magenta_squad
@system_test  # IgnoreDeprecation
@ignore_leftovers
@polarion_id("OCS-2716")
@skipif_ocs_version("<4.19")
class TestMCGRecovery(E2ETest):
    """
    Test MCG system recovery with dual NooBaa DB instances and db fill alert

    """

    @pytest.mark.parametrize(
        argnames=["bucket_amount", "object_amount"],
        argvalues=[pytest.param(5, 5)],
    )
    def test_mcg_recovery_with_dual_noobaa_db_fill_alerts(
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
        logger.test_step(
            f"Setup MCG background features with {bucket_amount} buckets "
            f"and {object_amount} objects per bucket"
        )
        feature_setup_map = setup_mcg_bg_features(
            num_of_buckets=bucket_amount,
            object_amount=object_amount,
            is_disruptive=True,
            skip_any_features=["nsfs", "rgw kafka", "caching"],
        )
        logger.info("MCG background features configured successfully")

        api = PrometheusAPI(threading_lock=threading_lock)

        logger.test_step("Scale primary NooBaa DB PVC for dual-instance alert scenario")
        noobaa_db_pvc_obj = get_pvc_objs(pvc_names=[constants.NOOBAA_DB_PVC_NAME])[0]
        pvc_capacity = get_pvc_size(noobaa_db_pvc_obj)
        logger.info(f"Current primary PVC capacity is {pvc_capacity} GB")

        increase_factor = (
            0.9 / 0.8
        )  # 80% of the increased PVC equals 90% of the original PVC
        primary_pvc_size = round(pvc_capacity * increase_factor)

        scale_noobaa_db_pod_pv_size(
            pv_size=primary_pvc_size,
            pvc_names=[constants.NOOBAA_DB_PVC_NAME],
        )
        logger.info(f"Primary PVC target size is {primary_pvc_size}GB")

        noobaa_db_pvc_obj = get_pvc_objs(pvc_names=[constants.NOOBAA_DB_PVC_NAME])[0]
        current_pvc_capacity = get_pvc_size(noobaa_db_pvc_obj)
        assert (
            current_pvc_capacity == primary_pvc_size
        ), f"Failed to set primary PVC size to {primary_pvc_size}GB, current size: {current_pvc_capacity}GB"
        logger.info(f"Primary instance PVC size set to {primary_pvc_size}GB")

        secondary_pvc_obj = get_pvc_objs(
            pvc_names=[constants.NOOBAA_DB_SECONDARY_PVC_NAME]
        )[0]
        secondary_pvc_capacity = get_pvc_size(secondary_pvc_obj)
        assert secondary_pvc_capacity == pvc_capacity, (
            f"Secondary PVC size changed unexpectedly: expected {pvc_capacity}GB, "
            f"found {secondary_pvc_capacity}GB"
        )
        logger.info(f"Secondary instance PVC size remains {secondary_pvc_capacity}GB")

        logger.test_step("Fill primary NooBaa DB to ~80% capacity using md_blow")
        primary_bucket = bucket_factory_session(1)[0].name
        md_blow_factory.upload_obj_using_md_blow(
            primary_bucket, threshold_pct=DB_FILL_WARNING_THRESHOLD
        )

        obj_count_pre_backup = len(
            list_objects_from_bucket(
                pod_obj=awscli_pod_session,
                s3_obj=mcg_obj_session,
                target=primary_bucket,
                recursive=True,
            )
        )
        logger.info(f"Object count before backup: {obj_count_pre_backup}")

        logger.test_step("Verify 80% warning alert for primary NooBaa DB instance")
        verify_noobaa_db_capacity_alert(
            api=api,
            pod_name=PRIMARY_DB_POD,
            alert_name=constants.ALERT_NOOBAA_DATABASE_REACHING_CAPACITY,
            threshold_pct=DB_CAPACITY_WARNING_THRESHOLD,
            severity="warning",
            impact_message=(
                "Plan to increase the PVC size soon to prevent service impact."
            ),
        )
        logger.info("Primary instance 80% warning alert verified successfully")

        logger.test_step("Perform NooBaa DB backup and recovery")
        noobaa_db_backup_and_recovery_locally(
            noobaa_pods_running_timeout=NOOBAA_PODS_RUNNING_TIMEOUT_AFTER_DB_FILL
        )

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
        logger.info(
            f"Backup completed successfully. Object count: {obj_count_post_backup}"
        )

        logger.test_step("Verify 90% critical alert for secondary NooBaa DB instance")
        verify_noobaa_db_capacity_alert(
            api=api,
            pod_name=SECONDARY_DB_POD,
            alert_name=constants.ALERT_NOOBAA_DATABASE_STORAGE_FULL,
            threshold_pct=DB_CAPACITY_CRITICAL_THRESHOLD,
            severity="critical",
            impact_message=(
                "Expand the PVC size now to avoid imminent service disruption."
            ),
        )
        logger.info("Secondary instance 90% critical alert verified successfully")

        logger.test_step("Perform second NooBaa DB backup and recovery")
        noobaa_db_backup_and_recovery_locally(
            noobaa_pods_running_timeout=NOOBAA_PODS_RUNNING_TIMEOUT_AFTER_DB_FILL
        )

        logger.test_step("Verify alert behavior after backup")
        alert_list = api.wait_for_alert(
            name=constants.ALERT_NOOBAA_DATABASE_REACHING_CAPACITY, state="firing"
        )
        primary_alerts = [
            alert
            for alert in alert_list
            if PRIMARY_DB_POD in alert.get("labels", {}).get("pod", "")
        ]
        assert (
            primary_alerts
        ), "Primary instance 80% warning alert should still be firing"
        logger.info("Primary instance 80% warning alert persists as expected")

        logger.info("Waiting for secondary critical alert to clear")
        sample = TimeoutSampler(
            timeout=300,
            sleep=15,
            func=lambda: not get_firing_alerts_for_pod(
                api,
                constants.ALERT_NOOBAA_DATABASE_STORAGE_FULL,
                SECONDARY_DB_POD,
            ),
        )
        assert sample.wait_for_func_status(
            result=True
        ), "Secondary instance 90% critical alert should have cleared"
        logger.info("Secondary instance 90% critical alert cleared as expected")

        alert_list = api.wait_for_alert(
            name=constants.ALERT_NOOBAA_DATABASE_REACHING_CAPACITY, state="firing"
        )
        secondary_warning_alerts = [
            alert
            for alert in alert_list
            if SECONDARY_DB_POD in alert.get("labels", {}).get("pod", "")
        ]
        assert (
            secondary_warning_alerts
        ), "Secondary instance 80% warning alert should be firing"
        logger.info("Secondary instance 80% warning alert is firing as expected")

        logger.info("Verifying default backingstore status")
        default_bs = OCP(
            kind=constants.BACKINGSTORE, namespace=config.ENV_DATA["cluster_namespace"]
        ).get(resource_name=constants.DEFAULT_NOOBAA_BACKINGSTORE)
        assert (
            default_bs["status"]["phase"] == constants.STATUS_READY
        ), "Default backingstore is not in ready state"
        logger.info("Default backingstore is in ready state")

        logger.test_step("Validate MCG background features after recovery")
        time.sleep(60)

        validate_mcg_bg_features(
            feature_setup_map,
            run_in_bg=False,
            skip_any_features=["nsfs", "rgw kafka", "caching"],
            object_amount=object_amount,
        )
        logger.info("MCG background feature validation completed successfully")
