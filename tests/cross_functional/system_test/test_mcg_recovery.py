import logging
import math

import pytest
import time

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    system,
    ignore_leftovers,
    polarion_id,
    skipif_ocs_version,
    magenta_squad,
)
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.ocs import constants
from ocs_ci.ocs.bucket_utils import list_objects_from_bucket, rm_object_recursive
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pvc import get_pvc_objs, get_pvc_size
from ocs_ci.utility.prometheus import PrometheusAPI, check_alert_list
from ocs_ci.utility.utils import (
    TimeoutSampler,
    get_primary_nb_db_pod,
    get_secondary_nb_db_pod,
)

logger = logging.getLogger(__name__)

DB_CAPACITY_WARNING_THRESHOLD = 80
DB_CAPACITY_CRITICAL_THRESHOLD = 90
DB_FILL_WARNING_THRESHOLD = 85
NOOBAA_PODS_RUNNING_TIMEOUT_AFTER_DB_FILL = 1800
ALERT_FIRING_TIMEOUT = 900
ALERT_SAMPLING_INTERVAL = 15
DB_FILL_CLEANUP_TIMEOUT = 3600
WARNING_ALERT_MSG = (
    "The NooBaa database on pod {pod} is consuming 80% of its PVC capacity. "
    "Plan to increase the PVC size soon to prevent service impact."
)
WARNING_ALERT_DESCRIPTION = (
    "The NooBaa database on pod {pod} has reached 80% of its PVC capacity."
)
CRITICAL_ALERT_MSG = (
    "The NooBaa database on pod {pod} has exceeded 90% of its PVC capacity. "
    "Expand the PVC size now to avoid imminent service disruption."
)
CRITICAL_ALERT_DESCRIPTION = (
    "The NooBaa database on pod {pod} has exceeded 90% of its PVC capacity. "
    "Immediate action is required"
)


def verify_db_capacity_alerts(
    api, expanded_pod_name, default_pod_name, phase, timeout=ALERT_FIRING_TIMEOUT
):
    """
    Verify the capacity alerts of both NooBaa DB instances.

    Args:
        api (PrometheusAPI): Prometheus API instance
        expanded_pod_name (str): NooBaa DB pod backed by the expanded PVC
        default_pod_name (str): NooBaa DB pod backed by the default sized PVC
        phase (str): Short description of the test phase, used in log messages
        timeout (int): Time in seconds to wait for each alert to fire

    Raises:
        TimeoutExpiredError: If an alert does not fire for its pod in time

    """
    logger.info(
        f"NooBaa DB CNPG roles {phase}: primary={get_primary_nb_db_pod().name}, "
        f"secondary={get_secondary_nb_db_pod().name}"
    )

    expected_alerts = [
        (
            expanded_pod_name,
            constants.ALERT_NOOBAA_DATABASE_REACHING_CAPACITY,
            DB_CAPACITY_WARNING_THRESHOLD,
            "warning",
            WARNING_ALERT_MSG,
            WARNING_ALERT_DESCRIPTION,
        ),
        (
            default_pod_name,
            constants.ALERT_NOOBAA_DATABASE_STORAGE_FULL,
            DB_CAPACITY_CRITICAL_THRESHOLD,
            "critical",
            CRITICAL_ALERT_MSG,
            CRITICAL_ALERT_DESCRIPTION,
        ),
    ]

    for (
        pod_name,
        alert_name,
        threshold_pct,
        severity,
        alert_msg,
        alert_description,
    ) in expected_alerts:
        logger.info(f"Waiting for the {alert_name} alert to fire for pod {pod_name}")
        for response in TimeoutSampler(
            timeout,
            ALERT_SAMPLING_INTERVAL,
            api.get,
            "alerts",
            payload={"silenced": False, "inhibited": False},
        ):
            pod_alerts = [
                alert
                for alert in response.json().get("data", {}).get("alerts", [])
                if alert.get("labels", {}).get("alertname") == alert_name
                and alert.get("labels", {}).get("pod") == pod_name
                and alert.get("state") == "firing"
            ]
            if pod_alerts:
                break

        check_alert_list(
            label=alert_name,
            msg=alert_msg.format(pod=pod_name),
            description=alert_description.format(pod=pod_name),
            states=["firing"],
            severity=severity,
            alerts=pod_alerts,
        )
        logger.info(
            f"{alert_name} ({severity}) alert verified for pod {pod_name} "
            f"at {threshold_pct}%"
        )

    logger.info(f"Both NooBaa DB capacity alerts verified {phase}")


@magenta_squad
@system
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
        request,
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
        2. Expand the PVC of the CNPG primary DB instance by a factor of
           90/80, so that the same amount of data is 80% of the expanded PVC
           and at least 90% of the replica PVC, which keeps its default size
        3. Fill the primary DB instance to 80% of its PVC using md_blow
        4. Verify the capacity alerts before backup and recovery:
           - Expanded PVC pod: NooBaaDatabaseReachingCapacity (80%, warning)
           - Default PVC pod: NooBaaDatabaseStorageFull (90%, critical)
        5. Back up and recover the DB from that backup
        6. Verify the object count is preserved after the recovery
        7. Verify the same two capacity alerts still fire after the recovery
        8. Verify the default backingstore is Ready and validate the MCG
           background features
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

        logger.test_step("Expand the PVC of the primary NooBaa DB instance")
        expanded_pvc_name = get_primary_nb_db_pod().name
        default_pvc_name = get_secondary_nb_db_pod().name
        logger.info(
            f"Primary NooBaa DB instance is {expanded_pvc_name}, "
            f"secondary instance is {default_pvc_name}"
        )

        default_pvc_capacity = get_pvc_size(
            get_pvc_objs(pvc_names=[default_pvc_name])[0]
        )
        logger.info(f"Current NooBaa DB PVC capacity is {default_pvc_capacity}GB")

        # Round up, so that 80% of the expanded PVC is guaranteed to be at least
        # 90% of the PVC that keeps its default size.
        expansion_factor = (
            DB_CAPACITY_CRITICAL_THRESHOLD / DB_CAPACITY_WARNING_THRESHOLD
        )
        expanded_pvc_size = math.ceil(default_pvc_capacity * expansion_factor)
        logger.info(f"Expanding {expanded_pvc_name} to {expanded_pvc_size}GB")

        scale_noobaa_db_pod_pv_size(
            pv_size=expanded_pvc_size,
            pvc_names=[expanded_pvc_name],
        )

        current_pvc_capacity = get_pvc_size(
            get_pvc_objs(pvc_names=[expanded_pvc_name])[0]
        )
        assert current_pvc_capacity == expanded_pvc_size, (
            f"Failed to expand {expanded_pvc_name} to {expanded_pvc_size}GB, "
            f"current size: {current_pvc_capacity}GB"
        )
        logger.info(f"{expanded_pvc_name} PVC size is now {expanded_pvc_size}GB")

        current_default_capacity = get_pvc_size(
            get_pvc_objs(pvc_names=[default_pvc_name])[0]
        )
        assert current_default_capacity == default_pvc_capacity, (
            f"{default_pvc_name} size changed unexpectedly: expected "
            f"{default_pvc_capacity}GB, found {current_default_capacity}GB"
        )
        logger.info(f"{default_pvc_name} PVC size remains {default_pvc_capacity}GB")

        logger.test_step(
            f"Fill the primary NooBaa DB to ~{DB_FILL_WARNING_THRESHOLD}% "
            f"capacity using md_blow"
        )
        blow_io = md_blow_factory(db_pod_name=expanded_pvc_name)
        fill_bucket = bucket_factory_session(1)[0].name

        def cleanup_db_fill():
            """
            Delete the objects written by md_blow, so that the NooBaa DB usage
            drops back and later tests do not start with a nearly full DB.
            """
            logger.info(f"Removing the md_blow objects from bucket {fill_bucket}")
            try:
                rm_object_recursive(
                    podobj=awscli_pod_session,
                    target=fill_bucket,
                    mcg_obj=mcg_obj_session,
                    timeout=DB_FILL_CLEANUP_TIMEOUT,
                )
                logger.info("md_blow objects removed from the NooBaa DB")
            except Exception as exc:
                logger.warning(f"Failed to clean up the NooBaa DB fill objects: {exc}")

        request.addfinalizer(cleanup_db_fill)

        blow_io.upload_obj_using_md_blow(
            fill_bucket, threshold_pct=DB_FILL_WARNING_THRESHOLD
        )

        obj_count_pre_recovery = len(
            list_objects_from_bucket(
                pod_obj=awscli_pod_session,
                s3_obj=mcg_obj_session,
                target=fill_bucket,
                recursive=True,
            )
        )
        logger.info(
            f"Object count before backup and recovery: {obj_count_pre_recovery}"
        )

        logger.test_step(
            "Verify both NooBaa DB capacity alerts before backup and recovery"
        )
        verify_db_capacity_alerts(
            api=api,
            expanded_pod_name=expanded_pvc_name,
            default_pod_name=default_pvc_name,
            phase="before backup and recovery",
        )

        logger.test_step("Perform NooBaa DB backup and recovery")
        noobaa_db_backup_and_recovery_locally(
            noobaa_pods_running_timeout=NOOBAA_PODS_RUNNING_TIMEOUT_AFTER_DB_FILL
        )

        logger.test_step("Verify the object count is preserved after the recovery")
        obj_count_post_recovery = len(
            list_objects_from_bucket(
                pod_obj=awscli_pod_session,
                s3_obj=mcg_obj_session,
                target=fill_bucket,
                recursive=True,
            )
        )
        assert obj_count_pre_recovery == obj_count_post_recovery, (
            f"Object count mismatch: before={obj_count_pre_recovery}, "
            f"after={obj_count_post_recovery}"
        )
        logger.info(
            f"Recovery completed successfully. Object count: {obj_count_post_recovery}"
        )

        logger.test_step(
            "Verify both NooBaa DB capacity alerts after backup and recovery"
        )
        verify_db_capacity_alerts(
            api=api,
            expanded_pod_name=expanded_pvc_name,
            default_pod_name=default_pvc_name,
            phase="after backup and recovery",
        )

        logger.test_step("Verify the default backingstore is in Ready state")
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
