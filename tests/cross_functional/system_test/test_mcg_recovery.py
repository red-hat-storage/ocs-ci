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
from ocs_ci.ocs.cluster import (
    CephCluster,
    get_osd_utilization,
    get_percent_used_capacity,
)
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import (
    get_noobaa_core_pod,
    get_pod_obj,
    wait_for_noobaa_pods_running,
)
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
DB_FILL_TARGET_PCT = 84
EXPANSION_FACTOR = 1.08
MAX_FILL_BATCHES = 500
MAX_DB_DISK_PCT = 98
MAX_OSD_USED_PCT = 80
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


def db_instance_pods():
    return (get_primary_nb_db_pod(), get_secondary_nb_db_pod())


def get_db_usage(db_pod_name):
    """
    Return the value the NooBaa DB capacity alert rules evaluate.

    Args:
        db_pod_name (str): NooBaa DB instance to measure. The CNPG PVC name is
            the instance pod name

    Returns:
        tuple: (used bytes, PVC requested bytes, usage percentage)

    """
    db_pod = get_pod_obj(db_pod_name, namespace=config.ENV_DATA["cluster_namespace"])
    total_bytes = get_pvc_size(get_pvc_objs(pvc_names=[db_pod_name])[0]) * constants.GB
    usage = db_pod.exec_cmd_on_pod(
        "psql -U postgres -tAc \"select pg_database_size('nbcore') + "
        "least(coalesce((select sum(size) from pg_ls_waldir()), 0)::bigint, "
        "pg_size_bytes(current_setting('wal_keep_size')))\"",
        container_name="postgres",
        shell=True,
    )
    try:
        used_bytes = int(str(usage).strip())
    except ValueError as exc:
        raise UnexpectedBehaviour(
            "Unexpected psql output while reading the NooBaa DB size from "
            f"{db_pod_name}: {usage!r}"
        ) from exc
    usage_pct = (used_bytes * 100) // total_bytes if total_bytes else 0
    return used_bytes, total_bytes, usage_pct


def most_full_osd():
    """
    Return the fullest OSD in the cluster.

    Returns:
        tuple: (osd name, used percentage)

    """
    utilization = get_osd_utilization()
    if not utilization:
        return "", 0
    name = max(utilization, key=utilization.get)
    return name, utilization[name]


def most_full_db_filesystem():
    """
    Return the fullest of the two NooBaa DB instance filesystems.

    Returns:
        tuple: (pod name, used percentage)

    """
    fullest = ("", 0)
    for pod_obj in db_instance_pods():
        output = pod_obj.exec_cmd_on_pod(
            "df -B1 | grep postgresql | awk '{print $3,$2}'",
            container_name="postgres",
            shell=True,
        )
        try:
            used, total = (int(field) for field in str(output).split())
        except ValueError as exc:
            raise UnexpectedBehaviour(
                "Unexpected 'df' output while checking free space on "
                f"{pod_obj.name}: {output!r}"
            ) from exc
        used_pct = used * 100 // total if total else 0
        if used_pct > fullest[1]:
            fullest = (pod_obj.name, used_pct)
    return fullest


def check_ceph_headroom(bytes_needed):
    """
    Fail before the fill if Ceph cannot hold the data it would write.

    Args:
        bytes_needed (int): Bytes the fill still has to write on one instance

    Raises:
        UnexpectedBehaviour: If the capacity left under the cap is short

    """
    instances = len(db_instance_pods())
    required_gb = bytes_needed * instances / constants.GB
    budget_gb = (
        CephCluster().get_ceph_capacity()
        * (MAX_OSD_USED_PCT - get_percent_used_capacity())
        / 100
    )
    logger.info(
        f"The fill needs {required_gb:.1f}GB of usable Ceph capacity across "
        f"{instances} DB instances, {budget_gb:.1f}GB is left under the "
        f"{MAX_OSD_USED_PCT}% cap"
    )
    if required_gb > budget_gb:
        raise UnexpectedBehaviour(
            f"Not enough Ceph capacity for the NooBaa DB fill: it needs "
            f"{required_gb:.1f}GB usable across {instances} DB instances and "
            f"only {budget_gb:.1f}GB is left under the {MAX_OSD_USED_PCT}% OSD "
            "cap. Run this test on a cluster with more storage"
        )


def fill_noobaa_db(blow_io, bucket_name, db_pod_name, threshold_pct):
    """
    Fill a NooBaa DB instance with md_blow up to threshold_pct.

    Args:
        blow_io (MdBlow): Object from the md_blow_factory fixture
        bucket_name (str): Bucket the objects are uploaded to
        db_pod_name (str): NooBaa DB instance the fill is measured against
        threshold_pct (int): Usage percentage to fill up to

    Raises:
        UnexpectedBehaviour: If the fill stalls, exceeds MAX_FILL_BATCHES, runs
            the cluster out of Ceph capacity, or cannot reach the threshold

    """
    used_bytes, total_bytes, current_pct = get_db_usage(db_pod_name)
    logger.info(
        f"md_blow fill of {db_pod_name} starting at {current_pct}% "
        f"({used_bytes}/{total_bytes} bytes), target {threshold_pct}%"
    )
    check_ceph_headroom((threshold_pct - current_pct) * total_bytes // 100)

    stall_batches = 0
    batch_num = 0
    fullest_pod, disk_pct = "", 0
    while current_pct < threshold_pct:
        batch_num += 1
        if batch_num > MAX_FILL_BATCHES:
            raise UnexpectedBehaviour(
                f"md_blow did not reach {threshold_pct}% after "
                f"{MAX_FILL_BATCHES} batches, stopped at {current_pct}%"
            )
        logger.info(f"Running md_blow batch {batch_num}")
        blow_io.noobaa_core_pod = get_noobaa_core_pod()
        blow_io.upload_obj_using_md_blow(bucket_name)

        new_used_bytes, total_bytes, current_pct = get_db_usage(db_pod_name)
        logger.info(
            f"DB usage after batch {batch_num}: {current_pct}% "
            f"({new_used_bytes}/{total_bytes} bytes)"
        )
        if new_used_bytes == used_bytes:
            stall_batches += 1
            if stall_batches >= 5:
                try:
                    restarts = blow_io.noobaa_core_pod.restart_count
                except Exception:
                    restarts = "unknown"
                raise UnexpectedBehaviour(
                    f"md_blow stalled: DB used bytes unchanged for "
                    f"{stall_batches} consecutive batches at {current_pct}% "
                    f"(target {threshold_pct}%). noobaa-core pod "
                    f"{blow_io.noobaa_core_pod.name} restart count is "
                    f"{restarts}, check its log for md_blow RPC errors"
                )
            logger.warning(
                f"Batch {batch_num} wrote nothing, waiting for the NooBaa pods "
                "to be Running before the next batch"
            )
            wait_for_noobaa_pods_running(timeout=600)
        else:
            stall_batches = 0
        used_bytes = new_used_bytes

        osd_name, osd_pct = most_full_osd()
        if osd_pct >= MAX_OSD_USED_PCT:
            raise UnexpectedBehaviour(
                f"Stopped the NooBaa DB fill at {current_pct}% of the "
                f"{threshold_pct}% target: OSD {osd_name} is {osd_pct:.1f}% "
                f"used, at the {MAX_OSD_USED_PCT}% cap. Filling further would "
                "make Ceph block IO on every pool"
            )

        fullest_pod, disk_pct = most_full_db_filesystem()
        if disk_pct >= MAX_DB_DISK_PCT:
            logger.warning(
                f"Stopping the fill, the {fullest_pod} filesystem is "
                f"{disk_pct}% used, at the {MAX_DB_DISK_PCT}% cap"
            )
            break

    if current_pct < threshold_pct:
        raise UnexpectedBehaviour(
            f"md_blow reached {current_pct}% of the {threshold_pct}% target "
            f"before the {fullest_pod} filesystem hit {disk_pct}%. The DB "
            "instances cannot hold enough data to raise the capacity alerts "
            "at their current PVC sizes"
        )

    logger.info(
        f"md_blow fill completed at {current_pct}% "
        f"({used_bytes}/{total_bytes} bytes), target was {threshold_pct}%"
    )


def verify_db_capacity_alerts(
    api, expanded_pod_name, default_pod_name, phase, timeout=900
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
            15,
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
    def test_mcg_recovery_with_noobaa_db_fill_alerts(
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
        2. Expand the PVC of the CNPG primary DB instance, so that one fill
           reads as between 80% and 90% of the expanded PVC and as over 90%
           of the replica PVC, which keeps its default size
        3. Fill the primary DB instance to DB_FILL_TARGET_PCT of its PVC using
           md_blow, measured the way the alert rules measure it
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

        expanded_pvc_size = math.ceil(default_pvc_capacity * EXPANSION_FACTOR)
        fill_pct_of_default = (
            DB_FILL_TARGET_PCT * expanded_pvc_size / default_pvc_capacity
        )
        assert fill_pct_of_default > DB_CAPACITY_CRITICAL_THRESHOLD, (
            f"Filling {expanded_pvc_size}GB to {DB_FILL_TARGET_PCT}% is only "
            f"{fill_pct_of_default:.1f}% of the {default_pvc_capacity}GB default "
            f"PVC, which cannot raise the {DB_CAPACITY_CRITICAL_THRESHOLD}% alert"
        )
        logger.info(
            f"Expanding {expanded_pvc_name} to {expanded_pvc_size}GB, where a "
            f"{DB_FILL_TARGET_PCT}% fill is {fill_pct_of_default:.1f}% of "
            f"{default_pvc_name}"
        )

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
            f"Fill the primary NooBaa DB to {DB_FILL_TARGET_PCT}% of its "
            f"expanded PVC using md_blow"
        )
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
                    timeout=3600,
                )
                logger.info("md_blow objects removed from the NooBaa DB")
            except Exception as exc:
                logger.warning(f"Failed to clean up the NooBaa DB fill objects: {exc}")

        request.addfinalizer(cleanup_db_fill)

        fill_noobaa_db(
            blow_io=md_blow_factory,
            bucket_name=fill_bucket,
            db_pod_name=expanded_pvc_name,
            threshold_pct=DB_FILL_TARGET_PCT,
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
        noobaa_db_backup_and_recovery_locally(noobaa_pods_running_timeout=1800)

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
