import json
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
from ocs_ci.ocs.exceptions import TimeoutExpiredError, UnexpectedBehaviour
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.md_blow import (
    checkpoint_db_instances,
    db_filesystem_bytes,
    db_filesystem_usage,
    db_instance_pods,
)
from ocs_ci.ocs.resources.pod import get_ceph_tools_pod, get_pod_obj
from ocs_ci.ocs.resources.pvc import get_pvc_objs, get_pvc_size
from ocs_ci.utility.prometheus import PrometheusAPI, check_alert_list
from ocs_ci.utility.utils import (
    TimeoutSampler,
    exec_nb_db_query,
    get_primary_nb_db_pod,
    get_secondary_nb_db_pod,
)

logger = logging.getLogger(__name__)

DB_CAPACITY_WARNING_THRESHOLD = 80
DB_CAPACITY_CRITICAL_THRESHOLD = 90
EXPANSION_FACTOR = DB_CAPACITY_CRITICAL_THRESHOLD / DB_CAPACITY_WARNING_THRESHOLD
DB_FILL_TARGET_PCT = 85
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
        'coalesce((select sum(size) from pg_ls_waldir()), 0)::bigint"',
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


def db_image_allocated_bytes(pvc_name):
    """
    Return what the Ceph pool has already allocated for a DB PVC.

    Args:
        pvc_name (str): NooBaa DB PVC, which is also the instance pod name

    Returns:
        int: Allocated bytes, or 0 when the image could not be read

    """
    try:
        pv_name = get_pvc_objs(pvc_names=[pvc_name])[0].get()["spec"]["volumeName"]
        attributes = OCP(kind=constants.PV).get(resource_name=pv_name)["spec"]["csi"][
            "volumeAttributes"
        ]
        output = get_ceph_tools_pod().exec_cmd_on_pod(
            f"rbd du -p {attributes['pool']} {attributes['imageName']} --format json",
            out_yaml_format=False,
        )
        return int(json.loads(str(output))["images"][0]["used_size"])
    except Exception as exc:
        logger.warning(f"Could not read the RBD allocation of {pvc_name}: {exc}")
        return 0


def check_ceph_headroom(target_bytes):
    """
    Fail before the fill if Ceph cannot hold the data it would write.

    Args:
        target_bytes (int): Bytes each instance filesystem holds at the end
            of the fill

    Raises:
        UnexpectedBehaviour: If the capacity left under the cap is short

    """
    required_gb = 0
    for pod_obj in db_instance_pods():
        allocated = db_image_allocated_bytes(pod_obj.name)
        new_bytes = max(0, target_bytes - allocated)
        logger.info(
            f"{pod_obj.name} holds {allocated / constants.GB:.1f}GB of Ceph "
            f"already and the fill takes it to {target_bytes / constants.GB:.1f}GB, "
            f"so it needs {new_bytes / constants.GB:.1f}GB more"
        )
        required_gb += new_bytes / constants.GB
    budget_gb = (
        CephCluster().get_ceph_capacity()
        * (MAX_OSD_USED_PCT - get_percent_used_capacity())
        / 100
    )
    logger.info(
        f"The fill needs {required_gb:.1f}GB of usable Ceph capacity it does "
        f"not already hold, {budget_gb:.1f}GB is left under the "
        f"{MAX_OSD_USED_PCT}% cap"
    )
    if required_gb > budget_gb:
        raise UnexpectedBehaviour(
            f"Not enough Ceph capacity for the NooBaa DB fill: it needs "
            f"{required_gb:.1f}GB usable beyond what the DB images already "
            f"hold and only {budget_gb:.1f}GB is left under the "
            f"{MAX_OSD_USED_PCT}% OSD cap. Run this test on a cluster with "
            "more storage"
        )


def nb_db_scalar(query, timeout=600):
    """
    Return a single number from the NooBaa DB.

    Args:
        query (str): Query returning one row of one column
        timeout (int): Timeout in seconds of the 'oc' command

    Returns:
        int: The value

    """
    return int(exec_nb_db_query(query, timeout=timeout)[0].strip())


def db_instances_healthy():
    """
    Return whether both NooBaa DB instances answer queries.

    Returns:
        bool: True when both instances answer

    """
    try:
        pods = db_instance_pods()
    except Exception as exc:
        logger.warning(f"The NooBaa DB CNPG roles could not be read: {exc}")
        return False
    for pod_obj in pods:
        try:
            pod_obj.exec_cmd_on_pod(
                'psql -U postgres -tAc "select 1"',
                container_name="postgres",
                shell=True,
                timeout=60,
            )
        except Exception as exc:
            logger.warning(f"{pod_obj.name} is not answering queries: {exc}")
            return False
    return True


def restore_db_instances():
    """
    Wait for both NooBaa DB instances to serve queries again.

    Returns:
        bool: True when both instances answer queries

    """
    try:
        for healthy in TimeoutSampler(1200, 30, db_instances_healthy):
            if healthy:
                logger.info("Both NooBaa DB instances are serving queries")
                return True
    except TimeoutExpiredError:
        logger.warning("The NooBaa DB instances are still not serving queries")
    return False


OBJECT_TABLES = "objectmds, objectparts, datachunks, datablocks"
PURGE_MD_BLOW_SQL = """
delete from datablocks d using buckets b
 where b.data->>'_id' = d.data->>'bucket' and b.data->>'name' = '{bucket}';
delete from datachunks c
 where exists (select 1 from objectparts op
                 join buckets b on b.data->>'_id' = op.data->>'bucket'
                where op.data->>'chunk' = c._id
                  and b.data->>'name' = '{bucket}');
delete from objectparts p using buckets b
 where b.data->>'_id' = p.data->>'bucket' and b.data->>'name' = '{bucket}';
delete from objectmds o using buckets b
 where b.data->>'_id' = o.data->>'bucket' and b.data->>'name' = '{bucket}';
"""


def purge_md_blow_rows(bucket_name):
    """
    Remove the rows md_blow wrote for one bucket.

    The fill leaves the disk nearly full, which is when a DELETE is least able
    to run: it needs WAL to record every row it removes, and it frees nothing
    until a VACUUM afterwards. TRUNCATE needs neither, and hands the files
    back to the filesystem on the spot, so it is used whenever the object
    tables hold nothing but the fill. That is the normal case for a cluster
    this test runs on, and it is checked rather than assumed.

    Args:
        bucket_name (str): Bucket whose md_blow rows are removed

    """
    other_rows = nb_db_scalar(
        "select count(*) from objectmds o where not exists (select 1 from "
        "buckets b where b.data->>'_id' = o.data->>'bucket' and "
        f"b.data->>'name' = '{bucket_name}')"
    )
    if other_rows:
        logger.info(
            f"{other_rows} object rows belong to buckets other than "
            f"{bucket_name}, deleting the fill row by row instead of "
            "truncating. This needs room for the WAL it writes, which a full "
            "DB may not have"
        )
        exec_nb_db_query(PURGE_MD_BLOW_SQL.format(bucket=bucket_name), timeout=3600)
        return

    logger.info(
        f"{bucket_name} holds every object in the DB, truncating {OBJECT_TABLES}"
    )
    exec_nb_db_query(f"truncate table {OBJECT_TABLES}", timeout=600)


def trim_db_filesystems():
    """
    Discard the freed blocks, so that Ceph takes the space back.
    """
    for pod_obj in db_instance_pods():
        try:
            mount_point = str(
                pod_obj.exec_cmd_on_pod(
                    "df | grep postgresql | head -1 | awk '{print $6}'",
                    container_name="postgres",
                    shell=True,
                )
            ).strip()
            output = pod_obj.exec_cmd_on_pod(
                f"fstrim -v {mount_point}",
                container_name="postgres",
                shell=True,
            )
            logger.info(f"Trimmed {pod_obj.name}: {str(output).strip()}")
        except Exception as exc:
            logger.warning(
                f"Could not trim {pod_obj.name}, so Ceph keeps the blocks the "
                f"VACUUM freed: {exc}"
            )


def reclaim_db_space(bucket_name):
    """
    Return the space the md_blow rows hold to the filesystem, then to Ceph.

    Args:
        bucket_name (str): Bucket md_blow filled

    """
    size_before = nb_db_scalar("select pg_database_size('nbcore')")
    checkpoint_db_instances()
    pod_name, disk_pct = db_filesystem_usage()
    logger.info(f"{pod_name} is {disk_pct}% full going into the purge")

    purge_md_blow_rows(bucket_name)

    free_bytes = None
    for pod_obj in db_instance_pods():
        usage = db_filesystem_bytes(pod_obj)
        if usage is None:
            continue
        used, total = usage
        free_bytes = (
            total - used if free_bytes is None else min(free_bytes, total - used)
        )
    if free_bytes is None:
        logger.warning("No NooBaa DB filesystem could be read, skipping the VACUUM")
        return

    largest_table_bytes = nb_db_scalar(
        "select coalesce(max(pg_total_relation_size(oid)), 0) from pg_class "
        "where relkind = 'r'"
    )
    current_size = nb_db_scalar("select pg_database_size('nbcore')")
    if current_size < size_before // 2:
        logger.info(
            f"nbcore is down to {current_size} bytes from {size_before}, "
            "no VACUUM needed"
        )
        checkpoint_db_instances()
        trim_db_filesystems()
        return
    if free_bytes > largest_table_bytes:
        statement = "VACUUM (FULL, ANALYZE)"
        logger.info(
            f"Running a VACUUM FULL on nbcore, the {free_bytes} free bytes "
            f"cover the {largest_table_bytes} byte rewrite of its largest table"
        )
    else:
        statement = "VACUUM (ANALYZE)"
        logger.warning(
            f"Only {free_bytes} bytes are free, which does not cover the "
            f"{largest_table_bytes} byte rewrite a VACUUM FULL of the largest "
            "nbcore table needs. Running a plain VACUUM instead, which frees "
            "the pages for reuse but returns only trailing ones to the disk"
        )
    exec_nb_db_query(statement, timeout=7200)
    checkpoint_db_instances()

    size_after = nb_db_scalar("select pg_database_size('nbcore')")
    logger.info(
        f"nbcore went from {size_before} to {size_after} bytes, "
        f"{size_before - size_after} freed"
    )
    trim_db_filesystems()


def fill_noobaa_db(blow_io, bucket_name, expanded_pod_name, default_pod_name):
    """
    Fill the NooBaa DB with md_blow until a DB filesystem is nearly full.

    Args:
        blow_io (MdBlow): Object from the md_blow_factory fixture
        bucket_name (str): Bucket the objects are uploaded to
        expanded_pod_name (str): Instance whose PVC was expanded. Its
            filesystem usage drives the fill
        default_pod_name (str): Instance that keeps its default PVC size

    Raises:
        UnexpectedBehaviour: If Ceph cannot hold the fill

    """
    used_bytes, total_bytes, current_pct = get_db_usage(expanded_pod_name)
    logger.info(
        f"md_blow fill starting at {current_pct}% of {expanded_pod_name} "
        f"({used_bytes}/{total_bytes} bytes), filling its filesystem to "
        f"{DB_FILL_TARGET_PCT}%"
    )
    check_ceph_headroom(DB_FILL_TARGET_PCT * total_bytes // 100)

    blow_io.upload_obj_using_md_blow(
        bucket_name,
        threshold_pct=DB_FILL_TARGET_PCT,
        db_pod_name=expanded_pod_name,
        max_disk_pct=MAX_DB_DISK_PCT,
    )

    checkpoint_db_instances()
    fullest_pod, disk_pct = db_filesystem_usage()
    logger.info(f"md_blow fill stopped with {fullest_pod} at {disk_pct}% of its disk")

    osd_name, osd_pct = most_full_osd()
    if osd_pct >= MAX_OSD_USED_PCT:
        raise UnexpectedBehaviour(
            f"The NooBaa DB fill left OSD {osd_name} {osd_pct:.1f}% used, at "
            f"the {MAX_OSD_USED_PCT}% cap, where Ceph starts blocking IO on "
            "every pool"
        )

    for pod_name, threshold in (
        (expanded_pod_name, DB_CAPACITY_WARNING_THRESHOLD),
        (default_pod_name, DB_CAPACITY_CRITICAL_THRESHOLD),
    ):
        used_bytes, total_bytes, current_pct = get_db_usage(pod_name)
        message = (
            f"{pod_name} is at {current_pct}% of its PVC "
            f"({used_bytes}/{total_bytes} bytes), needs {threshold}%"
        )
        if current_pct < threshold:
            logger.warning(f"{message}. Its capacity alert may not fire")
        else:
            logger.info(message)


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
           reads more than 80% of the expanded PVC and over 90% of the replica PVC
        3. Fill the primary DB instance to DB_FILL_TARGET_PCT of its
           filesystem using md_blow
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

        def restore_noobaa_db():
            """
            Restore NooBaa DB instances, remove the md_blow objects from bucket.

            """
            if not restore_db_instances():
                logger.error(
                    "The NooBaa DB instances could not be brought back up, so "
                    "the md_blow objects could not be deleted. The cluster is "
                    "left with a NooBaa DB that is out of disk"
                )
                return

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

            try:
                reclaim_db_space(fill_bucket)
            except Exception as exc:
                logger.warning(f"Failed to reclaim the NooBaa DB disk space: {exc}")

            pod_name, disk_pct = db_filesystem_usage()
            logger.info(
                f"NooBaa DB teardown finished with {pod_name} at {disk_pct}% of "
                "its disk"
            )
            if not restore_db_instances():
                logger.error(
                    "The NooBaa DB instances are not serving queries at the end "
                    "of the teardown"
                )

        request.addfinalizer(restore_noobaa_db)

        fill_noobaa_db(
            blow_io=md_blow_factory,
            bucket_name=fill_bucket,
            expanded_pod_name=expanded_pvc_name,
            default_pod_name=default_pvc_name,
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
            noobaa_pods_running_timeout=1800,
            align_db_volume_size=True,
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
            timeout=1200,
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
