import logging
import concurrent.futures
import time
import textwrap

from tabulate import tabulate

from ocs_ci.helpers.helpers import get_noobaa_db_size, get_noobaa_db_usage_percent
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.mcg import MCG
from ocs_ci.ocs.resources.objectbucket import OBC
from ocs_ci.ocs.resources.bucket_policy import NoobaaAccount
from ocs_ci.ocs.resources.mcg_lifecycle_policies import LifecyclePolicy, ExpirationRule
from ocs_ci.ocs.bucket_utils import (
    s3_copy_object,
    list_objects_from_bucket,
    sync_object_directory,
    rm_object_recursive,
    s3_list_objects_v2,
    list_objects_in_batches,
    s3_delete_objects,
)
from ocs_ci.utility.retry import retry
from ocs_ci.ocs.cluster import CephCluster
from ocs_ci.ocs.exceptions import (
    NoobaaHealthException,
    CephHealthException,
    CommandFailed,
)
from ocs_ci.ocs.resources.pod import pod_resource_utilization_raw_output_from_adm_top
from ocs_ci.utility.prometheus import PrometheusAPI

logger = logging.getLogger(__name__)


def upload_objs_to_buckets(
    mcg_obj, pod_obj, buckets, iteration_no, event=None, multiplier=1
):
    """
    This will upload objects present in the stress-cli pod
    to the buckets provided concurrently

    Args:
        mcg_obj (MCG): MCG object
        pod_obj (Pod): Pod object
        buckets (Dict): Map of bucket type and bucket object
        iteration_no (int): Integer value representing iteration
        event (threading.Event()): Event object to signal the execution
            completion

    """
    src_path = "/complex_directory/"

    logger.info(f"Uploading objects to all the buckets under prefix {iteration_no}")
    try:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = list()
            for type, bucket in buckets.items():
                if type == "rgw":
                    s3_obj = OBC(bucket.name)
                else:
                    s3_obj = mcg_obj
                logger.info(
                    f"OBJECT UPLOAD: Uploading objects to the bucket {bucket.name}"
                )
                for i in range(multiplier):
                    future = executor.submit(
                        sync_object_directory,
                        pod_obj,
                        src_path,
                        f"s3://{bucket.name}/{iteration_no}/{i+1}/",
                        s3_obj,
                        timeout=20000,
                    )
                    futures.append(future)

            logger.info(
                "OBJECT UPLOAD: Waiting for the objects upload to complete for all the buckets"
            )
            for future in concurrent.futures.as_completed(futures):
                future.result()
    finally:
        logger.info(
            "Setting the event to indicate that upload objects operation is either completed or failed"
        )
        if event:
            event.set()


def run_noobaa_metadata_intense_ops(
    mcg_obj, pod_obj, bucket_factory, bucket, iteration_no, event=None
):
    """
    Perfrom metdata intense operations to stress Noobaa

    Args:
        mcg_obj (MCG): MCG object
        pod_obj (Pod): Noobaa stress CLI pod object
        bucket_factory (fixture): Pytest fixture for creating bucket
        bucket (tuple): Tuple consisting of backend storage type and bucket object
        iteration_no (int): Iteration number or prefix from where should delete objects
        event (threading.Event()): Event object to signal the execution
            completion

    """
    bucket_type, bucket_obj = bucket
    bucket_name = bucket_obj.name

    # Run metadata specific to bucket
    def _run_bucket_ops():
        """
        This function will run bucket related operations such as
        new bucket creation, adding lifecycle policy, bucket deletion. Hence
        stressing the noobaa db through lot of metadata related operations

        """

        while True:
            buckets_created = list()
            for i in range(0, 10):
                # create 100K buckets
                bucket = bucket_factory()[0]
                logger.info(f"METADATA OP: Created bucket {bucket.name}")

                # set lifecycle config for each buckets
                lifecycle_policy = LifecyclePolicy(ExpirationRule(days=1))
                mcg_obj.s3_client.put_bucket_lifecycle_configuration(
                    Bucket=bucket.name,
                    LifecycleConfiguration=lifecycle_policy.as_dict(),
                )
                logger.info(
                    f"METADATA OP: Applied bucket lifecycle policy for the bucket {bucket.name}"
                )
                buckets_created.append(bucket)

            # delete the buckets
            for bucket in buckets_created:
                bucket.delete()
                logger.info(f"METADATA OP: Deleted bucket {bucket.name}")

            if event.is_set():
                logger.info(
                    f"Successfully completed bucket creation/deletion operation in the background"
                    f" for the current iteration {iteration_no+1}"
                )
                break

    def _run_object_metadata_ops():
        """
        This function will perform some metadata update operation
        on each object for the given bucket

        """
        # set metadata for each object present in the given bucket
        if bucket_type.upper() == "RGW":
            s3_obj = OBC(bucket_name)
        else:
            s3_obj = mcg_obj

        objs_in_bucket = list_objects_from_bucket(
            pod_obj=pod_obj,
            target=bucket_name,
            prefix=iteration_no,
            s3_obj=s3_obj,
            timeout=6000,
            recursive=True,
        )
        while True:
            for obj in objs_in_bucket:
                object_key = obj.split("/")[-1]
                metadata = {f"new-{object_key}": f"new-{object_key}"}
                s3_copy_object(
                    s3_obj,
                    bucket_name,
                    source=f"{bucket_name}/{obj}",
                    object_key=object_key,
                    metadata=metadata,
                )
                logger.info(
                    f"METADATA OP: Updated metadata for object {object_key} in bucket {bucket_name}"
                )

                if event.is_set():
                    break
            if event.is_set():
                logger.info(
                    f"Successfully completed the metadata update operation"
                    f" in the background for the iteration {iteration_no+1}"
                )
                break

    def _run_noobaa_account_ops():
        """
        This function performs noobaa account creation and update operation

        """

        # create 100K of noobaa accounts
        while True:
            nb_accounts_created = list()
            for i in range(0, 10):
                nb_account = NoobaaAccount(
                    mcg_obj,
                    name=f"nb-acc-{i}",
                    email=f"nb-acc-{i}@email",
                )
                nb_accounts_created.append(nb_account)
                logger.info(
                    f"METADATA OP: Created Noobaa account {nb_account.account_name}"
                )

            # for nb_acc in nb_accounts_created:
            #     nb_acc.update_account(new_email=f"new-{nb_acc.email_id}")
            #     logger.info(f"METADATA OP: Updated noobaa account {nb_acc.account_name}")

            for nb_acc in nb_accounts_created:
                nb_acc.delete_account()
                logger.info(
                    f"METADATA OP: Deleted noobaa account {nb_acc.account_name}"
                )

            if event.is_set():
                logger.info(
                    f"Successfully completed noobaa account creation/update/deletion operation"
                    f" in the background for the iteration {iteration_no+1}"
                )
                break

    # run the above metadata intense ops parallel
    logger.info("Initiating metadata ops")
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=3)
    futures_obj = list()
    futures_obj.append(executor.submit(_run_bucket_ops))
    futures_obj.append(executor.submit(_run_object_metadata_ops))
    futures_obj.append(executor.submit(_run_noobaa_account_ops))
    logger.info("Waiting until all the upload objects operations are completed")
    for future in futures_obj:
        future.result()


def delete_objs_from_bucket(pod_obj, bucket, iteration_no, event=None):
    """
    Delete all the objects from a bucket

    Args:
        pod_obj (Pod): Noobaa stress CLI pod object
        bucket (Tuple): Tuple consisting of backend storage type and bucket object
        iteration_no (int): Iteration number or prefix from where should delete objects
        event (threading.Event()): Event object to signal the execution
            completion

    """
    bucket_type, bucket_obj = bucket
    bucket_name = bucket_obj.name
    if bucket_type.upper() == "RGW":
        mcg_obj = OBC(bucket_name)
    else:
        mcg_obj = MCG()

    logger.info(
        f"DELETE OP: Delete objects recursively from the bucket {bucket_name} under prefix {iteration_no}"
    )

    rm_object_recursive(
        pod_obj,
        bucket_name,
        mcg_obj,
        prefix=iteration_no,
        timeout=6000,
    )
    logger.info(
        f"Successfully completed object deletion operation on bucket {bucket_name} under prefix {iteration_no}"
    )


def list_objs_from_bucket(bucket, iteration_no, event=None):
    """
    List objects from bucket

    Args:
        bucket (Tuple): Tuple consisting of backend storage type and bucket object
        iteration_no (int): Iteration number or prefix from where should list objects
        event (threading.Event()): Event object to signal the execution
            completion

    """
    bucket_type, bucket_obj = bucket
    bucket_name = bucket_obj.name
    if bucket_type.upper() == "RGW":
        mcg_obj = OBC(bucket_name)
    else:
        mcg_obj = MCG()

    logger.info(
        f"LIST OP: Listing objects from the bucket {bucket_name} under prefix {iteration_no}"
    )
    while True:
        s3_list_objects_v2(
            mcg_obj, bucket_name, prefix=str(iteration_no), delimiter="/"
        )

        if event.is_set():
            logger.info(
                f"Successfully completed object list operation on bucket {bucket_name} under prefix {iteration_no}"
            )
            break


def download_objs_from_bucket(pod_obj, bucket, target_dir, iteration_no, event=None):
    """
    Download objects from a bucket back to local directory

    Args:
        pod_obj (Pod): Noobaa stress CLI pod object
        bucket (Tuple): Tuple consisting of backend storage type and bucket object
        target_dir (str): Target directory to download objects
        iteration_no (int): Iteration number or prefix from where should download objects
        event (threading.Event()): Event object to signal the execution
            completion

    """
    bucket_type, bucket_obj = bucket
    bucket_name = bucket_obj.name
    if bucket_type.upper() == "RGW":
        mcg_obj = OBC(bucket_name)
    else:
        mcg_obj = MCG()

    logger.info(
        f"DOWNLOAD OP: Download objects from the bucket {bucket_name} under prefix {iteration_no}"
    )
    while True:
        sync_object_directory(
            pod_obj,
            f"s3://{bucket_name}/{iteration_no}",
            target_dir,
            mcg_obj,
            timeout=6000,
        )

        if event.is_set():
            logger.info(
                f"Successfully completed object download operation on bucket {bucket_name} under prefix {iteration_no}"
            )
            break


def delete_objects_in_batches(bucket, batch_size):
    """
    Delete objects from the bucket in batches

    Args:
        bucket (tuple): Tuple consisting of backend storage type and bucket object
        batch_size (int): Number of objects to delete at a time

    """
    bucket_type, bucket_obj = bucket
    bucket_name = bucket_obj.name
    if bucket_type.upper() == "RGW":
        mcg_obj = OBC(bucket_name)
    else:
        mcg_obj = MCG()

    logger.info(f"Deleting objects in bucket {bucket_name} in batches of {batch_size}")
    total_objs_deleted = 0
    for obj_batch in list_objects_in_batches(
        mcg_obj, bucket_name, batch_size=batch_size, yield_individual=False
    ):
        s3_delete_objects(mcg_obj, bucket_name, obj_batch)
        total_objs_deleted += batch_size
        logger.info(
            f"Total objects deleted {total_objs_deleted} in bucket {bucket_name}"
        )


def run_background_cluster_checks(scale_noobaa_db_pv, event=None, threading_lock=None):
    """
    Run background checks to verify noobaa health
    and cluster health overall

        1. Check Noobaa Health
        2. Check Ceph Health
        3. Check Noobaa db usage
        4. Check for any alerts
        5. Memory and CPU utilization

    """
    ceph_cluster = CephCluster()
    prometheus_api = PrometheusAPI(threading_lock=threading_lock)
    prometheus_alert_list = list()

    logger.info(
        "\n"
        "\nNow starting background check operations to check the following"
        "\n1. Nooba Health"
        "\n2. Ceph Health"
        "\n3. Noobaa DB usage"
        "\n4. Prometheus Alerts"
        "\n5. Memory and CPU utilization for Noobaa pods"
        "\n"
    )

    @retry(NoobaaHealthException, tries=10, delay=60)
    def check_noobaa_health():

        while True:

            ceph_cluster.noobaa_health_check()
            logger.info(
                "\n"
                "\n[BACKGROUND CHECK]"
                "\nNoobaa is healthy... rechecking in 1 minute"
                "\n"
            )

            if event.is_set():
                logger.info("[BACKGROUND CHECK] Stopping the Noobaa health check")
                break
            time.sleep(60)

    @retry(CephHealthException, tries=10, delay=60)
    def check_ceph_health():

        while True:

            if ceph_cluster.get_ceph_health() == constants.CEPH_HEALTH_ERROR:
                raise CephHealthException
            logger.info(
                "\n"
                "\n[BACKGROUND CHECK]"
                "\nCeph is healthy... rechecking in 1 minute"
                "\n"
            )

            if event.is_set():
                logger.info("[BACKGROUND CHECK] Stopping the Ceph health check")
                break
            time.sleep(60)

    @retry(CommandFailed, tries=10, delay=60)
    def check_noobaa_db_size():

        while True:
            used_percent = int(get_noobaa_db_usage_percent().split("%")[0])
            nb_db_pv_size = int(get_noobaa_db_size().split("G")[0])
            if used_percent > 85:
                logger.info(
                    f"\n"
                    f"\n[BACKGROUND CHECK]"
                    f"\nNoobaa db is {used_percent} percentage. Increasing the noobaa db by 50%"
                    f"\n"
                )
                new_size = int(nb_db_pv_size + (nb_db_pv_size // 2))
                scale_noobaa_db_pv(pvc_size=new_size)
                logger.info(
                    f"\n"
                    f"\n[BACKGROUND CHECK]"
                    f"\nScaled noobaa db to new size {new_size}"
                    f"\n"
                )
            logger.info(
                f"\n"
                f"\n[BACKGROUND CHECK]"
                f"\nCurrent noobaa db usage is at {used_percent}%... Rechecking in 5 minutes..."
                f"\n"
            )

            if event.is_set():
                logger.info("[BACKGROUND CHECK] Stopping the Noobaa db size check")
                break
            time.sleep(300)

    def check_prometheus_alerts():

        while True:

            prometheus_api.prometheus_log(prometheus_alert_list)
            alert_tab = list()
            alert_printed = list()
            alert_tab.append(["Alert Name", "Description", "State"])
            for alert in prometheus_alert_list:
                if alert["labels"]["alertname"] in alert_printed:
                    continue
                alert_tab.append(
                    [
                        alert["labels"]["alertname"].strip(),
                        "\n".join(
                            textwrap.wrap(alert["annotations"]["description"], width=50)
                        ),
                        alert["state"],
                    ]
                )
                alert_printed.append(alert["labels"]["alertname"])
            logger.info(
                f"\n"
                f"\n[BACKGROUND CHECK]"
                f"\nThese are the alerts so far in Prometheus: "
                f"\n{tabulate(alert_tab[1:], headers=alert_tab[0], tablefmt='grid')}"
                f"\n"
            )

            if event.is_set():
                logger.info("[BACKGROUND CHECK] Stopping Prometheus alert logging")
                break
            time.sleep(300)

    def check_noobaa_pod_resource_utilization():

        while True:
            logger.info(
                f"\n"
                f"\n[BACKGROUND CHECK]"
                f"\nCurrent noobaa pod resource utilization: "
                f"\n{pod_resource_utilization_raw_output_from_adm_top(selector=constants.NOOBAA_APP_LABEL)}"
                f"\n"
            )

            if event.is_set():
                logger.info(
                    "[BACKGROUND CHECK] Stopping noobaa pod resource utilization checks"
                )
                break
            time.sleep(300)

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)
    futures_obj = list()
    futures_obj.append(executor.submit(check_noobaa_health))
    futures_obj.append(executor.submit(check_ceph_health))
    futures_obj.append(executor.submit(check_noobaa_db_size))
    futures_obj.append(executor.submit(check_prometheus_alerts))
    futures_obj.append(executor.submit(check_noobaa_pod_resource_utilization))
    for future in futures_obj:
        future.result()
