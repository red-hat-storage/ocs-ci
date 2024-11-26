import logging
import concurrent.futures

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
)

logger = logging.getLogger(__name__)


def upload_objs_to_buckets(mcg_obj, pod_obj, buckets, iteration_no, event=None):
    """
    This will upload objects present in the stress-cli pod
    to the buckets provided concurrently

    Args:
        mcg_obj (MCG): MCG object
        pod_obj (Pod): Pod object
        buckets (Dict): Map of bucket type and bucket object

    """
    src_path = "/complex_directory/dir_0_0/dir_1_0/dir_2_0/dir_3_0/"

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
                future = executor.submit(
                    sync_object_directory,
                    pod_obj,
                    src_path,
                    f"s3://{bucket.name}/{iteration_no}/",
                    s3_obj,
                    timeout=1200,
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
        mcg_obj (MCG): MCG object
        bucket_name (str): Name of the bucket
        iteration_no (int): Iteration number or prefix from where should delete objects
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
    )
    logger.info(
        f"Successfully completed object deletion operation on bucket {bucket_name} under prefix {iteration_no}"
    )


def list_objs_from_bucket(bucket, iteration_no, event=None):
    """
    List objects from bucket

    Args:
        mcg_obj (MCG): MCG object
        bucket_name (str): Name of the bucket
        iteration_no (int): Iteration number or prefix from where should list objects
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
        mcg_obj (MCG): MCG object
        bucket_name (str): Name of the bucket
        target_dir (str): Target directory to download objects
        iteration_no (int): Iteration number or prefix from where should download objects
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
        )

        if event.is_set():
            logger.info(
                f"Successfully completed object download operation on bucket {bucket_name} under prefix {iteration_no}"
            )
            break
