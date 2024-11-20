import logging
import concurrent.futures

from ocs_ci.ocs.resources.objectbucket import OBC
from ocs_ci.ocs.resources.bucket_policy import NoobaaAccount
from ocs_ci.ocs.resources.mcg_lifecycle_policies import LifecyclePolicy, ExpirationRule
from ocs_ci.ocs.bucket_utils import (
    s3_copy_object,
    list_objects_from_bucket,
    sync_object_directory,
)

logger = logging.getLogger(__name__)


def upload_objs_to_buckets(mcg_obj, pod_obj, buckets):
    """
    This will upload objects present in the stress-cli pod
    to the buckets provided concurrently

    Args:
        mcg_obj (MCG): MCG object
        pod_obj (Pod): Pod object
        buckets (Dict): Map of bucket type and bucket object

    """
    src_path = "/complex_directory/dir_0_0/dir_1_0/dir_2_0/dir_3_0/dir_4_0/dir_5_0/dir_6_0/dir_7_0/dir_8_0/dir_9_0/"

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = list()
        for type, bucket in buckets.items():
            if type == "rgw":
                s3_obj = OBC(bucket.name)
            else:
                s3_obj = mcg_obj
            logger.info(f"OBJECT UPLOAD: Uploading objects to the bucket {bucket.name}")
            future = executor.submit(
                sync_object_directory, pod_obj, src_path, f"s3://{bucket.name}", s3_obj
            )
            futures.append(future)

        logger.info(
            "OBJECT UPLOAD: Waiting for the objects upload to complete for all the buckets"
        )
        for future in concurrent.futures.as_completed(futures):
            future.result()


def run_noobaa_metadata_intense_ops(mcg_obj, pod_obj, bucket_factory, bucket_name):

    # Run metadata specific to bucket
    def _run_bucket_ops():
        """
        This function will run bucket related operations such as
        new bucket creation, adding lifecycle policy, bucket deletion. Hence
        stressing the noobaa db through lot of metadata related operations

        """
        buckets_created = list()

        for i in range(0, 10):
            # create 100K buckets
            bucket = bucket_factory()[0]
            logger.info(f"METADATA OP: Created bucket {bucket.name}")

            # set lifecycle config for each buckets
            lifecycle_policy = LifecyclePolicy(ExpirationRule(days=1))
            mcg_obj.s3_client.put_bucket_lifecycle_configuration(
                Bucket=bucket.name, LifecycleConfiguration=lifecycle_policy.as_dict()
            )
            logger.info(
                f"METADATA OP: Applied bucket lifecycle policy for the bucket {bucket.name}"
            )
            buckets_created.append(bucket)

        # delete the buckets
        for bucket in buckets_created:
            bucket.delete()
            logger.info(f"METADATA OP: Deleted bucket {bucket.name}")

    def _run_object_metadata_ops():
        """
        This function will perform some metadata update operation
        on each object for the given bucket

        """
        # set metadata for each object present in the given bucket
        objs_in_bucket = list_objects_from_bucket(
            pod_obj=pod_obj,
            target=bucket_name,
            s3_obj=mcg_obj,
            recursive=True,
        )

        for obj in objs_in_bucket:
            object_key = obj.split("/")[-1]
            metadata = {f"new-{object_key}": f"new-{object_key}"}
            s3_copy_object(
                mcg_obj,
                bucket_name,
                source=f"{bucket_name}/{obj}",
                object_key=object_key,
                metadata=metadata,
            )
            logger.info(
                f"METADATA OP: Updated metadata for object {object_key} in bucket {bucket_name}"
            )

    def _run_noobaa_account_ops():
        """
        This function performs noobaa account creation and update operation

        """

        # create 100K of noobaa accounts
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

        for nb_acc in nb_accounts_created:
            nb_acc.update_account(new_email=f"new-{nb_acc.email_id}")
            logger.info(f"METADATA OP: Updated noobaa account {nb_acc.account_name}")

        for nb_acc in nb_accounts_created:
            nb_acc.delete_account()
            logger.info(f"METADATA OP: Deleted noobaa account {nb_acc.account_name}")

    # run the above metadata intense ops parallel
    logger.info(
        "---------------------------------Initiating metadata ops---------------------------------"
    )
    _run_bucket_ops()
    _run_object_metadata_ops()
    _run_noobaa_account_ops()
