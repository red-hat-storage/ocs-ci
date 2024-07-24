import logging

from ocs_ci.framework.pytest_customization.marks import (
    bugzilla,
    polarion_id,
    scale,
    mcg,
    red_squad,
)
from ocs_ci.ocs.bucket_utils import (
    sync_object_directory,
    expire_objects_in_bucket,
    sample_if_objects_expired,
    generate_empty_files,
)
from ocs_ci.ocs.resources.mcg_lifecycle_policies import (
    LifecyclePolicy,
    ExpirationRule,
    LifecycleFilter,
)

log = logging.getLogger(__name__)


@red_squad
@mcg
@scale
class TestObjectExpirationScale:
    @bugzilla("2279964")
    @polarion_id("")
    def test_object_expiration_with_millions_objs(
        self,
        scale_noobaa_resources_session,
        bucket_factory,
        test_directory_setup,
        awscli_pod_session,
        mcg_obj_session,
        reduce_expiration_interval,
        change_lifecycle_batch_size,
    ):
        """
        Test object expiration with millions of objects

        """
        # change the lifecycle interval to 1 minute
        reduce_expiration_interval(interval=1)
        log.info("Changed the expiration interval to 1 minute")

        # change lifecycle batch size to 10K
        change_lifecycle_batch_size(new_lifecycle_batch_size=10000)
        log.info("Increased the lifecycle batch size to 10K")

        # generate 1 million empty files with unique identifiers
        generate_empty_files(
            awscli_pod_session, dir=test_directory_setup.origin_dir, amount=1000
        )

        # create the bucket
        bucket = bucket_factory()[0]
        log.info(f"Created bucket {bucket.name}")

        # sync millions of objects to the bucket
        sync_object_directory(
            awscli_pod_session,
            test_directory_setup.origin_dir,
            f"s3://{bucket.name}",
            mcg_obj_session,
            timeout=1200,
        )
        log.info(f"Uploaded the million objects to the bucket {bucket.name}")

        # change the creation date for all the objects in the bucket
        expire_objects_in_bucket(bucket.name)
        log.info("Manually expired all the objects in the bucket")

        # apply the object expiration policy to the bucket
        log.info(f"Setting object expiration on bucket: {bucket.name}")
        lifecycle_policy = LifecyclePolicy(
            ExpirationRule(days=1, filter=LifecycleFilter())
        )
        mcg_obj_session.s3_client.put_bucket_lifecycle_configuration(
            Bucket=bucket.name, LifecycleConfiguration=lifecycle_policy.as_dict()
        )

        # make sure all the objects are expired
        sample_if_objects_expired(mcg_obj_session, bucket.name, timeout=3600)
        log.info("Verified all the objects are expired")
