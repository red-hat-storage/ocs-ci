import logging

from ocs_ci.framework.pytest_customization.marks import bugzilla
from ocs_ci.ocs.bucket_utils import (
    sync_object_directory,
    expire_objects_in_bucket,
    sample_if_objects_expired,
    generate_empty_files,
)

log = logging.getLogger(__name__)


class TestObjectExpirationScale:
    @bugzilla("2279964")
    def test_object_expiration_with_millions_objs(
        self,
        scale_noobaa_resources_session,
        bucket_factory,
        test_directory_setup,
        awscli_pod_session,
        mcg_obj_session,
        reduce_expiration_interval,
    ):
        """
        Test object expiration with millions of objects

        """
        reduce_expiration_interval(interval=1)

        generate_empty_files(
            awscli_pod_session, dir=test_directory_setup.origin_dir, amount=1000
        )

        bucket = bucket_factory()[0]
        log.info(f"Created bucket {bucket.name}")

        sync_object_directory(
            awscli_pod_session,
            test_directory_setup.origin_dir,
            f"s3://{bucket.name}",
            mcg_obj_session,
            timeout=1200,
        )
        log.info("uploaded the million objects to the bucket")

        expire_objects_in_bucket(bucket.name)
        log.info("manually expired all the objects in the bucket")

        sample_if_objects_expired(mcg_obj_session, bucket.name, timeout=3600)
        log.info("verified all the objects are expired")
