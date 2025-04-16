import logging

import pytest
import time

from threading import Event
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import datetime

from ocs_ci.ocs import hsbench
from ocs_ci.ocs.bucket_utils import (
    s3_delete_objects,
    list_objects_in_batches,
    s3_delete_object,
    random_object_round_trip_verification,
    gen_empty_file_and_upload,
    expire_objects_in_bucket,
    rm_object_recursive,
    verify_objs_deleted_from_objmds,
    sample_if_objects_expired,
)
from ocs_ci.framework.pytest_customization.marks import (
    polarion_id,
    scale,
    mcg,
    orange_squad,
    jira,
)
from ocs_ci.ocs.resources.mcg_lifecycle_policies import (
    LifecyclePolicy,
    ExpirationRule,
)

from ocs_ci.utility.retry import retry
from ocs_ci.ocs.exceptions import CommandFailed

log = logging.getLogger(__name__)


@pytest.fixture(scope="class")
def s3bench(request):

    s3bench = hsbench.HsBench()
    s3bench.create_resource_hsbench()
    s3bench.install_hsbench()

    def teardown():
        s3bench.cleanup()

    request.addfinalizer(teardown)
    return s3bench


@scale
@orange_squad
@mcg
class TestDeleteObjects:
    def run_background_io(
        self, io_pod, mcg_obj, bucket_name, upload_dir, download_dir, event
    ):
        """
        Run background IO for the given bucket

        Args:
            io_pod (Pod): Pod object representing aws-cli pod
            bucket_name (str): Name of the bucket
            upload_dir (str): Pod directory from where the objects are uploaded
            download_dir (str): Pod directory to where the objects are downloaded
            event (threading.Event()): Event() object

        """
        while True:

            retry(CommandFailed, tries=10, delay=5)(
                random_object_round_trip_verification
            )(
                io_pod,
                bucket_name,
                upload_dir,
                download_dir,
                cleanup=True,
                mcg_obj=mcg_obj,
            )

            if event.is_set():
                break
            time.sleep(60)
        log.info(f"Successfully verified background io for the bucket {bucket_name}")

    @polarion_id("OCS-4916")
    @pytest.mark.parametrize(
        argnames=["delete_mode"],
        argvalues=[
            # Below parameter is commented now because single deletion is causing
            # heavy toll on memory consumption for the jenkins agent and test run time,
            # as we have to delete one object at a time, till 2 million objects are deleted.
            # pytest.param("single"),
            pytest.param("batch"),
            # Below parameter needs to uncommented after we have fix for noobaa db performance
            # bottleneck
            # pytest.param("whole"),
        ],
    )
    def test_delete_objects(
        self,
        awscli_pod_session,
        mcg_obj_session,
        scale_noobaa_db_pod_pv_size,
        scale_noobaa_resources_session,
        bucket_factory,
        s3bench,
        mcg_obj,
        delete_mode,
        test_directory_setup,
    ):
        """
        Test deletion of objects, objectbucket and backingstore when there
        is huge number of objects (~2 million) stored
        """
        bucket_class_dict = {
            "interface": "OC",
            "backingstore_dict": {"aws": [(1, "eu-central-1")]},
            "timeout": 1800,
        }

        # create a bucket where we run some io continuously in the background.
        upload_dir = test_directory_setup.origin_dir
        download_dir = test_directory_setup.result_dir

        io_bucket = bucket_factory(bucketclass=bucket_class_dict, verify_health=False)[
            0
        ].name

        event = Event()

        executor = ThreadPoolExecutor(
            max_workers=1,
        )
        io_thread = executor.submit(
            self.run_background_io,
            awscli_pod_session,
            mcg_obj_session,
            io_bucket,
            upload_dir,
            download_dir,
            event,
        )

        # scale the noobaa db pv size
        scale_noobaa_db_pod_pv_size(pv_size="600")

        # create an object bucket
        bucket = bucket_factory(bucketclass=bucket_class_dict, verify_health=False)[0]
        bucket.verify_health(timeout=600)

        # write 4K and 4M size objects of 1M each to the bucket
        time_1 = datetime.now()
        s3bench.run_benchmark(
            num_obj=1000000,
            timeout=20000,
            object_size="4K",
            end_point=f"http://s3.openshift-storage.svc/{bucket.name}",
            access_key=mcg_obj.access_key_id,
            secret_key=mcg_obj.access_key,
            validate=False,
        )

        s3bench.run_benchmark(
            num_obj=1000000,
            timeout=80000,
            object_size="4M",
            end_point=f"http://s3.openshift-storage.svc/{bucket.name}",
            access_key=mcg_obj.access_key_id,
            secret_key=mcg_obj.access_key,
            validate=False,
        )
        time_2 = datetime.now()
        log.info(
            f" Time taken to generate and upload objects: {(time_2-time_1).total_seconds()}"
        )

        if delete_mode == "single":
            for obj_key in list_objects_in_batches(
                mcg_obj, bucket.name, batch_size=10000
            ):
                s3_delete_object(mcg_obj, bucket.name, obj_key)
            log.info("Deleted objects successfully!")
        elif delete_mode == "batch":
            # Delete objects in batch
            log.info("Deleting objects in batch of 1000 objects at a time")
            for obj_batch in list_objects_in_batches(
                mcg_obj, bucket.name, batch_size=10000, yield_individual=False
            ):
                s3_delete_objects(mcg_obj, bucket.name, obj_batch)
            log.info("Deleted objects in a batch of 1000 objects!")
        else:
            # Delete the whole bucket directly
            bucket.delete()
            log.info(f"Deleted bucket {bucket.name} directly!")

        # stop the io running in the background
        event.set()
        io_thread.result()

    @pytest.fixture(scope="session")
    def create_bucket_verify_object_deletion(
        self,
        request,
        reduce_expiration_interval_session,
        change_lifecycle_schedule_min_session,
        bucket_factory_session,
        mcg_obj_session,
    ):
        buckets = list()
        expirations = list()

        def factory(exp):
            """
            Factory function to create the bucket

            """

            # Reduce the expiration interval and lifecycle schedule delay
            log.info(
                "Reducing the expiration interval and lifecycle schedule delay to 1 minute"
            )
            reduce_expiration_interval_session(interval=1)
            change_lifecycle_schedule_min_session(interval=1)

            expirations.append(exp)
            bucket = bucket_factory_session(amount=1, interface="OC")[0]
            buckets.append(bucket)
            return bucket

        def teardown():
            """
            Teardown function to verify the object deletion
            in the bucket

            """
            for bucket, expiration in zip(buckets, expirations):
                verify_objs_deleted_from_objmds(bucket.name, timeout=20000, sleep=60)
                if expiration:
                    sample_if_objects_expired(
                        mcg_obj_session, bucket.name, timeout=36000, sleep=60
                    )
                log.info("Verified that all objects are deleted and marked deleted")

        request.addfinalizer(teardown)
        return factory

    @jira("DFBUGS-1106")
    @jira("DFBUGS-1116")
    @pytest.mark.parametrize(
        argnames=["expiration"],
        argvalues=[
            pytest.param(
                True,
                marks=[
                    pytest.mark.jira("DFBUGS-1116"),
                    pytest.mark.polarion_id("OCS-6097"),
                ],
            ),
            pytest.param(
                False,
                marks=[
                    pytest.mark.jira("DFBUGS-1106"),
                    pytest.mark.polarion_id("OCS-6096"),
                ],
            ),
        ],
    )
    def test_delete_objs_by_expiration_and_recursive_deletion(
        self,
        create_bucket_verify_object_deletion,
        mcg_obj_session,
        awscli_pod_session,
        test_directory_setup,
        reduce_expiration_interval,
        change_lifecycle_schedule_min,
        expiration,
    ):
        """
        Test to verify object deletion through object mds when we delete or expire
        objects in bucket at scale

        """

        # Create the bucket
        log.info("Create the bucket")
        bucket = create_bucket_verify_object_deletion(exp=expiration)

        # Generate and upload objects to the
        # bucket parallely
        gen_empty_file_and_upload(
            mcg_obj_session,
            awscli_pod_session,
            test_directory_setup.origin_dir,
            amount=500000,
            bucket=bucket.name,
            threads=10,
            timeout=7200,
        )

        # If expiration then setup the expiration and manually
        # expire the objects. Else recursively delete the objects.
        if expiration:
            log.info(f"Setting object expiration on bucket: {bucket}")
            lifecycle_policy = LifecyclePolicy(ExpirationRule(days=1))
            mcg_obj_session.s3_client.put_bucket_lifecycle_configuration(
                Bucket=bucket.name, LifecycleConfiguration=lifecycle_policy.as_dict()
            )

            log.info(f"Manually expiring objects in the bucket {bucket.name}")
            expire_objects_in_bucket(bucket.name)

        else:
            log.info("Deleting the objects inside the bucket recursively")
            rm_object_recursive(
                awscli_pod_session, f"{bucket.name}", mcg_obj_session, timeout=7200
            )
