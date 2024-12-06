import logging
import random

from ocs_ci.framework.pytest_customization.marks import magenta_squad
from threading import Event
from concurrent.futures import ThreadPoolExecutor
from ocs_ci.helpers.mcg_stress_helper import (
    upload_objs_to_buckets,
    run_noobaa_metadata_intense_ops,
    delete_objs_from_bucket,
    list_objs_from_bucket,
    download_objs_from_bucket,
    delete_objects_in_batches,
    run_background_cluster_checks,
)

logger = logging.getLogger(__name__)


@magenta_squad
class TestNoobaaUnderStress:

    base_setup_buckets = list()

    def test_noobaa_under_stress(
        self,
        setup_stress_testing_bucket,
        nb_stress_cli_pod,
        mcg_obj_session,
        rgw_obj_session,
        stress_test_directory_setup,
        bucket_factory,
        scale_noobaa_resources_session,
        scale_noobaa_db_pod_pv_size,
    ):
        """
        Stress Noobaa by performing bulk s3 operations. This consists mainly 3 stages
        mentioned below
            1. Base setup: Here we create the buckets of all possible types and then
            load them with million objects in deep directory
            2. S3 bulk operations: Here we perform various s3 operations such as list,
            download, delete, metadata intense op etc concurrently on each of the bucket
            respectively.
            3. At the end delete objects from all the bucket in batches

        """

        # Fetch buckets created for stress testing
        self.base_setup_buckets = setup_stress_testing_bucket()

        # Upload objects to the buckets created concurrently
        upload_objs_to_buckets(
            mcg_obj_session,
            nb_stress_cli_pod,
            self.base_setup_buckets,
            iteration_no=0,
        )

        bg_event = Event()
        bg_executor = ThreadPoolExecutor(max_workers=1)

        bg_future = bg_executor.submit(
            run_background_cluster_checks, scale_noobaa_db_pod_pv_size, event=bg_event
        )

        # Iterate and stress the cluster with object upload
        # and other IO operations
        total_iterations = 4
        executor = ThreadPoolExecutor(max_workers=5)
        futures_obj = list()
        for i in range(1, total_iterations):
            logger.info(f"Performing Iteration {i} of stressing the cluster")
            buckets = [
                (type, bucket) for type, bucket in self.base_setup_buckets.items()
            ]

            # Instantiate event object
            event = Event()

            # Perform object upload operation
            # concurrently
            futures_obj.append(
                executor.submit(
                    upload_objs_to_buckets,
                    mcg_obj_session,
                    nb_stress_cli_pod,
                    self.base_setup_buckets,
                    iteration_no=i,
                    event=event,
                )
            )

            # Perform metadata intense operations
            # on randomly selected bucket
            bucket = random.choice(buckets)
            futures_obj.append(
                executor.submit(
                    run_noobaa_metadata_intense_ops,
                    mcg_obj_session,
                    nb_stress_cli_pod,
                    bucket_factory,
                    bucket,
                    iteration_no=i - 1,
                    event=event,
                )
            )
            buckets.remove(bucket)

            # Perform object deletion on a
            # randomly selected bucket
            bucket = random.choice(buckets)
            futures_obj.append(
                executor.submit(
                    delete_objs_from_bucket,
                    nb_stress_cli_pod,
                    bucket,
                    iteration_no=i - 1,
                    event=event,
                )
            )
            buckets.remove(bucket)

            # Perform object listing on a
            # randomly selected bucket
            bucket = random.choice(buckets)
            futures_obj.append(
                executor.submit(
                    list_objs_from_bucket,
                    bucket,
                    iteration_no=i - 1,
                    event=event,
                )
            )
            buckets.remove(bucket)

            # Perform object download on
            # a randomly selected bucket
            bucket = random.choice(buckets)
            futures_obj.append(
                executor.submit(
                    download_objs_from_bucket,
                    nb_stress_cli_pod,
                    bucket,
                    stress_test_directory_setup.result_dir,
                    iteration_no=i - 1,
                    event=event,
                )
            )
            buckets.remove(bucket)
            nb_stress_cli_pod.exec_cmd_on_pod(
                f"rm -rf {stress_test_directory_setup.result_dir}/"
            )

            # Wait until all the object operations are done
            logger.info(
                "Waiting all the Object upload and IO operations for the current iteration is completed"
            )
            for future in futures_obj:
                future.result()

        # Delete all the objects from the bucket
        # in batches of 20K objects at a time
        buckets = [(type, bucket) for type, bucket in self.base_setup_buckets.items()]
        with ThreadPoolExecutor() as executor:
            futures = list()
            for bucket in buckets:
                future = executor.submit(
                    delete_objects_in_batches, bucket, batch_size=20000
                )
                futures.append(future)

            logger.info("Waiting for all the delete object operations to complete")
            for future in futures:
                future.result()

        bg_event.set()
        bg_future.result()
