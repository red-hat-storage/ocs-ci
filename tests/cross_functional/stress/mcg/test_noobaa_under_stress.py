import logging
import random

import pytest

from ocs_ci.framework.pytest_customization.marks import magenta_squad, stress
from threading import Event
from concurrent.futures import ThreadPoolExecutor, as_completed
from ocs_ci.helpers.mcg_stress_helper import (
    upload_objs_to_buckets,
    run_noobaa_metadata_intense_ops,
    delete_objs_from_bucket,
    list_objs_from_bucket,
    download_objs_from_bucket,
    delete_objects_in_batches,
    run_background_cluster_checks,
    induce_noobaa_failures,
)
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.utility.retry import retry


logger = logging.getLogger(__name__)


@stress
@magenta_squad
class TestNoobaaUnderStress:

    base_setup_buckets = list()

    @pytest.mark.parametrize(
        argnames=["test_type"],
        argvalues=[
            pytest.param("bulk"),
            pytest.param("breakpoint"),
            pytest.param("failure"),
        ],
    )
    def test_noobaa_under_stress(
        self,
        setup_stress_testing_buckets,
        nb_stress_cli_pods,
        mcg_obj_session,
        rgw_obj_session,
        stress_test_directory_setup,
        bucket_factory,
        nodes,
        scale_noobaa_resources_session,
        scale_noobaa_db_pod_pv_size,
        threading_lock,
        disable_debug_logs,
        test_type,
    ):
        """
        Stress Noobaa by performing bulk s3 operations under non-disruptive, disruptive and breakpoint
        situations. This consists mainly 3 stages
        mentioned below
            1. Base setup: Here we create the buckets of all possible types and then
            load them with a million objects in deep directory
            2. S3 object upload & S3 bulk operations:
                a. Upload objects to the bucket concurrently with objects count increased by multiplier times
                b. Perform various s3 operations such as list, download, delete, metadata intense op etc.
                   concurrently on each of the bucket respectively.
                   We also induce Noobaa specific failure if the test is disruptive.
            3. At the end delete objects from all the bucket in batches

        For,
            1. bulk - Multiplier goes from 1,2,3,4,5
            2. breakpoint - Multiplier goes from 1,2,3,4,5,4,3,2,1
            3. failure - Multiplier goes from 1,2,3,4,5

        """

        # Get pod objects
        nb_stress_cli_pod_1 = nb_stress_cli_pods[0]
        nb_stress_cli_pod_2 = nb_stress_cli_pods[1]

        # Scale noobaa pod resources
        scale_noobaa_resources_session(
            min_ep_count=2, max_ep_count=2, cpu=2, memory="10Gi"
        )

        # Start the background check process running
        bg_event = Event()
        bg_executor = ThreadPoolExecutor(max_workers=1)

        bg_future = bg_executor.submit(
            run_background_cluster_checks,
            scale_noobaa_db_pod_pv_size,
            event=bg_event,
            threading_lock=threading_lock,
        )

        # In-case of disruptive testing,we need to make sure we attempt
        # some re-tries
        if test_type == "failure":
            tries = 8
        else:
            tries = 1

        try:
            # Fetch buckets created for stress testing
            self.base_setup_buckets = setup_stress_testing_buckets()

            # Upload objects to the buckets created concurrently
            upload_objs_to_buckets(
                mcg_obj_session,
                nb_stress_cli_pod_1,
                self.base_setup_buckets,
                current_iteration=0,
            )

            # Iterate and stress the cluster with object upload
            # and other IO operations
            total_iterations = 1
            if test_type == "breakpoint":
                # In case of breakpoint test we increment the multiplier first and
                # then decrement the multiplier
                iterator = list(range(1, total_iterations + 1)) + list(
                    range(total_iterations - 1, 0, -1)
                )
            else:
                iterator = list(range(1, total_iterations + 1))

            for i in iterator:
                current_iteration = i
                multiplier = current_iteration + 1
                logger.info(
                    f"Performing Iteration {current_iteration} of stressing the cluster"
                )
                executor = ThreadPoolExecutor(max_workers=5)
                futures_obj = list()
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
                        nb_stress_cli_pod_1,
                        self.base_setup_buckets,
                        current_iteration=current_iteration,
                        event=event,
                        multiplier=multiplier,
                    )
                )

                # Perform metadata intense operations
                # on randomly selected bucket
                bucket = random.choice(buckets)
                futures_obj.append(
                    executor.submit(
                        retry(Exception, tries=tries, delay=10)(
                            run_noobaa_metadata_intense_ops
                        ),
                        mcg_obj_session,
                        nb_stress_cli_pod_2,
                        bucket_factory,
                        bucket,
                        prev_iteration=current_iteration - 1,
                        event=event,
                        multiplier=multiplier,
                    )
                )
                buckets.remove(bucket)

                # Perform object deletion on a
                # randomly selected bucket
                bucket = random.choice(buckets)
                futures_obj.append(
                    executor.submit(
                        retry(CommandFailed, tries=tries, delay=10)(
                            delete_objs_from_bucket
                        ),
                        nb_stress_cli_pod_2,
                        bucket,
                        prev_iteration=current_iteration - 1,
                        event=event,
                        multiplier=multiplier,
                    )
                )
                buckets.remove(bucket)

                # Perform object listing on a
                # randomly selected bucket
                bucket = random.choice(buckets)
                futures_obj.append(
                    executor.submit(
                        retry(Exception, tries=tries, delay=10)(list_objs_from_bucket),
                        bucket,
                        prev_iteration=current_iteration - 1,
                        event=event,
                    )
                )
                buckets.remove(bucket)

                # Perform object download on
                # a randomly selected bucket
                if len(buckets) != 0:
                    bucket = random.choice(buckets)
                futures_obj.append(
                    executor.submit(
                        retry(CommandFailed, tries=tries, delay=10)(
                            download_objs_from_bucket
                        ),
                        nb_stress_cli_pod_2,
                        bucket,
                        stress_test_directory_setup.result_dir,
                        prev_iteration=current_iteration - 1,
                        event=event,
                        multiplier=multiplier,
                    )
                )
                if len(buckets) != 0:
                    buckets.remove(bucket)

                # Induce the noobaa specific failure if the test
                # is with failure
                if test_type == "failure":
                    induce_noobaa_failures(nodes, delay=1800)

                # Wait until all the object operations are done
                logger.info(
                    "Waiting all the Object upload and IO operations for the current iteration is completed"
                )
                for future in as_completed(futures_obj):
                    future.result()

                executor.shutdown()

            # Delete all the objects from the bucket
            # in batches of 20K objects at a time
            buckets = [
                (type, bucket) for type, bucket in self.base_setup_buckets.items()
            ]
            with ThreadPoolExecutor() as executor:
                futures = list()
                for bucket in buckets:
                    future = executor.submit(
                        delete_objects_in_batches, bucket, batch_size=20000
                    )
                    futures.append(future)

                logger.info("Waiting for all the delete object operations to complete")
                for future in as_completed(futures):
                    future.result()
        finally:
            bg_event.set()
            bg_future.result()
            bg_executor.shutdown()
