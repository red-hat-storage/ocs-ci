import logging
import random

from threading import Event
from concurrent.futures import ThreadPoolExecutor
from ocs_ci.helpers.mcg_stress_helper import (
    upload_objs_to_buckets,
    run_noobaa_metadata_intense_ops,
    delete_objs_from_bucket,
    list_objs_from_bucket,
    download_objs_from_bucket,
)

logger = logging.getLogger(__name__)


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
    ):

        # fetch buckets created for stress testing
        self.base_setup_buckets = setup_stress_testing_bucket()

        # upload objects to the buckets created concurrently
        upload_objs_to_buckets(
            mcg_obj_session,
            nb_stress_cli_pod,
            self.base_setup_buckets,
            iteration_no=0,
        )

        # iterate and stress the cluster with object upload
        # and other IO operations
        total_iterations = 4
        executor = ThreadPoolExecutor(max_workers=5)
        futures_obj = list()
        for i in range(1, total_iterations):
            logger.info(f"Performing Iteration {i} of stressing the cluster")
            # buckets = list(self.base_setup_buckets.keys())
            buckets = [
                (type, bucket) for type, bucket in self.base_setup_buckets.items()
            ]

            # instantiate event object
            event = Event()

            # perform object upload operation
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

            # perform metadata intense operations
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

            # perform object deletion on a
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

            # perform object listing on a
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

            # perform object download on
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

            # wait until all the object operations are done
            logger.info(
                "Waiting all the Object upload and IO operations for the current iteration is completed"
            )
            for future in futures_obj:
                future.result()
