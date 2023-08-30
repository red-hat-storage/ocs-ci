import logging
import pytest
import psutil

from datetime import datetime
from ocs_ci.ocs import hsbench
from ocs_ci.ocs.bucket_utils import (
    s3_delete_objects,
    list_objects_in_batches,
)
from ocs_ci.framework.pytest_customization.marks import bugzilla, polarion_id, scale

log = logging.getLogger(__name__)


def measure_memory_usage():
    process = psutil.Process()
    memory_info = process.memory_info()
    log.info(f"Memory used: {memory_info.rss / 1024 / 1024} MB")


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
class TestDeleteObjects:
    @bugzilla("2181535")
    @polarion_id("OCS-4916")
    @pytest.mark.parametrize(
        argnames=["delete_mode"],
        argvalues=[
            pytest.param("batch"),
            pytest.param("whole"),
        ],
    )
    def test_delete_objects(
        self,
        scale_noobaa_db_pod_pv_size,
        scale_noobaa_pods_resources_session,
        bucket_factory,
        s3bench,
        mcg_obj,
        delete_mode,
    ):
        """
        Test deletion of objects, objectbucket and backingstore when there
        is huge number of objects (~2 million) stored
        """
        scale_noobaa_db_pod_pv_size(pv_size="600")

        bucket_class_dict = {
            "interface": "OC",
            "backingstore_dict": {"aws": [(1, "eu-central-1")]},
        }
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

        # if delete_mode == "single":
        #     for obj_key in list_objects_in_batches(
        #         mcg_obj, bucket.name, batch_size=10000
        #     ):
        #         s3_delete_object(mcg_obj, bucket.name, obj_key)
        #     log.info("Deleted objects successfully!")
        #     measure_memory_usage()
        if delete_mode == "batch":
            # Delete objects in batch
            log.info("Deleting objects in batch of 1000 objects at a time")
            for obj_batch in list_objects_in_batches(
                mcg_obj, bucket.name, batch_size=10000, yield_individual=False
            ):
                s3_delete_objects(mcg_obj, bucket.name, obj_batch)
            log.info("Deleted objects in a batch of 1000 objects!")
            measure_memory_usage()
        else:
            # Delete the whole bucket directly
            bucket.delete()
            log.info(f"Deleted bucket {bucket.name} directly!")
