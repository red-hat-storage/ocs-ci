import logging
import pytest

from datetime import datetime
from ocs_ci.ocs import hsbench
from ocs_ci.ocs.bucket_utils import (
    s3_delete_object,
    s3_delete_objects,
)
from ocs_ci.framework.pytest_customization.marks import bugzilla, polarion_id

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


class TestDeleteObjects:
    @bugzilla("2181535")
    @polarion_id("OCS-4916")
    @pytest.mark.parametrize(
        argnames=["delete_mode"],
        argvalues=[
            pytest.param("single"),
            pytest.param("batch"),
            pytest.param("whole"),
        ],
    )
    def test_delete_objects(
        self,
        scale_noobaa_db_pod_pv_size,
        scale_noobaa_resources_session,
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
        bucket.verify_health(timeout=300)

        # write 4K and 24M size objects of 1M each to the bucket
        time_1 = datetime.now()
        s3bench.run_benchmark(
            num_obj=1000000,
            timeout=48000,
            object_size="1M",
            end_point=f"http://s3.openshift-storage.svc/{bucket.name}",
            access_key=mcg_obj.access_key_id,
            secret_key=mcg_obj.access_key,
            validate=False,
        )

        # s3bench.run_benchmark(
        #     num_obj=1000000,
        #     timeout=48000,
        #     object_size="4M",
        #     end_point=f"http://s3.openshift-storage.svc/{bucket.name}",
        #     access_key=mcg_obj.access_key_id,
        #     secret_key=mcg_obj.access_key,
        # )
        time_2 = datetime.now()
        log.info(
            f" Time taken to generate 1 mil objects of 4k size: {(time_2-time_1).total_seconds()}"
        )

        # List all the objects in the bucket
        objects_list = [
            obj.key for obj in mcg_obj.s3_list_all_objects_in_bucket(bucket.name)
        ]

        if delete_mode == "single":
            # Delete objects one by one
            log.info(
                f"Deleting objects one by one. Total objects to be deleted: {len(objects_list)}"
            )
            for obj in objects_list:
                s3_delete_object(mcg_obj, bucket.name, obj)
            log.info("Deleted objects successfully!")
        elif delete_mode == "batch":
            # Delete objects in batch
            log.info("Deleting objects in batch of 1000 objects at a time")
            objects_list = [{"Key": key} for key in objects_list]
            while len(objects_list) >= 1000:
                batch = objects_list[:1000]
                objects_list = objects_list[1000:]
                s3_delete_objects(mcg_obj, bucket.name, batch)

            if len(objects_list) != 0:
                s3_delete_objects(mcg_obj, bucket.name, objects_list)
            log.info("Deleted objects in a batch of 1000 objects!")
        else:
            # Delete the whole bucket directly
            bucket.delete()
            log.info(f"Deleted bucket {bucket.name} directly!")
