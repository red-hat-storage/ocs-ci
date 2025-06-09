import pytest
from ocs_ci.framework.pytest_customization.marks import libtest
from ocs_ci.ocs.bucket_utils import (
    write_random_test_objects_to_bucket,
    delete_all_objects_in_batches,
)


@pytest.mark.parametrize(
    "amount, parallelize",
    [(500, False), (1500, False), (2345, True), (0, False), (0, True)],
)
@libtest
def test_delete_all_objects_in_batches(
    mcg_obj_session,
    bucket_factory_session,
    awscli_pod_session,
    test_directory_setup,
    amount,
    parallelize,
):
    """
    Test bucket_utils.py::delete_all_objects_in_batches with and without object prefixes.
    """
    bucket = bucket_factory_session()[0].name

    write_random_test_objects_to_bucket(
        io_pod=awscli_pod_session,
        bucket_to_write=bucket,
        file_dir=test_directory_setup.origin_dir,
        amount=amount,
        mcg_obj=mcg_obj_session,
        bs="1K",
    )

    delete_all_objects_in_batches(
        s3_resource=mcg_obj_session.s3_resource,
        bucket_name=bucket,
        parallelize=parallelize,
    )
    # Optional: Assert deletion success
    assert not list(
        mcg_obj_session.s3_resource.Bucket(bucket).objects.all()
    ), "Objects remain in bucket after deletion"


@libtest
def test_delete_all_objects_in_batches_many_objects(
    mcg_obj_session,
    bucket_factory_session,
    awscli_pod_session,
    test_directory_setup,
):
    """
    Test bucket_utils.py::delete_all_objects_in_batches with a large number of objects.
    Use parallel deletion.
    """
    amount = 170000
    bucket = bucket_factory_session()[0].name
    max_chunk_size = 5000

    # Write a large number of objects in chunks
    i = 0
    while amount >= max_chunk_size:
        write_random_test_objects_to_bucket(
            io_pod=awscli_pod_session,
            bucket_to_write=bucket,
            file_dir=test_directory_setup.origin_dir,
            amount=max_chunk_size,
            mcg_obj=mcg_obj_session,
            bs="1K",
            pattern=f"test-{i}",
        )
        amount -= max_chunk_size
        i += 1

    # Write remaining objects
    if amount:
        write_random_test_objects_to_bucket(
            io_pod=awscli_pod_session,
            bucket_to_write=bucket,
            file_dir=test_directory_setup.origin_dir,
            amount=amount,
            mcg_obj=mcg_obj_session,
            bs="1K",
        )

    delete_all_objects_in_batches(
        s3_resource=mcg_obj_session.s3_resource,
        bucket_name=bucket,
        parallelize=True,
    )
