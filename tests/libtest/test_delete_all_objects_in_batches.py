import pytest
from ocs_ci.framework.pytest_customization.marks import libtest
from ocs_ci.ocs.bucket_utils import write_random_test_objects_to_bucket


@pytest.mark.parametrize(
    "amount,prefix",
    [
        (500, None),
        (2345, "testprefix/"),
    ],
)
@libtest
def test_delete_all_objects_in_batches(
    mcg_obj, bucket_factory, awscli_pod_session, test_directory_setup, amount, prefix
):
    """
    Test bucket_utils.py::delete_all_objects_in_batches with and without object prefixes.
    """
    bucket = bucket_factory()[0].name

    write_random_test_objects_to_bucket(
        io_pod=awscli_pod_session,
        bucket_to_write=bucket,
        file_dir=test_directory_setup.origin_dir,
        amount=amount,
        mcg_obj=mcg_obj,
        prefix=prefix,
    )
