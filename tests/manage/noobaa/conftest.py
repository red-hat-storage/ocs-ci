import pytest

from ocs_ci.ocs.resources import noobaa


@pytest.fixture()
def noobaa_obj():
    """
    Returns a NooBaa resource that's connected to the S3 endpoint
    Returns:
        s3_res: A NooBaa resource

    """
    s3_res = noobaa.NooBaa()
    return s3_res
