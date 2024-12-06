import pytest

from ocs_ci.ocs.scale_noobaa_lib import fetch_noobaa_storage_class_name


@pytest.fixture(scope="session")
def noobaa_storage_class_name():
    return fetch_noobaa_storage_class_name()
