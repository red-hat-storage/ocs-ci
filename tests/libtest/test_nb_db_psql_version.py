import pytest
from ocs_ci.framework.testlib import libtest
from ocs_ci.helpers.helpers import verify_nb_db_psql_version


@libtest
@pytest.mark.parametrize(
    "check_image",
    [
        False,
        True,
    ],
)
def test_nb_db_psql_version(check_image):
    verify_nb_db_psql_version(check_image_name_version=check_image)
