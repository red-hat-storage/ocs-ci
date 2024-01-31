import logging

import pytest

from ocs_ci.utility import templating
from ocs_ci.ocs.constants import CEPHOBJECTSTORE_USER_YAML
from ocs_ci.helpers.helpers import create_unique_resource_name
from ocs_ci.ocs.resources.ocs import OCS

from ocs_ci.framework.pytest_customization.marks import tier3, bugzilla, polarion_id

logger = logging.getLogger(__name__)


class TestObjectStoreUserCaps:
    @pytest.fixture()
    def create_test_cosu(self, request):

        cosu_data = templating.load_yaml(CEPHOBJECTSTORE_USER_YAML)
        cosu_name = create_unique_resource_name("test", "cephobjectstoreuser")
        cosu_data["metadata"]["name"] = cosu_name
        test_cosu_obj = OCS(**cosu_data)
        test_cosu_obj.create()

        def teardown():
            test_cosu_obj.delete()
            logger.info(f"Deleted ceph-objectstoreuser {cosu_name}")

        request.addfinalizer(teardown)
        return test_cosu_obj

    @tier3
    @bugzilla("2196858")
    @polarion_id("")
    def test_cephobjectstore_user_roles_cap(self, create_test_cosu):
        """
        Create CephObjectStoreUser with roles cap and
        make sure creation is successfull without any issues

        """
        test_cosu_data = create_test_cosu.get()
        logger.info(f"Successfully created the ceph-objectstoreuser;\n{test_cosu_data}")
