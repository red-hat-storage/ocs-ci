import logging
import time

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.testlib import skipif_ocs_version, ui
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.ui.mcg_ui import ObcUI
from ocs_ci.ocs.utils import oc_get_all_obc_names
from ocs_ci.utility.utils import check_resource_existence

logger = logging.getLogger(__name__)


class TestObcUserInterface(object):
    """
    Test the OBC UI

    """

    def teardown(self):
        obc_lst = oc_get_all_obc_names()
        test_obcs = [obc_name for obc_name in obc_lst if "test-ui" in obc_name]
        for obc_name in test_obcs:
            OCP().delete(resource_name=obc_name)

    @ui
    @skipif_ocs_version("<4.8")
    @pytest.mark.parametrize(
        argnames=["obc_name", "storageclass", "bucketclass"],
        argvalues=[
            pytest.param(
                *[
                    "test-ui-obc",
                    "openshift-storage.noobaa.io",
                    "noobaa-default-bucket-class",
                ]
            )
        ],
    )
    def test_create_delete_obc(self, setup_ui, obc_name, storageclass, bucketclass):
        """
        Test creation and deletion of an OBC via the UI

        """
        obc_ui_obj = ObcUI(setup_ui)
        obc_ui_obj.create_obc_ui(obc_name, storageclass, bucketclass)
        time.sleep(5)

        test_obc = OCP(
            namespace=config.ENV_DATA["cluster_namespace"],
            kind="obc",
            resource_name=obc_name,
        )

        test_obc_obj = test_obc.get()

        obc_storageclass = test_obc_obj.get("spec").get("storageClassName")
        obc_bucketclass = (
            test_obc_obj.get("spec").get("additionalConfig").get("bucketclass")
        )
        assert (
            obc_storageclass == storageclass
        ), f"StorageClass mismatch. Expected: {storageclass}, found: {obc_storageclass}"
        assert (
            obc_bucketclass == bucketclass
        ), f"BucketClass mismatch. Expected: {bucketclass}, found: {obc_bucketclass}"

        logger.info(f"Delete {obc_name}")
        OCP(
            namespace=config.ENV_DATA["cluster_namespace"],
            kind="obc",
        ).delete(resource_name=obc_name)
        time.sleep(5)

        assert check_resource_existence(test_obc) is False
