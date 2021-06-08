import logging
from ocs_ci.ocs import constants
from ocs_ci.helpers.helpers import create_unique_resource_name
import time

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.testlib import skipif_ocs_version, skipif_disconnected_cluster, ui
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.ui.mcg_ui import BackingstoreUI, ObcUI
from ocs_ci.ocs.utils import oc_get_all_bs_names, oc_get_all_obc_names
from ocs_ci.utility.utils import check_resource_existence

logger = logging.getLogger(__name__)


class TestObcUserInterface(object):
    """
    Test the OBC UI

    """

    def teardown(self):
        obc_lst = oc_get_all_obc_names()
        test_obcs = [obc_name for obc_name in obc_lst if "obc-testing" in obc_name]
        for obc_name in test_obcs:
            OCP(kind="obc").delete(resource_name=obc_name)

    @ui
    @skipif_ocs_version("<4.8")
    @pytest.mark.parametrize(
        argnames=["storageclass", "bucketclass"],
        argvalues=[
            pytest.param(
                *[
                    "openshift-storage.noobaa.io",
                    "noobaa-default-bucket-class",
                ]
            )
        ],
    )
    def test_obc_creation_and_deletion(self, setup_ui, storageclass, bucketclass):
        """
        Test creation and deletion of an OBC via the UI

        """
        obc_name = create_unique_resource_name(
            resource_description="testing", resource_type="obc"
        )

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

        assert obc_ui_obj.verify_current_page_resource_status(
            constants.STATUS_BOUND
        ), "Created OBC was not ready in time"

        logger.info(f"Delete {obc_name}")
        obc_ui_obj.delete_obc_ui(obc_name)
        time.sleep(5)

        assert check_resource_existence(test_obc) is False


class TestBackingstoreUserInterface(object):
    """
    Test the BS UI

    """

    def teardown(self):
        bs_lst = oc_get_all_bs_names()
        test_backingstores = [
            bs_name for bs_name in bs_lst if "backingstore-aws" in bs_name
        ]
        for bs_name in test_backingstores:
            OCP(kind="backingstore").delete(resource_name=bs_name)

    @ui
    @skipif_ocs_version("<4.8")
    @skipif_disconnected_cluster
    def test_bs_creation_and_deletion(self, setup_ui, cld_mgr, cloud_uls_factory):
        """
        Test creation and deletion of a BS via the UI

        """
        uls_name = list(cloud_uls_factory({"aws": [(1, "us-east-2")]})["aws"])[0]

        bs_name = create_unique_resource_name(
            resource_description="aws", resource_type="backingstore"
        )

        bs_ui_obj = BackingstoreUI(setup_ui)
        bs_ui_obj.create_backingstore_ui(
            bs_name, cld_mgr.aws_client.secret.name, uls_name
        )

        assert bs_ui_obj.verify_current_page_resource_status(
            constants.STATUS_READY
        ), "Created backingstore was not ready in time"

        test_bs = OCP(
            namespace=config.ENV_DATA["cluster_namespace"],
            kind="backingstore",
            resource_name=bs_name,
        )

        OCP(kind="backingstore").wait_for_resource(
            condition="Ready", resource_name=bs_name, column="PHASE"
        )

        logger.info(f"Delete {bs_name}")
        bs_ui_obj.delete_backingstore_ui(bs_name)
        time.sleep(5)

        assert check_resource_existence(test_bs) is False
