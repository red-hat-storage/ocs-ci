import logging

from ocs_ci.framework.pytest_customization.marks import (
    bugzilla,
    on_prem_platform_required,
    black_squad,
    mcg,
)
from ocs_ci.ocs import constants
from ocs_ci.helpers.helpers import create_unique_resource_name

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    skipif_disconnected_cluster,
    tier1,
    skipif_ui_not_support,
    ui,
)
from ocs_ci.ocs.ocp import OCP, get_all_resource_names_of_a_kind
from ocs_ci.ocs.ui.mcg_ui import BucketClassUI, MCGStoreUI, ObcUI, ObUI

logger = logging.getLogger(__name__)


@black_squad
@mcg
@skipif_ui_not_support("mcg_stores")
class TestStoreUserInterface(object):
    """
    Test the MCG store UI

    """

    def teardown(self):
        for store_kind in ["namespacestore", "backingstore"]:
            test_stores = [
                store_name
                for store_name in get_all_resource_names_of_a_kind(store_kind)
                if f"{store_kind}-ui" in store_name
            ]
            for store_name in test_stores:
                OCP(
                    kind=store_kind, namespace=config.ENV_DATA["cluster_namespace"]
                ).delete(resource_name=store_name)

    @ui
    @tier1
    @skipif_ocs_version("!=4.8")
    @skipif_disconnected_cluster
    @pytest.mark.parametrize(
        argnames=["kind"],
        argvalues=[
            pytest.param(
                "backingstore",
                marks=pytest.mark.polarion_id("OCS-2549"),
            ),
            pytest.param(
                "namespacestore",
                marks=pytest.mark.polarion_id("OCS-2547"),
            ),
        ],
    )
    def test_store_creation_and_deletion(
        self, setup_ui_class, cld_mgr, cloud_uls_factory, kind
    ):
        """
        Test creation and deletion of MCG stores via the UI

        """
        uls_name = list(cloud_uls_factory({"aws": [(1, "us-east-2")]})["aws"])[0]

        store_name = create_unique_resource_name(
            resource_description="ui", resource_type=kind
        )

        store_ui_obj = MCGStoreUI()
        store_ui_obj.create_store_ui(
            kind, store_name, cld_mgr.aws_client.secret.name, uls_name
        )

        assert store_ui_obj.verify_current_page_resource_status(
            constants.STATUS_READY
        ), f"Created {kind} was not ready in time"

        logger.info(f"Delete {store_name}")
        store_ui_obj.delete_store_ui(kind, store_name)

        test_store = OCP(
            namespace=config.ENV_DATA["cluster_namespace"],
            kind=kind,
            resource_name=store_name,
        )

        assert test_store.check_resource_existence(should_exist=False)


@black_squad
@mcg
@ui
@skipif_ui_not_support("bucketclass")
@tier1
@skipif_ocs_version("!=4.8")
@skipif_disconnected_cluster
class TestBucketclassUserInterface(object):
    """
    Test the bucketclass UI

    """

    def teardown(self):
        bc_lst = get_all_resource_names_of_a_kind("bucketclass")
        test_bucketclasses = [
            bc_name for bc_name in bc_lst if "bucketclass-ui" in bc_name
        ]
        for bc_name in test_bucketclasses:
            OCP(
                kind="bucketclass", namespace=config.ENV_DATA["cluster_namespace"]
            ).delete(resource_name=bc_name)

    @pytest.mark.parametrize(
        argnames=["policy", "bs_amount"],
        argvalues=[
            pytest.param(
                *["spread", 2],
                marks=pytest.mark.polarion_id("OCS-2548"),
            ),
            pytest.param(
                *["mirror", 2],
                marks=pytest.mark.polarion_id("OCS-2543"),
            ),
        ],
    )
    def test_standard_bc_creation_and_deletion(
        self,
        setup_ui_class,
        backingstore_factory,
        policy,
        bs_amount,
    ):
        """
        Test creation and deletion of a BS via the UI

        """
        test_stores = backingstore_factory("oc", {"aws": [(bs_amount, "us-east-2")]})

        bc_name = create_unique_resource_name(
            resource_description="ui", resource_type="bucketclass"
        )

        bc_ui_obj = BucketClassUI()
        bc_ui_obj.create_standard_bucketclass_ui(
            bc_name, policy, [bs.name for bs in test_stores]
        )

        assert bc_ui_obj.verify_current_page_resource_status(
            constants.STATUS_READY
        ), "Created bucketclass was not ready in time"

        logger.info(f"Delete {bc_name}")
        bc_ui_obj.delete_bucketclass_ui(bc_name)

        test_bc = OCP(
            namespace=config.ENV_DATA["cluster_namespace"],
            kind="bucketclass",
            resource_name=bc_name,
        )

        assert test_bc.check_resource_existence(should_exist=False)

    @pytest.mark.parametrize(
        argnames=["policy", "amount"],
        argvalues=[
            pytest.param(
                *["single", 1],
                marks=pytest.mark.polarion_id("OCS-2544"),
            ),
            pytest.param(
                *["multi", 2],
                marks=pytest.mark.polarion_id("OCS-2545"),
            ),
            pytest.param(
                *["cache", 1],
                marks=pytest.mark.polarion_id("OCS-2546"),
            ),
        ],
    )
    def test_namespace_bc_creation_and_deletion(
        self,
        setup_ui_class,
        backingstore_factory,
        namespace_store_factory,
        policy,
        amount,
    ):
        """
        Test creation and deletion of a bucketclass via the UI

        """
        nss_names = [
            nss.name
            for nss in namespace_store_factory("oc", {"aws": [(amount, "us-east-2")]})
        ]

        bs_names = []
        if policy == "cache":
            bs_names = [
                bs.name
                for bs in backingstore_factory("oc", {"aws": [(amount, "us-east-2")]})
            ]

        bc_name = create_unique_resource_name(
            resource_description="ui", resource_type="bucketclass"
        )

        bc_ui_obj = BucketClassUI()
        bc_ui_obj.create_namespace_bucketclass_ui(bc_name, policy, nss_names, bs_names)

        assert bc_ui_obj.verify_current_page_resource_status(
            constants.STATUS_READY
        ), "Created bucketclass was not ready in time"

        logger.info(f"Delete {bc_name}")
        bc_ui_obj.delete_bucketclass_ui(bc_name)

        test_bc = OCP(
            namespace=config.ENV_DATA["cluster_namespace"],
            kind="bucketclass",
            resource_name=bc_name,
        )

        assert test_bc.check_resource_existence(should_exist=False)


@black_squad
@skipif_ui_not_support("obc")
class TestObcUserInterface(object):
    """
    Test the object bucket claim UI

    """

    def teardown(self):
        obc_lst = get_all_resource_names_of_a_kind("obc")
        test_obcs = [obc_name for obc_name in obc_lst if "obc-testing" in obc_name]
        for obc_name in test_obcs:
            OCP(kind="obc", namespace=config.ENV_DATA["cluster_namespace"]).delete(
                resource_name=obc_name
            )

    @ui
    @tier1
    @bugzilla("2097772")
    @pytest.mark.parametrize(
        argnames=["storageclass", "bucketclass", "delete_via", "verify_ob_removal"],
        argvalues=[
            pytest.param(
                *[
                    "openshift-storage.noobaa.io",
                    "noobaa-default-bucket-class",
                    "three_dots",
                    True,
                ],
                marks=[pytest.mark.polarion_id("OCS-4698"), mcg],
            ),
            pytest.param(
                *[
                    "openshift-storage.noobaa.io",
                    "noobaa-default-bucket-class",
                    "Actions",
                    True,
                ],
                marks=[pytest.mark.polarion_id("OCS-2542"), mcg],
            ),
            pytest.param(
                *[
                    "ocs-storagecluster-ceph-rgw",
                    None,
                    "three_dots",
                    True,
                ],
                marks=[pytest.mark.polarion_id("OCS-4845"), on_prem_platform_required],
            ),
        ],
    )
    def test_obc_creation_and_deletion(
        self, setup_ui_class, storageclass, bucketclass, delete_via, verify_ob_removal
    ):
        """
        Test creation and deletion of an OBC via the UI

        The test covers BZ #2097772 Introduce tooltips for contextual information
        The test covers BZ #2175685 RGW OBC creation via the UI is blocked by "Address form errors to proceed"
        """
        obc_name = create_unique_resource_name(
            resource_description="ui", resource_type="obc"
        )

        obc_ui_obj = ObcUI()

        if (
            config.DEPLOYMENT["external_mode"]
            and storageclass == "ocs-storagecluster-ceph-rgw"
        ):
            storageclass = "ocs-external-storagecluster-ceph-rgw"
        obc_ui_obj.create_obc_ui(obc_name, storageclass, bucketclass)

        assert obc_ui_obj.verify_current_page_resource_status(
            constants.STATUS_BOUND
        ), "Created OBC was not ready in time"

        test_obc = OCP(
            namespace=config.ENV_DATA["cluster_namespace"],
            kind="obc",
            resource_name=obc_name,
        )

        test_obc_obj = test_obc.get()

        obc_storageclass = test_obc_obj.get("spec").get("storageClassName")
        assert (
            obc_storageclass == storageclass
        ), f"StorageClass mismatch. Expected: {storageclass}, found: {obc_storageclass}"

        # no Bucket Classes available for ocs-storagecluster-ceph-rgw Storage Class
        if bucketclass:
            obc_bucketclass = (
                test_obc_obj.get("spec").get("additionalConfig").get("bucketclass")
            )
            assert (
                obc_bucketclass == bucketclass
            ), f"BucketClass mismatch. Expected: {bucketclass}, found: {obc_bucketclass}"

        # covers BZ 2097772
        if verify_ob_removal:
            ObUI().delete_object_bucket_ui(
                delete_via="three_dots", expect_fail=True, resource_name=obc_name
            )

        logger.info(f"Delete {obc_name}")
        obc_ui_obj.delete_obc_ui(obc_name, delete_via)

        assert test_obc.check_resource_existence(should_exist=False)
