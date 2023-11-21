import json
import logging

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    red_squad,
    tier1,
    tier2,
    pre_upgrade,
    post_upgrade,
    skipif_mcg_only,
    polarion_id,
)
from ocs_ci.framework.testlib import MCGTest
from ocs_ci.ocs import constants
from ocs_ci.ocs.bucket_utils import get_nb_bucket_stores
from ocs_ci.ocs.ocp import OCP

logger = logging.getLogger(__name__)


@pytest.fixture(scope="class")
def allow_default_backingstore_override(request):
    """
    Modify the noobaa CR to allow overriding the default backingstore

    """

    nb_ocp_obj = OCP(
        kind="noobaa",
        namespace=config.ENV_DATA["cluster_namespace"],
        resource_name="noobaa",
    )

    def patch_allow_manual_default_backingstore():
        """
        Patch "manualDefaultBackingStore: true" to the noobaa CR

        """
        add_op = [
            {"op": "add", "path": "/spec/manualDefaultBackingStore", "value": True}
        ]
        nb_ocp_obj.patch(
            resource_name=constants.NOOBAA_RESOURCE_NAME,
            params=json.dumps(add_op),
            format_type="json",
        )

    def finalizer():
        """
        Remove "manualDefaultBackingStore: true" from the noobaa CR

        """
        remove_op = [
            {
                "op": "remove",
                "path": "/spec/manualDefaultBackingStore",
            }
        ]
        nb_ocp_obj.patch(
            resource_name=constants.NOOBAA_RESOURCE_NAME,
            params=json.dumps(remove_op),
            format_type="json",
        )

    request.addfinalizer(finalizer)
    patch_allow_manual_default_backingstore()


def get_admin_default_resource_name(mcg_obj):
    """
    Get the default resource name of the admin account

    Args:
        mcg_obj (MCG): An MCG object

    Returns:
        str: The default resource name

    """

    read_account_output = mcg_obj.send_rpc_query(
        "account_api",
        "read_account",
        params={
            "email": mcg_obj.noobaa_user,
        },
    )
    return read_account_output.json()["reply"]["default_resource"]


def get_default_bc_backingstore_name(mcg_obj):
    """
    Get the default backingstore name of the default bucketclass

    Args:
        mcg_obj (MCG): An MCG object

    Returns:
        str: The default backingstore name

    """
    bucketclass_ocp_obj = OCP(
        kind=constants.BUCKETCLASS,
        namespace=config.ENV_DATA["cluster_namespace"],
        resource_name=constants.DEFAULT_NOOBAA_BUCKETCLASS,
    )
    return (
        bucketclass_ocp_obj.get()
        .get("spec")
        .get("placementPolicy")
        .get("tiers")[0]
        .get("backingStores")[0]
    )


@red_squad
@pytest.mark.usefixtures(allow_default_backingstore_override.__name__)
class TestDefaultBackingstoreOverride(MCGTest):
    """
    Test overriding the default noobaa backingstore

    """

    @pytest.fixture(scope="session")
    def override_nb_default_backingstore_session(self, request, mcg_obj_session):
        return self.override_nb_default_backingstore(request, mcg_obj_session)

    @pytest.fixture(scope="function")
    def override_nb_default_backingstore_function(self, request, mcg_obj_session):
        return self.override_nb_default_backingstore(request, mcg_obj_session)

    def override_nb_default_backingstore(self, request, mcg_obj_session):
        """
        Override the default noobaa backingstore to the given alternative backingstore

        """

        bucketclass_ocp_obj = OCP(
            kind=constants.BUCKETCLASS,
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name=constants.DEFAULT_NOOBAA_BUCKETCLASS,
        )

        def override_nb_default_backingstore_implementation(
            mcg_obj, alternative_backingstore_name
        ):
            """
            1. Update the new default resource of the admin account
            2. Patch the default bucketclass to use the new default backingstore

            Args:
                mcg_obj (MCG): An MCG object
                alternative_backingstore_name (str): The name of the alternative backingstore

            """

            # Update the new default resource of the admin account
            mcg_obj.exec_mcg_cmd(
                "".join(
                    (
                        f"account update {mcg_obj.noobaa_user} ",
                        f"--new_default_resource={alternative_backingstore_name}",
                    )
                )
            )

            # Patch the default bucketclass to use the new default backingstore
            update_op = [
                {
                    "op": "replace",
                    "path": "/spec/placementPolicy/tiers/0/backingStores/0",
                    "value": alternative_backingstore_name,
                }
            ]
            bucketclass_ocp_obj.patch(
                resource_name=constants.DEFAULT_NOOBAA_BUCKETCLASS,
                params=json.dumps(update_op),
                format_type="json",
            )

        def finalizer():
            """
            Change the default backingstore back to the original

            """
            override_nb_default_backingstore_implementation(
                mcg_obj_session, constants.DEFAULT_NOOBAA_BACKINGSTORE
            )

        request.addfinalizer(finalizer)
        return override_nb_default_backingstore_implementation

    @tier1
    @polarion_id("OCS-5193")
    def test_default_buckets_backingstore(
        self,
        mcg_obj_session,
        backingstore_factory,
        bucket_factory,
        override_nb_default_backingstore_function,
    ):
        """
        1. Override the default noobaa backingstore
        2. Create a new bucket using the mcg-cli with the default config
        3. Create a new OBC using oc and yamls without specifying the bucketclass
        4. Verify the buckets' backingstore is the new default backingstore

        """

        # 1. Override the default noobaa backingstore
        if config.ENV_DATA["mcg_only_deployment"]:
            uls_dict = {"aws": [(1, "eu-central-1")]}
        else:
            # Supported in all deployment types except mcg-only
            uls_dict = {"pv": [(1, 20, constants.DEFAULT_STORAGECLASS_RBD)]}
        alternative_backingstore = backingstore_factory("oc", uls_dict)[0]
        override_nb_default_backingstore_function(
            mcg_obj_session, alternative_backingstore.name
        )

        # 2. Create a new bucket using the mcg-cli with the default backingstore
        default_cli_bucket = bucket_factory(amount=1, interface="cli")[0]

        # 3. Create a new OBC using oc and yamls without specifying the bucketclass
        default_obc_bucket = bucket_factory(amount=1, interface="oc")[0]

        # 4. Verify the bucket's backingstore is the new default backingstore
        assert (
            get_nb_bucket_stores(mcg_obj_session, default_cli_bucket.name)[0]
            == alternative_backingstore.name
        ), "The default mcg-cli bucket does not use the new default backingstore!"
        assert (
            get_nb_bucket_stores(mcg_obj_session, default_obc_bucket.name)[0]
            == alternative_backingstore.name
        ), "The default OC bucket does not use the new default backingstore!"

    @pytest.fixture(scope="session")
    def alt_bs_for_upgrade_tc(self, mcg_obj_session, backingstore_factory_session):
        """
        Create a new backingstore with the same type of the current default.

        Returns:
            str: The name of the alternative backingstore

        """
        # Create the alternative backingstore that will be used as the new default.
        original_bs_type = OCP(
            kind="backingstore",
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name=constants.DEFAULT_NOOBAA_BACKINGSTORE,
        ).data["spec"]["type"]
        bs_type_to_platform_mapping = {
            constants.BACKINGSTORE_TYPE_AWS: "aws",
            constants.BACKINGSTORE_TYPE_AZURE: "azure",
            constants.BACKINGSTORE_TYPE_GOOGLE: "gcp",
            constants.BACKINGSTORE_TYPE_PV_POOL: "pv",
            constants.BACKINGSTORE_TYPE_S3_COMP: "rgw",
        }
        original_bs_platform_name = bs_type_to_platform_mapping[original_bs_type]
        alternative_backingstore = backingstore_factory_session(
            "oc",
            {original_bs_platform_name: [(1, None)]}
            if original_bs_platform_name != "pv"
            else {"pv": [(1, 20, constants.DEFAULT_STORAGECLASS_RBD)]},
        )[0]
        return alternative_backingstore.name

    @pre_upgrade
    @skipif_mcg_only  # We can't create a bs with the same type of the current default in mcg-only
    def test_default_backingstore_override_pre_upgrade(
        self,
        mcg_obj_session,
        alt_bs_for_upgrade_tc,
        override_nb_default_backingstore_session,
    ):
        """
        1. Create a new backingstore with the same type of the current default
            - We're using the same type of the current default to avoid affecting subsequent tests
            - This step is done in the alt_bs_for_upgrade_tc fixture above to avoid leftover erors
        2. Override the current default using the new backingstore
        3. Verify the new default is set before the upgrade

        """
        # Create a new backingstore with the same type of the current default (implemented in fixture)
        alt_bs = alt_bs_for_upgrade_tc

        # Override the default noobaa backingstore
        override_nb_default_backingstore_session(mcg_obj_session, alt_bs)

        # Verify the new default is set before the upgrade
        default_admin_resource = get_admin_default_resource_name(mcg_obj_session)
        default_bc_backingstore = get_default_bc_backingstore_name(mcg_obj_session)
        assert (
            default_admin_resource == default_bc_backingstore == alt_bs
        ), "The new default backingstore was not overriden before the upgrade!"

    @post_upgrade
    @polarion_id("OCS-5194")
    def test_default_backingstore_override_post_upgrade(
        self,
        mcg_obj_session,
        alt_bs_for_upgrade_tc,
    ):
        """
        Verify the new default is still set post-upgrade

        """
        default_admin_resource = get_admin_default_resource_name(mcg_obj_session)
        default_bc_backingstore = get_default_bc_backingstore_name(mcg_obj_session)
        assert (
            default_admin_resource == default_bc_backingstore == alt_bs_for_upgrade_tc
        ), "The new default backingstore was not preserved after the upgrade!"

    @tier2
    @polarion_id("OCS-5195")
    def test_bucketclass_replication_after_default_backingstore_override(
        self, override_nb_default_backingstore
    ):
        pass
