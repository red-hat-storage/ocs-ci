import json
import logging
from uuid import uuid4

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    red_squad,
    polarion_id,
    bugzilla,
    tier1,
    tier2,
    pre_upgrade,
    post_upgrade,
    skipif_aws_creds_are_missing,
    ignore_leftovers,
    mcg,
    skipif_disconnected_cluster,
    skipif_proxy_cluster,
)
from ocs_ci.framework.testlib import MCGTest
from ocs_ci.ocs import constants
from ocs_ci.ocs.bucket_utils import (
    get_nb_bucket_stores,
    write_random_test_objects_to_bucket,
    compare_bucket_object_list,
    patch_replication_policy_to_bucketclass,
)
from ocs_ci.ocs.ocp import OCP

logger = logging.getLogger(__name__)


@mcg
@red_squad
@ignore_leftovers  # needed for the upgrade TCs
@skipif_disconnected_cluster
@skipif_proxy_cluster
class TestDefaultBackingstoreOverride(MCGTest):
    """
    Test overriding the default noobaa backingstore

    """

    @tier1
    @polarion_id("OCS-5193")
    def test_default_buckets_backingstore(
        self,
        mcg_obj_session,
        override_default_backingstore,
        bucket_factory,
    ):
        """
        1. Override the default noobaa backingstore
        2. Create a new bucket using the mcg-cli with the default config
        3. Create a new OBC using oc and yamls without specifying the bucketclass
        4. Verify the buckets' backingstore is the new default backingstore

        """

        # 1. Override the default noobaa backingstore
        alt_default_bs_name = override_default_backingstore()

        # 2. Create a new bucket using the mcg-cli with the default backingstore
        default_cli_bucket = bucket_factory(amount=1, interface="cli")[0]

        # 3. Create a new OBC using oc and yamls without specifying the bucketclass
        default_obc_bucket = bucket_factory(amount=1, interface="oc")[0]

        # 4. Verify the bucket's backingstore is the new default backingstore
        assert (
            get_nb_bucket_stores(mcg_obj_session, default_cli_bucket.name)[0]
            == alt_default_bs_name
        ), "The default mcg-cli bucket does not use the new default backingstore!"
        assert (
            get_nb_bucket_stores(mcg_obj_session, default_obc_bucket.name)[0]
            == alt_default_bs_name
        ), "The default OC bucket does not use the new default backingstore!"

    @pre_upgrade
    def test_default_backingstore_override_pre_upgrade(
        self,
        request,
        mcg_obj_session,
        override_default_backingstore_session,
    ):
        """
        1. Override the current default using the new backingstore of the same type
        2. Verify the new default is set before the upgrade

        """
        # 1. Override the current default using the new backingstore of the same type
        alt_default_bs_name = override_default_backingstore_session()
        # Cache the new default backingstore name to pass to the post-upgrade test
        request.config.cache.set("pre_upgrade_alt_bs_name", alt_default_bs_name)

        # 2. Verify the new default is set before the upgrade
        default_admin_resource = mcg_obj_session.get_admin_default_resource_name()
        default_bc_bs = mcg_obj_session.get_default_bc_backingstore_name()
        assert (
            default_admin_resource == default_bc_bs == alt_default_bs_name
        ), "The new default backingstore was not overriden before the upgrade!"

    @post_upgrade
    @polarion_id("OCS-5194")
    def test_default_backingstore_override_post_upgrade(
        self,
        request,
        mcg_obj_session,
    ):
        """
        Verify the new default is still set post-upgrade

        """
        # Retrieve the new default backingstore name from the pre-upgrade test
        alt_default_bs_name = request.config.cache.get("pre_upgrade_alt_bs_name", None)

        # Verify the new default is still set post-upgrade
        default_admin_resource = mcg_obj_session.get_admin_default_resource_name()
        default_bc_backingstore = mcg_obj_session.get_default_bc_backingstore_name()
        assert (
            default_admin_resource == default_bc_backingstore == alt_default_bs_name
        ), "The new default backingstore was not preserved after the upgrade!"

    @pytest.fixture()
    def nb_default_bc_cleanup_fixture(self, request):
        """
        Clear all replication policies from the default noobaa bucketclass

        """

        def clear_replication_policies_from_nb_default_bucketclass():
            replication_policy_patch_dict = {"spec": {"replicationPolicy": None}}

            OCP(
                kind="bucketclass",
                namespace=config.ENV_DATA["cluster_namespace"],
                resource_name=constants.DEFAULT_NOOBAA_BUCKETCLASS,
            ).patch(
                params=json.dumps(replication_policy_patch_dict), format_type="merge"
            )

        request.addfinalizer(clear_replication_policies_from_nb_default_bucketclass)

    @tier2
    @skipif_aws_creds_are_missing
    @polarion_id("OCS-5195")
    @bugzilla("2237427")
    def test_bucketclass_replication_after_default_backingstore_override(
        self,
        mcg_obj_session,
        bucket_factory,
        override_default_backingstore,
        awscli_pod_session,
        test_directory_setup,
        nb_default_bc_cleanup_fixture,
    ):
        """
        1. Create a target bucket
        2. Set a bucketclass replication policy to the target bucket on the default bucket class
        3. Override the default noobaa backingstore
        4. Create a source OBC under the default bucketclass
        5. Upload objects to the source bucket and verify they are replicated to the target bucket

        """
        # 1. Create a target bucket
        target_bucketclass_dict = {
            "interface": "OC",
            "backingstore_dict": {"aws": [(1, None)]},
        }
        target_bucket = bucket_factory(bucketclass=target_bucketclass_dict)[0]

        # 2. Set a bucketclass replication policy to the target bucket on the default bucket class
        patch_replication_policy_to_bucketclass(
            bucketclass_name=constants.DEFAULT_NOOBAA_BUCKETCLASS,
            rule_id=uuid4().hex,
            destination_bucket_name=target_bucket.name,
        )

        # 3. Override the default noobaa backingstore
        override_default_backingstore()

        # 4. Create a source OBC using the new default backingstore
        source_bucket = bucket_factory(interface="OC")[0]

        # 5. Upload objects to the source bucket and verify they are replicated to the target bucket
        write_random_test_objects_to_bucket(
            awscli_pod_session,
            source_bucket.name,
            test_directory_setup.origin_dir,
            amount=5,
            mcg_obj=mcg_obj_session,
        )
        assert compare_bucket_object_list(
            mcg_obj_session,
            source_bucket.name,
            target_bucket.name,
        ), f"Objects in {source_bucket.name} and {target_bucket.name} dont match"
