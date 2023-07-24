import json
import logging
import pytest

from ocs_ci.framework import config
from ocs_ci.framework.testlib import MCGTest
from ocs_ci.ocs import constants
from ocs_ci.ocs.bucket_utils import delete_all_noobaa_buckets
from ocs_ci.ocs.ocp import OCP

logger = logging.getLogger(__name__)


class TestDefaultBackingstoreOverride(MCGTest):
    """
    Test overriding the default noobaa backingstore
    """

    @pytest.fixture(scope="function")
    def override_nb_default_backingstore_fixture(
        self, request, mcg_obj_session, backingstore_factory
    ):
        """ """

        nb_ocp_obj = OCP(
            kind="noobaa",
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name="noobaa",
        )

        bucketclass_ocp_obj = OCP(
            kind=constants.BUCKETCLASS,
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name=constants.DEFAULT_NOOBAA_BUCKETCLASS,
        )

        # Add manualDefaultBackingStore: true to the noobaa CR
        add_op = [
            {"op": "add", "path": "/spec/manualDefaultBackingStore", "value": True}
        ]
        nb_ocp_obj.patch(
            resource_name=constants.NOOBAA_RESOURCE_NAME,
            params=json.dumps(add_op),
            format_type="json",
        )

        def override_nb_default_backingstore_implementation(
            mcg_obj, alternative_backingstore_name
        ):
            """ """

            # Delete all the noobaa buckets
            delete_all_noobaa_buckets(mcg_obj, request)

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
            override_nb_default_backingstore_implementation(
                mcg_obj_session, constants.DEFAULT_NOOBAA_BACKINGSTORE
            )

            # Remove manualDefaultBackingStore: true to the noobaa CR
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
        return override_nb_default_backingstore_implementation

    def test_default_mcg_cli_buckets_use_new_backingstore(
        self,
        mcg_obj_session,
        backingstore_factory,
        bucket_factory,
        override_nb_default_backingstore_fixture,
    ):
        alternative_backingstore = backingstore_factory(
            *("oc", {"aws": [(1, "eu-central-1")]})
        )[0]
        override_nb_default_backingstore_fixture(
            mcg_obj_session, alternative_backingstore.name
        )

        default_cli_bucket = bucket_factory(amount=1, interface="cli")[0]
        # TODO assert that default_cli_bucket is using the new backingstore

        assert default_cli_bucket

    def test_default_obcs_use_new_backingstore(
        self, override_nb_default_backingstore_fixture
    ):
        pass

    def test_default_backingstore_override_post_upgrade(self):
        pass

    def test_bucketclass_replication_after_default_backingstore_override(
        self, override_nb_default_backingstore_fixture
    ):
        pass
