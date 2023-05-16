import logging
import pytest

from ocs_ci.framework import config
from ocs_ci.framework.testlib import MCGTest, tier1, tier3
from ocs_ci.framework.pytest_customization.marks import (
    skipif_mcg_only,
    skipif_ocs_version,
    ignore_leftover_label,
)
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs import constants
from ocs_ci.ocs.bucket_utils import random_object_round_trip_verification
from ocs_ci.ocs.exceptions import CommandFailed


from ocs_ci.ocs.resources.mcg_params import NSFS
from ocs_ci.utility.retry import retry

logger = logging.getLogger(__name__)


@skipif_mcg_only
@skipif_ocs_version("<4.10")
@ignore_leftover_label(constants.NOOBAA_ENDPOINT_POD_LABEL)
class TestNSFSObjectIntegrity(MCGTest):
    """
    Test the integrity of IO operations on NSFS buckets

    """

    @pytest.fixture(autouse=True, scope="class")
    def modify_endpoint_scc(self, request) -> None:
        """
        This fixture modifies the noobaa-endpoint SCC back to the way it was before ODF 4.12.
        See https://url.corp.redhat.com/5ceb453 for details.

        """

        ocp_scc = ocp.OCP(
            kind=constants.SCC, namespace=config.ENV_DATA["cluster_namespace"]
        )
        nb_endpoint_scc_name = constants.NOOBAA_ENDPOINT_SERVICE_ACCOUNT_NAME
        nb_endpoint_sa = constants.NOOBAA_ENDPOINT_SERVICE_ACCOUNT

        # Modify the noobaa-endpoint SCC back to the way it was in ODF 4.11
        json_payload = [
            {"op": "replace", "path": "/seLinuxContext/type", "value": "MustRunAs"},
            {"op": "add", "path": "/users/0", "value": f"{nb_endpoint_sa}"},
        ]

        ocp_scc.patch(
            resource_name=nb_endpoint_scc_name,
            params=json_payload,
            format_type="json",
        )

        # Verify the changes
        scc_dict = ocp_scc.get(resource_name=nb_endpoint_scc_name)
        assert (
            scc_dict["seLinuxContext"]["type"] == "MustRunAs"
        ), "Failed to modify the noobaa-db SCC seLinuxContext type"
        assert (
            constants.NOOBAA_ENDPOINT_SERVICE_ACCOUNT in scc_dict["users"]
        ), "The noobaa-endpoint SA wasn't added to the noobaa-endpoint SCC"

        def finalizer():
            """
            Restore the noobaa-db SCC back to default

            """

            # Restore the noobaa-endpoint SCC back to it's default state
            json_payload = [
                {"op": "replace", "path": "/seLinuxContext/type", "value": "RunAsAny"},
                {"op": "remove", "path": "/users/0", "value": f"{nb_endpoint_sa}"},
            ]

            ocp_scc.patch(
                resource_name=nb_endpoint_scc_name,
                params=json_payload,
                format_type="json",
            )

            # Verify the changes
            scc_dict = ocp_scc.get(resource_name=nb_endpoint_scc_name)
            assert (
                scc_dict["seLinuxContext"]["type"] == "RunAsAny"
            ), "Failed to return the noobaa-db SCC seLinuxContext type"
            assert (
                constants.NOOBAA_ENDPOINT_SERVICE_ACCOUNT not in scc_dict["users"]
            ), "Failed to remove the noobaa-endpoint SA from the noobaa-endpoint SCC"

        request.addfinalizer(finalizer)

    @pytest.mark.polarion_id("OCS-3735")
    @pytest.mark.parametrize(
        argnames="nsfs_obj",
        argvalues=[
            pytest.param(
                NSFS(
                    method="CLI",
                    pvc_size=25,
                ),
                marks=[tier1],
            ),
            pytest.param(
                NSFS(
                    method="OC",
                    pvc_size=20,
                    mount_existing_dir=True,
                ),
                marks=[tier1],
            ),
        ],
        ids=[
            "CLI-25Gi",
            "OC-20Gi-Export",
        ],
    )
    def test_nsfs_object_integrity(
        self, nsfs_bucket_factory, awscli_pod_session, test_directory_setup, nsfs_obj
    ):
        """
        Test NSFS object integrity -
        1. Write to the NSFS bucket
        2. Read the objects back
        3. Compare their checksums
        4. Also compare the checksums of the files that reside on the filesystem

        """
        nsfs_bucket_factory(nsfs_obj)
        retry(CommandFailed, tries=4, delay=10)(random_object_round_trip_verification)(
            io_pod=awscli_pod_session,
            bucket_name=nsfs_obj.bucket_name,
            upload_dir=test_directory_setup.origin_dir,
            download_dir=test_directory_setup.result_dir,
            amount=10,
            pattern="nsfs-test-obj-",
            s3_creds=nsfs_obj.s3_creds,
            result_pod=nsfs_obj.interface_pod,
            result_pod_path=nsfs_obj.mount_path + "/" + nsfs_obj.bucket_name,
        )

    @pytest.mark.polarion_id("OCS-3737")
    @pytest.mark.parametrize(
        argnames="nsfs_obj",
        argvalues=[
            pytest.param(
                NSFS(
                    method="CLI",
                    pvc_size=20,
                    mount_existing_dir=True,
                    existing_dir_mode=000,
                ),
                marks=[tier3],
            ),
        ],
        ids=[
            "CLI-20Gi",
        ],
    )
    def test_nsfs_object_integrity_with_wrong_permissions(
        self, nsfs_bucket_factory, awscli_pod_session, test_directory_setup, nsfs_obj
    ):
        """
        Test NSFS object integrity -
        1. Create an NSFS bucket on top of an existing directory with wrong permissions
        2. Verify that writing fails

        """
        nsfs_bucket_factory(nsfs_obj)
        try:
            retry(CommandFailed, tries=4, delay=10)(
                random_object_round_trip_verification
            )(
                io_pod=awscli_pod_session,
                bucket_name=nsfs_obj.bucket_name,
                upload_dir=test_directory_setup.origin_dir,
                download_dir=test_directory_setup.result_dir,
                amount=10,
                pattern="nsfs-test-obj-",
                s3_creds=nsfs_obj.s3_creds,
                result_pod=nsfs_obj.interface_pod,
                result_pod_path=nsfs_obj.mount_path + "/" + nsfs_obj.bucket_name,
            )
        except Exception as e:
            assert "AccessDenied" in str(
                e
            ), f"Test failed unexpectedly; Exception data: {str(e)}"
