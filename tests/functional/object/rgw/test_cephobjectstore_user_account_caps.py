"""
Test module for CephObjectStoreUser account admin
with account and userInfoWithoutKeys capabilities.
"""

import json
import logging

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    red_squad,
    rgw,
    skipif_external_mode,
    skipif_managed_service,
    skipif_mcg_only,
    skipif_ocs_version,
    tier2,
)
from ocs_ci.framework.testlib import ManageTest
from ocs_ci.helpers.helpers import create_unique_resource_name
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.resources.pod import get_ceph_tools_pod
from ocs_ci.utility import templating
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)

ACCOUNTS_CAP_TYPE = "accounts"
USER_INFO_WITHOUT_KEYS_CAP_TYPE = "user-info-without-keys"


@red_squad
@rgw
@tier2
@skipif_mcg_only
@skipif_managed_service
@skipif_external_mode
@skipif_ocs_version("<4.22")
# TODO: Add polarion ID
class TestCephObjectStoreUserAccountCaps(ManageTest):
    """
    Test for CephObjectStoreUser account admin account and userInfoWithoutKeys caps.
    """

    @pytest.fixture()
    def object_user_factory(self, request):
        """
        Create a CephObjectStoreUser and clean it up after the test.
        """
        created = []

        def factory(capabilities):
            """
            Args:
                capabilities (dict): Capability map for spec.capabilities

            Returns:
                OCS: Created CephObjectStoreUser object
            """
            object_user_data = templating.load_yaml(constants.CEPHOBJECTSTORE_USER_YAML)
            object_user_name = create_unique_resource_name(
                "test", "object-user-account-caps"
            )
            object_user_data["metadata"]["name"] = object_user_name
            object_user_data["metadata"]["namespace"] = config.ENV_DATA[
                "cluster_namespace"
            ]
            object_user_data["spec"]["displayName"] = object_user_name
            object_user_data["spec"]["store"] = constants.CEPHOBJECTSTORE_NAME
            object_user_data["spec"]["capabilities"] = capabilities

            object_user_obj = OCS(**object_user_data)
            created.append(object_user_obj)
            object_user_obj.create()
            return object_user_obj

        def teardown():
            for object_user_obj in created:
                try:
                    object_user_obj.delete()
                    logger.info(f"Deleted CephObjectStoreUser {object_user_obj.name}")
                except Exception as exc:
                    logger.warning(
                        f"Failed to delete CephObjectStoreUser {object_user_obj.name}: {exc}"
                    )

        request.addfinalizer(teardown)
        return factory

    def _wait_for_object_user_ready(self, object_user_obj, timeout=180):
        """
        Wait until CephObjectStoreUser reaches Ready phase.
        """
        logger.info(
            f"Waiting for CephObjectStoreUser {object_user_obj.name} to be Ready"
        )

        def _is_ready():
            object_user_obj.reload()
            phase = object_user_obj.data.get("status", {}).get("phase")
            logger.info(f"CephObjectStoreUser {object_user_obj.name} phase: {phase}")
            return phase == constants.STATUS_READY

        sample = TimeoutSampler(timeout=timeout, sleep=10, func=_is_ready)
        assert sample.wait_for_func_status(
            result=True
        ), f"CephObjectStoreUser {object_user_obj.name} did not reach Ready within {timeout}s"

    def _get_user_info(self, uid):
        """
        Fetch RGW user info via radosgw-admin.

        Args:
            uid (str): RGW user id (CephObjectStoreUser name)

        Returns:
            dict: Parsed user info JSON
        """
        store = constants.CEPHOBJECTSTORE_NAME
        toolbox = get_ceph_tools_pod()
        cmd = (
            f"radosgw-admin user info --uid={uid} "
            f"--rgw-realm={store} --rgw-zone={store} --rgw-zonegroup={store}"
        )
        output = toolbox.exec_cmd_on_pod(cmd, out_yaml_format=False)
        return json.loads(output) if isinstance(output, str) else output

    def _assert_caps(self, user_info, expected_caps):
        """
        Assert RGW user caps match expected type/perm pairs.

        Args:
            user_info (dict): Output of radosgw-admin user info
            expected_caps (dict): Mapping of cap type -> expected perm
        """
        caps = {cap["type"]: cap["perm"] for cap in user_info.get("caps", [])}
        logger.info(f"Observed RGW user caps: {caps}")
        for cap_type, expected_perm in expected_caps.items():
            logger.assertion(
                f"Expect cap {cap_type}={expected_perm}, observed={caps.get(cap_type)}"
            )
            assert (
                cap_type in caps
            ), f"Expected capability type '{cap_type}' not found in user caps: {caps}"
            assert caps[cap_type] == expected_perm, (
                f"Capability '{cap_type}' perm mismatch: "
                f"expected '{expected_perm}', got '{caps[cap_type]}'"
            )

    def test_cephobjectstore_user_account_admin_caps(self, object_user_factory):
        """
        Happy path: create and update CephObjectStoreUser with account admin caps.

        Steps:
        1. Create CephObjectStoreUser with accounts=* and userInfoWithoutKeys=read
        2. Wait for Ready phase
        3. Verify caps via radosgw-admin user info
        4. Update caps to accounts=write and userInfoWithoutKeys=write
        5. Verify updated caps via radosgw-admin user info
        """
        logger.test_step(
            "Create CephObjectStoreUser with accounts=* and userInfoWithoutKeys=read"
        )
        initial_capabilities = {
            "user": "*",
            "bucket": "*",
            "accounts": "*",
            "userInfoWithoutKeys": "read",
        }
        object_user_obj = object_user_factory(initial_capabilities)

        logger.test_step("Wait for CephObjectStoreUser to reach Ready")
        self._wait_for_object_user_ready(object_user_obj)

        logger.test_step("Verify initial account admin caps via radosgw-admin")
        user_info = self._get_user_info(object_user_obj.name)
        self._assert_caps(
            user_info,
            {
                ACCOUNTS_CAP_TYPE: "*",
                USER_INFO_WITHOUT_KEYS_CAP_TYPE: "read",
            },
        )

        logger.test_step(
            "Update CephObjectStoreUser caps to accounts=write and "
            "userInfoWithoutKeys=write"
        )
        updated_capabilities = {
            "user": "*",
            "bucket": "*",
            "accounts": "write",
            "userInfoWithoutKeys": "write",
        }
        object_user_obj.ocp.patch(
            resource_name=object_user_obj.name,
            params=json.dumps({"spec": {"capabilities": updated_capabilities}}),
            format_type="merge",
        )

        logger.test_step("Verify updated account admin caps via radosgw-admin")

        def _caps_updated():
            info = self._get_user_info(object_user_obj.name)
            caps = {cap["type"]: cap["perm"] for cap in info.get("caps", [])}
            return (
                caps.get(ACCOUNTS_CAP_TYPE) == "write"
                and caps.get(USER_INFO_WITHOUT_KEYS_CAP_TYPE) == "write"
            )

        sample = TimeoutSampler(timeout=180, sleep=10, func=_caps_updated)
        assert sample.wait_for_func_status(
            result=True
        ), "Updated account admin caps were not applied within timeout"

        user_info = self._get_user_info(object_user_obj.name)
        self._assert_caps(
            user_info,
            {
                ACCOUNTS_CAP_TYPE: "write",
                USER_INFO_WITHOUT_KEYS_CAP_TYPE: "write",
            },
        )

        logger.info(
            "Successfully validated CephObjectStoreUser account admin caps happy path"
        )
