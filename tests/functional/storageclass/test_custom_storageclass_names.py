import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import (
    green_squad,
    tier1,
    tier3,
    skipif_external_mode,
    skipif_ms_provider_and_consumer,
)
from ocs_ci.framework.testlib import skipif_ocs_version
from ocs_ci.framework import config
from ocs_ci.utility.utils import run_cmd
from ocs_ci.ocs.resources.storage_cluster import (
    patch_storage_cluster_for_custom_storage_class,
    check_custom_storageclass_presence,
)
from ocs_ci.ocs.constants import (
    OCS_COMPONENTS_MAP,
)
from fauxfactory import gen_alpha, gen_special
from ocs_ci.framework.testlib import on_prem_platform_required

logger = logging.getLogger(__name__)


@green_squad
@skipif_external_mode
@skipif_ocs_version("<4.14")
@skipif_ms_provider_and_consumer
class TestCustomStorageClassNames:
    def setup(self):
        self.custom_sc_list = []

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        """
        Fixture to restore custom storage class names after testing.
        """

        def cleanup_resources():
            for sc in self.custom_sc_list:
                run_cmd(f"oc delete sc {sc}", ignore_error=True)

        request.addfinalizer(cleanup_resources)

        def restore_custom_storage_class_names():
            if config.ENV_DATA.get("custom_default_storageclass_names"):
                for sc_type, sc_name in config.ENV_DATA.get(
                    "storageclassnames", {}
                ).items():
                    try:
                        patch_storage_cluster_for_custom_storage_class(
                            sc_type, storage_class_name=sc_name
                        )
                    except Exception as e:
                        logger.warning(f"Patch failed during teardown: {e}")

        request.addfinalizer(restore_custom_storage_class_names)

    @pytest.mark.polarion_id("OCS-5148")
    @pytest.mark.parametrize(
        argnames="interface",
        argvalues=[
            pytest.param(
                *[OCS_COMPONENTS_MAP["cephfs"]],
            ),
            pytest.param(
                *[OCS_COMPONENTS_MAP["blockpools"]],
            ),
            pytest.param(
                *[OCS_COMPONENTS_MAP["rgw"]],
                marks=on_prem_platform_required,
            ),
        ],
    )
    @tier1
    def test_custom_storageclass_post_deployment(self, interface):
        """
        Test custom storage class creation post deployment.

        Steps:
            1. Ensure Storagecluster is in ready state.
            2. Edit the storage cluster spec and add custom storage class names.
            3. Verify that the storage class has been created as per the updates mentioned in the spec.
            4. Remove the custom storage class names from the storage cluster spec.
            5. Delete the storage class mentioned in the storage cluster spec.

        """

        random_sc_name = f"custom-{interface}-{gen_alpha()}".lower()
        logger.test_step(
            f"Add custom StorageClass '{random_sc_name}' of type '{interface}'"
        )
        logger.info(
            f"Adding custom storageclass '{random_sc_name}' of type '{interface}' in storagecluster spec"
        )
        patch_result = patch_storage_cluster_for_custom_storage_class(
            interface, storage_class_name=random_sc_name
        )
        logger.assertion(
            f"Patch StorageCluster for custom SC '{random_sc_name}': "
            f"expected='True', actual='{patch_result}'"
        )
        assert patch_result, (
            f"Failed to add custom storageclass '{random_sc_name}' of type "
            f"'{interface}' in storagecluster spec."
        )
        self.custom_sc_list.append(random_sc_name)

        logger.test_step("Verify custom StorageClass presence")
        sc_presence = check_custom_storageclass_presence()
        logger.assertion(
            f"Custom StorageClass presence check: expected='True', "
            f"actual='{sc_presence}'"
        )
        assert sc_presence, "Error validating the created storage classes."

    @pytest.mark.polarion_id("OCS-5149")
    @pytest.mark.parametrize(
        "sc_name, str_length, expect_to_pass",
        [
            ("alpha", 253, True),
            ("alpha", 254, False),
            ("special", 10, False),
        ],
    )
    @tier3
    def test_custom_storageclass_names_character_limit(
        self, sc_name, str_length, expect_to_pass
    ):
        """
        Test custom storage class name with different characters and length limits."

        Steps:
            1. Verify that storagecluster is in 'Ready' state.
            2. Update storage class names in the storage cluster spec with the following conditions :
                a. Keep the storage class name length as 253 characters and verify
                that the storage class is being created successfully.
                b. Keep the storage class name > 253 characters and verify that it fails.
                c. Use special characters in the name and verify that the it fails.
            3. Remove the storage class names from the storage cluster spec.
            4. Delete the storage class mentioned in the storage cluster spec.
        """
        self.custom_sc_list = []
        if sc_name == "alpha":
            sc_custom_name = gen_alpha(str_length).lower()
        elif sc_name == "special":
            sc_custom_name = f"{gen_special(str_length).lower()}".replace(
                '"', ""
            ).replace("'", "")

        logger.test_step(
            f"Create custom StorageClass with '{sc_name}' characters "
            f"(length={str_length}, expect_to_pass={expect_to_pass})"
        )
        logger.info(
            f"Creating Custom Storageclass Name using '{sc_name}' "
            f"characters and length={str_length}: StorageclassName: {sc_custom_name}"
        )

        if expect_to_pass:
            logger.info("Testing with an expected passing scenario")
            assert patch_storage_cluster_for_custom_storage_class(
                OCS_COMPONENTS_MAP["cephfs"], storage_class_name=sc_custom_name
            )
            logger.info("Custom Storageclass created successfully")
            patch_storage_cluster_for_custom_storage_class(
                OCS_COMPONENTS_MAP["cephfs"], action="remove"
            )
            assert run_cmd(
                f"oc delete sc {sc_custom_name}"
            ), f"Failed to remove Storageclass {sc_custom_name}"
            logger.info(f"Custom Storageclass {sc_custom_name} removed successfully")
            self.custom_sc_list.append(sc_custom_name)
        else:
            logger.info("Testing with an expected failing scenario")
            assert not patch_storage_cluster_for_custom_storage_class(
                OCS_COMPONENTS_MAP["cephfs"], storage_class_name=sc_custom_name
            )
            logger.info("Custom Storageclass creation failed as expected")
