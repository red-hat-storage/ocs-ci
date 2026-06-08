import logging
import pytest
from ocs_ci.framework.testlib import config, tier2
from ocs_ci.ocs import constants
from ocs_ci.helpers.keyrotation_helper import PVKeyrotation
from ocs_ci.helpers.helpers import (
    create_pods,
    set_schedule_precedence,
    get_schedule_precedance_value_from_csi_addons_configmap,
)
from ocs_ci.framework.pytest_customization.marks import (
    green_squad,
    vault_kms_deployment_required,
)
from ocs_ci.framework.testlib import skipif_disconnected_cluster

logger = logging.getLogger(__name__)

# Constants
kmsprovider = constants.VAULT_KMS_PROVIDER

# Parametrize test cases based on environment
argnames = ["kv_version", "kms_provider", "use_vault_namespace"]
if config.ENV_DATA.get("vault_hcp"):
    argvalues = [
        pytest.param("v1", kmsprovider, True),
        pytest.param("v2", kmsprovider, True),
    ]
else:
    argvalues = [
        pytest.param("v1", kmsprovider, False),
        pytest.param("v2", kmsprovider, False),
    ]


class PVKeyrotationTestBase:
    """
    Base class to reuse common setup and utility methods for PV key rotation tests.
    """

    @pytest.fixture()
    def setup_common(
        self,
        kv_version,
        kms_provider,
        pv_encryption_kms_setup_factory,
        project_factory,
        storageclass_factory,
        multi_pvc_factory,
        pod_factory,
        use_vault_namespace,
    ):
        """
        Common setup for CSI-KMS connection details, storage class, and PVCs.
        """
        logger.test_step("Configure CSI-KMS connection details and resources")

        # Set up KMS configuration
        self.kms = pv_encryption_kms_setup_factory(kv_version, use_vault_namespace)
        logger.info("KMS setup successful.")

        # Create a project
        self.proj_obj = project_factory()
        logger.info(f"Project {self.proj_obj.namespace} created.")

        # Key rotation annotations
        keyrotation_annotations = {
            constants.KEYROTATION_SCHEDULE_ANNOTATION: "* * * * *"
        }

        # Create an encryption-enabled storage class
        self.sc_obj = storageclass_factory(
            interface=constants.CEPHBLOCKPOOL,
            encrypted=True,
            encryption_kms_id=self.kms.kmsid,
            annotations=keyrotation_annotations,
        )
        logger.info("Encryption-enabled storage class created.")

        # Create Vault CSI KMS token in tenant namespace
        self.kms.vault_path_token = self.kms.generate_vault_token()
        self.kms.create_vault_csi_kms_token(namespace=self.proj_obj.namespace)
        logger.info("Vault CSI KMS token created.")

        logger.test_step("Create encrypted PVCs and pods")
        self.pvc_objs = multi_pvc_factory(
            size=5,
            num_of_pvc=3,
            storageclass=self.sc_obj,
            access_modes=[
                f"{constants.ACCESS_MODE_RWX}-Block",
                f"{constants.ACCESS_MODE_RWO}-Block",
            ],
            wait_each=True,
            project=self.proj_obj,
        )
        logger.info(f"Created {len(self.pvc_objs)} PVCs successfully.")

        self.pod_objs = create_pods(
            self.pvc_objs,
            pod_factory,
            constants.CEPHBLOCKPOOL,
            pods_for_rwx=1,
            status=constants.STATUS_RUNNING,
        )
        logger.info(f"Created {len(self.pod_objs)} pods, all running.")

        # Initialize the PVKeyrotation helper
        self.pv_keyrotation_obj = PVKeyrotation(self.sc_obj)

        # Save original schedule precedence and set to 'pvc' for OCS 4.21+
        self.original_precedence = (
            get_schedule_precedance_value_from_csi_addons_configmap()
        )
        logger.debug(f"Original schedule precedence: {self.original_precedence}")

        if self.original_precedence != "pvc":
            logger.info("Setting schedule precedence to 'pvc' for test...")
            set_schedule_precedence("pvc")
            logger.info(
                "Schedule precedence set to 'pvc' and CSI Addons controller restarted."
            )

        yield

        # Teardown: Restore original schedule precedence
        current_precedence = get_schedule_precedance_value_from_csi_addons_configmap()
        if current_precedence != self.original_precedence:
            logger.info(
                f"Restoring schedule precedence to '{self.original_precedence}'..."
            )
            set_schedule_precedence(self.original_precedence)
            logger.info(
                f"Schedule precedence restored to '{self.original_precedence}'."
            )


@tier2
@green_squad
@pytest.mark.parametrize(
    argnames=argnames,
    argvalues=argvalues,
)
@vault_kms_deployment_required
@skipif_disconnected_cluster
class TestDisablePVKeyrotationOperation(PVKeyrotationTestBase):
    @pytest.mark.polarion_id("OCS-6323")
    def test_disable_pv_keyrotation_globally(self, setup_common):
        """
        Test disabling PV key rotation globally by annotating the storage class.

        Steps:
        1. Add annotation to the storage class to disable key rotation.
        2. Verify key rotation jobs are deleted.
        3. Remove the annotation from the storage class.
        4. Verify key rotation cronjobs are recreated.
        """
        logger.test_step("Disable key rotation globally via storage class annotation")
        self.pv_keyrotation_obj.set_keyrotation_state_by_annotation(False)
        logger.info("Key rotation disabled globally via storage class annotation.")

        logger.test_step("Verify key rotation cronjobs are deleted")
        logger.assertion("Key rotation cronjobs deletion: expected=True")
        assert self.pv_keyrotation_obj.wait_for_keyrotation_cronjobs_deletion(
            self.pvc_objs
        ), "Failed to delete key rotation cronjobs after disabling."
        logger.info("Verified key rotation cronjobs are removed.")

        logger.test_step("Re-enable key rotation globally via storage class annotation")
        self.pv_keyrotation_obj.set_keyrotation_state_by_annotation(True)
        logger.info("Key rotation re-enabled globally via storage class annotation.")

        logger.test_step(
            "Verify key rotation cronjobs are recreated and keys are rotated"
        )
        logger.assertion("Key rotation cronjobs recreation: expected=True")
        assert self.pv_keyrotation_obj.wait_for_keyrotation_cronjobs_recreation(
            self.pvc_objs
        ), "Failed to recreate key rotation cronjobs after re-enabling."
        logger.info("Key rotation cronjobs successfully recreated and active.")

        # Reset baseline to capture keys after re-enabling, before waiting for rotation
        self.pv_keyrotation_obj.reset_keyrotation_baseline()

        logger.assertion(
            "PV key rotation on Vault KMS after re-enabling: expected=True"
        )
        assert self.pv_keyrotation_obj.wait_till_all_pv_keyrotation_on_vault_kms(
            self.pvc_objs
        ), "Failed to re-enable PV key rotation."
        logger.info("Key rotation successfully re-enabled globally.")

    @pytest.mark.polarion_id("OCS-6324")
    def test_disable_pv_keyrotation_by_rbac_user(self, setup_common):
        """
        Test disabling specific PV key rotation by RBAC user permissions.

        Steps:
        1. Disable key rotation for specific PVCs.
        2. Verify key rotation cronjobs has state suspent = True.
        3. Re-enable key rotation for specific PVCs.
        4. Verify key rotation cronjobs are recreated.
        """
        logger.test_step("Disable key rotation for specific PVCs")
        self.pv_keyrotation_obj.change_pvc_keyrotation_cronjob_state(
            self.pvc_objs, disable=True
        )
        logger.info("Key rotation disabled for specific PVCs.")

        logger.test_step("Verify key rotation cronjobs are in suspended state")
        for pvc in self.pvc_objs:
            cron_obj = self.pv_keyrotation_obj.get_keyrotation_cronjob_for_pvc(pvc)
            logger.assertion(
                f"PVC {pvc.name} keyrotation cronjob suspended: "
                f"expected=True, actual={cron_obj.data['spec'].get('suspend', False)}"
            )
            assert cron_obj.data["spec"].get(
                "suspend", False
            ), "PVC keyrotation cronjob is not in 'suspend' state."

        logger.info("Keyrotation is disabled for all PVCs")

        logger.test_step("Re-enable key rotation for specific PVCs")
        self.pv_keyrotation_obj.change_pvc_keyrotation_cronjob_state(
            self.pvc_objs, disable=False
        )
        logger.info("Key rotation re-enabled for specific PVCs.")

        # Reset baseline to capture keys after re-enabling, before waiting for rotation
        self.pv_keyrotation_obj.reset_keyrotation_baseline()

        logger.test_step("Verify key rotation is functional after re-enabling")
        logger.assertion(
            "PV key rotation on Vault KMS after re-enabling for specific PVCs: expected=True"
        )
        assert self.pv_keyrotation_obj.wait_till_all_pv_keyrotation_on_vault_kms(
            self.pvc_objs
        ), "Failed to re-enable PV key rotation for specific PVCs."
        logger.info("Key rotation successfully re-enabled for specific PVCs.")
