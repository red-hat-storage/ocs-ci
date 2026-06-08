import pytest
import logging
from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import (
    green_squad,
    skipif_ocs_version,
    tier1,
)

logger = logging.getLogger(__name__)


class TestStorageClassEncryptionOptions:
    @green_squad
    @tier1
    @skipif_ocs_version("<4.13")
    @pytest.mark.polarion_id("OCS-5386")
    def test_storageclass_encryption_options(
        self,
        storageclass_factory,
        pvc_factory,
        pod_factory,
        project_factory,
    ):
        """
        StorageClass creation test with "encrypted='false'".
        Steps:

        1. Create a StorageClass with the option "encrypted='false'".
        2. Create a PVC that belongs to the previously created StorageClass.
        3. Verify that the StorageClass has the "encrypted='false'" option.
        4. Create a pod and attach the previously created PVC.
        5. Verify that the pod is in the 'Running' state.
        """

        # Create a project
        proj_obj = project_factory()

        logger.test_step("Create storage class with encryption='false'")
        sc_obj = storageclass_factory(encrypted=False)
        logger.info(f"Created storage class {sc_obj.name} with encryption=false")

        logger.test_step("Verify storage class has encrypted='false' parameter")
        encrypted_value = sc_obj.data["parameters"]["encrypted"]
        logger.assertion(
            f"Storageclass encrypted parameter: expected='false', actual='{encrypted_value}'"
        )
        assert (
            encrypted_value == "false"
        ), f"Storageclass {sc_obj.name} does not have encrypted='false' option."

        logger.test_step(f"Create PVC using storage class {sc_obj.name}")
        pvc_obj = pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            project=proj_obj,
            storageclass=sc_obj,
            size=5,
            status=constants.STATUS_BOUND,
        )

        logger.test_step("Create pod and attach the PVC")
        pod_obj = pod_factory(pvc=pvc_obj)

        logger.test_step("Verify pod reaches Running state")
        pod_phase = pod_obj.data["status"]["phase"]
        logger.assertion(
            f"Pod {pod_obj.name} phase: expected='{constants.STATUS_RUNNING}', actual='{pod_phase}'"
        )
        assert (
            pod_phase == constants.STATUS_RUNNING
        ), f"Pod {pod_obj.name} is not in {constants.STATUS_RUNNING} state."
