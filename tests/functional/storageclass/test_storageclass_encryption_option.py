import pytest
import logging
from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import (
    green_squad,
    skipif_ocs_version,
    tier1,
)

log = logging.getLogger(__name__)


class TestStorageClassEncryptionOptions:
    @green_squad
    @tier1
    @pytest.mark.bugzilla("2246388")
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

        # Create a storage class with encryption set to false
        log.info("Creating a storage class with encryption='false'.")
        sc_obj = storageclass_factory(encrypted=False)

        # Verify the storage class encryption option
        log.info("Verifying the storage class encryption option.")
        assert (
            sc_obj.data["parameters"]["encrypted"] == "false"
        ), f"Storageclass {sc_obj.name} does not have encrypted='false' option."

        # Create PVC
        log.info("Creating a PVC using the storage class.")
        pvc_obj = pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            project=proj_obj,
            storageclass=sc_obj,
            size=5,
            status=constants.STATUS_BOUND,
        )

        # Create a POD
        log.info("Creating a pod and attaching the PVC.")
        pod_obj = pod_factory(pvc=pvc_obj)

        # Verify the pod status
        log.info("Verifying the pod status.")
        assert (
            pod_obj.data["status"]["phase"] == constants.STATUS_RUNNING
        ), f"Pod {pod_obj.name} is not in {constants.STATUS_RUNNING} state."
