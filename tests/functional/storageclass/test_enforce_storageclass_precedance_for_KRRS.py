import logging

from ocs_ci.ocs import constants
from ocs_ci.ocs.constants import (
    RECLAIMSPACE_SCHEDULE_ANNOTATION,
    KEYROTATION_SCHEDULE_ANNOTATION,
)
from ocs_ci.framework.pytest_customization.marks import (
    green_squad,
    polarion_id,
    tier1,
    kms_config_required,
)

logger = logging.getLogger(__name__)

# Constants imported from conftest.py
DEFAULT_PVC_SIZE_GIB = 5
WEEKLY_SCHEDULE = "@weekly"
DAILY_SCHEDULE = "@daily"
STORAGECLASS_PRECEDENCE = "storageclass"
PVC_PRECEDENCE = "pvc"


@green_squad
class TestEnforceStorageclassPrecedenceForReclaimSpace:
    """
    Tests enforcement of storage class precedence for ReclaimSpace operations.
    Creates a matrix of RBD PVCs and pods:
      - RBD (Filesystem): RWO
      - RBD (Block): RWO
    """

    @tier1
    @polarion_id("OCS-6933")
    def test_storageclass_precedence_for_reclaimspace(
        self,
        storageclass_factory,
        multi_pvc_factory,
        pod_factory,
        reclaimspace_precedence_helper,
    ):
        """
        Validates storage class precedence for ReclaimSpace operations.

        Steps:
        1. Create an RBD StorageClass
        2. Create RBD PVCs (Filesystem and Block) and attach pods
        3. Ensure all pods are in Running state
        4. Annotate StorageClass(@weekly) and PVCs(@daily) with reclaim-space schedules
        5. Verify StorageClass precedence in ReclaimSpaceCronJobs is by default
        6. Switch precedence to PVC and verify behavior
        7. Revert precedence to StorageClass and verify behavior
        """
        logger.test_step("Create RBD StorageClass for ReclaimSpace precedence test")
        sc_rbd = storageclass_factory(interface=constants.CEPHBLOCKPOOL)
        logger.info(f"Created StorageClass: {sc_rbd.name}")

        logger.test_step("Create RBD PVCs (Filesystem and Block) and attach pods")
        reclaimspace_precedence_helper._prepare_pvcs_and_workloads(
            multi_pvc_factory=multi_pvc_factory,
            pod_factory=pod_factory,
            sc_rbd=sc_rbd,
            size_gib=DEFAULT_PVC_SIZE_GIB,
        )
        logger.info("RBD workloads and PVCs created successfully")

        logger.test_step("Ensure all pods are in Running state")
        reclaimspace_precedence_helper._ensure_pods_running()
        logger.info("All pods are running successfully")

        logger.test_step(
            "Annotate StorageClass with @weekly and PVCs with @daily reclaim-space schedules"
        )
        reclaimspace_precedence_helper._annotate_storageclass(
            sc_rbd, RECLAIMSPACE_SCHEDULE_ANNOTATION, WEEKLY_SCHEDULE
        )
        reclaimspace_precedence_helper._annotate_pvcs(
            reclaimspace_precedence_helper.rbd_blk_pvcs,
            RECLAIMSPACE_SCHEDULE_ANNOTATION,
            DAILY_SCHEDULE,
        )
        logger.info("Annotations applied to StorageClass and PVCs")

        logger.test_step(
            "Verify StorageClass precedence (default) in ReclaimSpaceCronJobs"
        )
        reclaimspace_precedence_helper._ensure_precedence_setting(
            STORAGECLASS_PRECEDENCE
        )
        reclaimspace_precedence_helper._verify_precedence_behavior(
            sc_rbd, RECLAIMSPACE_SCHEDULE_ANNOTATION, STORAGECLASS_PRECEDENCE
        )
        logger.info("StorageClass precedence verified successfully")

        logger.test_step("Switch precedence to PVC and verify ReclaimSpace behavior")
        from ocs_ci.helpers.helpers import set_schedule_precedence

        set_schedule_precedence(PVC_PRECEDENCE)
        reclaimspace_precedence_helper._verify_precedence_behavior(
            sc_rbd, RECLAIMSPACE_SCHEDULE_ANNOTATION, PVC_PRECEDENCE
        )
        logger.info("PVC precedence verified successfully")

        logger.test_step(
            "Revert precedence to StorageClass and verify ReclaimSpace behavior"
        )
        set_schedule_precedence(STORAGECLASS_PRECEDENCE)
        reclaimspace_precedence_helper._verify_precedence_behavior(
            sc_rbd, RECLAIMSPACE_SCHEDULE_ANNOTATION, STORAGECLASS_PRECEDENCE
        )
        logger.info("StorageClass precedence revert verified successfully")

        logger.info("All ReclaimSpace precedence tests completed successfully")


@green_squad
@kms_config_required
class TestEnforceStorageclassPrecedenceForKeyRotation:
    """
    Tests enforcement of storage class precedence for KeyRotation operations.
    Creates a matrix of RBD PVCs and pods:
      - RBD (Filesystem): RWO
      - RBD (Block): RWO
    """

    @tier1
    @polarion_id("OCS-6934")
    def test_storageclass_precedence_for_keyrotation(
        self,
        project_factory,
        pv_encryption_kms_setup_factory,
        storageclass_factory,
        multi_pvc_factory,
        pod_factory,
        keyrotation_precedence_helper,
    ):
        """
        Validates storage class precedence for KeyRotation operations.

        Steps:
        1. Create an RBD encrypted StorageClass
        2. Create encrypted PVC and attach pods
        3. Ensure all pods are in Running state
        4. Annotate StorageClass(@weekly) and PVCs(@daily) with keyrotation schedules
        5. Verify StorageClass precedence in KeyrotationCronjob is by default.
        6. Switch precedence to PVC and verify KeyrotationCronjob following PVC schedule
        7. Revert precedence to StorageClass and verify behavior
        """
        logger.test_step("Set up encryption for KeyRotation precedence test")
        proj_obj = project_factory()
        vault = pv_encryption_kms_setup_factory("v1", False)
        logger.info(f"Created project: {proj_obj.namespace}")
        logger.info(f"Setup KMS vault: {vault.kmsid}")

        logger.test_step("Create encrypted RBD StorageClass")
        sc_rbd = storageclass_factory(
            interface=constants.CEPHBLOCKPOOL,
            encrypted=True,
            encryption_kms_id=vault.kmsid,
        )
        logger.info(f"Created encrypted StorageClass: {sc_rbd.name}")

        vault.vault_path_token = vault.generate_vault_token()
        vault.create_vault_csi_kms_token(namespace=proj_obj.namespace)
        logger.info("KMS token created successfully")

        logger.test_step("Create encrypted PVCs and attach pods")
        keyrotation_precedence_helper._prepare_pvcs_and_workloads(
            multi_pvc_factory=multi_pvc_factory,
            pod_factory=pod_factory,
            sc_rbd=sc_rbd,
            size_gib=DEFAULT_PVC_SIZE_GIB,
            proj_obj=proj_obj,
        )
        logger.info("Encrypted RBD workloads and PVCs created successfully")

        logger.test_step("Ensure all pods are in Running state")
        keyrotation_precedence_helper._ensure_pods_running()
        logger.info("All pods are running successfully")

        logger.test_step(
            "Annotate StorageClass with @weekly and PVCs with @daily keyrotation schedules"
        )
        keyrotation_precedence_helper._annotate_storageclass(
            sc_rbd, KEYROTATION_SCHEDULE_ANNOTATION, WEEKLY_SCHEDULE
        )
        keyrotation_precedence_helper._annotate_pvcs(
            keyrotation_precedence_helper.rbd_blk_pvcs,
            KEYROTATION_SCHEDULE_ANNOTATION,
            DAILY_SCHEDULE,
        )
        logger.info("Annotations applied to StorageClass and PVCs")

        logger.test_step(
            "Verify StorageClass precedence (default) in KeyRotationCronJobs"
        )
        keyrotation_precedence_helper._ensure_precedence_setting(
            STORAGECLASS_PRECEDENCE
        )
        keyrotation_precedence_helper._verify_precedence_behavior(
            sc_rbd, KEYROTATION_SCHEDULE_ANNOTATION, STORAGECLASS_PRECEDENCE
        )
        logger.info("StorageClass precedence verified successfully")

        logger.test_step("Switch precedence to PVC and verify KeyRotation behavior")
        from ocs_ci.helpers.helpers import set_schedule_precedence

        set_schedule_precedence(PVC_PRECEDENCE)
        keyrotation_precedence_helper._verify_precedence_behavior(
            sc_rbd, KEYROTATION_SCHEDULE_ANNOTATION, PVC_PRECEDENCE
        )
        logger.info("PVC precedence verified successfully")

        logger.test_step(
            "Revert precedence to StorageClass and verify KeyRotation behavior"
        )
        set_schedule_precedence(STORAGECLASS_PRECEDENCE)
        keyrotation_precedence_helper._verify_precedence_behavior(
            sc_rbd, KEYROTATION_SCHEDULE_ANNOTATION, STORAGECLASS_PRECEDENCE
        )
        logger.info("StorageClass precedence revert verified successfully")

        logger.info("All KeyRotation precedence tests completed successfully")
