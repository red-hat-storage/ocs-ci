import logging

from ocs_ci.ocs import constants
from ocs_ci.helpers.helpers import (
    get_schedule_precedance_value_from_csi_addons_configmap,
    set_schedule_precedence,
    get_reclaimspacecronjob_for_pvc,
)
from ocs_ci.framework.pytest_customization.marks import (
    green_squad,
    polarion_id,
)

logger = logging.getLogger(__name__)


@green_squad
class TestEnforceStorageclassPrecedenceForReclaimSpace:
    """
    Tests enforcement of storage class precedence for ReclaimSpace operations.
    Creates a matrix of RBD PVCs and pods:
      - RBD (Filesystem): RWO
      - RBD (Block): RWO
    """

    def _ensure_pvcs_bound(self, pvcs, timeout=300):
        """Ensure all provided PVCs reach the Bound state."""
        for pvc in pvcs:
            assert pvc.ocp.wait_for_resource(
                condition=constants.STATUS_BOUND,
                resource_name=pvc.name,
                timeout=timeout,
            ), f"PVC {pvc.name} did not reach Bound state"

    def _prepare_pvcs_and_workloads(
        self, multi_pvc_factory, pod_factory, sc_rbd, size_gib=5
    ):
        """
        Create RBD PVCs (Filesystem and Block) and attach pods to them.

        Args:
            multi_pvc_factory: Factory to create multiple PVCs
            pod_factory: Factory to create pods
            sc_rbd: RBD StorageClass object
            size_gib (int): Size of PVCs in GiB (default: 5)
        """
        self.pod_objs = []

        # Create RBD Filesystem PVCs (RWO)
        rbd_fs_pvcs = multi_pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            access_modes=[constants.ACCESS_MODE_RWO],
            size=size_gib,
            num_of_pvc=2,
            storageclass=sc_rbd,
        )
        self._ensure_pvcs_bound(rbd_fs_pvcs)
        for pvc in rbd_fs_pvcs:
            self.pod_objs.append(
                pod_factory(
                    interface=constants.CEPHBLOCKPOOL,
                    pvc=pvc,
                    status=constants.STATUS_RUNNING,
                )
            )

        # Create RBD Block PVCs (RWO)
        self.rbd_blk_pvcs = multi_pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            access_modes=[f"{constants.ACCESS_MODE_RWO}-Block"],
            size=size_gib,
            num_of_pvc=2,
            storageclass=sc_rbd,
        )
        self._ensure_pvcs_bound(self.rbd_blk_pvcs)
        for pvc in self.rbd_blk_pvcs:
            self.pod_objs.append(
                pod_factory(
                    interface=constants.CEPHBLOCKPOOL,
                    pvc=pvc,
                    raw_block_pv=True,
                    status=constants.STATUS_RUNNING,
                )
            )

        logger.info(
            "Prepared pods: RBD-FS=%d, RBD-Block=%d (total pods=%d)",
            len(rbd_fs_pvcs),
            len(self.rbd_blk_pvcs),
            len(self.pod_objs),
        )

    @polarion_id("ODF-XXXX")
    def test_storageclass_precedence_for_reclaimspace(
        self, storageclass_factory, multi_pvc_factory, pod_factory
    ):
        """
        Validates storage class precedence for ReclaimSpace operations.

        Steps:
        1. Create an RBD StorageClass
        2. Create RBD PVCs (Filesystem and Block) and attach pods
        3. Ensure all pods are in Running state
        4. Annotate StorageClass(@weekly) and PVCs (@daily) with reclaim-space schedules
        5. Verify StorageClass precedence in ReclaimSpaceCronJobs is by default
        6. Switch precedence to PVC and verify behavior
        7. Revert precedence to StorageClass and verify behavior
        """
        # Create RBD StorageClass
        sc_rbd = storageclass_factory(interface=constants.CEPHBLOCKPOOL)

        # Prepare RBD PVCs and pods
        self._prepare_pvcs_and_workloads(
            multi_pvc_factory=multi_pvc_factory,
            pod_factory=pod_factory,
            sc_rbd=sc_rbd,
            size_gib=5,
        )
        logger.info("RBD workloads and PVCs created successfully.")

        if get_schedule_precedance_value_from_csi_addons_configmap() != "storageclass":
            set_schedule_precedence("storageclass")

        # Annotate StorageClass with weekly reclaim-space schedule
        sc_rbd.annotate("reclaimspace.csiaddons.openshift.io/schedule=@weekly")

        # Annotate PVCs with daily reclaim-space schedule
        for pvc_obj in self.rbd_blk_pvcs:
            pvc_obj.annotate("reclaimspace.csiaddons.openshift.io/schedule=@daily")
            pvc_obj.reload()

        # Verify all pods are Running
        for pod in self.pod_objs:
            assert pod.ocp.wait_for_resource(
                condition=constants.STATUS_RUNNING,
                resource_name=pod.name,
                timeout=300,
            ), f"Pod {pod.name} is not in Running state"
        logger.info("All pods are running before reclaim-space operation.")

        # Reload StorageClass to get updated annotations
        sc_rbd.reload()
        sc_schedule = sc_rbd.data["metadata"]["annotations"][
            "reclaimspace.csiaddons.openshift.io/schedule"
        ]
        logger.info(f"StorageClass reclaim-space schedule: {sc_schedule}")

        # Verify StorageClass precedence
        for pvc_obj in self.rbd_blk_pvcs:
            logger.info(
                f"RBD-Block PVC: {pvc_obj.name}, StorageClass: {pvc_obj.storageclass}"
            )
            pvc_obj.reload()
            pvc_schedule = pvc_obj.data["metadata"]["annotations"][
                "reclaimspace.csiaddons.openshift.io/schedule"
            ]
            logger.info(
                f"RBD-Block PVC: {pvc_obj.name}, reclaim-space schedule: {pvc_schedule}"
            )

            reclaimspace_cronjob = get_reclaimspacecronjob_for_pvc(pvc_obj)
            cronjob_schedule = reclaimspace_cronjob.data["spec"]["schedule"]
            assert sc_schedule == cronjob_schedule, (
                f"RBD-Block PVC {pvc_obj.name} ReclaimSpaceCronJob schedule "
                f"'{cronjob_schedule}' does not match StorageClass schedule '{sc_schedule}'"
            )
            assert (
                sc_schedule == cronjob_schedule
            ), f"ReclaimspaceCronJOb schedule :{cronjob_schedule} does not inherit StorageClass reclaim-space schedule"
        logger.info(
            "RBD-Block PVCs correctly inherit reclaim-space schedule from StorageClass."
        )

        # Switch to PVC precedence and verify
        set_schedule_precedence("pvc")
        for pvc_obj in self.rbd_blk_pvcs:
            pvc_obj.reload()
            pvc_schedule = pvc_obj.data["metadata"]["annotations"][
                "reclaimspace.csiaddons.openshift.io/schedule"
            ]
            reclaimspace_cronjob = get_reclaimspacecronjob_for_pvc(pvc_obj)
            cronjob_schedule = reclaimspace_cronjob.data["spec"]["schedule"]
            assert pvc_schedule == cronjob_schedule, (
                f"RBD-Block PVC {pvc_obj.name} ReclaimSpaceCronJob schedule "
                f"'{cronjob_schedule}' does not match PVC schedule '{pvc_schedule}'"
            )

        # Revert to StorageClass precedence and verify
        set_schedule_precedence("storageclass")
        for pvc_obj in self.rbd_blk_pvcs:
            pvc_obj.reload()
            reclaimspace_cronjob = get_reclaimspacecronjob_for_pvc(pvc_obj)
            cronjob_schedule = reclaimspace_cronjob.data["spec"]["schedule"]
            assert sc_schedule == cronjob_schedule, (
                f"RBD-Block PVC {pvc_obj.name} ReclaimSpaceCronJob schedule "
                f"'{cronjob_schedule}' does not match StorageClass schedule '{sc_schedule}'"
            )


class TestEnforceStorageclassPrecedenceForKeyRotation:
    """
    Tests enforcement of storage class precedence for Keyrotation operations.
    Creates a matrix of RBD PVCs and pods:
      - RBD (Filesystem): RWO
      - RBD (Block): RWO
    """

    def _ensure_pvcs_bound(self, pvcs, timeout=300):
        """Ensure all provided PVCs reach the Bound state."""
        for pvc in pvcs:
            assert pvc.ocp.wait_for_resource(
                condition=constants.STATUS_BOUND,
                resource_name=pvc.name,
                timeout=timeout,
            ), f"PVC {pvc.name} did not reach Bound state"

    def _prepare_pvcs_and_workloads(
        self, multi_pvc_factory, pod_factory, sc_rbd, size_gib=5, proj_obj=None
    ):
        """
        Create RBD PVCs (Filesystem and Block) and attach pods to them.

        Args:
            multi_pvc_factory: Factory to create multiple PVCs
            pod_factory: Factory to create pods
            sc_rbd: RBD StorageClass object
            size_gib (int): Size of PVCs in GiB (default: 5)
        """
        self.pod_objs = []

        # Create RBD Filesystem PVCs (RWO)
        rbd_fs_pvcs = multi_pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            access_modes=[constants.ACCESS_MODE_RWO],
            size=size_gib,
            num_of_pvc=2,
            storageclass=sc_rbd,
            project=proj_obj,
        )
        self._ensure_pvcs_bound(rbd_fs_pvcs)
        for pvc in rbd_fs_pvcs:
            self.pod_objs.append(
                pod_factory(
                    interface=constants.CEPHBLOCKPOOL,
                    pvc=pvc,
                    status=constants.STATUS_RUNNING,
                )
            )

        # Create RBD Block PVCs (RWO)
        self.rbd_blk_pvcs = multi_pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            access_modes=[f"{constants.ACCESS_MODE_RWO}-Block"],
            size=size_gib,
            num_of_pvc=2,
            storageclass=sc_rbd,
        )
        self._ensure_pvcs_bound(self.rbd_blk_pvcs)
        for pvc in self.rbd_blk_pvcs:
            self.pod_objs.append(
                pod_factory(
                    interface=constants.CEPHBLOCKPOOL,
                    pvc=pvc,
                    raw_block_pv=True,
                    status=constants.STATUS_RUNNING,
                )
            )

        logger.info(
            "Prepared pods: RBD-FS=%d, RBD-Block=%d (total pods=%d)",
            len(rbd_fs_pvcs),
            len(self.rbd_blk_pvcs),
            len(self.pod_objs),
        )

    @polarion_id("ODF-XXXX")
    def test_storageclass_precedence_for_keyrotation(
        self,
        project_factory,
        storageclass_factory,
        multi_pvc_factory,
        pod_factory,
        pv_encryption_kms_setup_factory,
    ):
        """
        Validates storage class precedence for ReclaimSpace operations.

        Steps:
        1. Create an RBD encrypted StorageClass
        2. Create encrypted PVC and attach pods
        3. Ensure all pods are in Running state
        4. Annotate StorageClass (@weekly) and PVCs (@daily) with reclaim-space schedules
        5. Verify StorageClass precedence in KeyrotationCronjob is by default.
        6. Switch precedence to PVC and verify KeyrotationCronjob following PVC schedule
        7. Revert precedence to StorageClass and verify behavior
        """
        # Create RBD StorageClass
        self.proj_obj = project_factory()
        self.vault = pv_encryption_kms_setup_factory("v1", False)
        sc_rbd = storageclass_factory(
            interface=constants.CEPHBLOCKPOOL,
            encrypted=True,
            encryption_kms_id=self.vault.kmsid,
        )
        self.vault.vault_path_token = self.vault.generate_vault_token()
        self.vault.create_vault_csi_kms_token(namespace=self.proj_obj.namespace)

        # Prepare RBD PVCs and pods
        self._prepare_pvcs_and_workloads(
            multi_pvc_factory=multi_pvc_factory,
            pod_factory=pod_factory,
            sc_rbd=sc_rbd,
            size_gib=5,
            proj_obj=self.proj_obj,
        )
        logger.info("RBD workloads and PVCs created successfully.")

        if get_schedule_precedance_value_from_csi_addons_configmap() != "storageclass":
            set_schedule_precedence("storageclass")

        # Annotate StorageClass with weekly reclaim-space schedule
        sc_rbd.annotate("keyrotation.csiaddons.openshift.io/schedule=@weekly")

        # Annotate PVCs with daily reclaim-space schedule
        for pvc_obj in self.rbd_blk_pvcs:
            pvc_obj.annotate("keyrotation.csiaddons.openshift.io/schedule=@daily")
            pvc_obj.reload()

        # Verify all pods are Running
        for pod in self.pod_objs:
            assert pod.ocp.wait_for_resource(
                condition=constants.STATUS_RUNNING,
                resource_name=pod.name,
                timeout=300,
            ), f"Pod {pod.name} is not in Running state"
        logger.info("All pods are running before reclaim-space operation.")

        # Reload StorageClass to get updated annotations
        sc_rbd.reload()
        sc_schedule = sc_rbd.data["metadata"]["annotations"][
            "keyrotation.csiaddons.openshift.io/schedule"
        ]
        logger.info(f"StorageClass reclaim-space schedule: {sc_schedule}")

        # Verify StorageClass precedence
        for pvc_obj in self.rbd_blk_pvcs:
            logger.info(
                f"RBD-Block PVC: {pvc_obj.name}, StorageClass: {pvc_obj.storageclass}"
            )
            pvc_obj.reload()
            pvc_schedule = pvc_obj.data["metadata"]["annotations"][
                "keyrotation.csiaddons.openshift.io/schedule"
            ]
            logger.info(
                f"RBD-Block PVC: {pvc_obj.name}, reclaim-space schedule: {pvc_schedule}"
            )

            reclaimspace_cronjob = get_reclaimspacecronjob_for_pvc(pvc_obj)
            cronjob_schedule = reclaimspace_cronjob.data["spec"]["schedule"]
            assert sc_schedule == cronjob_schedule, (
                f"RBD-Block PVC {pvc_obj.name} ReclaimSpaceCronJob schedule "
                f"'{cronjob_schedule}' does not match StorageClass schedule '{sc_schedule}'"
            )
            assert (
                sc_schedule == cronjob_schedule
            ), f"ReclaimspaceCronJOb schedule :{cronjob_schedule} does not inherit StorageClass reclaim-space schedule"
        logger.info(
            "RBD-Block PVCs correctly inherit reclaim-space schedule from StorageClass."
        )

        # Switch to PVC precedence and verify
        set_schedule_precedence("pvc")
        for pvc_obj in self.rbd_blk_pvcs:
            pvc_obj.reload()
            pvc_schedule = pvc_obj.data["metadata"]["annotations"][
                "keyrotation.csiaddons.openshift.io/schedule"
            ]
            reclaimspace_cronjob = get_reclaimspacecronjob_for_pvc(pvc_obj)
            cronjob_schedule = reclaimspace_cronjob.data["spec"]["schedule"]
            assert pvc_schedule == cronjob_schedule, (
                f"RBD-Block PVC {pvc_obj.name} ReclaimSpaceCronJob schedule "
                f"'{cronjob_schedule}' does not match PVC schedule '{pvc_schedule}'"
            )

        # Revert to StorageClass precedence and verify
        set_schedule_precedence("storageclass")
        for pvc_obj in self.rbd_blk_pvcs:
            pvc_obj.reload()
            reclaimspace_cronjob = get_reclaimspacecronjob_for_pvc(pvc_obj)
            cronjob_schedule = reclaimspace_cronjob.data["spec"]["schedule"]
            assert sc_schedule == cronjob_schedule, (
                f"RBD-Block PVC {pvc_obj.name} ReclaimSpaceCronJob schedule "
                f"'{cronjob_schedule}' does not match StorageClass schedule '{sc_schedule}'"
            )
