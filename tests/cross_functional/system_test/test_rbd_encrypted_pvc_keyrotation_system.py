import logging
import time
import pytest

from ocs_ci.framework.pytest_customization.marks import (
    magenta_squad,
    system_test,
    ignore_leftovers,
    kms_config_required,
    skipif_managed_service,
    skipif_hci_provider_and_client,
    skipif_disconnected_cluster,
    skipif_proxy_cluster,
)
from ocs_ci.framework.testlib import E2ETest, tier1
from ocs_ci.ocs import constants
from ocs_ci.helpers.keyrotation_helper import PVKeyrotation
from ocs_ci.ocs import ocp
from ocs_ci.ocs.resources import ocs
from ocs_ci.framework import config
from ocs_ci.utility import nfs_utils
from ocs_ci.ocs.cluster import change_ceph_full_ratio
from ocs_ci.helpers import helpers

log = logging.getLogger(__name__)


@magenta_squad
@system_test
@ignore_leftovers
@kms_config_required
@skipif_managed_service
@skipif_hci_provider_and_client
@skipif_disconnected_cluster
@skipif_proxy_cluster
class TestRBDEncryptedPVCKeyRotation(E2ETest):
    """
    Test class for RBD encrypted PVC key rotation with Vault KMS.
    """

    @pytest.fixture(scope="function")
    def setup_nfs_feature(self, request):
        """
        Fixture to enable NFS feature for in-cluster NFS and disable after test completion.

        This fixture:
        1. Enables the Ceph NFS Ganesha feature
        2. Registers finalizer to disable NFS feature after test

        Args:
            request: pytest request object for finalizer registration

        Returns:
            str: NFS Ganesha pod name

        """
        namespace = config.ENV_DATA["cluster_namespace"]
        storage_cluster_obj = ocp.OCP(
            kind=constants.STORAGECLUSTER, namespace=namespace
        )
        config_map_obj = ocp.OCP(kind=constants.CONFIGMAP, namespace=namespace)
        pod_obj = ocp.OCP(kind=constants.POD, namespace=namespace)

        # Enable NFS feature
        nfs_ganesha_pod_name = nfs_utils.nfs_enable(
            storage_cluster_obj,
            config_map_obj,
            pod_obj,
            namespace,
        )
        log.info(
            f"NFS feature enabled successfully. NFS Ganesha pod: {nfs_ganesha_pod_name}"
        )

        def finalizer():
            """
            Finalizer to disable NFS feature after test completion.
            """
            # Disable NFS feature
            nfs_sc = constants.NFS_STORAGECLASS_NAME
            sc = ocs.OCS(kind=constants.STORAGECLASS, metadata={"name": nfs_sc})

            nfs_utils.nfs_disable(
                storage_cluster_obj,
                config_map_obj,
                pod_obj,
                sc,
                nfs_ganesha_pod_name,
            )
            log.info("NFS feature disabled successfully")

        request.addfinalizer(finalizer)
        return nfs_ganesha_pod_name

    @pytest.fixture(scope="function")
    def setup_prerequisites(self, setup_nfs_feature, create_multiple_storage_pvcs_pods):
        """
        Fixture to create storage resources for test prerequisites.

        This fixture:
        1. Creates PVCs and pods for all storage types (4 PVCs each)
        2. Starts FIO workload on all created pods

        Args:
            setup_nfs_feature: Fixture that enables NFS feature
            create_multiple_storage_pvcs_pods: Factory fixture to create storage resources

        Returns:
            dict: Storage resources dictionary with nested structure

        """
        # Create prerequisites resources for testcase
        fixture_storage_resources = create_multiple_storage_pvcs_pods(
            proj_obj=self.proj_obj,
            sc_obj=self.sc_obj,
            total_pvcs=4,
            pvc_name_prefix="fixture-",
        )
        log.info("Starting FIO workload on setup pods")

        # Extract fixture resources
        fixture_pod_objs = fixture_storage_resources["encrypted_rbd"]["pods"]
        fixture_pvc_combinations = fixture_storage_resources["encrypted_rbd"][
            "combinations"
        ]
        fixture_non_enc_pod_objs = fixture_storage_resources["non_encrypted_rbd"][
            "pods"
        ]
        fixture_non_enc_combinations = fixture_storage_resources["non_encrypted_rbd"][
            "combinations"
        ]
        fixture_cephfs_pod_objs = fixture_storage_resources["cephfs"]["pods"]
        fixture_nfs_pod_objs = fixture_storage_resources["nfs"]["pods"]

        self.start_fio_on_pods(
            pod_objs=fixture_pod_objs,
            pvc_combinations=fixture_pvc_combinations,
            non_enc_pod_objs=fixture_non_enc_pod_objs,
            non_enc_combinations=fixture_non_enc_combinations,
            cephfs_pod_objs=fixture_cephfs_pod_objs,
            nfs_pod_objs=fixture_nfs_pod_objs,
        )

        return fixture_storage_resources

    @pytest.fixture(scope="function")
    def verify_pvc_key_rotation(self):

        def _verify_key_rotation(pvc_objs, pvk_obj):
            """
            Args:
                pvc_objs (list): List of PVC objects to verify key rotation
                pvk_obj (PVKeyrotation): PVKeyrotation object for verification

            Returns:
                list: List of boolean results for each PVC key rotation verification
            """
            rotation_results = []
            for idx, pvc_obj in enumerate(pvc_objs, start=1):
                log.info(
                    f"Verifying key rotation for PVC {idx}/{len(pvc_objs)}: {pvc_obj.name}"
                )
                volume_handle = pvc_obj.get_pv_volume_handle_name

                # Verify key rotation occurred
                try:
                    rotation_success = pvk_obj.wait_till_keyrotation(volume_handle)
                    rotation_results.append(rotation_success)

                except Exception as e:
                    log.error(f"Key rotation failed for PVC {pvc_obj.name}: {str(e)}")
                    rotation_results.append(False)
                    raise

            log.info("Key rotation verified successfully for all PV's")
            return rotation_results

        return _verify_key_rotation

    def start_fio_on_pods(
        self,
        pod_objs=None,
        pvc_combinations=None,
        non_enc_pod_objs=None,
        non_enc_combinations=None,
        cephfs_pod_objs=None,
        nfs_pod_objs=None,
    ):
        """
        Start FIO workload on pods and wait for completion.

        Args:
            pod_objs (list): List of encrypted RBD pod objects
            pvc_combinations (list): List of PVC configurations for encrypted RBD
            non_enc_pod_objs (list): List of non-encrypted RBD pod objects
            non_enc_combinations (list): List of PVC configurations for non-encrypted RBD
            cephfs_pod_objs (list): List of CephFS pod objects
            nfs_pod_objs (list): List of NFS pod objects

        """
        # Prepare pods with config for FIO
        fs_config = {"volume_mode": constants.VOLUME_MODE_FILESYSTEM}
        all_pods_with_config = (
            list(zip(pod_objs or [], pvc_combinations or []))
            + list(zip(non_enc_pod_objs or [], non_enc_combinations or []))
            + [
                (pod, {**fs_config, "pod_type": "cephfs"})
                for pod in (cephfs_pod_objs or [])
            ]
            + [(pod, {**fs_config, "pod_type": "nfs"}) for pod in (nfs_pod_objs or [])]
        )

        for idx, (pod_obj, pvc_config) in enumerate(all_pods_with_config, start=1):
            # Determine IO type based on volume mode
            if pvc_config["volume_mode"] == constants.VOLUME_MODE_BLOCK:
                io_type = "block"
                log.info("  - IO Type: Block device")
            else:
                io_type = "fs"
                log.info("  - IO Type: Filesystem")

            is_nfs_pod = pvc_config.get("pod_type") == "nfs"

            if is_nfs_pod:
                # NFS pods: use direct I/O to prevent memory exhaustion
                pod_obj.run_io(
                    storage_type=io_type,
                    size="500M",
                    runtime=60,
                    fio_filename=pod_obj.name,
                    direct=1,
                )
            else:
                # RBD/CephFS pods: use verify=True
                pod_obj.run_io(
                    storage_type=io_type,
                    size="500M",
                    verify=True,
                    runtime=300,
                )
        log.info(f"FIO workload started on all {len(all_pods_with_config)} pods")

        # Wait for IO completion on all pods
        for idx, (pod_obj, pvc_config) in enumerate(all_pods_with_config, start=1):
            pod_obj.get_fio_results()
        log.info(f"FIO completed on all {len(all_pods_with_config)} pods")

    @pytest.fixture(autouse=True)
    def setup_encrypted_storage(
        self,
        request,
        project_factory,
        pv_encryption_kms_setup_factory,
        storageclass_factory,
    ):
        """
        Setup fixture to configure Vault KMS and create encrypted storage class.

        This fixture:
        1. Creates a test project/namespace
        2. Initializes Vault KMS with kv_version v1
        3. Creates an encrypted RBD storage class with Immediate binding mode
        4. Adds key rotation annotation with schedule '*/2 * * * *'
        5. Generates and creates Vault CSI KMS token in the namespace

        Args:
            request: pytest request object for finalizer registration
            project_factory: Factory fixture to create projects
            pv_encryption_kms_setup_factory: Factory to setup PV encryption with KMS
            storageclass_factory: Factory fixture to create storage classes
        """

        log.info("SETUP: Configuring Vault KMS and encrypted storage class")

        self.proj_obj = project_factory()
        log.info(f"Created project: {self.proj_obj.namespace}")

        log.info(" Initializing Vault KMS with kv_version v1")
        self.kms = pv_encryption_kms_setup_factory(
            kv_version="v1", use_vault_namespace=False
        )
        log.info(f"Vault KMS initialized with KMS ID: {self.kms.kmsid}")

        log.info(" Creating encrypted RBD storage class")
        self.sc_obj = storageclass_factory(
            interface=constants.CEPHBLOCKPOOL,
            encrypted=True,
            encryption_kms_id=self.kms.kmsid,
            allow_volume_expansion=True,
            volume_binding_mode=constants.IMMEDIATE_VOLUMEBINDINGMODE,
            reclaim_policy=constants.RECLAIM_POLICY_DELETE,
        )
        log.info(f"Created encrypted storage class: {self.sc_obj.name}")

        log.info(" Configuring PV encryption with Vault service")
        self.kms.vault_path_token = self.kms.generate_vault_token()
        self.kms.create_vault_csi_kms_token(namespace=self.proj_obj.namespace)
        log.info(f"Created Vault CSI KMS token in namespace: {self.proj_obj.namespace}")

        # Add key rotation annotation to storage class
        self.pvk_obj = PVKeyrotation(self.sc_obj)
        self.pvk_obj.annotate_storageclass_key_rotation(schedule="*/2 * * * *")
        log.info(
            "Added annotation: keyrotation.csiaddons.openshift.io/schedule='*/2 * * * *'"
        )
        log.info("SETUP COMPLETE: Ready to create PVCs and test key rotation")

        def finalizer():

            change_ceph_full_ratio(85)

        request.addfinalizer(finalizer)

    @tier1
    def test_rbd_encrypted_pvc_keyrotation_all_combinations(
        self,
        create_multiple_storage_pvcs_pods,
        setup_nfs_feature,
        setup_prerequisites,
        verify_pvc_key_rotation,
        snapshot_factory,
        snapshot_restore_factory,
        pvc_clone_factory,
        create_clones_until_cluster_full,
        threading_lock,
    ):
        """
        Test RBD encrypted PVC key rotation for all volume/access mode combinations.

        This test validates key rotation functionality for encrypted RBD PVCs
        across the supported combinations of volume modes and access modes.

        Test Steps:
        1. Create encrypted RBD storage class with Vault KMS provider (kv_version v1)
        2. Add key rotation annotation with schedule '*/2 * * * *'
        3. Configure PV encryption settings with Vault service
        4. Create 10 encrypted PVCs cycling through:
               Filesystem×RWO, Block×RWO, Block×RWX
        5. Create Deployment pods that utilize each encrypted PVC
        5b. Create 10 non-encrypted RBD PVCs cycling through:
               Filesystem×RWO, Filesystem×RWOP, Block×RWO, Block×RWX, Block×RWOP
            and their deployment pods
        6. Start FIO workload on all pods with verify=True option
        7. Wait for 2 minutes for key rotation to occur
        8. Verify key rotation happened for all encrypted PVs
        9. Create snapshots, clones, expand PVCs, and restore from snapshots for test PVCs
        10. Start cloning multiple PVCs of both RBD and CephFS till cluster reaches 85% full ratio,
            ensuring at least 7 pending clone jobs

        """

        storage_resources = create_multiple_storage_pvcs_pods(
            proj_obj=self.proj_obj,
            sc_obj=self.sc_obj,
            total_pvcs=7,
            pvc_name_prefix="test-",
        )

        # Extract all resources from above fixture using nested structure
        pvc_objs = storage_resources["encrypted_rbd"]["pvcs"]
        pod_objs = storage_resources["encrypted_rbd"]["pods"]
        pvc_combinations = storage_resources["encrypted_rbd"]["combinations"]
        non_enc_pod_objs = storage_resources["non_encrypted_rbd"]["pods"]
        non_enc_combinations = storage_resources["non_encrypted_rbd"]["combinations"]
        cephfs_pod_objs = storage_resources["cephfs"]["pods"]
        nfs_pod_objs = storage_resources["nfs"]["pods"]

        log.info(" Starting FIO workload on all pods")

        # Start FIO on all pods using helper method and wait for completion
        self.start_fio_on_pods(
            pod_objs=pod_objs,
            pvc_combinations=pvc_combinations,
            non_enc_pod_objs=non_enc_pod_objs,
            non_enc_combinations=non_enc_combinations,
            cephfs_pod_objs=cephfs_pod_objs,
            nfs_pod_objs=nfs_pod_objs,
        )

        log.info(" Waiting for key rotation to occur")
        wait_time = 120  # 2 minutes
        buffer_time = 30
        total_wait = wait_time + buffer_time
        time.sleep(total_wait)

        log.info(" Verifying key rotation for all encrypted PVs")
        verify_pvc_key_rotation(pvc_objs, self.pvk_obj)

        log.info(
            " Performing snapshot, clone, expansion, and restore operations on test PVCs"
        )

        # Get all test PVCs (encrypted RBD, non-encrypted RBD, CephFS)
        # Exclude NFS PVCs as they don't support snapshots/clones
        non_enc_pvc_objs = storage_resources["non_encrypted_rbd"]["pvcs"]
        cephfs_pvc_objs = storage_resources["cephfs"]["pvcs"]
        all_test_pvcs = pvc_objs + non_enc_pvc_objs + cephfs_pvc_objs

        log.info(f"Creating snapshots for {len(all_test_pvcs)} test PVCs")
        snapshots = []
        for idx, pvc_obj in enumerate(all_test_pvcs, start=1):
            snap_obj = snapshot_factory(pvc_obj=pvc_obj, wait=True)
            snapshots.append(snap_obj)
        log.info(f"All {len(snapshots)} snapshots created successfully")

        log.info(f"Creating clones for {len(all_test_pvcs)} test PVCs")
        clones = []
        for idx, pvc_obj in enumerate(all_test_pvcs, start=1):
            clone_obj = pvc_clone_factory(
                pvc_obj=pvc_obj, status=constants.STATUS_BOUND, timeout=300
            )
            clones.append(clone_obj)
        log.info(f"All {len(clones)} clones created successfully")

        log.info(f"Expanding {len(all_test_pvcs)} test PVCs")
        for idx, pvc_obj in enumerate(all_test_pvcs, start=1):
            current_size = pvc_obj.size
            new_size = current_size + 1  # Expand by 1Gi
            pvc_obj.resize_pvc(new_size, verify=True)
        log.info(f"All {len(all_test_pvcs)} PVCs expanded successfully")

        log.info(f"Restoring {len(snapshots)} snapshots to new PVCs")
        restored_pvcs = []
        for idx, snap_obj in enumerate(snapshots, start=1):
            original_pvc = all_test_pvcs[idx - 1]
            original_sc = original_pvc.backed_sc
            restored_pvc = snapshot_restore_factory(
                snapshot_obj=snap_obj,
                storageclass=original_sc,
                volume_mode=snap_obj.parent_volume_mode,
                access_mode=snap_obj.parent_access_mode,
                status=constants.STATUS_BOUND,
                timeout=300,
            )
            restored_pvcs.append(restored_pvc)
        log.info(f"All {len(restored_pvcs)} snapshots restored successfully")

        log.info(
            " completed: All snapshot, clone, expansion, and restore operations successful"
        )

        try:
            log.info(
                " Creating clones until cluster reaches 85% full ratio with at least 7 pending clones"
            )

            pvcs_to_clone = pvc_objs + non_enc_pvc_objs + cephfs_pvc_objs
            log.info(f"Total PVCs available for cloning: {len(pvcs_to_clone)}")

            full_ratio_clones = create_clones_until_cluster_full(
                pvcs_to_clone=pvcs_to_clone,
                clone_batch_size=10,
                max_attempts=12,
                expected_alerts=["CephOSDCriticallyFull"],
                flatten_rbd_clones=True,
                min_pending_clones=7,
            )

            log.info(
                f" Created {len(full_ratio_clones)} clones to fill cluster to 85% capacity"
            )

            log.info(
                " Making cluster out of full by increasing full ratio from 85% to 95%"
            )
            change_ceph_full_ratio(95)

            log.info(" Deleting clones in pending state one by one")

            pending_clones = []
            bound_clones = []

            for clone in full_ratio_clones:
                try:
                    clone.reload()
                    if clone.status == constants.STATUS_BOUND:
                        bound_clones.append(clone)
                    else:
                        pending_clones.append(clone)
                except Exception as e:
                    log.warning(f"Could not check status of clone {clone.name}: {e}")
                    pending_clones.append(clone)  # Assume pending if status check fails

            log.info(
                f"Found {len(pending_clones)} pending clones and {len(bound_clones)} bound clones"
            )

            # Delete pending clones one by one
            timeout = 300
            for idx, clone in enumerate(pending_clones, start=1):
                try:
                    pvc_reclaim_policy = clone.reclaim_policy
                    clone.delete()
                    clone.ocp.wait_for_delete(clone.name, timeout)

                    # Validate PV deletion if reclaim policy is Delete
                    if pvc_reclaim_policy == constants.RECLAIM_POLICY_DELETE:
                        helpers.validate_pv_delete(clone.backed_pv)

                except Exception as e:
                    log.warning(f"Failed to delete clone {clone.name}: {e}")

        finally:
            # This ALWAYS runs, even if above step fails
            # Ensures cluster is recovered from full/read-only state
            change_ceph_full_ratio(95)

        log.info("Waiting for 2 minutes to ensure key rotation has occurred")
        time.sleep(120)  # Wait 2 minutes for key rotation schedule
        verify_pvc_key_rotation(pvc_objs, self.pvk_obj)
