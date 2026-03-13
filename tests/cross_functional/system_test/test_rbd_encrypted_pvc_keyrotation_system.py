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
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.resources import ocs
from ocs_ci.framework import config
from ocs_ci.utility import nfs_utils

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

    This test validates key rotation for encrypted RBD PVCs across all
    combinations of volume modes (Block, Filesystem) and access modes (ROX, RWX).
    """

    @pytest.fixture(scope="function")
    def setup_nfs_feature(self, request):
        """
        Fixture to enable NFS feature for in-cluster NFS and disable it after test completion.

        This fixture enables the Ceph NFS Ganesha feature before the test runs and ensures
        it is disabled after the test completes. This is required for creating NFS PVCs
        using the ocs-storagecluster-ceph-nfs storage class.

        Args:
            request: pytest request object for finalizer registration

        Yields:
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
            nfs_sc = constants.NFS_STORAGECLASS_NAME
            sc = ocs.OCS(kind=constants.STORAGECLASS, metadata={"name": nfs_sc})

            # Disable NFS feature
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
    def verify_pvc_key_rotation(self):
        """
        Fixture to verify key rotation for encrypted PVCs.

        This fixture provides a reusable method to verify key rotation for a list
        of PVC objects. It waits for key rotation to occur and validates that new
        keys are different from original keys.

        Returns:
            function: A verification function that takes pvc_objs and pvk_obj as arguments

        """

        def _verify_key_rotation(pvc_objs, pvk_obj):
            """
            Verify key rotation for all provided PVC objects.

            Args:
                pvc_objs (list): List of PVC objects to verify key rotation
                pvk_obj (PVKeyrotation): PVKeyrotation object for verification

            Returns:
                list: List of boolean results for each PVC key rotation verification

            Raises:
                AssertionError: If any key rotation verification fails

            """
            rotation_results = []
            for idx, pvc_obj in enumerate(pvc_objs, start=1):
                log.info(
                    f"\nVerifying key rotation for PVC {idx}/{len(pvc_objs)}: {pvc_obj.name}"
                )

                # Get PV volume handle name
                volume_handle = pvc_obj.get_pv_volume_handle_name
                log.info(f"  - PV Volume Handle: {volume_handle}")

                # Verify key rotation occurred
                try:
                    rotation_success = pvk_obj.wait_till_keyrotation(volume_handle)
                    rotation_results.append(rotation_success)
                    log.info(
                        f"✓ Key rotation verified for PVC {pvc_obj.name} - New key is different from original key"
                    )
                except Exception as e:
                    log.error(f"✗ Key rotation failed for PVC {pvc_obj.name}: {str(e)}")
                    rotation_results.append(False)
                    raise

            # Assert all key rotations were successful
            assert all(rotation_results), (
                f"Key rotation failed for one or more PVCs. "
                f"Success: {sum(rotation_results)}/{len(rotation_results)}"
            )
            log.info(
                f"\n✓ Key rotation verified successfully for all {len(pvc_objs)} PVs"
            )
            return rotation_results

        return _verify_key_rotation

    @pytest.fixture(autouse=True)
    def setup_encrypted_storage(
        self,
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
            project_factory: Factory fixture to create projects
            pv_encryption_kms_setup_factory: Factory to setup PV encryption with KMS
            storageclass_factory: Factory fixture to create storage classes
        """

        log.info("SETUP: Configuring Vault KMS and encrypted storage class")

        # Step 1: Create a test project
        log.info("Step 1: Creating test project/namespace")
        self.proj_obj = project_factory()
        log.info(f"Created project: {self.proj_obj.namespace}")

        # Step 2: Initialize Vault KMS with kv_version v1
        log.info("Step 2: Initializing Vault KMS with kv_version v1")
        self.kms = pv_encryption_kms_setup_factory(
            kv_version="v1", use_vault_namespace=False
        )
        log.info(f"Vault KMS initialized with KMS ID: {self.kms.kmsid}")

        # Step 3: Create encrypted RBD storage class with Immediate binding mode
        log.info("Step 3: Creating encrypted RBD storage class")
        self.sc_obj = storageclass_factory(
            interface=constants.CEPHBLOCKPOOL,
            encrypted=True,
            encryption_kms_id=self.kms.kmsid,
            allow_volume_expansion=False,
            volume_binding_mode=constants.IMMEDIATE_VOLUMEBINDINGMODE,
            reclaim_policy=constants.RECLAIM_POLICY_DELETE,
        )
        log.info(f"Created encrypted storage class: {self.sc_obj.name}")

        # Step 4: Generate Vault token and create ceph-csi-kms-token in namespace
        log.info("Step 4: Configuring PV encryption with Vault service")
        self.kms.vault_path_token = self.kms.generate_vault_token()
        self.kms.create_vault_csi_kms_token(namespace=self.proj_obj.namespace)
        log.info(f"Created Vault CSI KMS token in namespace: {self.proj_obj.namespace}")

        # Step 5: Add key rotation annotation to storage class
        log.info("Step 5: Adding key rotation annotation to storage class")
        self.pvk_obj = PVKeyrotation(self.sc_obj)
        self.pvk_obj.annotate_storageclass_key_rotation(schedule="*/2 * * * *")
        log.info(
            "Added annotation: keyrotation.csiaddons.openshift.io/schedule='*/2 * * * *'"
        )

        log.info("=" * 80)
        log.info("SETUP COMPLETE: Ready to create PVCs and test key rotation")
        log.info("=" * 80)

    @tier1
    def test_rbd_encrypted_pvc_keyrotation_all_combinations(
        self,
        create_multiple_storage_pvcs_pods,
        setup_nfs_feature,
        verify_pvc_key_rotation,
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

        """

        # Call the fixture factory to create all PVCs and deployment pods
        storage_resources = create_multiple_storage_pvcs_pods(
            proj_obj=self.proj_obj, sc_obj=self.sc_obj, total_pvcs=10
        )

        # Extract all resources from fixture
        pvc_objs = storage_resources["pvc_objs"]
        pod_objs = storage_resources["pod_objs"]
        pvc_combinations = storage_resources["pvc_combinations"]
        non_enc_pod_objs = storage_resources["non_enc_pod_objs"]
        non_enc_combinations = storage_resources["non_enc_combinations"]
        cephfs_pod_objs = storage_resources["cephfs_pod_objs"]
        nfs_pvc_objs = storage_resources["nfs_pvc_objs"]
        nfs_pod_objs = storage_resources["nfs_pod_objs"]

        log.info("Fixture factory created all resources successfully:")

        # Step 6: Start FIO workload on ALL pods (encrypted RBD + non-encrypted RBD + CephFS + NFS)
        log.info("Step 6: Starting FIO workload on all pods (verify=True)")

        # Combine all pods with their configurations
        all_pods_with_config = list(zip(pod_objs, pvc_combinations)) + list(
            zip(non_enc_pod_objs, non_enc_combinations)
        )

        # Add CephFS pods (all are Filesystem mode)
        for cephfs_pod in cephfs_pod_objs:
            all_pods_with_config.append(
                (
                    cephfs_pod,
                    {
                        "volume_mode": constants.VOLUME_MODE_FILESYSTEM,
                        "pod_type": "cephfs",
                    },
                )
            )

        # Add NFS pods (all are Filesystem mode, mark as NFS type)
        for nfs_pod in nfs_pod_objs:
            all_pods_with_config.append(
                (
                    nfs_pod,
                    {
                        "volume_mode": constants.VOLUME_MODE_FILESYSTEM,
                        "pod_type": "nfs",
                    },
                )
            )

        # Start FIO on all pods
        for idx, (pod_obj, pvc_config) in enumerate(all_pods_with_config, start=1):
            log.info(
                f"\nStarting FIO on pod {idx}/{len(all_pods_with_config)}: {pod_obj.name}"
            )

            # Determine IO type based on volume mode
            if pvc_config["volume_mode"] == constants.VOLUME_MODE_BLOCK:
                io_type = "block"
                log.info("  - IO Type: Block device")
            else:
                io_type = "fs"
                log.info("  - IO Type: Filesystem")

            # Check if this is an NFS pod - use different FIO parameters
            is_nfs_pod = pvc_config.get("pod_type") == "nfs"

            if is_nfs_pod:
                # NFS pods: use fio_filename, no verify (to avoid permission issues)
                log.info(
                    "  - FIO Parameters: size=500M, runtime=60s, fio_filename (NFS pod)"
                )
                pod_obj.run_io(
                    storage_type=io_type,
                    size="500M",
                    runtime=60,
                    fio_filename=pod_obj.name,
                )
            else:
                # RBD/CephFS pods: use verify=True
                log.info("  - FIO Parameters: verify=True, size=500M, runtime=300s")
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
        log.info(f"IO completed on all {len(all_pods_with_config)} pods")

        # Step 7: Wait for 2 minutes for key rotation to occur (encrypted RBD PVs only)
        log.info("Step 7: Waiting for key rotation to occur")

        wait_time = 120  # 2 minutes
        log.info("Key rotation schedule: */2 * * * * (every 2 minutes)")

        # Add buffer time to ensure rotation completes
        buffer_time = 30
        total_wait = wait_time + buffer_time
        log.info(f"Total wait time with buffer: {total_wait} seconds")
        time.sleep(total_wait)

        # Step 8: Verify key rotation for all encrypted PVs using fixture
        log.info("Step 8: Verifying key rotation for all encrypted PVs")
        verify_pvc_key_rotation(pvc_objs, self.pvk_obj)

        log.info("TEST COMPLETED SUCCESSFULLY")

        # Cleanup NFS resources at the end of test execution
        if nfs_pod_objs:
            for nfs_pod_obj in nfs_pod_objs:
                pod.delete_deployment_pods(nfs_pod_obj)
            log.info(
                f"All {len(nfs_pod_objs)} NFS deployment pods deleted successfully"
            )

        if nfs_pvc_objs:
            for nfs_pvc_obj in nfs_pvc_objs:
                nfs_pvc_obj.delete()
                nfs_pvc_obj.ocp.wait_for_delete(
                    resource_name=nfs_pvc_obj.name, timeout=180
                )
            log.info(f" All {len(nfs_pvc_objs)} NFS PVCs deleted successfully")


# Made with Bob
