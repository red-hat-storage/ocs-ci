import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import magenta_squad, ec_allowed
from ocs_ci.ocs.cluster import is_ec_pool_supported
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    skipif_ocp_version,
    E2ETest,
    tier2,
    skipif_external_mode,
    skipif_hci_provider_and_client,
    skipif_disconnected_cluster,
    skipif_proxy_cluster,
)
from ocs_ci.ocs.benchmark_operator import BMO_NAME
from ocs_ci.ocs.constants import (
    VOLUME_MODE_FILESYSTEM,
    CEPHBLOCKPOOL,
    STATUS_READYTOUSE,
)
from ocs_ci.ocs.ocp import OCP

logger = logging.getLogger(__name__)


@magenta_squad
@skipif_hci_provider_and_client
@tier2
@skipif_disconnected_cluster
@skipif_proxy_cluster
class TestCompressedSCAndSupportSnapClone(E2ETest):
    """
    Tests to create new compressed sc and their support for
    snapshot/cloned/restored pgsql pvcs
    """

    @pytest.fixture()
    def pgsql_teardown(self, request):
        self.pgsql_obj_list = []

        def teardown():
            # Delete created postgres and pgbench pods
            logger.info("Deleting postgres pods which are attached to restored PVCs")
            for pgsql_obj in self.pgsql_obj_list:
                pgsql_obj.delete()

        request.addfinalizer(teardown)

    def create_snapshot_and_clone(
        self,
        pgsql,
        postgres_pvcs_obj,
        sc_name,
        multi_snapshot_factory,
        multi_snapshot_restore_factory,
        multi_pvc_clone_factory,
    ):
        pvc_size_new = 25
        logger.info(f"Target resize size: {pvc_size_new}Gi, storage class: {sc_name}")

        logger.test_step("Create snapshots from PostgreSQL PVCs")
        snapshots = multi_snapshot_factory(
            pvc_obj=postgres_pvcs_obj, snapshot_name_suffix="snap", wait=False
        )
        logger.info(
            f"Created {len(snapshots)} snapshot(s), waiting for ReadyToUse state"
        )

        for idx, snap_obj in enumerate(snapshots, 1):
            logger.debug(
                f"Waiting for snapshot {idx}/{len(snapshots)}: {snap_obj.name} (BZ 1969427 - extended timeout)"
            )
            ocs_obj = OCP(kind=snap_obj.kind, namespace=snap_obj.namespace)
            ocs_obj.wait_for_resource(
                condition="true",
                resource_name=snap_obj.name,
                column=STATUS_READYTOUSE,
                timeout=1200,
            )
        logger.info(f"All {len(snapshots)} snapshot(s) are in ReadyToUse state")

        logger.test_step("Restore PVCs from snapshots")
        restored_pvc_objs = multi_snapshot_restore_factory(
            snapshot_obj=snapshots,
            restore_pvc_suffix="restore",
            storageclass=sc_name,
        )
        logger.info(f"Created {len(restored_pvc_objs)} restored PVC(s) from snapshots")

        logger.test_step("Attach PostgreSQL pods to restored PVCs")
        sset_list = pgsql.attach_pgsql_pod_to_claim_pvc(
            pvc_objs=restored_pvc_objs,
            postgres_name="postgres-restore",
            run_benchmark=False,
        )
        self.pgsql_obj_list.extend(sset_list)
        logger.info(f"Attached {len(sset_list)} PostgreSQL pod(s) to restored PVCs")

        logger.test_step(f"Resize restored PVCs to {pvc_size_new}Gi")
        for idx, pvc_obj in enumerate(restored_pvc_objs, 1):
            logger.info(
                f"Resizing restored PVC {idx}/{len(restored_pvc_objs)}: {pvc_obj.name} to {pvc_size_new}Gi"
            )
            pvc_obj.resize_pvc(pvc_size_new, True)
        logger.info(
            f"All {len(restored_pvc_objs)} restored PVC(s) resized successfully"
        )

        logger.test_step("Clone PostgreSQL PVCs")
        cloned_pvcs = multi_pvc_clone_factory(
            pvc_obj=postgres_pvcs_obj,
            volume_mode=VOLUME_MODE_FILESYSTEM,
            storageclass=sc_name,
        )
        logger.info(f"Created {len(cloned_pvcs)} cloned PVC(s) from PostgreSQL volumes")

        logger.test_step("Attach PostgreSQL pods to cloned PVCs")
        sset_list = pgsql.attach_pgsql_pod_to_claim_pvc(
            pvc_objs=cloned_pvcs, postgres_name="postgres-clone", run_benchmark=False
        )
        self.pgsql_obj_list.extend(sset_list)
        logger.info(f"Attached {len(sset_list)} PostgreSQL pod(s) to cloned PVCs")

        logger.test_step(f"Resize cloned PVCs to {pvc_size_new}Gi")
        for idx, pvc_obj in enumerate(cloned_pvcs, 1):
            logger.info(
                f"Resizing cloned PVC {idx}/{len(cloned_pvcs)}: {pvc_obj.name} to {pvc_size_new}Gi"
            )
            pvc_obj.resize_pvc(pvc_size_new, True)
        logger.info(f"All {len(cloned_pvcs)} cloned PVC(s) resized successfully")

    @skipif_external_mode
    @skipif_ocs_version("<4.6")
    @skipif_ocp_version("<4.6")
    @pytest.mark.parametrize(
        argnames=["replica", "compression", "erasure_coded"],
        argvalues=[
            pytest.param(
                *[3, "aggressive", False], marks=pytest.mark.polarion_id("OCS-2536")
            ),
            pytest.param(
                *[2, "aggressive", False], marks=pytest.mark.polarion_id("OCS-2305")
            ),
            pytest.param(
                *[3, "none", True],
                marks=[
                    ec_allowed,
                    tier2,
                    pytest.mark.polarion_id("OCS-7963"),
                    pytest.mark.skipif(
                        not is_ec_pool_supported(),
                        reason="Erasure coded pools are not supported on this cluster",
                    ),
                ],
            ),
        ],
    )
    def test_compressed_sc_and_support_snap_clone(
        self,
        storageclass_factory,
        pgsql_factory_fixture,
        multi_snapshot_factory,
        multi_snapshot_restore_factory,
        multi_pvc_clone_factory,
        pgsql_teardown,
        replica,
        compression,
        erasure_coded,
    ):
        """
        1. Create new sc with compression
        2. Deploy PGSQL workload using those new sc created
        3. Take a snapshot of the pgsql PVC.
        4. Create a new PVC out of that snapshot or restore snapshot
        5. Attach a new pgsql pod to it.
        6. Resize the new PVC
        7. Clone pgsql PVC and attach a new pgsql pod to it
        8. Resize cloned PVC

        """
        pgsql_teardown

        logger.test_step(
            f"Create storage class with compression: {compression}, replica: {replica}, EC: {erasure_coded}"
        )
        interface_type = CEPHBLOCKPOOL
        sc_obj = storageclass_factory(
            interface=interface_type,
            new_rbd_pool=True,
            replica=replica,
            compression=compression,
            erasure_coded=erasure_coded,
        )
        logger.info(
            f"Created storage class: {sc_obj.name} (compression={compression}, replica={replica}, EC={erasure_coded})"
        )

        logger.test_step("Deploy PostgreSQL workload")
        logger.info(f"Deploying pgsql workload with storage class: {sc_obj.name}")
        pgsql = pgsql_factory_fixture(replicas=1, sc_name=sc_obj.name)
        logger.info("PostgreSQL workload deployed successfully")

        postgres_pvcs_obj = pgsql.get_postgres_pvc()
        logger.info(f"Retrieved {len(postgres_pvcs_obj)} PostgreSQL PVC(s)")

        logger.test_step("Execute snapshot/clone/resize workflow")
        self.create_snapshot_and_clone(
            pgsql,
            postgres_pvcs_obj,
            sc_obj.name,
            multi_snapshot_factory,
            multi_snapshot_restore_factory,
            multi_pvc_clone_factory,
        )
        logger.info("Snapshot/clone/resize workflow completed successfully")

    @skipif_external_mode
    @skipif_ocs_version("<4.9")
    @skipif_ocp_version("<4.9")
    @pytest.mark.parametrize(
        argnames=["kv_version", "replica", "compression"],
        argvalues=[
            pytest.param(
                "v1", 3, "aggressive", marks=pytest.mark.polarion_id("OCS-2707")
            ),
            pytest.param(
                "v2", 3, "aggressive", marks=pytest.mark.polarion_id("OCS-2712")
            ),
        ],
    )
    def test_encrypted_compressed_sc_and_support_snap_clone(
        self,
        kv_version,
        replica,
        compression,
        pv_encryption_kms_setup_factory,
        storageclass_factory,
        pgsql_factory_fixture,
        multi_snapshot_factory,
        multi_snapshot_restore_factory,
        multi_pvc_clone_factory,
        pgsql_teardown,
    ):
        """
        1. Create new sc with compression and encryption enabled
        2. Deploy PGSQL workload using those new sc created
        3. Take a snapshot of the pgsql PVC.
        4. Create a new PVC out of that snapshot or restore snapshot
        5. Attach a new pgsql pod to it.
        6. Resize the new PVC
        7. Clone pgsql PVC and attach a new pgsql pod to it
        8. Resize cloned PVC
        """
        pgsql_teardown

        logger.test_step(f"Setup KMS encryption with Vault KV version: {kv_version}")
        logger.info("Setting up csi-kms-connection-details configmap")
        self.vault = pv_encryption_kms_setup_factory(kv_version)
        logger.info(
            f"csi-kms-connection-details setup successful, KMS ID: {self.vault.kmsid}"
        )

        logger.test_step(
            f"Create encrypted storage class with compression: {compression}, replica: {replica}"
        )
        sc_obj = storageclass_factory(
            interface=CEPHBLOCKPOOL,
            new_rbd_pool=True,
            replica=replica,
            compression=compression,
            encrypted=True,
            encryption_kms_id=self.vault.kmsid,
        )
        logger.info(
            f"Created encrypted storage class: {sc_obj.name} (compression={compression}, replica={replica})"
        )

        logger.test_step(f"Create Vault CSI KMS token in namespace: {BMO_NAME}")
        self.vault.vault_path_token = self.vault.generate_vault_token()
        self.vault.create_vault_csi_kms_token(namespace=BMO_NAME)
        logger.info(f"Created Vault CSI KMS token in namespace: {BMO_NAME}")

        logger.test_step("Deploy PostgreSQL workload with encrypted storage")
        logger.info(
            f"Deploying pgsql workload with encrypted storage class: {sc_obj.name}"
        )
        pgsql = pgsql_factory_fixture(replicas=1, sc_name=sc_obj.name)
        logger.info("PostgreSQL workload deployed successfully with encryption")

        postgres_pvcs_obj = pgsql.get_postgres_pvc()
        logger.info(f"Retrieved {len(postgres_pvcs_obj)} encrypted PostgreSQL PVC(s)")

        logger.test_step("Execute snapshot/clone/resize workflow with encrypted PVCs")
        self.create_snapshot_and_clone(
            pgsql,
            postgres_pvcs_obj,
            sc_obj.name,
            multi_snapshot_factory,
            multi_snapshot_restore_factory,
            multi_pvc_clone_factory,
        )
        logger.info("Encrypted snapshot/clone/resize workflow completed successfully")
