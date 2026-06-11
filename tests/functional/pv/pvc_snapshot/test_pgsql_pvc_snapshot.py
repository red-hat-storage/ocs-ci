import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import magenta_squad
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    skipif_ocp_version,
    E2ETest,
    tier2,
    skipif_hci_provider_and_client,
    skipif_disconnected_cluster,
    skipif_proxy_cluster,
)
from ocs_ci.ocs.benchmark_operator import BMO_NAME
from ocs_ci.ocs.constants import CEPHBLOCKPOOL
from ocs_ci.ocs.exceptions import ResourceNotFoundError
from ocs_ci.helpers.helpers import get_snapshot_content_obj
from ocs_ci.utility import kms

logger = logging.getLogger(__name__)


@magenta_squad
@tier2
@skipif_disconnected_cluster
@skipif_proxy_cluster
class TestPvcSnapshotOfWorkloads(E2ETest):
    """
    Tests to verify PVC snapshot feature for pgsql workloads
    """

    @pytest.fixture()
    def pgsql_teardown(self, request):
        self.pgsql_obj_list = []

        def teardown():
            logger.info("Deleting postgres pods which are attached to restored PVCs")
            for pgsql_obj in self.pgsql_obj_list:
                pgsql_obj.delete()

        request.addfinalizer(teardown)

    def create_snapshot(
        self,
        pgsql,
        postgres_pvcs_obj,
        snapshot_factory,
        snapshot_restore_factory,
        sc_name=None,
    ):
        logger.info(f"Creating snapshots of {len(postgres_pvcs_obj)} PostgreSQL PVC(s)")
        snapshots = []
        for idx, pvc_obj in enumerate(postgres_pvcs_obj, 1):
            logger.debug(
                f"Creating snapshot {idx}/{len(postgres_pvcs_obj)} for PVC: {pvc_obj.name}"
            )
            snap_obj = snapshot_factory(
                pvc_obj=pvc_obj, snapshot_name=f"{pvc_obj.name}-snap"
            )
            snapshots.append(snap_obj)
        logger.info(f"All {len(snapshots)} snapshot(s) created and in Ready state")

        logger.info("Creating new PVCs from snapshots")
        restore_pvc_objs = []
        for idx, snapshot in enumerate(snapshots, 1):
            logger.debug(
                f"Creating PVC {idx}/{len(snapshots)} from snapshot: {snapshot.name}"
            )
            restore_pvc_obj = snapshot_restore_factory(
                snapshot_obj=snapshot,
                restore_pvc_name=f"{snapshot.name}-restored",
                storageclass=sc_name,
                volume_mode=snapshot.parent_volume_mode,
                access_mode=snapshot.parent_access_mode,
            )
            logger.debug(
                f"Created PVC {restore_pvc_obj.name} from snapshot {snapshot.name}"
            )
            restore_pvc_objs.append(restore_pvc_obj)
        logger.info(
            f"Created {len(restore_pvc_objs)} new PVC(s) from snapshots in Bound state"
        )

        logger.info("Attaching PostgreSQL pods to restored PVCs")
        self.pgsql_obj_list = pgsql.attach_pgsql_pod_to_claim_pvc(
            pvc_objs=restore_pvc_objs,
            postgres_name="postgres-snap",
            pgbench_name="pgbench-snap",
        )
        logger.info(
            f"Attached {len(self.pgsql_obj_list)} PostgreSQL pod(s) to restored PVCs"
        )

        return snapshots, restore_pvc_objs

    @skipif_ocs_version("<4.6")
    @pytest.mark.polarion_id("OCS-2302")
    def test_pvc_snapshot(
        self,
        pgsql_factory_fixture,
        snapshot_factory,
        snapshot_restore_factory,
        pgsql_teardown,
    ):
        """
        1. Deploy PGSQL workload
        2. Take a snapshot of the pgsql PVC.
        3. Create a new PVC out of that snapshot or restore snapshot
        4. Attach a new pgsql pod to it.
        5. Create pgbench benchmark to new pgsql pod

        """
        pgsql_teardown

        logger.test_step("Deploy PostgreSQL workload")
        logger.info("Deploying PostgreSQL workload with 1 replica")
        pgsql = pgsql_factory_fixture(replicas=1)
        logger.info("PostgreSQL workload deployed successfully")

        postgres_pvcs_obj = pgsql.get_postgres_pvc()
        logger.info(f"Retrieved {len(postgres_pvcs_obj)} PostgreSQL PVC(s)")

        logger.test_step("Create snapshots and restore to new PVCs")
        snapshots, restore_pvc_objs = self.create_snapshot(
            pgsql, postgres_pvcs_obj, snapshot_factory, snapshot_restore_factory
        )
        logger.info(
            f"Snapshot/restore workflow completed: {len(snapshots)} snapshots, {len(restore_pvc_objs)} restored PVCs"
        )

    @pytest.mark.parametrize(
        argnames=["kv_version"],
        argvalues=[
            pytest.param("v1", marks=pytest.mark.polarion_id("OCS-2713")),
            pytest.param("v2", marks=pytest.mark.polarion_id("OCS-2714")),
        ],
    )
    @skipif_ocs_version("<4.8")
    @skipif_ocp_version("<4.8")
    @skipif_hci_provider_and_client
    def test_encrypted_pvc_snapshot(
        self,
        kv_version,
        pv_encryption_kms_setup_factory,
        storageclass_factory,
        pgsql_factory_fixture,
        snapshot_factory,
        snapshot_restore_factory,
        pgsql_teardown,
    ):
        """
        1. Create encrypted storage class
        2. Deploy PGSQL workload using created sc
        3. Take a snapshot of the pgsql PVC.
        4. Create a new PVC out of that snapshot or restore snapshot
        5. Attach a new pgsql pod to it.
        6. Create pgbench benchmark to new pgsql pod
        7. Verify if key is created

        """
        pgsql_teardown

        logger.info("Setting up csi-kms-connection-details configmap")
        self.vault = pv_encryption_kms_setup_factory(kv_version)
        logger.info("csi-kms-connection-details setup successful")

        # Create an encryption enabled storageclass for RBD
        self.sc_obj = storageclass_factory(
            interface=CEPHBLOCKPOOL,
            encrypted=True,
            encryption_kms_id=self.vault.kmsid,
        )

        # Create ceph-csi-kms-token in the tenant namespace
        self.vault.vault_path_token = self.vault.generate_vault_token()
        self.vault.create_vault_csi_kms_token(namespace=BMO_NAME)

        # Deploy PGSQL workload
        logger.info("Deploying pgsql workloads")
        pgsql = pgsql_factory_fixture(replicas=1, sc_name=self.sc_obj.name)

        # Get postgres pvc list obj
        postgres_pvcs_obj = pgsql.get_postgres_pvc()

        # Take a snapshot of it
        snapshots, restore_pvc_objs = self.create_snapshot(
            pgsql,
            postgres_pvcs_obj,
            snapshot_factory,
            snapshot_restore_factory,
            self.sc_obj.name,
        )

        # Verify encryption keys are created for snapshots in Vault
        for snap_obj in snapshots:
            snapshot_content = get_snapshot_content_obj(snap_obj=snap_obj)
            snap_handle = snapshot_content.get().get("status").get("snapshotHandle")
            if kms.is_key_present_in_path(
                key=snap_handle, path=self.vault.vault_backend_path
            ):
                logger.info(f"Vault: Found key for snapshot {snap_obj.name}")
            else:
                raise ResourceNotFoundError(
                    f"Vault: Key not found for snapshot {snap_obj.name}"
                )

        # Verify encryption keys are created for restored PVCs in Vault
        for pvc_obj in restore_pvc_objs:
            pv_obj = pvc_obj.backed_pv_obj
            vol_handle = pv_obj.get().get("spec").get("csi").get("volumeHandle")
            if kms.is_key_present_in_path(
                key=vol_handle, path=self.vault.vault_backend_path
            ):
                logger.info(f"Vault: Found key for restore PVC {pvc_obj.name}")
            else:
                raise ResourceNotFoundError(
                    f"Vault: Key not found for restored PVC {pvc_obj.name}"
                )
