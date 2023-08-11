import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import magenta_squad
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    skipif_ocp_version,
    E2ETest,
    flowtests,
)
from ocs_ci.ocs.constants import CEPHBLOCKPOOL
from ocs_ci.ocs.benchmark_operator import BMO_NAME


log = logging.getLogger(__name__)


@magenta_squad
@flowtests
class TestPvcSnapshotAndCloneWithBaseOperation(E2ETest):
    """
    Tests Story/Flow based test scenario for pgsql snapshot and clone
    """

    @skipif_ocs_version("<4.6")
    @skipif_ocp_version("<4.6")
    @pytest.mark.polarion_id("OCS-2310")
    def test_pvc_snapshot_and_clone(
        self, pgsql_factory_fixture, multiple_snapshot_and_clone_of_postgres_pvc_factory
    ):
        """
        1. Deploy PGSQL workload
        2. Take a snapshot of the pgsql PVC.
        3. Create a new PVC out of that snapshot or restore snapshot
        4. Create a clone of restored snapshot
        5. Attach a new pgsql pod to it.
        6. Resize cloned pvc
        7. Create snapshots of cloned pvc and restore those snapshots
        8. Attach a new pgsql pod to it and Resize the new restored pvc

        """

        # Deploy PGSQL workload
        log.info("Deploying pgsql workloads")
        pgsql = pgsql_factory_fixture(replicas=1)

        log.info("Starting multiple creation & clone of postgres PVC")
        multiple_snapshot_and_clone_of_postgres_pvc_factory(
            pvc_size_new=25, pgsql=pgsql
        )

    @skipif_ocs_version("<4.9")
    @skipif_ocp_version("<4.9")
    @pytest.mark.parametrize(
        argnames=["kv_version"],
        argvalues=[
            pytest.param("v1", marks=pytest.mark.polarion_id("OCS-2709")),
            pytest.param("v2", marks=pytest.mark.polarion_id("OCS-2710")),
        ],
    )
    def test_encrypted_pvc_snapshot_and_clone(
        self,
        kv_version,
        pv_encryption_kms_setup_factory,
        storageclass_factory,
        pgsql_factory_fixture,
        multiple_snapshot_and_clone_of_postgres_pvc_factory,
    ):
        """
        1. Deploy PGSQL workload using encrypted storageclass
        2. Take a snapshot of the pgsql PVC.
        3. Create a new PVC out of that snapshot or restore snapshot
        4. Create a clone of restored snapshot
        5. Attach a new pgsql pod to it.
        6. Resize cloned pvc
        7. Create snapshots of cloned pvc and restore those snapshots
        8. Attach a new pgsql pod to it and Resize the new restored pvc

        """
        log.info("Setting up csi-kms-connection-details configmap")
        self.vault = pv_encryption_kms_setup_factory(kv_version)
        log.info("csi-kms-connection-details setup successful")

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
        log.info("Deploying pgsql workloads")
        pgsql = pgsql_factory_fixture(replicas=1, sc_name=self.sc_obj.name)

        log.info("Starting multiple creation & clone of postgres PVC")
        multiple_snapshot_and_clone_of_postgres_pvc_factory(
            pvc_size_new=25, pgsql=pgsql, sc_name=self.sc_obj.name
        )
