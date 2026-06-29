import logging
import pytest
import time

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
from ocs_ci.ocs.constants import STATUS_COMPLETED, VOLUME_MODE_FILESYSTEM, CEPHBLOCKPOOL
from ocs_ci.ocs.exceptions import ResourceNotFoundError
from ocs_ci.ocs.resources.pod import get_pod_obj
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.utility import kms

logger = logging.getLogger(__name__)


@magenta_squad
@tier2
@skipif_proxy_cluster
@skipif_disconnected_cluster
class TestPvcCloneOfWorkloads(E2ETest):
    """
    Tests to create multiple clones of same pgsql PVC at different utilization
    """

    @pytest.fixture()
    def pgsql_teardown(self, request):
        self.sset_list = []

        def teardown():

            # Delete created postgres and pgbench pods
            logger.info("Deleting postgres pods which are attached to restored PVCs")
            for pgsql_obj in self.sset_list:
                pgsql_obj.delete()

        request.addfinalizer(teardown)

    def create_cloned_pvc_and_verify_data(
        self,
        pgsql,
        postgres_pvcs_obj,
        postgres_pods_obj,
        pvc_clone_factory,
        sc_name=None,
    ):
        logger.info(f"Creating and verifying {len(postgres_pvcs_obj)} cloned PVCs")
        for i in range(3):
            logger.info(f"Creating clones of {len(postgres_pvcs_obj)} Postgres PVC(s)")
            cloned_pvcs = [
                pvc_clone_factory(
                    pvc_obj, volume_mode=VOLUME_MODE_FILESYSTEM, storageclass=sc_name
                )
                for pvc_obj in postgres_pvcs_obj
            ]
            logger.info(f"Created {len(cloned_pvcs)} cloned PVC(s), all in Bound state")

            logger.info(
                f"Attaching new PostgreSQL pods to {len(cloned_pvcs)} cloned PVCs: postgres-cloned-{i}"
            )
            self.pgsql_obj_list = pgsql.attach_pgsql_pod_to_claim_pvc(
                pvc_objs=cloned_pvcs,
                postgres_name=f"postgres-cloned-{i}",
                run_benchmark=False,
            )
            self.sset_list.extend(self.pgsql_obj_list)
            logger.info(
                f"Attached {len(self.pgsql_obj_list)} PostgreSQL pods to cloned PVCs"
            )

            logger.info("Getting file space usage from parent PostgreSQL pods")
            parent_pods_obj = pgsql.get_postgres_used_file_space(postgres_pods_obj)

            logger.info(f"Getting cloned pod objects with pattern: postgres-cloned-{i}")
            cloned_pods_list = get_pod_name_by_pattern(
                pattern=f"postgres-cloned-{i}", namespace=BMO_NAME
            )
            cloned_pods_obj = [
                get_pod_obj(name=pods, namespace=BMO_NAME) for pods in cloned_pods_list
            ]
            logger.info(
                f"Found {len(cloned_pods_obj)} cloned pod(s), getting file space usage"
            )
            cloned_obj = pgsql.get_postgres_used_file_space(cloned_pods_obj)
            for pod_obj in parent_pods_obj:
                if (
                    pod_obj.filespace
                    != cloned_obj[parent_pods_obj.index(pod_obj)].filespace
                ):
                    # ToDo: Before clone need to check data is synced
                    if (
                        not abs(
                            int(pod_obj.filespace.strip("M"))
                            - int(
                                cloned_obj[
                                    parent_pods_obj.index(pod_obj)
                                ].filespace.strip("M")
                            )
                        )
                        < 3
                    ):
                        raise Exception(
                            f"Parent pvc {pod_obj.name} used file space is {pod_obj.filespace}. "
                            f"And for cloned pvc {cloned_obj[parent_pods_obj.index(pod_obj)].name} "
                            f"used file space is {cloned_obj[parent_pods_obj.index(pod_obj)].filespace}"
                        )
                    logger.warning(
                        f"File space mismatch within tolerance (<3M): "
                        f"Parent {pod_obj.name}={pod_obj.filespace}, "
                        f"Cloned {cloned_obj[parent_pods_obj.index(pod_obj)].name}="
                        f"{cloned_obj[parent_pods_obj.index(pod_obj)].filespace}"
                    )
            logger.info("All cloned PVCs match parent PVC data")

            logger.info(f"Creating pgbench benchmark on parent PVC: pgbench-{i}")
            pgsql.create_pgbench_benchmark(
                replicas=1, pgbench_name=f"pgbench-{i}", wait=False
            )
            logger.info(f"pgbench-{i} benchmark created")

            wait_time = 180
            logger.info(f"Waiting {wait_time}s for pgbench client pods to start")
            time.sleep(180)

            logger.info(
                f"Waiting for pgbench-{i} benchmark to reach Completed state (timeout: 1800s)"
            )
            pgsql.wait_for_pgbench_status(status=STATUS_COMPLETED, timeout=1800)
            logger.info(f"pgbench-{i} benchmark completed successfully")

            return cloned_pvcs

    @skipif_ocs_version("<4.6")
    @skipif_ocp_version("<4.6")
    @pytest.mark.polarion_id("OCS-2342")
    def test_pvc_clone(self, pgsql_factory_fixture, pvc_clone_factory, pgsql_teardown):
        """
        1. Deploy PGSQL workload
        2. Create multiple clone of same PVC when the PVC usage is different
        3. Attach a new pgsql pod to it.
        4. Create pgbench benchmark to new pgsql pod
        """
        pgsql_teardown

        logger.test_step("Deploy PostgreSQL workload")
        logger.info("Deploying PostgreSQL workload with 1 replica")
        pgsql = pgsql_factory_fixture(replicas=1)
        logger.info("PostgreSQL workload deployed successfully")

        logger.test_step("Get PostgreSQL PVCs and pods")
        postgres_pvcs_obj = pgsql.get_postgres_pvc()
        logger.info(f"Retrieved {len(postgres_pvcs_obj)} PostgreSQL PVC(s)")

        postgres_pods_obj = pgsql.get_postgres_pods()
        logger.info(f"Retrieved {len(postgres_pods_obj)} PostgreSQL pod(s)")

        logger.test_step("Create clones and verify data integrity")
        self.create_cloned_pvc_and_verify_data(
            pgsql, postgres_pvcs_obj, postgres_pods_obj, pvc_clone_factory
        )
        logger.info("PVC clone workflow completed successfully")

    @skipif_ocs_version("<4.8")
    @skipif_ocp_version("<4.8")
    @skipif_hci_provider_and_client
    @pytest.mark.parametrize(
        argnames=["kv_version"],
        argvalues=[
            pytest.param("v1", marks=pytest.mark.polarion_id("OCS-2715")),
            pytest.param("v2", marks=pytest.mark.polarion_id("OCS-2708")),
        ],
    )
    def test_encrypted_pvc_clone(
        self,
        kv_version,
        pv_encryption_kms_setup_factory,
        storageclass_factory,
        pgsql_factory_fixture,
        pvc_clone_factory,
        pgsql_teardown,
    ):
        """
        1. Create encrypted storage class
        2. Deploy PGSQL workload using created sc
        3. Create multiple clone of same PVC when the PVC usage is different
        4. Attach a new pgsql pod to it.
        5. Create pgbench benchmark to new pgsql pod
        6. Verify if key is created for cloned pvc
        """
        pgsql_teardown

        logger.test_step(f"Setup KMS encryption with Vault KV version: {kv_version}")
        logger.info("Setting up csi-kms-connection-details configmap")
        self.vault = pv_encryption_kms_setup_factory(kv_version)
        logger.info(
            f"csi-kms-connection-details setup successful, KMS ID: {self.vault.kmsid}"
        )

        logger.test_step("Create encrypted storage class for RBD")
        self.sc_obj = storageclass_factory(
            interface=CEPHBLOCKPOOL,
            encrypted=True,
            encryption_kms_id=self.vault.kmsid,
        )
        logger.info(f"Created encrypted storage class: {self.sc_obj.name}")

        logger.test_step(f"Create Vault CSI KMS token in namespace: {BMO_NAME}")
        self.vault.vault_path_token = self.vault.generate_vault_token()
        self.vault.create_vault_csi_kms_token(namespace=BMO_NAME)
        logger.info(f"Created Vault CSI KMS token in namespace: {BMO_NAME}")

        logger.test_step("Deploy PostgreSQL workload with encrypted storage")
        logger.info(
            f"Deploying PostgreSQL workload with encrypted storage class: {self.sc_obj.name}"
        )
        pgsql = pgsql_factory_fixture(replicas=1, sc_name=self.sc_obj.name)
        logger.info("PostgreSQL workload deployed successfully with encryption")

        logger.test_step("Get PostgreSQL PVCs and pods")
        postgres_pvcs_obj = pgsql.get_postgres_pvc()
        logger.info(f"Retrieved {len(postgres_pvcs_obj)} PostgreSQL PVC(s)")

        postgres_pods_obj = pgsql.get_postgres_pods()
        logger.info(f"Retrieved {len(postgres_pods_obj)} PostgreSQL pod(s)")

        logger.test_step("Create encrypted clones and verify data integrity")
        cloned_pvcs = self.create_cloned_pvc_and_verify_data(
            pgsql,
            postgres_pvcs_obj,
            postgres_pods_obj,
            pvc_clone_factory,
            self.sc_obj.name,
        )
        logger.info(
            f"Created and verified {len(cloned_pvcs) if cloned_pvcs else 0} cloned PVC(s)"
        )

        logger.test_step("Verify encryption keys for cloned PVCs in Vault")
        logger.info(
            f"Verifying Vault encryption keys for {len(cloned_pvcs) if cloned_pvcs else 0} cloned PVC(s)"
        )
        logger.assertion(
            f"Expected at least one cloned PVC before Vault key verification, "
            f"actual count: {len(cloned_pvcs) if cloned_pvcs else 0}"
        )
        assert (
            cloned_pvcs
        ), "Expected at least one cloned PVC before Vault key verification"

        for idx, pvc_obj in enumerate(cloned_pvcs, 1):
            pv_obj = pvc_obj.backed_pv_obj
            vol_handle = pv_obj.get().get("spec").get("csi").get("volumeHandle")
            logger.debug(
                f"Checking Vault key {idx}/{len(cloned_pvcs)} for PVC: {pvc_obj.name}"
            )
            if kms.is_key_present_in_path(
                key=vol_handle, path=self.vault.vault_backend_path
            ):
                logger.info(f"Vault: Found key for cloned PVC {pvc_obj.name}")
            else:
                raise ResourceNotFoundError(
                    f"Vault: Key not found for cloned PVC {pvc_obj.name}"
                )
        logger.info(
            "All encryption keys verified in Vault - encrypted PVC clone workflow completed"
        )
