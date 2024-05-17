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

log = logging.getLogger(__name__)


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
            log.info("Deleting postgres pods which are attached to restored PVCs")
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

        for i in range(3):

            # Create clone of pgsql pvc
            log.info("Creating clone of the Postgres PVCs")
            cloned_pvcs = [
                pvc_clone_factory(
                    pvc_obj, volume_mode=VOLUME_MODE_FILESYSTEM, storageclass=sc_name
                )
                for pvc_obj in postgres_pvcs_obj
            ]
            log.info("Created clone of the PVCs and all cloned PVCs are in Bound state")

            # Attach to new postgres pod
            self.pgsql_obj_list = pgsql.attach_pgsql_pod_to_claim_pvc(
                pvc_objs=cloned_pvcs,
                postgres_name=f"postgres-cloned-{i}",
                run_benchmark=False,
            )
            self.sset_list.extend(self.pgsql_obj_list)

            # Get usage of pgsql pvc
            parent_pods_obj = pgsql.get_postgres_used_file_space(postgres_pods_obj)

            # Validate cloned pvcs file space matches with parent
            cloned_pods_list = get_pod_name_by_pattern(
                pattern=f"postgres-cloned-{i}", namespace=BMO_NAME
            )
            cloned_pods_obj = [
                get_pod_obj(name=pods, namespace=BMO_NAME) for pods in cloned_pods_list
            ]
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
                    log.warn(
                        f"Parent pvc {pod_obj.name} used file space is {pod_obj.filespace}. "
                        f"And for cloned pvc {cloned_obj[parent_pods_obj.index(pod_obj)].name} "
                        f"used file space is {cloned_obj[parent_pods_obj.index(pod_obj)].filespace}"
                    )
            log.info("All cloned PVC matches the parent PVC data")

            # Run benchmark on parent PVC
            pgsql.create_pgbench_benchmark(
                replicas=1, pgbench_name=f"pgbench-{i}", wait=False
            )

            # Wait till pgbench client pods up
            wait_time = 180
            log.info(f"Waiting {wait_time} seconds for pgbench client pods to be up")
            time.sleep(180)

            # Wait for pg_bench pod to initialized and complete
            log.info("Checking all pgbench benchmark reached Completed state")
            pgsql.wait_for_pgbench_status(status=STATUS_COMPLETED, timeout=1800)

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

        # Deploy PGSQL workload
        log.info("Deploying pgsql workloads")
        pgsql = pgsql_factory_fixture(replicas=1)

        # Get postgres pvcs obj list
        postgres_pvcs_obj = pgsql.get_postgres_pvc()

        # Get postgres pods obj list
        postgres_pods_obj = pgsql.get_postgres_pods()

        self.create_cloned_pvc_and_verify_data(
            pgsql, postgres_pvcs_obj, postgres_pods_obj, pvc_clone_factory
        )

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

        # Get postgres pvcs obj list
        postgres_pvcs_obj = pgsql.get_postgres_pvc()

        # Get postgres pods obj list
        postgres_pods_obj = pgsql.get_postgres_pods()

        cloned_pvcs = self.create_cloned_pvc_and_verify_data(
            pgsql,
            postgres_pvcs_obj,
            postgres_pods_obj,
            pvc_clone_factory,
            self.sc_obj.name,
        )

        # Verify encryption keys are created for cloned PVCs in Vault
        for pvc_obj in cloned_pvcs:
            pv_obj = pvc_obj.backed_pv_obj
            vol_handle = pv_obj.get().get("spec").get("csi").get("volumeHandle")
            if kms.is_key_present_in_path(
                key=vol_handle, path=self.vault.vault_backend_path
            ):
                log.info(f"Vault: Found key for restore PVC {pvc_obj.name}")
            else:
                raise ResourceNotFoundError(
                    f"Vault: Key not found for restored PVC {pvc_obj.name}"
                )
