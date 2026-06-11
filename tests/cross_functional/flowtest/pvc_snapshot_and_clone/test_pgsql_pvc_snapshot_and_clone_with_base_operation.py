import logging
import pytest

from concurrent.futures import ThreadPoolExecutor
from ocs_ci.framework.pytest_customization.marks import magenta_squad
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    skipif_ocp_version,
    E2ETest,
    flowtests,
    ignore_leftovers,
    skipif_hci_provider_and_client,
)
from ocs_ci.ocs.constants import CEPHBLOCKPOOL
from ocs_ci.ocs.benchmark_operator import BMO_NAME
from ocs_ci.ocs.node import drain_nodes, schedule_nodes
from ocs_ci.helpers.disruption_helpers import Disruptions
from ocs_ci.ocs import flowtest

logger = logging.getLogger(__name__)


@magenta_squad
@flowtests
@ignore_leftovers
class TestPvcSnapshotAndCloneWithBaseOperation(E2ETest):
    """
    Tests Story/Flow based test scenario for pgsql snapshot and clone
    """

    def run_in_bg(
        self, nodes, multiple_snapshot_and_clone_of_postgres_pvc_factory, sc_name=None
    ):
        logger.test_step("Start background snapshot/clone operations")
        logger.info(
            "Starting multiple creation & clone of postgres PVC in background (target size: 25Gi)"
        )
        bg_handler = flowtest.BackgroundOps()
        executor_run_bg_ops = ThreadPoolExecutor(max_workers=1)
        pgsql_snapshot_and_clone = executor_run_bg_ops.submit(
            bg_handler.handler,
            multiple_snapshot_and_clone_of_postgres_pvc_factory,
            pvc_size_new=25,
            pgsql=self.pgsql,
            sc_name=sc_name,
            iterations=1,
        )
        logger.info("Background snapshot/clone operations started")

        flow_ops = flowtest.FlowOperations()
        logger.test_step("Operation 1: Pod Restarts")
        disruption = Disruptions()
        pod_obj_list = [
            "osd",
            "mon",
            "mgr",
            "operator",
            "rbdplugin",
            "rbdplugin_provisioner",
        ]
        logger.info(
            f"Restarting {len(pod_obj_list)} Ceph/OCS pods: {', '.join(pod_obj_list)}"
        )
        for pod in pod_obj_list:
            disruption.set_resource(resource=f"{pod}")
            disruption.delete_resource()
        logger.info("Verifying exit criteria for operation 1: Pod Restarts")
        flow_ops.validate_cluster(
            node_status=True, pod_status=True, operation_name="Pod Restarts"
        )
        logger.info("Operation 1 completed: Pod Restarts successful")

        logger.test_step("Operation 2: Node Reboot")
        node_names = flow_ops.node_operations_entry_criteria(
            node_type="worker", number_of_nodes=3, operation_name="Node Reboot"
        )
        logger.info(
            f"Rebooting {len(node_names)} worker node(s): {[n.name for n in node_names]}"
        )
        nodes.restart_nodes(node_names)
        logger.info("Verifying exit criteria for operation 2: Node Reboot")
        flow_ops.validate_cluster(
            node_status=True, pod_status=True, operation_name="Node Reboot"
        )
        logger.info("Operation 2 completed: Node Reboot successful")

        logger.test_step("Operation 3: Node Drain")
        node_name = flow_ops.node_operations_entry_criteria(
            node_type="worker", number_of_nodes=1, operation_name="Node Drain"
        )
        logger.info(f"Draining node: {node_name[0].name}")
        drain_nodes([node_name[0].name])
        logger.info(f"Making node schedulable again: {node_name[0].name}")
        schedule_nodes([node_name[0].name])
        logger.info("Verifying exit criteria for operation 3: Node Drain")
        flow_ops.validate_cluster(
            node_status=True, pod_status=True, operation_name="Node Drain"
        )
        logger.info("Operation 3 completed: Node Drain successful")

        logger.test_step("Wait for all background operations to complete")
        logger.info(
            "Waiting for background snapshot/clone operations to complete (timeout: 600s)"
        )
        bg_handler.wait_for_bg_operations([pgsql_snapshot_and_clone], timeout=600)
        logger.info("All background operations completed successfully")

    @skipif_ocs_version("<4.6")
    @skipif_ocp_version("<4.6")
    @pytest.mark.polarion_id("OCS-2302")
    def test_pvc_snapshot_and_clone(
        self,
        pgsql_factory_fixture,
        nodes,
        multiple_snapshot_and_clone_of_postgres_pvc_factory,
    ):
        """
        1. Deploy PGSQL workload
        2. Take a snapshot of the pgsql PVC.
        3. Create a new PVC out of that snapshot or restore snapshot
        4. Create a clone of restored snapshot
        5. Attach a new pgsql pod to it.
         5. Resize cloned pvc
        7. Create snapshots of cloned pvc and restore those snapshots
        8. Attach a new pgsql pod to it and Resize the new restored pvc
        9. Repeat the above steps in bg when performing base operation:
            restart pods, worker node reboot, node drain, device replacement

        """
        logger.test_step("Deploy PostgreSQL workload")
        logger.info("Deploying PostgreSQL workload with 1 replica")
        self.pgsql = pgsql_factory_fixture(replicas=1)
        logger.info("PostgreSQL workload deployed successfully")

        logger.test_step(
            "Execute disruption operations with background snapshot/clone workload"
        )
        self.run_in_bg(nodes, multiple_snapshot_and_clone_of_postgres_pvc_factory)

    @skipif_ocs_version("<4.9")
    @skipif_ocp_version("<4.9")
    @skipif_hci_provider_and_client
    @pytest.mark.parametrize(
        argnames=["kv_version"],
        argvalues=[
            pytest.param("v1", marks=pytest.mark.polarion_id("OCS-2711")),
            pytest.param("v2", marks=pytest.mark.polarion_id("OCS-2706")),
        ],
    )
    def test_encrypted_pvc_snapshot_and_clone(
        self,
        kv_version,
        pv_encryption_kms_setup_factory,
        storageclass_factory,
        pgsql_factory_fixture,
        nodes,
        multiple_snapshot_and_clone_of_postgres_pvc_factory,
    ):
        """
        1. Deploy PGSQL workload using encrypted sc
        2. Take a encrypted snapshot of the pgsql PVC.
        3. Create a new PVC out of that snapshot or restore snapshot
        4. Create a encrypted clone of restored snapshot
        5. Attach a new pgsql pod to it.
         5. Resize cloned pvc
        7. Create snapshots of cloned pvc and restore those snapshots
        8. Attach a new pgsql pod to it and Resize the new restored pvc
        9. Repeat the above steps in bg when performing base operation:
            restart pods, worker node reboot, node drain, device replacement

        """
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
        logger.info(f"Generated Vault token: {self.vault.vault_path_token[:20]}...")
        self.vault.create_vault_csi_kms_token(namespace=BMO_NAME)
        logger.info(f"Created Vault CSI KMS token in namespace: {BMO_NAME}")

        logger.test_step("Deploy PostgreSQL workload with encrypted storage")
        logger.info(
            f"Deploying PostgreSQL workload with encrypted storage class: {self.sc_obj.name}"
        )
        self.pgsql = pgsql_factory_fixture(replicas=1, sc_name=self.sc_obj.name)
        logger.info("PostgreSQL workload deployed successfully with encryption")

        logger.test_step(
            "Execute disruption operations with background encrypted snapshot/clone workload"
        )
        self.run_in_bg(
            nodes, multiple_snapshot_and_clone_of_postgres_pvc_factory, self.sc_obj.name
        )
