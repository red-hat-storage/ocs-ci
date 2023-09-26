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
)
from ocs_ci.ocs.constants import CEPHBLOCKPOOL
from ocs_ci.ocs.benchmark_operator import BMO_NAME
from ocs_ci.ocs.node import drain_nodes, schedule_nodes
from ocs_ci.helpers.disruption_helpers import Disruptions
from ocs_ci.ocs import flowtest

log = logging.getLogger(__name__)


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
        log.info("Starting multiple creation & clone of postgres PVC in Background")
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
        log.info("Started creation of snapshots & clones in background")

        flow_ops = flowtest.FlowOperations()
        log.info("Starting operation 1: Pod Restarts")
        disruption = Disruptions()
        pod_obj_list = [
            "osd",
            "mon",
            "mgr",
            "operator",
            "rbdplugin",
            "rbdplugin_provisioner",
        ]
        for pod in pod_obj_list:
            disruption.set_resource(resource=f"{pod}")
            disruption.delete_resource()
        log.info("Verifying exit criteria for operation 1: Pod Restarts")
        flow_ops.validate_cluster(
            node_status=True, pod_status=True, operation_name="Pod Restarts"
        )

        log.info("Starting operation 2: Node Reboot")
        node_names = flow_ops.node_operations_entry_criteria(
            node_type="worker", number_of_nodes=3, operation_name="Node Reboot"
        )
        # Reboot node
        nodes.restart_nodes(node_names)
        log.info("Verifying exit criteria for operation 2: Node Reboot")
        flow_ops.validate_cluster(
            node_status=True, pod_status=True, operation_name="Node Reboot"
        )

        log.info("Starting operation 3: Node Drain")
        node_name = flow_ops.node_operations_entry_criteria(
            node_type="worker", number_of_nodes=1, operation_name="Node Drain"
        )
        # Node maintenance - to gracefully terminate all pods on the node
        drain_nodes([node_name[0].name])
        # Make the node schedulable again
        schedule_nodes([node_name[0].name])
        log.info("Verifying exit criteria for operation 3: Node Drain")
        flow_ops.validate_cluster(
            node_status=True, pod_status=True, operation_name="Node Drain"
        )

        log.info("Waiting for background operations to be completed")
        bg_handler.wait_for_bg_operations([pgsql_snapshot_and_clone], timeout=600)

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
        # Deploy PGSQL workload
        log.info("Deploying pgsql workloads")
        self.pgsql = pgsql_factory_fixture(replicas=1)

        self.run_in_bg(nodes, multiple_snapshot_and_clone_of_postgres_pvc_factory)

    @skipif_ocs_version("<4.9")
    @skipif_ocp_version("<4.9")
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
        self.pgsql = pgsql_factory_fixture(replicas=1, sc_name=self.sc_obj.name)

        self.run_in_bg(
            nodes, multiple_snapshot_and_clone_of_postgres_pvc_factory, self.sc_obj.name
        )
