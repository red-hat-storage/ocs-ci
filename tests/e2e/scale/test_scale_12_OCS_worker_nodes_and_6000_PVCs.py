import logging
import pytest
import os

from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility import utils, templating
from ocs_ci.ocs import constants, scale_lib, machine
from ocs_ci.utility.utils import ocsci_log_path
from ocs_ci.ocs.scale_lib import FioPodScale
from ocs_ci.ocs.resources import storage_cluster
from ocs_ci.helpers import disruption_helpers
from ocs_ci.ocs.node import get_worker_nodes
from ocs_ci.framework.testlib import (
    scale_changed_layout,
    E2ETest,
    ignore_leftovers,
)
from ocs_ci.framework.pytest_customization.marks import (
    skipif_aws_i3,
    skipif_bm,
    skipif_external_mode,
    skipif_ibm_cloud,
    skipif_ibm_power,
    skipif_lso,
    ipi_deployment_required,
)
from ocs_ci.ocs.exceptions import UnexpectedBehaviour

logger = logging.getLogger(__name__)

# Scale data file
log_path = ocsci_log_path()
SCALE_DATA_FILE = f"{log_path}/scale_data_file.yaml"


@scale_changed_layout
@skipif_aws_i3
@skipif_bm
@skipif_lso
@skipif_ibm_cloud
@skipif_ibm_power
@skipif_external_mode
@ipi_deployment_required
@ignore_leftovers
class TestAddNode(E2ETest):
    """
    Automates adding worker nodes to the cluster while IOs
    """

    skip_all = False

    @pytest.mark.skipif("TestAddNode.skip_all")
    @pytest.mark.polarion_id("OCS-2610")
    def test_scale_node_and_capacity(self):
        """
        Test for scaling 12 OCS worker nodes to the cluster
        Scale 12*3 = 36 OSDs
        """

        expected_worker_count = 12
        osds_per_node = 3

        try:
            # Gather existing deviceset, OSD and node count in setup
            existing_ocs_worker_list = get_worker_nodes()
            existing_deviceset_count = storage_cluster.get_deviceset_count()
            osd_replication_count = storage_cluster.get_osd_replica_count()
            expected_deviceset_count = (
                expected_worker_count / osds_per_node
            ) * osd_replication_count

            # Check existing OCS worker node count and add nodes if required
            if len(existing_ocs_worker_list) < expected_worker_count:
                scale_worker_count = expected_worker_count - len(
                    existing_ocs_worker_list
                )
                assert scale_lib.scale_ocs_node(node_count=scale_worker_count)

            # Check existing OSD count and add OSDs if required
            if existing_deviceset_count < expected_deviceset_count:
                add_deviceset_count = (
                    expected_deviceset_count - existing_deviceset_count
                )
                assert scale_lib.scale_capacity_with_deviceset(
                    add_deviceset_count=add_deviceset_count
                )

            # Check ceph health statuss
            utils.ceph_health_check(tries=30)

        except UnexpectedBehaviour:
            TestAddNode.skip_all = True
            logging.info("Cluster is not in expected state, unexpected behaviour")
            raise

    @pytest.mark.skipif("TestAddNode.skip_all")
    @pytest.mark.polarion_id("OCS-609")
    def test_scale_pvcs_pods(self):
        """
        Scale 6000 PVCs and PODs in cluster with 12 worker nodes
        """

        scale_count = 6000
        pvcs_per_pod = 20

        try:
            # Scale
            fioscale = FioPodScale(
                kind=constants.DEPLOYMENTCONFIG,
                node_selector=constants.SCALE_NODE_SELECTOR,
            )
            kube_pod_obj_list, kube_pvc_obj_list = fioscale.create_scale_pods(
                scale_count=scale_count, pvc_per_pod_count=pvcs_per_pod
            )

            namespace = fioscale.namespace
            scale_round_up_count = scale_count + 80

            # Get PVCs and PODs count and list
            pod_running_list, pvc_bound_list = ([], [])
            for pod_objs in kube_pod_obj_list:
                pod_running_list.extend(
                    scale_lib.check_all_pod_reached_running_state_in_kube_job(
                        kube_job_obj=pod_objs,
                        namespace=namespace,
                        no_of_pod=int(scale_round_up_count / 160),
                    )
                )
            for pvc_objs in kube_pvc_obj_list:
                pvc_bound_list.extend(
                    scale_lib.check_all_pvc_reached_bound_state_in_kube_job(
                        kube_job_obj=pvc_objs,
                        namespace=namespace,
                        no_of_pvc=int(scale_round_up_count / 16),
                    )
                )

            logging.info(
                f"Running PODs count {len(pod_running_list)} & "
                f"Bound PVCs count {len(pvc_bound_list)} "
                f"in namespace {fioscale.namespace}"
            )

            # Get kube obj files in the list to update in scale_data_file
            pod_obj_file_list, pvc_obj_file_list = ([], [])
            files = os.listdir(ocsci_log_path())
            for f in files:
                if "pod" in f:
                    pod_obj_file_list.append(f)
                elif "pvc" in f:
                    pvc_obj_file_list.append(f)

            # Write namespace, PVC and POD data in a SCALE_DATA_FILE which
            # will be used during post_upgrade validation tests
            with open(SCALE_DATA_FILE, "a+") as w_obj:
                w_obj.write(str("# Scale Data File\n"))
                w_obj.write(str(f"NAMESPACE: {namespace}\n"))
                w_obj.write(str(f"POD_SCALE_LIST: {pod_running_list}\n"))
                w_obj.write(str(f"PVC_SCALE_LIST: {pvc_bound_list}\n"))
                w_obj.write(str(f"POD_OBJ_FILE_LIST: {pod_obj_file_list}\n"))
                w_obj.write(str(f"PVC_OBJ_FILE_LIST: {pvc_obj_file_list}\n"))

            # Check ceph health status
            utils.ceph_health_check(tries=30)

        except UnexpectedBehaviour:
            TestAddNode.skip_all = True
            logging.info("Cluster is not in expected state, unexpected behaviour")
            raise

    @ignore_leftovers
    @pytest.mark.skipif("TestAddNode.skip_all")
    @pytest.mark.parametrize(
        argnames="resource_to_delete",
        argvalues=[
            pytest.param(*["mgr"], marks=[pytest.mark.polarion_id("OCS-766")]),
            pytest.param(*["mon"], marks=[pytest.mark.polarion_id("OCS-669")]),
            pytest.param(*["osd"], marks=[pytest.mark.polarion_id("OCS-610")]),
            pytest.param(*["mds"], marks=[pytest.mark.polarion_id("OCS-613")]),
            pytest.param(
                *["cephfsplugin"], marks=[pytest.mark.polarion_id("OCS-1891")]
            ),
            pytest.param(*["rbdplugin"], marks=[pytest.mark.polarion_id("OCS-1891")]),
        ],
    )
    def test_respin_ceph_pods(self, resource_to_delete):
        """
        Test re-spin of Ceph daemond pods, Operator and CSI Pods
        in Scaled cluster
        """

        # Get info from SCALE_DATA_FILE for validation
        if os.path.exists(SCALE_DATA_FILE):
            file_data = templating.load_yaml(SCALE_DATA_FILE)
            namespace = file_data.get("NAMESPACE")
            pod_scale_list = file_data.get("POD_SCALE_LIST")
            pvc_scale_list = file_data.get("PVC_SCALE_LIST")
        else:
            raise FileNotFoundError

        # perform disruption test
        disruption = disruption_helpers.Disruptions()
        disruption.set_resource(resource=resource_to_delete)
        no_of_resource = disruption.resource_count
        for i in range(0, no_of_resource):
            disruption.delete_resource(resource_id=i)

        utils.ceph_health_check()

        # Validate all PVCs from namespace are in Bound state
        assert scale_lib.validate_all_pvcs_and_check_state(
            namespace=namespace, pvc_scale_list=pvc_scale_list
        )

        # Validate all PODs from namespace are up and running
        assert scale_lib.validate_all_pods_and_check_state(
            namespace=namespace, pod_scale_list=pod_scale_list
        )

        # Check ceph health status
        utils.ceph_health_check(tries=20)

    @ignore_leftovers
    @pytest.mark.skipif("TestAddNode.skip_all")
    def test_add_node_cleanup(self):
        """
        Test to cleanup possible resources created in TestAddNode class
        """

        # Get info from SCALE_DATA_FILE for validation
        if os.path.exists(SCALE_DATA_FILE):
            file_data = templating.load_yaml(SCALE_DATA_FILE)
            namespace = file_data.get("NAMESPACE")
            pod_obj_file_list = file_data.get("POD_OBJ_FILE_LIST")
            pvc_obj_file_list = file_data.get("PVC_OBJ_FILE_LIST")
        else:
            raise FileNotFoundError

        ocs_obj = OCP(namespace=namespace)

        # Delete pods
        for obj_file in pod_obj_file_list:
            obj_file_path = f"{log_path}/{obj_file}"
            cmd_str = f"delete -f {obj_file_path}"
            ocs_obj.exec_oc_cmd(command=cmd_str)

        # Delete pvcs
        for obj_file in pvc_obj_file_list:
            obj_file_path = f"{log_path}/{obj_file}"
            cmd_str = f"delete -f {obj_file_path}"
            ocs_obj.exec_oc_cmd(command=cmd_str)

        # Delete machineset
        for obj in machine.get_machineset_objs():
            if "app" in obj.name:
                machine.delete_custom_machineset(obj.name)
