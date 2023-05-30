import logging
import pytest
import os

from ocs_ci.ocs.node import get_nodes
from ocs_ci.utility import utils, templating
from ocs_ci.utility.utils import ocsci_log_path
from ocs_ci.ocs.scale_lib import FioPodScale
from ocs_ci.helpers import disruption_helpers
from ocs_ci.ocs.resources.pod import wait_for_storage_pods
from ocs_ci.ocs import constants, scale_lib, platform_nodes, machine
from ocs_ci.framework.testlib import scale, E2ETest, ignore_leftovers
from ocs_ci.framework.pytest_customization.marks import (
    skipif_external_mode,
    ipi_deployment_required,
    skipif_vsphere_ipi,
    bugzilla,
)

log = logging.getLogger(__name__)

# Scale data file
log_path = ocsci_log_path()
SCALE_DATA_FILE = f"{log_path}/scale_3ocs_worker_data_file.yaml"


@pytest.fixture(scope="session")
def fioscale(request):
    """
    FIO Scale fixture to create expected number of POD+PVC
    """

    scale_pvc = 1500
    pvc_per_pod_count = 20

    # Scale FIO pods in the cluster
    fioscale = FioPodScale(
        kind=constants.DEPLOYMENTCONFIG, node_selector=constants.SCALE_NODE_SELECTOR
    )
    kube_pod_obj_list, kube_pvc_obj_list = fioscale.create_scale_pods(
        scale_count=scale_pvc, pvc_per_pod_count=pvc_per_pod_count
    )

    scale_lib.collect_scale_data_in_file(
        namespace=fioscale.namespace,
        kube_pod_obj_list=kube_pod_obj_list,
        kube_pvc_obj_list=kube_pvc_obj_list,
        scale_count=scale_pvc,
        pvc_per_pod_count=pvc_per_pod_count,
        scale_data_file=SCALE_DATA_FILE,
    )

    def teardown():
        fioscale.cleanup()

    request.addfinalizer(teardown)
    return fioscale


@scale
@ignore_leftovers
@skipif_external_mode
@ipi_deployment_required
@pytest.mark.parametrize(
    argnames="resource_to_delete",
    argvalues=[
        pytest.param(*["mgr"], marks=[pytest.mark.polarion_id("OCS-766")]),
        pytest.param(*["mon"], marks=[pytest.mark.polarion_id("OCS-764")]),
        pytest.param(*["osd"], marks=[pytest.mark.polarion_id("OCS-765")]),
        pytest.param(*["mds"], marks=[pytest.mark.polarion_id("OCS-613")]),
    ],
)
class TestScaleRespinCephPods(E2ETest):
    """
    Scale the OCS cluster to reach 1500 PVC+POD
    """

    def test_pv_scale_out_create_pvcs_and_respin_ceph_pods(
        self,
        fioscale,
        resource_to_delete,
    ):
        """
        Test case to scale PVC+POD with multi projects and reach expected PVC count
        """

        # Get info from SCALE_DATA_FILE for validation
        if os.path.exists(SCALE_DATA_FILE):
            file_data = templating.load_yaml(SCALE_DATA_FILE)
            namespace = file_data.get("NAMESPACE")
            pod_scale_list = file_data.get("POD_SCALE_LIST")
            pvc_scale_list = file_data.get("PVC_SCALE_LIST")
        else:
            raise FileNotFoundError

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


@scale
@ignore_leftovers
@bugzilla("2092737")
@skipif_external_mode
@ipi_deployment_required
@pytest.mark.parametrize(
    argnames="resource_to_delete",
    argvalues=[
        pytest.param(*["noobaa_operator"], marks=[pytest.mark.polarion_id("OCS-4880")]),
        pytest.param(*["ocs_operator"], marks=[pytest.mark.polarion_id("OCS-4881")]),
        pytest.param(*["odf_operator"], marks=[pytest.mark.polarion_id("OCS-4882")]),
        pytest.param(*["operator"], marks=[pytest.mark.polarion_id("OCS-4883")]),
    ],
)
class TestScaleRespinOperatorPods(E2ETest):
    """
    Respin Operator pods in cluster with 1500+ PVC & 76 Pods
    operator ==> OPERATOR_LABEL = "app=rook-ceph-operator"
    ocs-operator ==> OCS_OPERATOR_LABEL = "name=ocs-operator"
    odf-operator ==> ODF_OPERATOR_CONTROL_MANAGER_LABEL = "control-plane=controller-manager"
    noobaa-operator ==> NOOBAA_OPERATOR_POD_LABEL = "noobaa-operator=deployment"
    """

    def test_respin_operator_pods(
        self,
        fioscale,
        resource_to_delete,
    ):
        """
        Test case to respin operator pods in cluster with scaled PVC+POD's counts
        """

        # Get info from SCALE_DATA_FILE for validation
        if os.path.exists(SCALE_DATA_FILE):
            file_data = templating.load_yaml(SCALE_DATA_FILE)
            namespace = file_data.get("NAMESPACE")
            pod_scale_list = file_data.get("POD_SCALE_LIST")
            pvc_scale_list = file_data.get("PVC_SCALE_LIST")
        else:
            raise FileNotFoundError("scale data file unavailable")

        for itr in range(5):
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

            log.info(
                f"Iteration {itr} respin successful for operator {resource_to_delete}"
            )


@scale
@ignore_leftovers
@skipif_external_mode
@skipif_vsphere_ipi
@ipi_deployment_required
@pytest.mark.parametrize(
    argnames=["node_type"],
    argvalues=[
        pytest.param(
            *[constants.MASTER_MACHINE], marks=pytest.mark.polarion_id("OCS-761")
        ),
        pytest.param(
            *[constants.WORKER_MACHINE], marks=pytest.mark.polarion_id("OCS-762")
        ),
    ],
)
class TestRebootNodes(E2ETest):
    """
    Reboot nodes in scaled up cluster
    """

    def test_rolling_reboot_node(self, node_type):
        """
        Test to rolling reboot of nodes
        """
        node_list = list()

        # Get info from SCALE_DATA_FILE for validation
        if os.path.exists(SCALE_DATA_FILE):
            file_data = templating.load_yaml(SCALE_DATA_FILE)
            namespace = file_data.get("NAMESPACE")
            pod_scale_list = file_data.get("POD_SCALE_LIST")
            pvc_scale_list = file_data.get("PVC_SCALE_LIST")
        else:
            raise FileNotFoundError

        # Rolling reboot nodes
        if node_type == constants.WORKER_MACHINE:
            tmp_list = get_nodes(node_type=node_type)
            ocs_node_list = machine.get_labeled_nodes(constants.OPERATOR_NODE_LABEL)
            for tmp in tmp_list:
                if tmp.name in ocs_node_list:
                    node_list.append(tmp)
        else:
            node_list = get_nodes(node_type=node_type)

        factory = platform_nodes.PlatformNodesFactory()
        nodes = factory.get_nodes_platform()

        for node in node_list:
            nodes.restart_nodes(nodes=[node])
            scale_lib.validate_node_and_oc_services_are_up_after_reboot()

        # Validate storage pods are running
        wait_for_storage_pods()

        # Validate cluster health ok and all pods are running
        assert utils.ceph_health_check(
            delay=180
        ), "Ceph health in bad state after node reboots"

        # Validate all PVCs from namespace are in Bound state
        assert scale_lib.validate_all_pvcs_and_check_state(
            namespace=namespace, pvc_scale_list=pvc_scale_list
        )

        # Validate all PODs from namespace are up and running
        assert scale_lib.validate_all_pods_and_check_state(
            namespace=namespace, pod_scale_list=pod_scale_list
        )
