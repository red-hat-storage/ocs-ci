import logging
import pytest
import json
import time

from ocs_ci.ocs import ocp, constants
from ocs_ci.ocs.resources import pod
from ocs_ci.utility import templating
from ocs_ci.ocs.resources.pod import (
    get_ceph_tools_pod,
    get_pods_having_label,
    get_operator_pods,
)
from ocs_ci.helpers import helpers
from ocs_ci.helpers.helpers import (
    set_watch_for_node_failure_rook_ceph_operator,
    get_last_log_time_date,
)

from ocs_ci.framework.testlib import (
    E2ETest,
    ignore_leftovers,
    skipif_hci_provider_and_client,
)
from ocs_ci.ocs.node import (
    taint_nodes,
    untaint_nodes,
    get_node_objs,
)
from ocs_ci.framework.pytest_customization.marks import bugzilla, brown_squad
from ocs_ci.framework.testlib import skipif_ocs_version
from ocs_ci.helpers.sanity_helpers import Sanity

log = logging.getLogger(__name__)


@brown_squad
@ignore_leftovers
@skipif_hci_provider_and_client
@skipif_ocs_version("<=4.16")
@bugzilla("1992472")
@pytest.mark.polarion_id("OCS-2705")
class TestVolumeRecoveryPostTaint(E2ETest):
    """
    Test to check CephFS and RBD volume recovery post Noschedule node taints
    """
    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance
        """
        self.sanity_helpers = Sanity()


    def get_all_networkfences(self):
        """
        Function to get all network fences that got created upon tainting the node with Noschedule taint

        Returns:
            List of network fences that are created.

        """
        get_nf_cmd = "get networkfence -o json"
        oc_obj = ocp.OCP()
        nf_wide_out = oc_obj.exec_oc_cmd(get_nf_cmd, out_yaml_format=False)
        nf_out_json = json.loads(nf_wide_out)
        networkfence_creation = False
        self.CIDR_IP = []
        if (self.interface == constants.CEPHFILESYSTEM):
            if not nf_out_json["items"]:
                log.info("Looking for message no active mds in rook ceph operator log")
                target_log = "ceph-cluster-controller: no active mds clients found for cephfs subvolume"
                operator_logs = pod.get_pod_logs(pod_name=self.rook_operator_pod_name)
                target_log_found = target_log in operator_logs
                log.info(target_log_found)
            else:
                for item in nf_out_json["items"]:
                    if item["spec"]["driver"] == constants.NFCEPHFSDRIVER:
                        if item["spec"]["fenceState"] == "Fenced" and item["status"]["result"] == "Succeeded":
                            self.CIDR_IP.append(item["spec"]["cidrs"])
                            networkfence_creation = True
                            break
                if networkfence_creation:
                    log.info("Network fence is created and is in succeded state post tainting the node with Noschedule taint")
                    return True
                else:
                    log.error("Failed to create networkfence post tainting the node with Noschedule taint")
                    return False
        else:
            for item in nf_out_json["items"]:
                if item["spec"]["driver"] == constants.NFRBDDRIVER:
                    if nf["spec"]["fenceState"] == "Fenced" and nf["status"]["result"] == "Succeeded":
                        networkfece_creation = True
                        break
            if networkfece_creation:
                log.info("Network fence is created and is in succeded state post tainting the node with Noschedule taint")
                return True
            else:
                log.error("Failed to create networkfence post tainting the node with Noschedule taint")
                return False

    def check_cidr_is_blocklisted(self, CIDR_untaint=None):
        """
        Function to check if CIDR IPs from network fence is block listed
        Returns:
        """
        is_present = False
        ct_pod = get_ceph_tools_pod()
        tree_output = ct_pod.exec_ceph_cmd(
            ceph_cmd="ceph osd blocklist ls", format="yaml"
        )
        log.info(f"Tree{tree_output}")
        log.info(f"### OSD blocklist output = {tree_output}")
        if self.interface == constants.CEPHFILESYSTEM:
            if not self.CIDR_IP:
                self.CIDR_IP = CIDR_untaint
            for CIDR in self.CIDR_IP:
                log.info(f"CIDRR{CIDR}")
                CIDR_IP = CIDR[0].split('/')[0]
                is_present = CIDR_IP in tree_output
            if is_present:
                return True
            elif CIDR_untaint:
                return False
            else:
                start_log_datetime = get_last_log_time_date()
                status = helpers.get_rook_ceph_pod_events_by_keyword(
                    "no active mds clients found for cephfs subvolume"
                )
                last_pod_event_line = status[-1]
                last_pod_event_datetime = status(last_pod_event_line)
                if last_pod_event_datetime > start_log_datetime:
                    log.info(
                        "As no MDS clients were there networkfence will not be created"
                    )
                    return True
                else:
                    return False

    @pytest.mark.parametrize(
        argnames=["interface_type"],
        argvalues=[
            pytest.param(
                constants.CEPHFILESYSTEM, marks=pytest.mark.polarion_id("OCS-XXXX")
            ),
        ],
    )
    def test_networkfence_on_rbd_and_cephfs(self, interface_type, ):
        """
        Test runs the following steps
        1. Create deployment pods
        2. Taint the node on which deployment pod is running
        3. Check that network fence is created and CIDR IP is added to blocklist IPs
        4. Wait for the pod to come up on new node
        5. Remove the taint
        6. Network fence should be deleted and CIDR block listed IP should be removed
        """
        self.namespace = "default"
        self.interface = interface_type
        try:
            pvc_obj = helpers.create_pvc(
                sc_name=constants.CEPHFILESYSTEM_SC,
                namespace=self.namespace,
                pvc_name="logwriter-cephfs-once",
                size="10Gi",
                access_mode=constants.ACCESS_MODE_RWO,
                volume_mode="Filesystem",
            )

            # Create deployment for app pod
            log.info("----Creating deployment ---")
            deployment_data = templating.load_yaml(constants.LOGWRITER_CEPH_FS_POD_YAML)
            helpers.create_resource(**deployment_data)
            time.sleep(60)
            log.info("All the workloads pods are successfully up and running")
            pods = get_pods_having_label(label=constants.LOGWRITER_CEPHFS_LABEL, namespace=self.namespace)
            pod_name = []
            taint_node = []
            for podd in pods:
                node_name = podd.get("spec").get("nodeName")
                pod_name.append(podd.get("metadata").get("name"))
                taint_node.append(node_name)
            self.interface = interface_type
            log.info(
                f"Taint worker node {node_name}with nodeshutdown:NoExecute taint"
            )

            taint_nodes(
                nodes=taint_node,
                taint_label="node.kubernetes.io/out-of-service=nodeshutdown:NoExecute",
            )
            log.info("Inducing a delay of two minutes as rook ceph operator pod might get restarted post node taint")
            time.sleep(120)

            rook_operator_pods = get_operator_pods()
            self.rook_operator_pod = rook_operator_pods[0]
            self.rook_operator_pod_name = self.rook_operator_pod.name
            log.info(f"rook operator pod found: {self.rook_operator_pod}")
            time.sleep(60)

            # Get the node name that has the rook operator pod running on
            pod_info = self.rook_operator_pod.get()
            node = pod_info["spec"]["nodeName"]
            # Wait for network fence to be created
            if  node_name == node:
                time.sleep(180)
            else:
                time.sleep(60)

            #Check if networkfence is created post taint
            assert self.get_all_networkfences(), "Failed to create network fence"
            CIDR_untaint = self.CIDR_IP

            # Check if CIDR IP is blocklisted post taint
            assert self.check_cidr_is_blocklisted(), "Failed to blocklist CIDRs"

            untaint_node_objs = get_node_objs(taint_node)

            # Untaint the node
            assert untaint_nodes(
                taint_label="node.kubernetes.io/out-of-service=nodeshutdown:NoExecute",
                nodes_to_untaint=untaint_node_objs,
            ), "Failed to untaint"

            log.info("Inducing delay so that network fence operation is completed")
            time.sleep(60)

            #Check if networkfence is removed post untaint
            assert not self.get_all_networkfences(), "Network fence still exists"

            # Check if CIDR IP is removed from blocklisted IPs post untaint
            assert not self.check_cidr_is_blocklisted(CIDR_untaint), "CIDR IPs stil exist in blocklist"
            log.info("Volume recovery was successful post tainting the node")
        except:
            log.info("failed")

