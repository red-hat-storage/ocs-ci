import logging

from ocs_ci.ocs.resources.pod import get_pods_having_label, Pod, get_pod_node
from ocs_ci.ocs import constants
from ocs_ci.ocs.node import drain_nodes, unschedule_nodes, schedule_nodes

log = logging.getLogger(__name__)


class TestCSIHolderPods:
    def test_csi_holder_pods(self):

        # get csi holder daemonset image

        # get all the csi holder pods
        csi_holder_pods_label = [
            constants.CSI_RBD_PLUGIN_HOLDER_LABEL,
            constants.CSI_CEPHFS_PLUGIN_HOLDER_LABEL,
            constants.CSI_NFS_PLUGIN_HOLDER_LABEL,
        ]
        pod_objs = []
        for label in csi_holder_pods_label:
            pod_objs.extend([Pod(**pod) for pod in get_pods_having_label(label)])
        log.info("Collected all the csi plugin holder pods")

        # identify the nodes where these pods
        # are running
        node_to_pod_map = dict()
        for pod_obj in pod_objs:
            node_obj = get_pod_node(pod_obj)
            node_to_pod_map.get(node_obj.resource_name, list()).append(pod_obj)
        log.info("Find the nodes for each csi plugin holder pods")

        for node_obj in node_to_pod_map:
            # mark the node unscheduled
            unschedule_nodes([node_obj.resource_name])
            log.info(f"unschedule node {node_obj.resource_name}")

            # drain the node
            drain_nodes([node_obj.resource_name])
            log.info(f"drain node {node_obj.resource_name}")

            # delete the holder pods on this node
            for pod_obj in node_to_pod_map[node_obj.resource_name]:
                pod_obj.delete()
            log.info(f"deleted all the pods running in {node_obj.resource_name}")

            # mark nodes as schedulable
            schedule_nodes([node_obj.resource_name])
            log.info(f"schedule nodes {node_obj.resource_name}")
