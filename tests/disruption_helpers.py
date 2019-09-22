import logging

from ocs_ci.ocs.resources import pod
from ocs_ci.ocs import constants, ocp
from ocs_ci.framework import config


logger = logging.getLogger(__name__)

POD = ocp.OCP(kind=constants.POD, namespace=config.ENV_DATA['cluster_namespace'])


class Disruptions:
    """
    This class contains methods of disrupt operations
    """
    resource = None
    resource_obj = None
    resource_count = 0

    def set_resource(self, resource):
        self.resource = resource
        resource_count = 0
        if self.resource == 'mgr':
            self.resource_obj = pod.get_mgr_pods()
            self.selector = constants.MGR_APP_LABEL
        if self.resource == 'mon':
            self.resource_obj = pod.get_mon_pods()
            self.selector = constants.MON_APP_LABEL
        if self.resource == 'osd':
            self.resource_obj = pod.get_osd_pods()
            self.selector = constants.OSD_APP_LABEL
        if self.resource == 'mds':
            self.resource_obj = pod.get_mds_pods()
            self.selector = constants.MDS_APP_LABEL
        if self.resource == 'cephfsplugin':
            self.resource_obj = pod.get_plugin_pods(
                interface=constants.CEPHFILESYSTEM
            )
            self.selector = constants.CSI_CEPHFSPLUGIN_LABEL
        if self.resource == 'rbdplugin':
            self.resource_obj = pod.get_plugin_pods(
                interface=constants.CEPHBLOCKPOOL
            )
            self.selector = constants.CSI_RBDPLUGIN_LABEL
        if self.resource == 'cephfsplugin_provisioner':
            self.resource_obj = [pod.plugin_provisioner_leader(
                interface=constants.CEPHFILESYSTEM
            )]
            self.selector = constants.CSI_CEPHFSPLUGIN_PROVISIONER_LABEL
            resource_count = len(pod.get_cephfsplugin_provisioner_pods())
        if self.resource == 'rbdplugin_provisioner':
            self.resource_obj = [pod.plugin_provisioner_leader(
                interface=constants.CEPHBLOCKPOOL
            )]
            self.selector = constants.CSI_RBDPLUGIN_PROVISIONER_LABEL
            resource_count = len(pod.get_rbdfsplugin_provisioner_pods())

        self.resource_count = resource_count or len(self.resource_obj)

    def delete_resource(self, resource_id=0):
        self.resource_obj[resource_id].delete(force=True)
        assert POD.wait_for_resource(
            condition='Running', selector=self.selector,
            resource_count=self.resource_count, timeout=300
        )
