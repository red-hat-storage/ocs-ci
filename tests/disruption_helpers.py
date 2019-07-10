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
        if self.resource == 'mgr':
            self.resource_obj = pod.get_mgr_pods()
        if self.resource == 'mon':
            self.resource_obj = pod.get_mon_pods()
        if self.resource == 'osd':
            self.resource_obj = pod.get_osd_pods()
        if self.resource == 'mds':
            self.resource_obj = pod.get_mds_pods()
        self.resource_count = len(self.resource_obj)

    def delete_resource(self):
        self.resource_obj[0].delete(force=True)
        assert POD.wait_for_resource(
            condition='Running', selector=f'app=rook-ceph-{self.resource}',
            resource_count=self.resource_count, timeout=300
        )
