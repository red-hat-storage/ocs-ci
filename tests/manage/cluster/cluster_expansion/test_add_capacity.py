import logging
import pytest

from ocs_ci.framework.testlib import tier1, ignore_leftovers, ManageTest
from ocs_ci.ocs import machine as machine_utils
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs import defaults
from ocs_ci.framework import config

logger = logging.getLogger(__name__)


@ignore_leftovers
@tier1
class TestAddCapacity(ManageTest):
    """
    Automates adding capacity to the cluster while IOs running
    """

    def test_add_capacity(self):
        """
        Testing adding capacity to the cluster while IOs running
        """
        dt = config.ENV_DATA['deployment_type']
        if dt == 'ipi':
            osd_count = pod.get_pod_count(label=constants.OSD_APP_LABEL)
            storage_cluster = machine_utils.get_storage_cluster(namespace=defaults.ROOK_CLUSTER_NAMESPACE)
            machine_utils.add_capacity(storagecluster_name=storage_cluster, count=osd_count + 3)
            pod_obj = ocp.OCP(kind=constants.POD, namespace=defaults.ROOK_CLUSTER_NAMESPACE)
            assert pod_obj.wait_for_resource(
                condition=constants.STATUS_RUNNING, selector=constants.OSD_APP_LABEL,
                resource_count=osd_count + 3, timeout=600
            ), "OSD pods failed to reach RUNNING state"
        else:
            pytest.skip("UPI not yet supported")
        # ToDo run IOs
