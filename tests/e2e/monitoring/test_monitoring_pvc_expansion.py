"""
This testcase starts with minimum number of osds(one osd)
on each node and slowly scaling it into 6 osds and then reboot worker
nodes
"""

import logging
import pytest

from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import scale, E2ETest, ignore_leftovers
from ocs_ci.framework.pytest_customization.marks import (
    skipif_external_mode, skipif_aws_i3
)


logger = logging.getLogger(__name__)


@scale
@ignore_leftovers
@skipif_external_mode
@pytest.mark.parametrize(
    argnames=["interface"],
    argvalues=[
        pytest.param(
            constants.CEPHBLOCKPOOL, marks=pytest.mark.polarion_id("OCS-XXXX")
        ),
        pytest.param(
            constants.CEPHFILESYSTEM, marks=pytest.mark.polarion_id("OCS-XXXX")
        )
    ]
)
@skipif_aws_i3
class TestScaleOSDsRebootNodes(E2ETest):
    """
    Add first set of OSD to a minimum cluster with 50%
    of storage utilization and wait for rebalance
    Reboot worker nodes after rebalance
    """
    num_of_pvcs = 10
    pvc_size = 5

    def test_scale_osds_reboot_nodes(self, interface, project_factory, multi_pvc_factory, dc_pod_factory):
        """
        Check storage utilization, if its less then runs IO,
        Scale osds from 3-6, check for rebalance and reboot workers
        """
        from ocs_ci.ocs import ocp, constants
        ocp_obj = ocp.OCP(kind='project')
        project_obj = ocp_obj.get(resource_name=constants.MONITORING_NAMESPACE)

        from ocs_ci.ocs.resources.pvc import get_all_pvc_objs
        pvc_objs = get_all_pvc_objs(namespace=constants.MONITORING_NAMESPACE)
        pvc_obj = pvc_objs[0]
        pvc_obj.project = project_obj
        dc_pod_obj = dc_pod_factory(pvc=pvc_obj)
        dc_pod_obj.run_io(
            storage_type='fs', size='3G', runtime='60', fio_filename=f'{dc_pod_obj.name}_io'
        )
