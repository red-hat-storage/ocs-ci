import logging
import pytest

from ocs_ci.framework import config
from ocs_ci.framework.testlib import tier4, ignore_leftovers
from tests.manage.cluster.nodes.base_node_ops import BaseNodes


logger = logging.getLogger(__name__)


@tier4
@ignore_leftovers
class TestNodesRestart(BaseNodes):
    """
    Test ungraceful cluster shutdown
    """

    @pytest.mark.skipif(
        condition=config.ENV_DATA['platform'] != 'AWS',
        reason="Tests are not running on AWS deployed cluster"
    )
    @pytest.mark.parametrize(
        argnames=["force"],
        argvalues=[
            pytest.param(*[True], marks=pytest.mark.polarion_id("OCS-894")),
            pytest.param(*[False], marks=pytest.mark.polarion_id("OCS-895"))
        ]
    )
    def test_nodes_restart_aws(self, resources, instances, aws_obj, force):
        """
        Test ungraceful cluster shutdown - AWS
        """
        aws_obj.restart_ec2_instances(instances=instances, wait=True, force=force)
        self.validate_cluster(resources=resources, nodes=list(instances.values()))

# TODO: Add a test cases for VMWare and RHHI.Next
