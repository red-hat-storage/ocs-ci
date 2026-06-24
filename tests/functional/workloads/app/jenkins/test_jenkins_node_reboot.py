import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import (
    skipif_vsphere_ipi,
    magenta_squad,
)
from ocs_ci.framework.testlib import E2ETest, workloads, ignore_leftovers
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.ocs.constants import STATUS_COMPLETED, MASTER_MACHINE, WORKER_MACHINE
from ocs_ci.ocs.jenkins import Jenkins
from ocs_ci.ocs.node import get_node_objs

logger = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def jenkins(request, nodes):

    jenkins = Jenkins()

    def teardown():
        jenkins.cleanup()
        # Make sure all VMs are up by the end of the test
        nodes.restart_nodes_by_stop_and_start_teardown()

    request.addfinalizer(teardown)
    return jenkins


@magenta_squad
@workloads
@ignore_leftovers
@skipif_vsphere_ipi
class TestJenkinsNodeReboot(E2ETest):
    """
    Test running Jenkins and Node Reboot
    """

    @pytest.fixture()
    def jenkins_setup(self, jenkins):
        """
        JENKINS test setup
        """
        logger.info(
            "Setting up Jenkins environment with node reboot test configuration"
        )
        self.sanity_helpers = Sanity()

        jenkins.create_ocs_jenkins_template()
        logger.info("Jenkins OCS template created")

    @pytest.mark.parametrize(
        argnames=["node_type", "num_projects", "num_of_builds"],
        argvalues=[
            pytest.param(
                *[MASTER_MACHINE, 2, 15], marks=pytest.mark.polarion_id("OCS-2202")
            ),
            pytest.param(
                *[WORKER_MACHINE, 2, 15], marks=pytest.mark.polarion_id("OCS-2178")
            ),
        ],
    )
    @pytest.mark.usefixtures(jenkins_setup.__name__)
    def test_run_jenkins_node_reboot(
        self, jenkins, nodes, node_type, num_projects, num_of_builds
    ):
        """

        Test Node Reboot jenkins
        """
        logger.test_step(
            f"Configure Jenkins: {num_projects} projects, {num_of_builds} builds/project"
        )
        jenkins.number_projects = num_projects
        logger.info(f"Number of projects: {num_projects}")

        logger.test_step("Create Jenkins application resources")
        jenkins.create_app_jenkins()
        logger.info("Jenkins application created")

        jenkins.create_jenkins_pvc()
        logger.info("Jenkins PVC created")

        jenkins.create_jenkins_build_config()
        logger.info("Jenkins build config created")

        logger.test_step("Wait for Jenkins deployment to complete")
        jenkins.wait_for_jenkins_deploy_status(status=STATUS_COMPLETED)
        logger.info(f"Jenkins deployment reached status: {STATUS_COMPLETED}")

        logger.test_step(f"Identify {node_type} node for reboot operation")
        nodes_reboot = jenkins.get_node_name_where_jenkins_pod_not_hosted(
            node_type=node_type, num_of_nodes=1
        )
        logger.info(f"Nodes to reboot: {nodes_reboot if nodes_reboot else 'None'}")

        logger.test_step(f"Start {num_of_builds} builds per project")
        jenkins.number_builds_per_project = num_of_builds
        jenkins.start_build()
        logger.info(f"Started builds for {num_projects} projects")

        if len(nodes_reboot) > 0:
            logger.test_step(f"Reboot {node_type} node during build execution")
            logger.info(
                f"Rebooting {len(nodes_reboot)} node(s): {nodes_reboot if nodes_reboot else 'None'}"
            )
            nodes.restart_nodes(get_node_objs(nodes_reboot))
            logger.info("Node reboot completed")
        else:
            logger.warning(f"No {node_type} nodes available for reboot operation")

        logger.test_step("Wait for all builds to complete")
        jenkins.wait_for_build_to_complete()
        logger.info("All builds completed successfully after node reboot")

        logger.test_step("Display build results")
        jenkins.print_completed_builds_results()
        logger.info("Build results displayed")

        logger.test_step("Verify cluster and Ceph health")
        self.sanity_helpers.health_check(tries=40)
        logger.info("Cluster and Ceph health checks passed")
