import logging
import pytest

from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.ocs.jenkins import Jenkins
from ocs_ci.ocs.node import drain_nodes, schedule_nodes
from ocs_ci.framework.pytest_customization.marks import magenta_squad
from ocs_ci.framework.testlib import E2ETest, workloads, ignore_leftovers
from ocs_ci.ocs.constants import STATUS_COMPLETED, MASTER_MACHINE, WORKER_MACHINE

logger = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def jenkins(request):

    jenkins = Jenkins()

    def teardown():
        jenkins.cleanup()

    request.addfinalizer(teardown)
    return jenkins


@magenta_squad
@workloads
@ignore_leftovers
class TestJenkinsNodeDrain(E2ETest):
    """
    Test running Jenkins and Node Drain
    """

    @pytest.fixture()
    def jenkins_setup(self, jenkins):
        """
        JENKINS test setup
        """
        logger.info("Setting up Jenkins environment with node drain test configuration")
        self.sanity_helpers = Sanity()

        jenkins.create_ocs_jenkins_template()
        logger.info("Jenkins OCS template created")

    @pytest.mark.parametrize(
        argnames=["node_type", "num_projects", "num_of_builds"],
        argvalues=[
            pytest.param(
                *[WORKER_MACHINE, 4, 3], marks=pytest.mark.polarion_id("OCS-2252")
            ),
            pytest.param(
                *[MASTER_MACHINE, 3, 6], marks=pytest.mark.polarion_id("OCS-2176")
            ),
        ],
    )
    @pytest.mark.usefixtures(jenkins_setup.__name__)
    def test_run_jenkins_drain_node(
        self, jenkins, node_type, num_projects, num_of_builds
    ):
        """

        Test Node Drain jenkins
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

        logger.test_step(f"Identify {node_type} node for drain operation")
        nodes_drain = jenkins.get_node_name_where_jenkins_pod_not_hosted(
            node_type=node_type, num_of_nodes=1
        )
        logger.info(
            f"Nodes to drain: {[n.name for n in nodes_drain] if nodes_drain else 'None'}"
        )

        logger.test_step(f"Start {num_of_builds} builds per project")
        jenkins.number_builds_per_project = num_of_builds
        jenkins.start_build()
        logger.info(f"Started builds for {num_projects} projects")

        if len(nodes_drain) > 0:
            logger.test_step(f"Drain {node_type} node during build execution")
            logger.info(
                f"Draining {len(nodes_drain)} node(s): {[n.name for n in nodes_drain]}"
            )
            drain_nodes(nodes_drain)
            logger.info("Node drain completed")

            logger.info(
                f"Making node(s) schedulable again: {[n.name for n in nodes_drain]}"
            )
            schedule_nodes(nodes_drain)
            logger.info("Node(s) marked schedulable")
        else:
            logger.warning(f"No {node_type} nodes available for drain operation")

        logger.test_step("Wait for all builds to complete")
        jenkins.wait_for_build_to_complete()
        logger.info("All builds completed successfully after node drain")

        logger.test_step("Display build results")
        jenkins.print_completed_builds_results()
        logger.info("Build results displayed")

        logger.test_step("Verify cluster and Ceph health")
        self.sanity_helpers.health_check(tries=40)
        logger.info("Cluster and Ceph health checks passed")
