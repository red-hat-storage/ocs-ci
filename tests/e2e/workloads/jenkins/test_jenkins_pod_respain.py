import logging
import pytest

from ocs_ci.framework.testlib import (
    E2ETest, workloads
)
from ocs_ci.ocs.jenkins import Jenkins
from ocs_ci.framework.testlib import ignore_leftovers
from tests import disruption_helpers

log = logging.getLogger(__name__)


@pytest.fixture(scope='function')
def jenkins(request):

    jenkins = Jenkins()

    def teardown():
        jenkins.cleanup()
    request.addfinalizer(teardown)
    return jenkins


@ignore_leftovers
@workloads
@pytest.mark.polarion_id("OCS-2175")
class TestJenkinsPodRespin(E2ETest):
    """
    Test running Jenkins and with Ceph pods respin
    """
    @pytest.fixture()
    def jenkins_setup(self, jenkins):
        """
        JENKINS test setup
        """
        # Deployment of jenkins
        jenkins.setup_jenkins()

    @pytest.mark.parametrize(
        argnames=[
            'num_of_builds', 'pod_name'
        ],
        argvalues=[
            pytest.param(
                *[5, 'mon'], marks=pytest.mark.polarion_id("OCS-2204")
            ),
            pytest.param(
                *[5, 'osd'], marks=pytest.mark.polarion_id("OCS-2179")
            ),
            pytest.param(
                *[5, 'mgr'], marks=pytest.mark.polarion_id("OCS-2205")
            ),
        ]
    )
    @pytest.mark.usefixtures(jenkins_setup.__name__)
    def test_run_jenkins_respin_pod(self, jenkins, num_of_builds, pod_name):
        """
        Test jenkins workload
        """
        while num_of_builds > 0:
            # Start Build
            jenkins.start_build()

            # Respin pod
            log.info(f"Respin pod {pod_name}")
            disruption = disruption_helpers.Disruptions()
            disruption.set_resource(resource=f'{pod_name}')
            disruption.delete_resource()

            # Wait build reach Complete state
            jenkins.wait_for_build_status(status='Complete')

            num_of_builds -= 1
