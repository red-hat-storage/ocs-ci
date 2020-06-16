import logging
import pytest

from ocs_ci.framework.testlib import (
    E2ETest, workloads
)
from ocs_ci.ocs.jenkins import Jenkins
from ocs_ci.framework.testlib import ignore_leftovers

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
class TestJenkinsWorkload(E2ETest):
    """
    Test running Jenkins
    """
    @pytest.fixture()
    def jenkins_setup(self, jenkins):
        """
        JENKINS test setup
        """
        # Deployment of jenkins
        jenkins.setup_jenkins()

    @pytest.mark.usefixtures(jenkins_setup.__name__)
    def test_jenkins_workload_simple(self, jenkins):
        """
        Test jenkins workload
        """
        # Start Build
        jenkins.start_build()
        # Wait build reach Complete state
        jenkins.wait_for_build_status(status='Complete')
