import logging
import pytest
from ocs_ci.deployment.metallb import MetalLBInstaller
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import purple_squad, libtest

logger = logging.getLogger(__name__)


@libtest
class TestMetalLB(object):
    """
    Test MetalLB installation
    """

    metallb = MetalLBInstaller()
    config.ENV_DATA.update({"ips_to_reserve": 2})

    @pytest.fixture(autouse=True)
    def tearDown_fixture(self, request):
        """
        Clean up the environment after testing
        """

        def teardown():
            """
            Clean up the environment after testing
            """
            self.metallb.undeploy()

        request.addfinalizer(teardown)

    @purple_squad
    def test_install_uninstall_metallb(self):
        """
        Test MetalLB installation
        """
        self.metallb.deploy_lb()
