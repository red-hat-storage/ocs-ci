import logging
import pytest
from ocs_ci.deployment.metallb import MetalLBInstaller
from ocs_ci.framework import config

logger = logging.getLogger(__name__)


class TestMetalLB(object):
    """
    Test MetalLB installation
    """

    metallb = MetalLBInstaller()
    config.ENV_DATA.update({"reserved_ips_num": 2})

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

    def test_install_metallb(self):
        """
        Test MetalLB installation
        """
        self.metallb.deploy()
