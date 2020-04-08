import logging
import pytest
from importlib import import_module
from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import scale, E2ETest

log = logging.getLogger(__name__)

#
# Note: This is a prototype layout for future tests.  This layout can
# be abandoned if not right.  The log.info calls in scale_setup are
# an experiment my me to make sure that these objects can be referenced
#
@scale
class TestScaling(E2ETest):
    @pytest.fixture()
    def scale_setup(self):
        log.info("scale "+constants.ACCESS_MODE_RWO)
        log.info("scale "+self.object_from_name('ocs_ci.ocs.constants', 'ACCESS_MODE_RWO'))
        log.info("scale setup goes here")

    def object_from_name(self, module_name, object_name):
        return getattr(import_module(module_name), object_name)

    def teardown(self):
        log.info("scale teardown goes here")

    @pytest.mark.usefixtures(scale_setup.__name__)
    def test_scale(self, scale_setup):
        log.info("scale test body goes here")

