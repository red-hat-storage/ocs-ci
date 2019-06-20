import logging
import pytest

from ocsci import config
from utility.environment_check import environment_checker  # noqa: F401


logger = logging.getLogger(__name__)


@pytest.fixture(scope="session", autouse=True)
def polarion_testsuite_properties(record_testsuite_property):
    """
    Configures polarion testsuite properties for junit xml
    """
    polarion_project_id = config.REPORTING['polarion']['project_id']
    record_testsuite_property('polarion-projct-id', polarion_project_id)
