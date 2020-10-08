import pytest

from ocs_ci.framework.pytest_customization.marks import *  # noqa: F403
from ocs_ci.ocs.constants import MIN_NB_ENDPOINT_COUNT, MAX_NB_ENDPOINT_COUNT


@pytest.mark.usefixtures(  # noqa: F405
    'environment_checker'
)
class BaseTest:
    """
    Base test class for our testing.
    If some functionality/property needs to be implemented in all test classes
    here is the place to put your code.
    """
    pass


@e2e  # noqa: F405
class E2ETest(BaseTest):
    """
    Base class for E2E team
    """
    pass


@manage  # noqa: F405
class ManageTest(BaseTest):
    """
    Base class for E2E team
    """
    pass


@ecosystem  # noqa: F405
class EcosystemTest(BaseTest):
    """
    Base class for E2E team
    """
    pass


@pytest.mark.usefixtures(
    "nb_ensure_endpoint_count"
)
class MCGTest(ManageTest):
    MIN_ENDPOINT_COUNT = MIN_NB_ENDPOINT_COUNT
    MAX_ENDPOINT_COUNT = MAX_NB_ENDPOINT_COUNT
