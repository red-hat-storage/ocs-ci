import pytest

from ocs_ci.framework.pytest_customization.marks import *  # noqa: F403
from ocs_ci.ocs.constants import (
    MCG_TESTS_MIN_NB_ENDPOINT_COUNT,
    MAX_NB_ENDPOINT_COUNT,
    NOOBAA_ENDPOINT_POD_LABEL,
)


@pytest.mark.usefixtures("environment_checker")  # noqa: F405
@pytest.mark.usefixtures("resource_checker")
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


# nb endpoint pods might change during MCG tests due to scaling or mounts
@ignore_leftover_label(NOOBAA_ENDPOINT_POD_LABEL)  # noqa: F405
@pytest.mark.usefixtures("nb_ensure_endpoint_count")
class MCGTest(ManageTest):
    MIN_ENDPOINT_COUNT = MCG_TESTS_MIN_NB_ENDPOINT_COUNT
    MAX_ENDPOINT_COUNT = MAX_NB_ENDPOINT_COUNT
