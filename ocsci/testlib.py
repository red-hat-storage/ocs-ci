from ocsci.pytest_customization.marks import *  # noqa: F403
import traceback
from pytest_customization.marks import e2e, ecosystem, manage


class BaseTest():
    """
    Base test class for our testing.
    If some functionallity/property needs to be implemented in all test classes
    here is the place to put your code.
    """
    @property
    def __name__(self):
        return traceback.extract_stack(None, 2)[0][2]
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
