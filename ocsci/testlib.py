from pytest_customization.marks import e2e, ecosystem, manage


class BaseTest():
    """
    Base test class for our testing.
    If some functionallity/property needs to be implemented in all test classes
    here is the place to put your code.
    """
    pass


@e2e
class E2ETest(BaseTest):
    """
    Base class for E2E team
    """
    pass


@manage
class ManageTest(BaseTest):
    """
    Base class for E2E team
    """
    pass


@ecosystem
class EcosystemTest(BaseTest):
    """
    Base class for E2E team
    """
    pass
