import pytest


@pytest.mark.last
class TestFailurePropagator:
    """
    Test class for failure propagator test case. The test intention is to run last and propagate
    teardown failures caught during the test execution, so regular test cases won't false negatively fail
    """

    def test_failure_propagator(self):
        pass
