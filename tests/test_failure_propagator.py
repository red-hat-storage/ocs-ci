import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    acceptance,
    tier1,
    tier2,
    tier3,
    tier4a,
    tier4b,
    tier4c,
)


@tier1
@acceptance
@tier2
@tier3
@tier4a
@tier4b
@tier4c
class TestFailurePropagator:
    """
    Test class for failure propagator test case. The test intention is to run last and propagate
    teardown failures caught during the test execution, so regular test cases won't false negatively fail
    """

    @pytest.mark.second_to_last
    def test_report_skip_triggering_test(self):
        pass_rate_counting_ceph_health_skips = (
            config.RUN["skipped_tests_ceph_health"] / config.RUN["number_of_tests"]
        )
        message = (
            f"This run had {1 - (pass_rate_counting_ceph_health_skips * 100)}% of the "
            f"tests skipped due to Ceph health not OK. "
        )
        if (
            config.RUN["skipped_tests_ceph_health"] / config.RUN["number_of_tests"]
            > 0.2
        ):
            if config.RUN["skip_reason_test_found"]:
                message = (
                    message
                    + f"The test that is likely to cause this is {config.RUN['skip_reason_test_found']['test_name']} "
                )
                if config.RUN["skip_reason_test_found"]["squad"]:
                    message = (
                        message
                        + f"which is under {config.RUN['skip_reason_test_found']['squad']}'s responsibility"
                    )

            else:
                message = message + "Couldn't identify the test case that caused this"
            pytest.fail(message)
        pass

    @pytest.mark.last
    def test_failure_propagator(self):
        pass
