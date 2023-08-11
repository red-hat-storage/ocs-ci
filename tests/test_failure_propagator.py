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
    pre_upgrade,
    post_upgrade,
    pre_ocs_upgrade,
    pre_ocp_upgrade,
    post_ocp_upgrade,
    post_ocs_upgrade,
    purple_squad,
    workloads,
    performance,
    scale,
)


@purple_squad
@tier1
@acceptance
@tier2
@tier3
@tier4a
@tier4b
@tier4c
@pre_upgrade
@post_upgrade
@pre_ocs_upgrade
@pre_ocp_upgrade
@post_ocp_upgrade
@post_ocs_upgrade
@workloads
@performance
@scale
class TestFailurePropagator:
    """
    Test class for failure propagator test case
    """

    @pytest.mark.second_to_last
    def test_report_skip_triggering_test(self, request):
        """
        This test runs second to last and examines the skipped test cases of the execution.
        In case of high rate of skipped tests due to Ceph health not OK, which indicates something went wrong
        with the cluster during the execution, it will fail and report the potential test case that caused
        this problematic state
        """
        number_of_eligible_tests = config.RUN.get("number_of_tests") - 2

        skipped_on_ceph_health_percent = (
            config.RUN.get("skipped_tests_ceph_health") / number_of_eligible_tests
        )
        message = (
            f"This run had {skipped_on_ceph_health_percent * 100}% of the "
            f"tests skipped due to Ceph health not OK."
        )
        if skipped_on_ceph_health_percent > 0.25:
            if config.RUN.get("skip_reason_test_found"):
                test_name = config.RUN.get("skip_reason_test_found").get("test_name")
                message = (
                    message + f" The test that is likely to cause this is {test_name}"
                )
                squad = config.RUN.get("skip_reason_test_found").get("squad")
                if squad:
                    message = message + f" which is under {squad}'s responsibility"
                    request.node.add_marker(squad)

            else:
                message = message + " Couldn't identify the test case that caused this"
            pytest.fail(message)

    @pytest.mark.last
    def test_failure_propagator(self):
        """
        This test intention is to run last and propagate teardown failures caught during the test execution,
        so regular test cases won't false negatively fail
        """
        pass
