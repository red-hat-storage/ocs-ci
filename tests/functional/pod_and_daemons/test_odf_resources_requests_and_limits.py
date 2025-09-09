import logging

from ocs_ci.ocs import constants

from ocs_ci.framework.testlib import (
    tier1,
    skipif_ocs_version,
    BaseTest,
)
from ocs_ci.framework.pytest_customization.marks import brown_squad
from ocs_ci.ocs.resources.pod import (
    get_all_pods,
)
from ocs_ci.helpers.pod_helpers import (
    get_pods_resources_details,
    check_odf_resources_requests_and_limits,
)

log = logging.getLogger(__name__)


@tier1
@skipif_ocs_version("<4.20")
class TestResourcesRequestsAndLimits(BaseTest):
    """
    A functional test to verify that the resource requests and limits
    for ODF pods match the expected values defined in a JSON file.
    """

    @brown_squad
    def test_resources_requests_and_limits(self):
        """
        Tests whether the live pod resource requests and limits in the cluster
        are consistent with the predefined expected values.

        The test performs the following steps:
        1. Retrieves resource details for all ODF pods currently running in the cluster.
        2. Retrieves the expected resource details from a standardized JSON file.
        3. Compares the live data with the expected data to find any differences.
        4. Asserts that no differences were found, ensuring the cluster's
           resource configurations are as expected.

        """
        # Step 1: Get live pod objects from the cluster, excluding transient pods
        pod_name_exclude_patterns = ["storageclient-"]
        log.info(
            f"Retrieving live pod objects from the cluster, excluding patterns: '{pod_name_exclude_patterns}'"
        )
        pod_objs = get_all_pods(namespace=constants.OPENSHIFT_STORAGE_NAMESPACE)
        pod_objs = [
            p
            for p in pod_objs
            if not any(
                p.name.startswith(pattern) for pattern in pod_name_exclude_patterns
            )
        ]
        log.info(f"pod objs = {pod_objs}")

        # Step 2: Get resource details from the live pods and format them into a dictionary
        log.info("Extracting resource details and normalizing pod names.")
        pods_resources_details_dict = get_pods_resources_details(pod_objs)

        # Step 3: Compare the live data with the expected data
        log.info("Comparing live pod resource data with expected configurations.")
        has_mismatches = check_odf_resources_requests_and_limits(
            pods_resources_details_dict
        )

        # Step 4: Assert that there are no differences
        error_message = "Found resource mismatches. Please check the logs for details."
        assert has_mismatches, error_message

        log.info(
            "Successfully verified that all pod resource values match the expected configurations."
        )
