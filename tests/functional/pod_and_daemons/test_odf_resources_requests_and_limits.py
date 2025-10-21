import json
import logging

from ocs_ci.ocs import constants

from ocs_ci.framework.testlib import (
    tier1,
    skipif_ocs_version,
    BaseTest,
    polarion_id,
)
from ocs_ci.framework.pytest_customization.marks import brown_squad
from ocs_ci.ocs.resources.pod import (
    get_all_pods,
)
from ocs_ci.helpers.pod_helpers import (
    get_all_pods_container_resource_details,
    validate_all_pods_container_resources,
)

log = logging.getLogger(__name__)


@tier1
@skipif_ocs_version("<4.20")
class TestLiveResourcesPresenceAndFormat(BaseTest):
    """
    Functional test to verify that live pod resource values (requests/limits)
    exist and start with a digit (no None/'N/A'/'null'/non-numeric prefixes).
    """

    @brown_squad
    @polarion_id("OCS-7362")
    def test_live_resources_presence_and_format(self):
        """
        Steps:
        1) Get live ODF pods (exclude transient patterns).
        2) Extract per-container resource details.
        3) Check that each live value exists and starts with a digit.
        4) Assert no invalid values were found.

        """
        pod_name_exclude_patterns = [
            "storageclient-",
            "rook-ceph-osd-prepare-ocs-deviceset-",
        ]
        log.info(
            f"Retrieving live pod objects from the cluster, "
            f"excluding patterns: {pod_name_exclude_patterns}"
        )
        pod_objs = get_all_pods(namespace=constants.OPENSHIFT_STORAGE_NAMESPACE)
        pod_objs = [
            p
            for p in pod_objs
            if not any(
                p.name.startswith(pattern) for pattern in pod_name_exclude_patterns
            )
        ]
        log.info(f"Found {len(pod_objs)} pods after filtering.")

        log.info("Extracting live resource details for validation.")
        pods_resources_details_dict = get_all_pods_container_resource_details(pod_objs)

        log.info("Checking live pod resource values (exist + start with digit).")
        validation = validate_all_pods_container_resources(pods_resources_details_dict)

        if not validation["result"]:
            pretty = json.dumps(validation["invalid_values"], indent=2, sort_keys=True)
            error_message = (
                "Invalid or missing live resource values detected for one or more containers.\n"
                f"Details:\n{pretty}"
            )
        else:
            error_message = ""

        assert validation["result"], error_message
        log.info("All live pod resource values exist and are well-formed.")
