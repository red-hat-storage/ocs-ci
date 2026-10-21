import json
import logging
import pytest

from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import (
    tier1,
    skipif_ocs_version,
    BaseTest,
    polarion_id,
)
from ocs_ci.framework.pytest_customization.marks import brown_squad, jira
from ocs_ci.ocs.resources.pod import (
    get_all_pods,
    is_pod_owned_by_job,
    wait_for_pods_to_be_in_statuses,
)
from ocs_ci.helpers.pod_helpers import (
    get_all_pods_container_resource_details,
    validate_all_pods_container_resources,
)
from ocs_ci.ocs.node import get_worker_nodes, drain_nodes, schedule_nodes
from ocs_ci.framework.custom_logger import reset_step_counts
from ocs_ci.ocs.exceptions import TimeoutExpiredError, CommandFailed

# Guideline: Instantiated immediately after imports using __name__
logger = logging.getLogger(__name__)


@tier1
@skipif_ocs_version("<4.20")
@jira("RHSTOR-6148")
class TestLiveResourcesPresenceAndFormat(BaseTest):
    """
    Functional test to verify that live ODF pod resource values (requests/limits)
    exist and start with a digit after performing a worker node drain operation.
    """

    @brown_squad
    @polarion_id("OCS-7362")
    def test_live_resources_presence_and_format(self):

        # Guideline: Use logger.test_step() for workflow phases  (no manual formatting/numbering)
        logger.test_step("Identify an available operational ODF worker node")
        worker_nodes = get_worker_nodes()
        if not worker_nodes:
            pytest.fail(
                "No worker nodes found in the cluster to execute the drain operation."
            )

        target_node = worker_nodes[0]
        logger.info(f"Target worker node selected for eviction: {target_node}")

        pod_name_exclude_patterns = [
            "storageclient-",
            "rook-ceph-tools-external-",
            "rook-ceph-osd-prepare-",
            "pod-test-",
            "test",
            "session",
            "debug",
            "must-gather",
            "ocs-ci",
            "java-s3",
        ]

        try:
            logger.test_step(
                f"Evacuate worker node '{target_node}' to force ODF pod rescheduling"
            )
            drain_nodes([target_node])

            logger.test_step(
                "Wait for remaining and relocated storage pods to settle into Running status"
            )
            pods_stabilized = wait_for_pods_to_be_in_statuses(
                expected_statuses=[constants.STATUS_RUNNING],
                namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
                exclude_pod_name_prefixes=pod_name_exclude_patterns,
                timeout=300,
                sleep=10,
            )

            logger.assertion(
                f"Verify all non-transient ODF pods are successfully Running: state={pods_stabilized}"
            )
            assert (
                pods_stabilized
            ), "One or more core ODF pods failed to return to Running status after node drain."

            logger.test_step(
                "Gather live post-drain pod objects from the target namespace"
            )
            pod_objs = get_all_pods(namespace=constants.OPENSHIFT_STORAGE_NAMESPACE)
            filtered_pods = []

            reset_step_counts(__name__)
            for p in pod_objs:
                if any(keyword in p.name for keyword in pod_name_exclude_patterns):
                    continue
                if is_pod_owned_by_job(p):
                    continue
                filtered_pods.append(p)

            logger.info(
                f"Found {len(filtered_pods)} live pods for resource format verification."
            )

            logger.test_step(
                "Extract per-container resource request and limit configurations"
            )
            pods_resources_details_dict = get_all_pods_container_resource_details(
                filtered_pods
            )

            logger.test_step(
                "Validate that live resource definitions exist and begin with a valid digit"
            )
            validation = validate_all_pods_container_resources(
                pods_resources_details_dict
            )

            if not validation["result"]:
                pretty = json.dumps(
                    validation["invalid_values"], indent=2, sort_keys=True
                )
                error_message = (
                    "Invalid, missing, or malformed live resource specifications detected post-drain.\n"
                    f"Details:\n{pretty}"
                )
            else:
                error_message = ""

            logger.assertion(
                f"Resource structural compliance format check: result={validation['result']}"
            )
            assert validation["result"], error_message
            logger.info(
                "Success! All live rescheduled pod resource metrics exist and are well-formed."
            )

        except (TimeoutExpiredError, CommandFailed, AssertionError) as e:
            logger.error(
                f"A targeted operational error or assertion failure occurred during evaluation: {e}"
            )
            raise

        finally:
            logger.info(
                f"Restoring cluster state. Un-draining / scheduling worker node: {target_node}"
            )
            schedule_nodes([target_node])
