"""
Dummy tests for validating the AI live analysis feature (ai_analyzer.py).

These tests are intentionally designed to FAIL in specific ways so that the
AI analyzer can be exercised against realistic failure scenarios:

  - test_framework_issue_wrong_assertion:
      Simulates a FRAMEWORK_ISSUE — the test logic itself is wrong (incorrect
      assertion against a valid cluster response). Claude should classify this
      as FRAMEWORK_ISSUE because the product is behaving correctly but the test
      assertion is flawed.

  - test_product_bug_storagecluster_degraded:
      Simulates a PRODUCT_BUG — the test checks StorageCluster health and finds
      it in a degraded/error state. Claude should classify this as PRODUCT_BUG
      because the cluster component is not functioning as expected.

NOTE: These tests are temporary scaffolding for AI analyzer validation.
      Remove this file once the AI analyzer has been validated end-to-end.
"""

import logging
import pytest

from ocs_ci.framework.testlib import ManageTest
from ocs_ci.framework.pytest_customization.marks import brown_squad

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Test 1: FRAMEWORK_ISSUE — wrong assertion in test code
# ---------------------------------------------------------------------------


@brown_squad
@pytest.mark.polarion_id("AI-DUMMY-001")
class TestAiAnalyzerFrameworkIssue(ManageTest):
    """
    Dummy test that simulates a FRAMEWORK_ISSUE.

    The test queries the OCP API for the list of namespaces (a valid, always-
    available resource) and then makes a deliberately wrong assertion — it
    asserts that the 'openshift-storage' namespace does NOT exist, which is
    false on any ODF-installed cluster.

    Expected AI classification: FRAMEWORK_ISSUE
    Reason: The cluster is healthy; the test assertion is incorrect.
    """

    def test_framework_issue_wrong_assertion(self):
        """
        Intentionally wrong assertion to simulate a framework/test-logic bug.

        The test asserts that 'openshift-storage' namespace is absent, which
        is always false on an ODF cluster. This mimics a test that was written
        with an inverted condition or wrong expected value.
        """
        from ocs_ci.ocs import ocp
        from ocs_ci.ocs import constants

        log.info("Fetching namespace list to simulate a framework-issue test failure")
        namespace_obj = ocp.OCP(kind=constants.NAMESPACE)
        namespaces = namespace_obj.get()
        namespace_names = [
            item["metadata"]["name"] for item in namespaces.get("items", [])
        ]
        log.info(f"Found namespaces: {namespace_names}")

        # INTENTIONALLY WRONG ASSERTION — simulates a framework/test-logic bug.
        # The test author mistakenly asserted the namespace should NOT exist.
        # On any ODF cluster this will always fail.
        assert "openshift-storage" not in namespace_names, (
            "DUMMY FRAMEWORK FAILURE: Test incorrectly asserts that "
            "'openshift-storage' namespace should not exist. "
            "This is a test logic error — the namespace always exists on ODF clusters. "
            "Expected AI category: FRAMEWORK_ISSUE"
        )


# ---------------------------------------------------------------------------
# Test 2: PRODUCT_BUG — StorageCluster reports a degraded/error phase
# ---------------------------------------------------------------------------


@brown_squad
@pytest.mark.polarion_id("AI-DUMMY-002")
class TestAiAnalyzerProductBug(ManageTest):
    """
    Dummy test that simulates a PRODUCT_BUG.

    The test checks the StorageCluster CR phase and conditions. It injects a
    simulated degraded state by patching the observed phase value before the
    assertion, mimicking what would happen if the operator reported an error
    phase (e.g. 'Error', 'Progressing' stuck, or 'Degraded').

    Expected AI classification: PRODUCT_BUG
    Reason: The StorageCluster CR is reporting a non-Ready phase, which
    indicates a defect in the ODF operator or underlying Ceph cluster.
    """

    def test_product_bug_storagecluster_degraded(self):
        """
        Simulates a product bug where StorageCluster is in a degraded state.

        Fetches the real StorageCluster CR, then simulates what the test would
        see if the operator had reported phase='Error' or a degraded condition.
        The assertion fails with a realistic error message that looks like a
        genuine product failure, giving the AI analyzer rich context to work with.
        """
        from ocs_ci.ocs import ocp, constants
        from ocs_ci.framework import config

        cluster_namespace = config.ENV_DATA["cluster_namespace"]
        log.info(f"Checking StorageCluster health in namespace: {cluster_namespace}")

        storagecluster_obj = ocp.OCP(
            kind=constants.STORAGECLUSTER,
            namespace=cluster_namespace,
        )

        try:
            sc_data = storagecluster_obj.get(resource_name="ocs-storagecluster")
            real_phase = sc_data.get("status", {}).get("phase", "Unknown")
            real_conditions = sc_data.get("status", {}).get("conditions", [])
            log.info(f"Real StorageCluster phase: {real_phase}")
            log.info(f"Real StorageCluster conditions: {real_conditions}")
        except Exception as e:
            log.warning(f"Could not fetch StorageCluster CR: {e}")
            real_phase = "Unknown"
            real_conditions = []

        # SIMULATE a degraded phase — override the observed phase to mimic
        # what the test would see if the operator reported an error state.
        # This makes the failure look like a genuine product bug to the AI.
        simulated_phase = "Error"
        simulated_conditions = [
            {
                "type": "Available",
                "status": "False",
                "reason": "CephClusterUnhealthy",
                "message": (
                    "Ceph cluster is in HEALTH_ERR state: "
                    "1 osds down, Degraded data redundancy: "
                    "1/3 objects degraded (33.333%), 1 pg degraded"
                ),
            },
            {
                "type": "Progressing",
                "status": "True",
                "reason": "Reconciling",
                "message": "StorageCluster reconciliation is stuck",
            },
            {
                "type": "Degraded",
                "status": "True",
                "reason": "OSDsDown",
                "message": "1 OSD pod(s) are not running: rook-ceph-osd-0-xxxxxxxxx",
            },
        ]

        log.error(
            f"StorageCluster phase is '{simulated_phase}' "
            f"(real observed phase: '{real_phase}')"
        )
        log.error(f"StorageCluster conditions: {simulated_conditions}")

        # This assertion fails with a realistic product-bug error message
        assert simulated_phase == "Ready", (
            f"DUMMY PRODUCT BUG FAILURE: StorageCluster 'ocs-storagecluster' "
            f"is in phase '{simulated_phase}' instead of 'Ready'.\n"
            f"Conditions:\n"
            + "\n".join(
                f"  [{c['type']}] status={c['status']} "
                f"reason={c['reason']}: {c['message']}"
                for c in simulated_conditions
            )
            + "\n\nThis simulates an ODF operator defect where the StorageCluster "
            "fails to reconcile due to OSD failures. "
            "Expected AI category: PRODUCT_BUG"
        )


# Made with Bob
