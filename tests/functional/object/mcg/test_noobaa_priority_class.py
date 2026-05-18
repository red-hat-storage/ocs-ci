import json
import logging

from ocs_ci.framework.pytest_customization.marks import (
    mcg,
    red_squad,
    polarion_id,
    tier2,
)
from ocs_ci.framework.testlib import MCGTest
from ocs_ci.framework import config
from ocs_ci.helpers import helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.mcg import MCG
from ocs_ci.ocs.resources.pod import (
    get_noobaa_core_pod,
    get_noobaa_endpoint_pods,
    get_pods_having_label,
    Pod,
)

logger = logging.getLogger(__name__)

SYSTEM_CLUSTER_CRITICAL = "system-cluster-critical"
OPENSHIFT_USER_CRITICAL = "openshift-user-critical"


@mcg
@red_squad
class TestNoobaaPriorityClass(MCGTest):
    """Test NooBaa per-component PriorityClass configuration via the NooBaa CR."""

    @tier2
    @polarion_id("OCS-7938")
    @config.run_with_provider_context_if_available
    def test_noobaa_priority_class(self, teardown_factory):
        """
        Test that per-component PriorityClassNames configured on the NooBaa CR propagate
        to the corresponding pods after operator reconciliation.

        1. Verify NooBaa CR does not have PriorityClassName fields set
        2. Create two custom PriorityClasses: priorityclass-noobaa-1, priorityclass-noobaa-2
        3. Patch NooBaa CR with per-component fields:
           corePriorityClassName=system-cluster-critical,
           dbPriorityClassName=openshift-user-critical,
           endpointPriorityClassName=priorityclass-noobaa-1
        4. Wait for NooBaa operator to reconcile core and endpoint pods; restart db pods
        5. Verify all NooBaa pods are Running with the correct priorityClassName
        6. Patch NooBaa CR to set all three components to priorityclass-noobaa-2
        7. Wait for reconciliation; restart db pods
        8. Verify all NooBaa pods are Running with priorityclass-noobaa-2
        9. Patch NooBaa CR to remove PriorityClass from all three components
        10. Wait for reconciliation; restart db pods
        11. Verify all NooBaa pods are Running without any priorityClassName set
        """
        namespace = config.ENV_DATA["cluster_namespace"]
        noobaa_ocp = OCP(kind=constants.NOOBAA_RESOURCE_NAME, namespace=namespace)

        # 1. Verify NooBaa CR has no priority class fields set initially
        logger.test_step("Step 1: Verifying NooBaa CR has no PriorityClass fields set")
        noobaa_cr = noobaa_ocp.get(resource_name=constants.NOOBAA_RESOURCE_NAME)
        for field in (
            "corePriorityClassName",
            "dbPriorityClassName",
            "endpointPriorityClassName",
        ):
            assert (
                field not in noobaa_cr["spec"]
            ), f"NooBaa CR already has {field} set; test requires a clean initial state"

        # 2. Create two custom PriorityClasses
        logger.test_step("Step 2: Creating custom PriorityClasses")
        pc1 = helpers.create_priority_class("noobaa-1", 500000)
        teardown_factory(pc1)
        pc2 = helpers.create_priority_class("noobaa-2", 400000)
        teardown_factory(pc2)

        # 3. Patch NooBaa CR with mixed per-component priority classes
        logger.test_step(
            f"Step 3: Patching NooBaa CR — core={SYSTEM_CLUSTER_CRITICAL}, "
            f"db={OPENSHIFT_USER_CRITICAL}, endpoint={pc1.name}"
        )
        self._patch_noobaa_priority_classes(
            noobaa_ocp=noobaa_ocp,
            core_pc=SYSTEM_CLUSTER_CRITICAL,
            db_pc=OPENSHIFT_USER_CRITICAL,
            endpoint_pc=pc1.name,
        )

        # 4. Wait for reconciliation and restart db pods
        logger.test_step("Step 4: Waiting for reconciliation and restarting db pods")
        self._restart_db_and_wait(namespace=namespace)

        # 5. Verify priority classes on all pods
        logger.test_step("Step 5: Verifying pod priority classes after first patch")
        self._verify_pod_priority_classes(
            namespace=namespace,
            core_pc=SYSTEM_CLUSTER_CRITICAL,
            db_pc=OPENSHIFT_USER_CRITICAL,
            endpoint_pc=pc1.name,
        )

        # 6. Patch NooBaa CR to set all three components to pc2
        logger.test_step(f"Step 6: Patching NooBaa CR — all components to {pc2.name}")
        self._patch_noobaa_priority_classes(
            noobaa_ocp=noobaa_ocp,
            core_pc=pc2.name,
            db_pc=pc2.name,
            endpoint_pc=pc2.name,
        )

        # 7. Wait for reconciliation and restart db pods
        logger.test_step("Step 7: Waiting for reconciliation and restarting db pods")
        self._restart_db_and_wait(namespace=namespace)

        # 8. Verify updated priority classes on all pods
        logger.test_step("Step 8: Verifying pod priority classes after second patch")
        self._verify_pod_priority_classes(
            namespace=namespace,
            core_pc=pc2.name,
            db_pc=pc2.name,
            endpoint_pc=pc2.name,
        )

        # 9. Remove priority class fields from NooBaa CR
        logger.test_step("Step 9: Removing PriorityClass fields from NooBaa CR")
        self._remove_noobaa_priority_classes(noobaa_ocp=noobaa_ocp)

        # 10. Wait for reconciliation and restart db pods
        logger.test_step("Step 10: Waiting for reconciliation and restarting db pods")
        self._restart_db_and_wait(namespace=namespace)

        # 11. Verify pods run without any priorityClassName set
        logger.test_step("Step 11: Verifying pods run without any priorityClassName")
        self._verify_pod_priority_classes(
            namespace=namespace,
            core_pc=None,
            db_pc=None,
            endpoint_pc=None,
        )

    def _patch_noobaa_priority_classes(self, noobaa_ocp, core_pc, db_pc, endpoint_pc):
        """Merge-patch the NooBaa CR to set per-component PriorityClassNames."""
        params = {
            "spec": {
                "corePriorityClassName": core_pc,
                "dbPriorityClassName": db_pc,
                "endpointPriorityClassName": endpoint_pc,
            }
        }
        noobaa_ocp.patch(
            resource_name=constants.NOOBAA_RESOURCE_NAME,
            params=json.dumps(params),
            format_type="merge",
        )

    def _remove_noobaa_priority_classes(self, noobaa_ocp):
        """JSON-patch the NooBaa CR to remove all three PriorityClassName fields."""
        patch_params = [
            {"op": "remove", "path": "/spec/corePriorityClassName"},
            {"op": "remove", "path": "/spec/dbPriorityClassName"},
            {"op": "remove", "path": "/spec/endpointPriorityClassName"},
        ]
        noobaa_ocp.patch(
            resource_name=constants.NOOBAA_RESOURCE_NAME,
            params=json.dumps(patch_params),
            format_type="json",
        )

    def _restart_db_and_wait(self, namespace):
        """
        Wait for the NooBaa operator to reconcile core and endpoint pods, then manually
        delete all CNPG DB pods so they are recreated with the updated priority class.
        """
        # Wait for core and endpoint pods to be reconciled by the operator
        MCG.wait_for_ready_status()

        # Delete all CNPG DB pods; CNPG will recreate them with the updated spec
        logger.info("Restarting NooBaa DB pods")
        db_pod_data_list = get_pods_having_label(
            label=constants.NOOBAA_DB_LABEL_419_AND_ABOVE,
            namespace=namespace,
        )
        for pod_data in db_pod_data_list:
            Pod(**pod_data).delete(wait=True)

        MCG.wait_for_ready_status()

    def _verify_pod_priority_classes(self, namespace, core_pc, db_pc, endpoint_pc):
        """
        Assert that all NooBaa pods carry the expected priorityClassName.
        Pass None to assert the field is absent (no priority class configured).
        """
        core_pod = get_noobaa_core_pod()
        actual = core_pod.get()["spec"].get("priorityClassName")
        assert (
            actual == core_pc
        ), f"noobaa-core priorityClassName mismatch: expected={core_pc!r}, actual={actual!r}"

        for ep_pod in get_noobaa_endpoint_pods():
            actual = ep_pod.get()["spec"].get("priorityClassName")
            assert actual == endpoint_pc, (
                f"noobaa-endpoint priorityClassName mismatch: "
                f"expected={endpoint_pc!r}, actual={actual!r}"
            )

        for pod_data in get_pods_having_label(
            label=constants.NOOBAA_DB_LABEL_419_AND_ABOVE,
            namespace=namespace,
        ):
            db_pod = Pod(**pod_data)
            actual = db_pod.get()["spec"].get("priorityClassName")
            assert actual == db_pc, (
                f"noobaa-db pod {db_pod.name} priorityClassName mismatch: "
                f"expected={db_pc!r}, actual={actual!r}"
            )
