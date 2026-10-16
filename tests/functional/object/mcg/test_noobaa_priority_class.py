import json
import logging

from ocs_ci.framework.pytest_customization.marks import (
    mcg,
    red_squad,
    polarion_id,
    tier2,
    tier4,
)
from ocs_ci.framework.testlib import MCGTest
from ocs_ci.framework import config
from ocs_ci.helpers import helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.constants import MIN_PV_BACKINGSTORE_SIZE_IN_GB, CEPHBLOCKPOOL_SC
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.mcg import MCG
from ocs_ci.ocs.resources.pod import (
    get_noobaa_core_pod,
    get_noobaa_endpoint_pods,
    get_noobaa_operator_pod,
    get_noobaa_pvpool_pods,
    get_pods_having_label,
    Pod,
)
from ocs_ci.utility.utils import TimeoutSampler

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

        endpoint_pods = get_noobaa_endpoint_pods()
        assert (
            endpoint_pods
        ), "No NooBaa endpoint pods found for priorityClassName verification"
        for ep_pod in endpoint_pods:
            actual = ep_pod.get()["spec"].get("priorityClassName")
            assert actual == endpoint_pc, (
                f"noobaa-endpoint priorityClassName mismatch: "
                f"expected={endpoint_pc!r}, actual={actual!r}"
            )

        db_pods = get_pods_having_label(
            label=constants.NOOBAA_DB_LABEL_419_AND_ABOVE,
            namespace=namespace,
        )
        assert db_pods, "No NooBaa DB pods found for priorityClassName verification"
        for pod_data in db_pods:
            db_pod = Pod(**pod_data)
            actual = db_pod.get()["spec"].get("priorityClassName")
            assert actual == db_pc, (
                f"noobaa-db pod {db_pod.name} priorityClassName mismatch: "
                f"expected={db_pc!r}, actual={actual!r}"
            )

    @tier4
    @polarion_id("OCS-8021")
    @config.run_with_provider_context_if_available
    def test_priority_class_persists_after_operator_restart(self, request):
        """
        Test that PriorityClassName settings persist across a NooBaa operator
        restart and that the operator does not trigger unnecessary pod restarts.

        1. Patch NooBaa CR with per-component PriorityClassNames
        2. Wait for operator reconciliation; restart db pods
        3. Verify all NooBaa pods have the correct PriorityClassName
        4. Record the creation times of all NooBaa core, db, and endpoint pods
        5. Restart the NooBaa operator pod
        6. Wait for the NooBaa operator pod to be ready
        7. Verify all NooBaa pods still have the correct PriorityClassName
        8. Verify no pods were unnecessarily restarted (creation times unchanged)
        """
        namespace = config.ENV_DATA["cluster_namespace"]
        noobaa_ocp = OCP(kind=constants.NOOBAA_RESOURCE_NAME, namespace=namespace)

        def finalizer():
            logger.info("Removing PriorityClass fields from NooBaa CR")
            self._remove_noobaa_priority_classes(noobaa_ocp=noobaa_ocp)
            self._restart_db_and_wait(namespace=namespace)

        request.addfinalizer(finalizer)

        # 1. Patch NooBaa CR with per-component priority classes
        logger.test_step(
            f"Step 1: Patching NooBaa CR - core={SYSTEM_CLUSTER_CRITICAL}, "
            f"db={OPENSHIFT_USER_CRITICAL}, endpoint={SYSTEM_CLUSTER_CRITICAL}"
        )
        self._patch_noobaa_priority_classes(
            noobaa_ocp=noobaa_ocp,
            core_pc=SYSTEM_CLUSTER_CRITICAL,
            db_pc=OPENSHIFT_USER_CRITICAL,
            endpoint_pc=SYSTEM_CLUSTER_CRITICAL,
        )

        # 2. Wait for reconciliation and restart db pods
        logger.test_step("Step 2: Waiting for reconciliation and restarting db pods")
        self._restart_db_and_wait(namespace=namespace)

        # 3. Verify priority classes on all pods
        logger.test_step("Step 3: Verifying pod priority classes")
        self._verify_pod_priority_classes(
            namespace=namespace,
            core_pc=SYSTEM_CLUSTER_CRITICAL,
            db_pc=OPENSHIFT_USER_CRITICAL,
            endpoint_pc=SYSTEM_CLUSTER_CRITICAL,
        )

        # 4. Capture pod creation times before operator restart
        logger.test_step("Step 4: Recording creation times of all NooBaa pods")
        noobaa_pods = get_pods_having_label(
            label=constants.NOOBAA_APP_LABEL, namespace=namespace
        )
        creation_times_before = {
            p["metadata"]["name"]: p["metadata"]["creationTimestamp"]
            for p in noobaa_pods
            if "noobaa-operator" not in p["metadata"]["name"]
        }
        logger.info(
            f"Pod creation times before operator restart: {creation_times_before}"
        )

        # 5. Restart the NooBaa operator pod
        logger.test_step("Step 5: Restarting the NooBaa operator pod")
        operator_pod = get_noobaa_operator_pod()
        logger.info(f"Deleting NooBaa operator pod {operator_pod.name}")
        operator_pod.delete(wait=True)

        # 6. Wait for the operator pod to be ready
        logger.test_step("Step 6: Waiting for the NooBaa operator pod to be ready")
        for operator_pod_list in TimeoutSampler(
            timeout=300,
            sleep=10,
            func=get_pods_having_label,
            label=constants.NOOBAA_OPERATOR_POD_LABEL,
            namespace=namespace,
        ):
            if operator_pod_list:
                new_operator_pod = Pod(**operator_pod_list[0])
                status = new_operator_pod.ocp.get_resource_status(new_operator_pod.name)
                if status == constants.STATUS_RUNNING:
                    logger.info(
                        f"NooBaa operator pod {new_operator_pod.name} is Running"
                    )
                    break
        MCG.wait_for_ready_status()

        # 7. Verify PriorityClassNames persist after operator restart
        logger.test_step(
            "Step 7: Verifying PriorityClassNames persist after operator restart"
        )
        self._verify_pod_priority_classes(
            namespace=namespace,
            core_pc=SYSTEM_CLUSTER_CRITICAL,
            db_pc=OPENSHIFT_USER_CRITICAL,
            endpoint_pc=SYSTEM_CLUSTER_CRITICAL,
        )

        # 8. Verify no pods were unnecessarily restarted
        logger.test_step("Step 8: Verifying no pods were unnecessarily restarted")
        noobaa_pods = get_pods_having_label(
            label=constants.NOOBAA_APP_LABEL, namespace=namespace
        )
        creation_times_after = {
            p["metadata"]["name"]: p["metadata"]["creationTimestamp"]
            for p in noobaa_pods
            if "noobaa-operator" not in p["metadata"]["name"]
        }
        logger.info(
            f"Pod creation times after operator restart: {creation_times_after}"
        )
        for pod_name, created_before in creation_times_before.items():
            created_after = creation_times_after.get(pod_name)
            assert created_after == created_before, (
                f"Pod {pod_name} was unnecessarily restarted after operator restart: "
                f"creationTimestamp changed from {created_before} to {created_after}"
            )
        logger.info("No pods were unnecessarily restarted after operator restart")

    def _wait_for_pvpool_pods_priority_class(
        self, bs_name, expected_pc, namespace, expected_count=None, timeout=360
    ):
        """
        Poll PVPool pods until all carry the expected priorityClassName and are Running.
        Pass None for expected_pc to wait for the field to be absent.
        If expected_count is set, also wait until exactly that many pods exist.
        """
        for pods in TimeoutSampler(
            timeout=timeout,
            sleep=15,
            func=get_noobaa_pvpool_pods,
            backingstore_name=bs_name,
            namespace=namespace,
        ):
            if not pods:
                continue
            if expected_count is not None and len(pods) != expected_count:
                continue
            all_match = True
            for pod in pods:
                pod_dict = pod.get()
                actual_pc = pod_dict["spec"].get("priorityClassName")
                phase = pod_dict.get("status", {}).get("phase")
                if actual_pc != expected_pc or phase != "Running":
                    all_match = False
                    break
            if all_match:
                return

    @tier2
    @polarion_id("OCS-7954")
    @config.run_with_provider_context_if_available
    def test_pvpool_backingstore_priority_class(self, bucket_factory, teardown_factory):
        """
        Test that priorityClassName configured on a PVPool backingstore CR
        propagates to its corresponding pod after operator reconciliation.

        1. Create 2 PVPool backingstores via bucket_factory (1 volume each)
        2. Verify backingstore CRs and pods have no priorityClassName
        3. Create 2 custom PriorityClasses (one per backingstore)
        4. Patch each backingstore CR with its corresponding PriorityClass
        5. Wait for the operator to reconcile and pods to restart
        6. Verify each PvPool pod has the expected priorityClassName
        7. Patch each backingstore CR to remove the priorityClassName
        8. Wait for the operator to reconcile and pods to restart
        9. Verify all PvPool pods run without any priorityClassName
        """
        namespace = config.ENV_DATA["cluster_namespace"]
        bs_ocp = OCP(kind="backingstore", namespace=namespace)

        # 1. Create 2 PVPool backingstores via bucket_factory
        logger.test_step("Step 1: Creating 2 PVPool backingstores")
        bucketclass_dict = {
            "interface": "OC",
            "backingstore_dict": {
                "pv": [(1, MIN_PV_BACKINGSTORE_SIZE_IN_GB, CEPHBLOCKPOOL_SC)]
            },
        }
        bs_names = []
        for _ in range(2):
            bucket = bucket_factory(1, "OC", bucketclass=bucketclass_dict)[0]
            bs_names.append(bucket.bucketclass.backingstores[0].name)

        # 2. Verify backingstore CRs and pods have no priorityClassName
        logger.test_step(
            "Step 2: Verifying backingstore CRs and pods have no priorityClassName"
        )
        for name in bs_names:
            bs_cr = bs_ocp.get(resource_name=name)
            assert (
                "priorityClassName" not in bs_cr["spec"]["pvPool"]
            ), f"Backingstore {name} already has priorityClassName set"
            pods = get_noobaa_pvpool_pods(name, namespace)
            assert pods, f"No PVPool pods found for backingstore {name}"
            for pod in pods:
                assert (
                    "priorityClassName" not in pod.get()["spec"]
                ), f"PVPool pod {pod.name} already has priorityClassName set"

        # 3. Create 2 custom PriorityClasses
        logger.test_step("Step 3: Creating 2 custom PriorityClasses")
        pc_map = {}
        for i, name in enumerate(bs_names, start=1):
            pc_obj = helpers.create_priority_class(name, 500000 + i)
            teardown_factory(pc_obj)
            pc_map[name] = pc_obj.name

        # 4. Patch each backingstore CR with its corresponding PriorityClass
        logger.test_step("Step 4: Patching each backingstore CR with its PriorityClass")
        for name in bs_names:
            patch = {"spec": {"pvPool": {"priorityClassName": pc_map[name]}}}
            bs_ocp.patch(
                resource_name=name,
                params=json.dumps(patch),
                format_type="merge",
            )

        # 5. Wait for the operator to reconcile and pods to restart
        logger.test_step("Step 5: Waiting for operator reconciliation")
        for name in bs_names:
            self._wait_for_pvpool_pods_priority_class(
                bs_name=name, expected_pc=pc_map[name], namespace=namespace
            )

        # 6. Verify each PvPool pod has the expected priorityClassName
        logger.test_step("Step 6: Verifying pod priorityClassNames after patching")
        for name in bs_names:
            for pod in get_noobaa_pvpool_pods(name, namespace):
                actual = pod.get()["spec"].get("priorityClassName")
                assert actual == pc_map[name], (
                    f"PVPool pod {pod.name} priorityClassName mismatch: "
                    f"expected={pc_map[name]}, actual={actual}"
                )

        # 7. Remove priorityClassName from all backingstore CRs
        logger.test_step("Step 7: Removing priorityClassName from all backingstore CRs")
        for name in bs_names:
            remove_patch = [{"op": "remove", "path": "/spec/pvPool/priorityClassName"}]
            bs_ocp.patch(
                resource_name=name,
                params=json.dumps(remove_patch),
                format_type="json",
            )

        # 8. Wait for the operator to reconcile and pods to restart
        logger.test_step("Step 8: Waiting for operator reconciliation")
        for name in bs_names:
            self._wait_for_pvpool_pods_priority_class(
                bs_name=name, expected_pc=None, namespace=namespace
            )

        # 9. Verify all PvPool pods run without any priorityClassName
        logger.test_step(
            "Step 9: Verifying all PvPool pods run without priorityClassName"
        )
        for name in bs_names:
            for pod in get_noobaa_pvpool_pods(name, namespace):
                actual = pod.get()["spec"].get("priorityClassName")
                assert actual is None, (
                    f"PVPool pod {pod.name} still has priorityClassName={actual} "
                    f"after removal from backingstore CR"
                )

    @tier2
    @polarion_id("OCS-7955")
    @config.run_with_provider_context_if_available
    def test_pvpool_multi_volume_priority_class(self, bucket_factory):
        """
        Test that priorityClassName set on a multi-volume PVPool backingstore CR
        propagates to all of its corresponding pods after operator reconciliation.

        1. Create a PVPool backingstore with 3 volumes
        2. Patch the backingstore CR with priorityClassName=openshift-user-critical
        3. Wait for the NooBaa operator to reconcile and for the pods to restart
        4. Verify that all 3 PVPool pods have priorityClassName=openshift-user-critical
        5. Patch the backingstore CR to remove the priorityClassName field
        6. Wait for the NooBaa operator to reconcile and for the pods to restart
        7. Verify that all 3 PVPool pods are Running without any priorityClassName
        """
        namespace = config.ENV_DATA["cluster_namespace"]
        bs_ocp = OCP(kind="backingstore", namespace=namespace)
        num_volumes = 3

        # 1. Create a PVPool backingstore with 3 volumes
        logger.test_step(
            f"Step 1: Creating a PVPool backingstore with {num_volumes} volumes"
        )
        bucketclass_dict = {
            "interface": "OC",
            "backingstore_dict": {
                "pv": [(num_volumes, MIN_PV_BACKINGSTORE_SIZE_IN_GB, CEPHBLOCKPOOL_SC)]
            },
        }
        bucket = bucket_factory(1, "OC", bucketclass=bucketclass_dict)[0]
        bs_name = bucket.bucketclass.backingstores[0].name

        # 2. Patch the backingstore CR with priorityClassName
        logger.test_step(
            f"Step 2: Patching backingstore {bs_name} with "
            f"priorityClassName={OPENSHIFT_USER_CRITICAL}"
        )
        patch = {"spec": {"pvPool": {"priorityClassName": OPENSHIFT_USER_CRITICAL}}}
        bs_ocp.patch(
            resource_name=bs_name,
            params=json.dumps(patch),
            format_type="merge",
        )

        # 3. Wait for the operator to reconcile and pods to restart
        logger.test_step("Step 3: Waiting for operator reconciliation")
        self._wait_for_pvpool_pods_priority_class(
            bs_name=bs_name,
            expected_pc=OPENSHIFT_USER_CRITICAL,
            namespace=namespace,
            expected_count=num_volumes,
        )

        # 4. Verify all PVPool pods have the expected priorityClassName
        logger.test_step(
            "Step 4: Verifying all PVPool pods have "
            f"priorityClassName={OPENSHIFT_USER_CRITICAL}"
        )
        pods = get_noobaa_pvpool_pods(bs_name, namespace)
        assert (
            len(pods) == num_volumes
        ), f"Expected {num_volumes} PVPool pods for {bs_name}, got {len(pods)}"
        for pod in pods:
            actual = pod.get()["spec"].get("priorityClassName")
            assert actual == OPENSHIFT_USER_CRITICAL, (
                f"PVPool pod {pod.name} priorityClassName mismatch: "
                f"expected={OPENSHIFT_USER_CRITICAL}, actual={actual}"
            )

        # 5. Remove priorityClassName from the backingstore CR
        logger.test_step("Step 5: Removing priorityClassName from backingstore CR")
        remove_patch = [{"op": "remove", "path": "/spec/pvPool/priorityClassName"}]
        bs_ocp.patch(
            resource_name=bs_name,
            params=json.dumps(remove_patch),
            format_type="json",
        )

        # 6. Wait for the operator to reconcile and pods to restart
        logger.test_step("Step 6: Waiting for operator reconciliation")
        self._wait_for_pvpool_pods_priority_class(
            bs_name=bs_name,
            expected_pc=None,
            namespace=namespace,
            expected_count=num_volumes,
        )

        # 7. Verify all PVPool pods are Running without any priorityClassName
        logger.test_step(
            "Step 7: Verifying all PVPool pods run without priorityClassName"
        )
        pods = get_noobaa_pvpool_pods(bs_name, namespace)
        assert (
            len(pods) == num_volumes
        ), f"Expected {num_volumes} PVPool pods for {bs_name}, got {len(pods)}"
        for pod in pods:
            actual = pod.get()["spec"].get("priorityClassName")
            assert actual is None, (
                f"PVPool pod {pod.name} still has priorityClassName={actual} "
                f"after removal from backingstore CR"
            )
