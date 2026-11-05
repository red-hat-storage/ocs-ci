import json

import pytest

from ocs_ci.framework import config
from ocs_ci.helpers.scc_helpers import get_scc_pinning_errors
from ocs_ci.framework.pytest_customization.marks import (
    brown_squad,
    post_upgrade,
    skipif_external_mode,
    skipif_mcg_only,
    skipif_ocs_version,
    tier4a,
)
from ocs_ci.framework.testlib import ManageTest
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.deployment import Deployment
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.resources.pod import Pod, wait_for_pods_deletion
from ocs_ci.utility.utils import TimeoutSampler


def _get_validated_pods(deployment_obj: Deployment, scc_name: str) -> list:
    """Return stable running pods that require and use the expected SCC."""
    deployment_obj.wait_for_available_replicas()
    pods = deployment_obj.pods
    if (
        not pods
        or len(pods) != deployment_obj.replicas
        or any(
            pod.data["metadata"].get("deletionTimestamp")
            or pod.data.get("status", {}).get("phase") != constants.STATUS_RUNNING
            for pod in pods
        )
    ):
        raise UnexpectedBehaviour(f"{deployment_obj.name} pod baseline is not stable")
    errors = [
        error
        for pod in pods
        for error in get_scc_pinning_errors(pod.data, deployment_obj.name, scc_name)
    ]
    if errors:
        raise UnexpectedBehaviour("Invalid SCC baseline:\n" + "\n".join(errors))
    return pods


def _get_failed_create_events(
    original_pod: Pod, namespace: str
) -> tuple[OCP, dict[str, int]]:
    """Return the controlling ReplicaSet event query and baseline counts."""
    replica_set = next(
        (
            owner
            for owner in original_pod.data["metadata"].get("ownerReferences", [])
            if owner["kind"] == "ReplicaSet" and owner.get("controller")
        ),
        None,
    )
    if not replica_set:
        raise UnexpectedBehaviour(
            f"Pod {original_pod.name} has no ReplicaSet controller"
        )
    event_ocp = OCP(
        kind="Event",
        namespace=namespace,
        field_selector=f"involvedObject.uid={replica_set['uid']},reason=FailedCreate",
    )
    original_counts = {
        event["metadata"]["uid"]: event.get("count", 1)
        for event in event_ocp.get()["items"]
    }
    return event_ocp, original_counts


def _validate_direct_scc_access(
    original_pod: Pod, scc_data: dict, namespace: str
) -> str:
    """Validate toolbox SCC grants and return its service-account identity."""
    if original_pod.data["spec"].get("serviceAccountName") != "rook-ceph-default":
        raise UnexpectedBehaviour("Toolbox does not use rook-ceph-default")
    service_account = f"system:serviceaccount:{namespace}:rook-ceph-default"
    users = scc_data.get("users") or []
    if service_account not in users:
        raise UnexpectedBehaviour("rook-ceph-default has no direct SCC grant")
    service_account_groups = [
        "system:serviceaccounts",
        f"system:serviceaccounts:{namespace}",
    ]
    if set(service_account_groups).intersection(scc_data.get("groups") or []):
        raise UnexpectedBehaviour("rook-ceph-default also has group SCC access")
    access_review_obj = OCS(
        apiVersion="authorization.k8s.io/v1",
        kind="SubjectAccessReview",
        metadata={},
        spec={
            "user": service_account,
            "groups": service_account_groups,
            "resourceAttributes": {
                "namespace": namespace,
                "verb": "use",
                "group": "security.openshift.io",
                "resource": "securitycontextconstraints",
                "name": scc_data["metadata"]["name"],
            },
        },
    )
    access = access_review_obj.create(do_reload=False)["status"]
    if access.get("evaluationError"):
        raise UnexpectedBehaviour(
            f"SCC access review failed: {access['evaluationError']}"
        )
    if access["allowed"]:
        raise UnexpectedBehaviour("rook-ceph-default also has RBAC SCC access")
    return service_account


def _wait_for_scc_rejection(
    deployment_obj: Deployment,
    event_ocp: OCP,
    original_counts: dict[str, int],
    original_uids: set[str],
    expected_errors: tuple[str, ...],
) -> None:
    """Wait for a new SCC rejection with no active replacement pods."""
    for snapshot in TimeoutSampler(timeout=180, sleep=5, func=event_ocp.get):
        rejected = any(
            event.get("count", 1) > original_counts.get(event["metadata"]["uid"], 0)
            and all(error in event.get("message", "") for error in expected_errors)
            for event in snapshot["items"]
        )
        replacements = [
            pod
            for pod in deployment_obj.pods
            if pod.data["metadata"]["uid"] not in original_uids
            and not pod.data["metadata"].get("deletionTimestamp")
        ]
        if rejected and not replacements:
            break
        # Retry replacements admitted before the SCC change reaches the cache.
        for pod_obj in replacements:
            pod_obj.delete(wait=False)


@brown_squad
@tier4a
@post_upgrade
@skipif_ocs_version("<4.23")
@skipif_external_mode
@skipif_mcg_only
class TestSCCAdmissionFailures(ManageTest):
    """Test admission failures for pinned ODF workloads."""

    @pytest.fixture()
    def suspended_ocs_operator(self, request: pytest.FixtureRequest) -> None:
        """Stop SCC reconciliation and restore the operator after each test."""
        namespace = config.ENV_DATA["cluster_namespace"]
        deployment_ocp = OCP(kind=constants.DEPLOYMENT, namespace=namespace)
        operator_obj = Deployment(
            **deployment_ocp.get(resource_name=constants.OCS_OPERATOR_DEPLOYMENT)
        )
        original_replicas = operator_obj.data["spec"].get("replicas", 1)
        if original_replicas < 1:
            raise UnexpectedBehaviour("ocs-operator is already scaled down")
        operator_obj.wait_for_available_replicas()

        def restore_operator() -> None:
            operator_obj.scale(replicas=original_replicas)
            operator_obj.wait_for_available_replicas()

        request.addfinalizer(restore_operator)
        operator_obj.scale(replicas=0)
        wait_for_pods_deletion(
            label=constants.OCS_OPERATOR_LABEL,
            namespace=namespace,
            timeout=300,
        )

    @pytest.mark.parametrize(
        "deployment_name,scc_name,revoke_access",
        [
            pytest.param(
                "odf-blackbox-exporter",
                constants.SCC_ODF_BLACKBOX,
                False,
                marks=pytest.mark.polarion_id("OCS-8400"),
                id="missing-blackbox-scc",
            ),
            pytest.param(
                "rook-ceph-tools",
                constants.SCC_ROOK_CEPH,
                True,
                marks=pytest.mark.polarion_id("OCS-8401"),
                id="rook-ceph-scc-access-revoked",
            ),
        ],
    )
    def test_pinned_odf_pod_rejected(
        self,
        request: pytest.FixtureRequest,
        suspended_ocs_operator,
        deployment_name: str,
        scc_name: str,
        revoke_access: bool,
    ) -> None:
        """Reject pod recreation after its pinned SCC or SCC access is removed."""
        namespace = config.ENV_DATA["cluster_namespace"]
        deployment_ocp = OCP(kind=constants.DEPLOYMENT, namespace=namespace)
        deployment_obj = Deployment(**deployment_ocp.get(resource_name=deployment_name))
        pods = _get_validated_pods(deployment_obj, scc_name)

        original_pod = pods[0]
        original_uids = {pod.data["metadata"]["uid"] for pod in pods}
        event_ocp, original_counts = _get_failed_create_events(original_pod, namespace)

        scc_ocp = OCP(kind=constants.SCC, namespace=namespace)
        scc_data = scc_ocp.get(resource_name=scc_name)
        for field in (
            "uid",
            "resourceVersion",
            "creationTimestamp",
            "generation",
            "managedFields",
        ):
            scc_data["metadata"].pop(field, None)
        scc_obj = OCS(**scc_data)

        if revoke_access:
            service_account = _validate_direct_scc_access(
                original_pod, scc_data, namespace
            )

        def restore_workload() -> None:
            scc_obj.apply(**scc_data)
            deployment_obj.wait_for_available_replicas()

        request.addfinalizer(restore_workload)
        if revoke_access:
            if not scc_ocp.patch(
                resource_name=scc_name,
                params=json.dumps(
                    {
                        "users": [
                            user
                            for user in (scc_data.get("users") or [])
                            if user != service_account
                        ]
                    }
                ),
                format_type="merge",
            ):
                raise UnexpectedBehaviour(
                    "Failed to revoke rook-ceph-default SCC access"
                )
            expected_errors = (
                f'provider "{scc_name}"',
                "not usable by user or serviceaccount",
            )
        else:
            scc_obj.delete()
            expected_errors = (
                f'failed to retrieve the required SCC "{scc_name}"',
                "not found",
            )
        original_pod.delete()
        _wait_for_scc_rejection(
            deployment_obj, event_ocp, original_counts, original_uids, expected_errors
        )
