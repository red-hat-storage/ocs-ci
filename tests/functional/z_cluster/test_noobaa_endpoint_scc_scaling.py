import json
import logging

import pytest

from ocs_ci.framework import config
from ocs_ci.helpers.scc_helpers import get_scc_pinning_errors
from ocs_ci.framework.pytest_customization.marks import (
    brown_squad,
    skipif_external_mode,
    skipif_ocs_version,
    tier4a,
)
from ocs_ci.framework.testlib import ManageTest
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.deployment import Deployment
from ocs_ci.ocs.resources.pod import get_pods_having_label
from ocs_ci.utility.utils import TimeoutSampler


logger = logging.getLogger(__name__)


@brown_squad
@tier4a
@skipif_ocs_version("<4.23")
@skipif_external_mode
class TestNooBaaEndpointSCCScaling(ManageTest):
    """Verify endpoint SCC pins after scaling to zero and back."""

    @pytest.fixture()
    def suspended_noobaa_operator(self, request: pytest.FixtureRequest) -> None:
        """Stop endpoint reconciliation and restore the operator after the test."""
        namespace = config.ENV_DATA["cluster_namespace"]
        deployment_ocp = OCP(kind=constants.DEPLOYMENT, namespace=namespace)
        operator_obj = Deployment(
            **deployment_ocp.get(resource_name=constants.NOOBAA_OPERATOR_DEPLOYMENT)
        )
        original_replicas = operator_obj.data["spec"].get("replicas", 1)
        if original_replicas < 1:
            raise UnexpectedBehaviour("noobaa-operator is already scaled down")
        operator_obj.wait_for_available_replicas()

        noobaa_ocp = OCP(kind=constants.NOOBAA_KIND, namespace=namespace)
        noobaa = noobaa_ocp.get(resource_name=constants.NOOBAA_RESOURCE_NAME)
        autoscaler = noobaa["spec"].get("autoscaler") or {}
        if autoscaler.get("autoscalerType") == "keda":
            scaled_object_ocp = OCP(kind=constants.SCALED_OBJECT, namespace=namespace)
            logger.info("Waiting for the NooBaa KEDA ScaledObject and HPA name")
            for scaled_object in TimeoutSampler(
                timeout=120,
                sleep=5,
                func=scaled_object_ocp.get,
                resource_name=constants.NOOBAA_RESOURCE_NAME,
                dont_raise=True,
            ):
                if scaled_object and scaled_object.get("status", {}).get("hpaName"):
                    break

        def restore_operator() -> None:
            operator_obj.scale(replicas=original_replicas)
            operator_obj.wait_for_available_replicas()

        request.addfinalizer(restore_operator)
        operator_obj.scale(replicas=0)
        pods_sampler = TimeoutSampler(
            timeout=300,
            sleep=5,
            func=get_pods_having_label,
            label=constants.NOOBAA_OPERATOR_POD_LABEL,
            namespace=namespace,
        )
        pods_sampler.wait_for_func_value([])

    def _restore_endpoints(
        self, endpoint_obj: Deployment, deployment_ocp: OCP, replicas: int
    ) -> None:
        """Restore endpoint replicas and wait for the Deployment rollout."""
        endpoint_obj.scale(replicas=replicas)
        deployment_ocp.exec_oc_cmd(
            f"rollout status deployment/{constants.NOOBAA_ENDPOINT_DEPLOYMENT} --timeout=600s",
            out_yaml_format=False,
            timeout=660,
        )

    def _restore_autoscaling(
        self, scaled_object_ocp: OCP, pause_annotation: str, original_pause: str | None
    ) -> None:
        """Restore the KEDA pause annotation and wait if scaling resumes."""
        if not scaled_object_ocp.patch(
            resource_name=constants.NOOBAA_RESOURCE_NAME,
            params=json.dumps(
                {"metadata": {"annotations": {pause_annotation: original_pause}}}
            ),
            format_type="merge",
        ):
            raise UnexpectedBehaviour("Failed to restore KEDA pause annotation")

        # Preserve a saved pause; KEDA treats invalid values as true.
        if original_pause not in (None, "0", "f", "F", "false", "FALSE", "False"):
            return

        hpa_ocp = OCP(kind=constants.HPA, namespace=scaled_object_ocp.namespace)
        logger.info("Waiting for NooBaa KEDA autoscaling to resume")
        for state in TimeoutSampler(
            timeout=120,
            sleep=5,
            func=scaled_object_ocp.get,
            resource_name=constants.NOOBAA_RESOURCE_NAME,
        ):
            if any(
                condition["type"] == "Paused" and condition["status"] == "True"
                for condition in state.get("status", {}).get("conditions", [])
            ):
                continue
            hpa_name = state.get("status", {}).get("hpaName")
            if hpa_name and hpa_ocp.is_exist(resource_name=hpa_name):
                break

    def _wait_for_endpoint_baseline(
        self, deployment_ocp: OCP, namespace: str
    ) -> tuple[int, list[dict]]:
        """Wait for endpoint replicas and pods to stabilize."""
        logger.info("Waiting for the NooBaa endpoint Deployment and pods to be ready")
        for deployment in TimeoutSampler(
            timeout=600,
            sleep=5,
            func=deployment_ocp.get,
            resource_name=constants.NOOBAA_ENDPOINT_DEPLOYMENT,
        ):
            original_replicas = deployment["spec"].get("replicas", 1)
            deployment_status = deployment.get("status", {})
            pods = get_pods_having_label(
                label=constants.NOOBAA_ENDPOINT_POD_LABEL, namespace=namespace
            )
            if (
                original_replicas > 0
                and deployment_status.get("observedGeneration", 0)
                >= deployment["metadata"]["generation"]
                and deployment_status.get("updatedReplicas", 0) == original_replicas
                and deployment_status.get("availableReplicas", 0) == original_replicas
                and len(pods) == original_replicas
                and all(
                    not pod["metadata"].get("deletionTimestamp")
                    and pod.get("status", {}).get("phase") == constants.STATUS_RUNNING
                    for pod in pods
                )
            ):
                logger.info(
                    "NooBaa endpoint baseline is ready with %s replicas",
                    original_replicas,
                )
                return original_replicas, pods

    @pytest.mark.polarion_id("OCS-8402")
    def test_endpoint_scc_survives_scale_to_zero(
        self, request: pytest.FixtureRequest, suspended_noobaa_operator
    ) -> None:
        """Verify SCC pins on all endpoint pods recreated after scaling to zero."""
        namespace = config.ENV_DATA["cluster_namespace"]
        deployment_ocp = OCP(kind=constants.DEPLOYMENT, namespace=namespace)
        endpoint_obj = Deployment(
            **deployment_ocp.get(resource_name=constants.NOOBAA_ENDPOINT_DEPLOYMENT)
        )
        original_replicas, pods = self._wait_for_endpoint_baseline(
            deployment_ocp, namespace
        )
        errors = [
            error
            for pod in pods
            for error in get_scc_pinning_errors(
                pod, "noobaa-endpoint", constants.SCC_NOOBAA_ENDPOINT
            )
        ]
        if errors:
            raise UnexpectedBehaviour("Invalid SCC baseline:\n" + "\n".join(errors))
        original_uids = {pod["metadata"]["uid"] for pod in pods}

        noobaa_ocp = OCP(kind=constants.NOOBAA_KIND, namespace=namespace)
        noobaa = noobaa_ocp.get(resource_name=constants.NOOBAA_RESOURCE_NAME)
        autoscaler = noobaa["spec"].get("autoscaler") or {}
        hpa_ocp = OCP(kind=constants.HPA, namespace=namespace)
        # Pause KEDA so it cannot change replicas during manual endpoint scaling.
        if autoscaler.get("autoscalerType") == "keda":
            scaled_object_ocp = OCP(kind=constants.SCALED_OBJECT, namespace=namespace)
            scaled_object = scaled_object_ocp.get(
                resource_name=constants.NOOBAA_RESOURCE_NAME
            )
            if (
                scaled_object["spec"]["scaleTargetRef"]["name"]
                != constants.NOOBAA_ENDPOINT_DEPLOYMENT
            ):
                raise UnexpectedBehaviour("KEDA targets a different Deployment")
            annotations = scaled_object["metadata"].get("annotations") or {}
            if "autoscaling.keda.sh/paused-replicas" in annotations:
                raise UnexpectedBehaviour("KEDA has an explicit paused replica count")
            pause_annotation = "autoscaling.keda.sh/paused"
            original_pause = annotations.get(pause_annotation)
            hpa_name = scaled_object.get("status", {}).get("hpaName")
            if not hpa_name:
                raise UnexpectedBehaviour("KEDA has not reported its HPA name")

            def restore_autoscaling() -> None:
                self._restore_autoscaling(
                    scaled_object_ocp, pause_annotation, original_pause
                )

            request.addfinalizer(restore_autoscaling)
            if not scaled_object_ocp.patch(
                resource_name=constants.NOOBAA_RESOURCE_NAME,
                params=json.dumps(
                    {"metadata": {"annotations": {pause_annotation: "true"}}}
                ),
                format_type="merge",
            ):
                raise UnexpectedBehaviour("Failed to pause KEDA autoscaling")
            for state in TimeoutSampler(
                timeout=120,
                sleep=5,
                func=scaled_object_ocp.get,
                resource_name=constants.NOOBAA_RESOURCE_NAME,
            ):
                if any(
                    condition["type"] == "Paused" and condition["status"] == "True"
                    for condition in state.get("status", {}).get("conditions", [])
                ):
                    break
            hpa_ocp.wait_for_delete(resource_name=hpa_name, timeout=120)
        else:
            endpoint_hpas = [
                hpa
                for hpa in hpa_ocp.get()["items"]
                if hpa["spec"]["scaleTargetRef"].get("kind") == constants.DEPLOYMENT
                and hpa["spec"]["scaleTargetRef"]["name"]
                == constants.NOOBAA_ENDPOINT_DEPLOYMENT
            ]
            if len(endpoint_hpas) > 1 or any(
                hpa["spec"].get("minReplicas", 1) < 1 for hpa in endpoint_hpas
            ):
                raise UnexpectedBehaviour(
                    "Expected at most one endpoint HPA with minReplicas > 0"
                )

        def restore_endpoints() -> None:
            self._restore_endpoints(endpoint_obj, deployment_ocp, original_replicas)

        request.addfinalizer(restore_endpoints)
        endpoint_obj.scale(replicas=0)
        pods_sampler = TimeoutSampler(
            timeout=300,
            sleep=5,
            func=get_pods_having_label,
            label=constants.NOOBAA_ENDPOINT_POD_LABEL,
            namespace=namespace,
        )
        pods_sampler.wait_for_func_value([])
        if endpoint_obj.replicas != 0:
            raise UnexpectedBehaviour("Endpoint Deployment did not remain at zero")
        restore_endpoints()

        pods = [
            pod
            for pod in get_pods_having_label(
                label=constants.NOOBAA_ENDPOINT_POD_LABEL, namespace=namespace
            )
            if not pod["metadata"].get("deletionTimestamp")
        ]
        if not pods:
            raise UnexpectedBehaviour("No endpoint pods after scaling back up")
        for pod in pods:
            if pod["metadata"]["uid"] in original_uids:
                errors.append(
                    f"Endpoint pod '{pod['metadata']['name']}' was not recreated"
                )
            errors.extend(
                get_scc_pinning_errors(
                    pod, "noobaa-endpoint", constants.SCC_NOOBAA_ENDPOINT
                )
            )
        assert not errors, "Endpoint SCC checks failed after scaling:\n" + "\n".join(
            errors
        )
