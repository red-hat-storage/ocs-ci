import json
import logging
from functools import partial
from uuid import uuid4

import pytest

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.csv import CSV
from ocs_ci.ocs.resources.pod import get_pods_having_label
from ocs_ci.ocs.utils import get_non_acm_cluster_config
from ocs_ci.helpers.scc_helpers import (
    REQUIRED_PREFIXES_BY_CSV,
    SCC_BY_CATEGORY,
    SCC_BY_POD_PREFIX,
    get_active_scc_pods,
    get_expected_scc_for_label,
    get_matching_scc_components,
    get_pod_scc_annotations,
    get_required_scc_labels,
    get_scc_pinning_errors,
    pod_matches_scc_label,
    verify_pod_scc_pinning,
)
from ocs_ci.framework.pytest_customization.marks import (
    brown_squad,
    polarion_id,
    post_upgrade,
    skipif_external_mode,
    skipif_mcg_only,
    skipif_ocs_version,
    tier1,
    tier2,
)
from ocs_ci.framework.testlib import ManageTest
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)


@brown_squad
@post_upgrade
@skipif_ocs_version("<4.23")
@skipif_external_mode
class TestSCCEnforcement(ManageTest):
    """
    Tests for RHSTOR-8757: Enforce least-privileged SCC across ODF pods.

    Verifies that all ODF pods carry the openshift.io/required-scc annotation
    with the correct SCC name, and that the admission controller enforces
    the pinning by setting openshift.io/scc to the same value.
    """

    def _verify_components_scc(self, category_name: str) -> None:
        """Check SCCs on all active pods in one component category."""
        namespace = config.ENV_DATA["cluster_namespace"]
        all_pods = get_active_scc_pods(namespace)
        checked_count = 0
        errors = []
        required_labels = get_required_scc_labels()

        for selector, default_scc in SCC_BY_CATEGORY[category_name].items():
            # Reuse the namespace snapshot to check all active pods for this component.
            selected = [pod for pod in all_pods if pod_matches_scc_label(pod, selector)]
            if not selected and selector in required_labels:
                errors.append(
                    f"Required component with label '{selector}' has no active pod"
                )
            for pod in selected:
                expected_scc = get_expected_scc_for_label(pod, selector, default_scc)
                errors.extend(get_scc_pinning_errors(pod, selector, expected_scc))
            # Count each checked pod so the final assertion rejects an empty category.
            checked_count += len(selected)

        assert not errors, "SCC pinning failed:\n" + "\n".join(errors)
        assert checked_count > 0, f"No {category_name} pods found in {namespace}"

    # --- Category tests ---
    # Values follow the merged RHSTOR-8757 operator changes. NooBaa only sets
    # the CNPG DB required-SCC annotation when it creates a new DB cluster.

    @tier1
    @skipif_mcg_only
    @polarion_id("OCS-8070")
    def test_rook_ceph_daemons_scc(self):
        """
        Verify Rook-Ceph workloads use their expected SCCs.

        Checks all active Rook-Ceph pods covered by the shared mapping.

        """
        self._verify_components_scc("Rook-Ceph")

    @tier1
    @skipif_mcg_only
    @polarion_id("OCS-8069")
    def test_csi_plugins_scc(self):
        """
        Verify all CSI plugin pods are pinned to ceph-csi-op-scc.

        Checks CephFS, RBD, and optional NFS plugin pods.

        """
        self._verify_components_scc("CSI")

    @tier1
    @polarion_id("OCS-8071")
    def test_noobaa_scc(self):
        """
        Verify all NooBaa pods are pinned to their expected SCCs.

        noobaa-core and noobaa-endpoint use dedicated custom SCCs.
        noobaa-operator, CNPG DB on fresh installs, cnpg-controller-manager,
        and prometheus-adapter use restricted-v2.

        Note: noobaa-agent pods are conditional (require a pv-pool
        BackingStore) and ephemeral — covered by the audit test when
        present, not tested here.

        """
        self._verify_components_scc("NooBaa")

    @tier1
    @polarion_id("OCS-8068")
    def test_odf_operators_scc(self):
        """
        Verify all ODF operator pods are pinned to their expected SCCs.

        Includes operator managers and other ODF-managed workloads. Metrics
        exporter SCC depends on host networking.

        """
        self._verify_components_scc("ODF operators")

    @tier1
    @skipif_mcg_only
    @polarion_id("OCS-8072")
    def test_blackbox_exporter_scc(self):
        """
        Verify blackbox exporter pod is pinned to odf-blackbox-scc.

        """
        self._verify_components_scc("blackbox exporter")

    # --- Standalone tests ---

    @tier1
    @polarion_id("OCS-8073")
    def test_blackbox_exporter_scc_no_multus(self):
        """
        Verify blackbox exporter pod has correct SCC on a non-Multus cluster.

        Checks requested and admitted SCC values on existing exporter pods.
        Skips when the StorageCluster uses Multus networking.

        """
        namespace = config.ENV_DATA["cluster_namespace"]
        storage_clusters = OCP(
            kind=constants.STORAGECLUSTER, namespace=namespace
        ).get()["items"]
        assert storage_clusters, f"No StorageCluster found in {namespace}"
        if any(
            sc.get("spec", {}).get("network", {}).get("provider") == "multus"
            for sc in storage_clusters
        ):
            pytest.skip("StorageCluster uses Multus networking")

        pods = get_pods_having_label(
            label=constants.BLACKBOX_POD_LABEL_422_AND_ABOVE,
            namespace=namespace,
        )
        if not pods:
            pytest.skip(
                "No blackbox-exporter pods found. "
                "Verify StorageCluster has monitoring enabled."
            )

        logger.info(
            "Checking blackbox exporter SCC on non-Multus cluster " "(%d pod(s) found)",
            len(pods),
        )
        for pod_data in pods:
            result = verify_pod_scc_pinning(pod_data, constants.SCC_ODF_BLACKBOX)
            assert result["required_match"], (
                f"Blackbox pod '{result['pod_name']}': "
                f"{constants.SCC_REQUIRED_ANNOTATION}="
                f"'{result['required_scc']}', "
                f"expected '{constants.SCC_ODF_BLACKBOX}'."
            )
            assert result["admission_match"], (
                f"Blackbox pod '{result['pod_name']}': admission SCC "
                f"'{result['admission_scc']}' does not match required "
                f"SCC '{result['required_scc']}'"
            )

    @tier2
    @skipif_mcg_only
    @polarion_id("OCS-8074")
    def test_daemonset_scc_annotation_every_node(self):
        """
        Verify requested and admitted SCCs on all existing CSI node pods.

        Checks CephFS and RBD pods. DaemonSet scheduling and expected node
        coverage are outside this test's scope.

        """
        namespace = config.ENV_DATA["cluster_namespace"]
        daemonset_labels = [
            (
                constants.CSI_CEPHFSPLUGIN_LABEL_419,
                "cephfs-nodeplugin",
                constants.SCC_CEPH_CSI_OP,
            ),
            (
                constants.CSI_RBDPLUGIN_LABEL_419,
                "rbd-nodeplugin",
                constants.SCC_CEPH_CSI_OP,
            ),
        ]

        for label, ds_name, expected_scc in daemonset_labels:
            pods = get_pods_having_label(label=label, namespace=namespace)
            assert pods, (
                f"No pods found for DaemonSet '{ds_name}' " f"with label '{label}'"
            )
            logger.info(
                "Checking required-scc on %d %s pod(s) across nodes",
                len(pods),
                ds_name,
            )
            for pod_data in pods:
                errors = get_scc_pinning_errors(pod_data, ds_name, expected_scc)
                assert not errors, "\n".join(errors)

    @tier2
    def test_devicefinder_scc_annotations(self):
        """Check SCC and management annotations when discovery is deployed."""
        namespace = config.ENV_DATA["cluster_namespace"]
        daemonsets = OCP(kind=constants.DAEMONSET, namespace=namespace).get(
            selector="app=devicefinder-discovery"
        )["items"]
        if not daemonsets:
            pytest.skip("Device discovery DaemonSet is not deployed")

        pods = get_pods_having_label(
            label="app=devicefinder-discovery", namespace=namespace
        )
        assert pods, "Device discovery is deployed but has no pods"
        management_annotation = "target.workload.openshift.io/management"
        for pod_data in pods:
            pod_name = pod_data["metadata"]["name"]
            errors = get_scc_pinning_errors(
                pod_data, "devicefinder-discovery", constants.PRIVILEGED
            )
            assert not errors, "\n".join(errors)
            annotations = pod_data["metadata"].get("annotations") or {}
            assert json.loads(annotations.get(management_annotation, "{}")) == {
                "effect": "PreferredDuringScheduling"
            }, f"Discovery pod '{pod_name}' has an incorrect management annotation"

    @tier2
    @skipif_mcg_only
    def test_blackbox_exporter_preserves_multus_annotations(self):
        """Check SCC and configured Multus annotations on blackbox pods."""
        namespace = config.ENV_DATA["cluster_namespace"]
        storage_clusters = OCP(
            kind=constants.STORAGECLUSTER, namespace=namespace
        ).get()["items"]
        assert storage_clusters, f"No StorageCluster found in {namespace}"
        if not any(
            sc.get("spec", {}).get("network", {}).get("selectors", {}).get("cluster")
            for sc in storage_clusters
        ):
            pytest.skip("No Ceph cluster network attachment is configured")

        deployments = OCP(kind=constants.DEPLOYMENT, namespace=namespace).get(
            selector=constants.BLACKBOX_POD_LABEL_422_AND_ABOVE
        )["items"]
        assert len(deployments) == 1, "Expected one blackbox exporter Deployment"
        network_annotation = "k8s.v1.cni.cncf.io/networks"
        template = deployments[0]["spec"]["template"]
        expected_network = (template["metadata"].get("annotations") or {}).get(
            network_annotation
        )
        assert (
            expected_network
        ), "Blackbox template lost its configured network attachment"
        pods = get_pods_having_label(
            label=constants.BLACKBOX_POD_LABEL_422_AND_ABOVE, namespace=namespace
        )
        assert pods, "No blackbox exporter pods on a cluster with a network attachment"
        for pod_data in pods:
            errors = get_scc_pinning_errors(
                pod_data, "blackbox exporter", constants.SCC_ODF_BLACKBOX
            )
            assert not errors, "\n".join(errors)
            assert pod_data["metadata"]["annotations"].get(network_annotation) == (
                expected_network
            ), f"Blackbox pod '{pod_data['metadata']['name']}' lost its network attachment"

    @tier2
    @skipif_mcg_only
    def test_csi_annotations_survive_reconciliation(self, request):
        """Check configured SCC/CNI annotations before and after CSI reconciliation.

        Requires CNI annotations in a Driver or its OperatorConfig defaults.
        Temporarily change minReadySeconds on each applicable workload. The
        CSI controller rebuilds the spec and restores this field to zero.
        This triggers an observable reconciliation without a pod rollout.
        """
        namespace = config.ENV_DATA["cluster_namespace"]
        drivers = OCP(kind=constants.CEPH_DRIVER_CSI, namespace=namespace).get()[
            "items"
        ]
        assert drivers, "No Ceph CSI Driver resources found"
        operator_configs = OCP(
            kind="operatorconfigs.csi.ceph.io", namespace=namespace
        ).get()["items"]
        assert len(operator_configs) == 1, "Expected one Ceph CSI OperatorConfig"
        defaults = operator_configs[0]["spec"].get("driverSpecDefaults") or {}
        network_annotation = "k8s.v1.cni.cncf.io/networks"
        checked_count = 0

        for plugin, kind in (
            ("controllerPlugin", constants.DEPLOYMENT),
            ("nodePlugin", constants.DAEMONSET),
        ):
            workload_ocp = OCP(kind=kind, namespace=namespace)
            workloads = workload_ocp.get()["items"]
            for driver in drivers:
                annotations = (driver["spec"].get(plugin) or {}).get("annotations")
                if annotations is None:
                    annotations = (defaults.get(plugin) or {}).get("annotations") or {}
                expected_network = annotations.get(network_annotation)
                if not expected_network:
                    continue

                selected = [
                    workload
                    for workload in workloads
                    if any(
                        ref.get("kind") == "Driver"
                        and ref.get("uid") == driver["metadata"]["uid"]
                        for ref in workload["metadata"].get("ownerReferences", [])
                    )
                ]
                assert selected, f"No {kind} for Driver '{driver['metadata']['name']}'"
                for workload in selected:
                    name = workload["metadata"]["name"]
                    original_delay = workload["spec"].get("minReadySeconds", 0)
                    assert (
                        original_delay == 0
                    ), f"{kind} '{name}' has not reconciled minReadySeconds to zero"
                    labels = workload["spec"]["selector"]["matchLabels"]
                    selector = ",".join(
                        f"{key}={value}" for key, value in labels.items()
                    )
                    # Check both snapshots with the same assertions below.
                    for phase in ("before", "after"):
                        if phase == "after":
                            request.addfinalizer(
                                partial(
                                    workload_ocp.patch,
                                    resource_name=name,
                                    params=json.dumps(
                                        {"spec": {"minReadySeconds": original_delay}}
                                    ),
                                    format_type="merge",
                                )
                            )
                            assert workload_ocp.patch(
                                resource_name=name,
                                params=json.dumps({"spec": {"minReadySeconds": 1}}),
                                format_type="merge",
                            ), f"Failed to trigger reconciliation of {kind} '{name}'"
                            for workload in TimeoutSampler(
                                timeout=120,
                                sleep=3,
                                func=workload_ocp.get,
                                resource_name=name,
                            ):
                                if (
                                    workload["spec"].get("minReadySeconds", 0)
                                    == original_delay
                                ):
                                    break

                        template_annotations = (
                            workload["spec"]["template"]["metadata"].get("annotations")
                            or {}
                        )
                        assert (
                            template_annotations.get(network_annotation)
                            == expected_network
                        ), f"{kind} '{name}' lost its network attachment {phase} reconciliation"
                        assert (
                            template_annotations.get(constants.SCC_REQUIRED_ANNOTATION)
                            == constants.SCC_CEPH_CSI_OP
                        ), f"{kind} '{name}' lost its SCC pin {phase} reconciliation"
                        pods = get_pods_having_label(
                            label=selector, namespace=namespace
                        )
                        assert (
                            pods
                        ), f"No pods for {kind} '{name}' {phase} reconciliation"
                        for pod_data in pods:
                            errors = get_scc_pinning_errors(
                                pod_data, name, constants.SCC_CEPH_CSI_OP
                            )
                            assert not errors, "\n".join(errors)
                            assert (
                                pod_data["metadata"]["annotations"].get(
                                    network_annotation
                                )
                                == expected_network
                            ), (
                                f"Pod '{pod_data['metadata']['name']}' lost its network "
                                f"attachment {phase} reconciliation"
                            )
                    checked_count += 1

        if not checked_count:
            pytest.skip(
                "No CNI attachment is configured for CSI controller or node plugins"
            )

    @tier2
    def test_spoke_token_exchange_agent_scc(self):
        """Require the expected SCC on token exchange agents on DR spokes."""
        if (
            not config.multicluster
            or config.MULTICLUSTER.get("multicluster_mode") != constants.RDR_MODE
        ):
            pytest.skip("Requires a configured regional DR topology")

        spokes = get_non_acm_cluster_config()
        assert spokes, "Regional DR topology has no managed clusters"
        for spoke in spokes:
            with config.RunWithConfigContext(spoke.MULTICLUSTER["multicluster_index"]):
                namespace = config.ENV_DATA["cluster_namespace"]
                pods = get_pods_having_label(
                    label=constants.TOKEN_EXCHANGE_AGENT_LABEL, namespace=namespace
                )
                assert pods, (
                    "No token-exchange-agent pod on spoke "
                    f"'{config.ENV_DATA['cluster_name']}'"
                )
                for pod_data in pods:
                    errors = get_scc_pinning_errors(
                        pod_data, "token-exchange-agent", constants.SCC_RESTRICTED_V2
                    )
                    assert not errors, "\n".join(errors)

    @tier2
    @skipif_mcg_only
    @pytest.mark.parametrize("job_prefix", ["ocs-extend-cluster", "ocs-osd-removal"])
    def test_job_pods_carry_scc_annotation(self, job_prefix):
        """Check explicitly selected Jobs and their pods, including completed pods.

        Trigger expansion/removal in their normal workflows first. Set
        RUN.scc_jobs in the run configuration to a mapping from job prefix
        to the resulting current Job name. This test does not trigger
        storage changes or select historical Jobs automatically.
        """
        configured_jobs = config.RUN.get("scc_jobs", {})
        assert isinstance(configured_jobs, dict), "RUN.scc_jobs must be a mapping"
        job_name = configured_jobs.get(job_prefix)
        if not job_name:
            pytest.skip(f"Set RUN.scc_jobs['{job_prefix}'] to a current Job name")
        assert isinstance(job_name, str) and job_name.startswith(
            job_prefix
        ), f"Expected a Job name starting with '{job_prefix}', got {job_name!r}"

        namespace = config.ENV_DATA["cluster_namespace"]
        job = OCP(kind=constants.JOB, namespace=namespace).get(resource_name=job_name)
        expected_scc = SCC_BY_POD_PREFIX[job_prefix]
        required_scc, _ = get_pod_scc_annotations(job["spec"]["template"])
        assert required_scc == expected_scc, (
            f"Job '{job_name}' template requests '{required_scc}', "
            f"expected '{expected_scc}'"
        )
        pods = get_pods_having_label(label=f"job-name={job_name}", namespace=namespace)
        pods = [
            pod
            for pod in pods
            if any(
                ref.get("kind") == "Job" and ref.get("uid") == job["metadata"]["uid"]
                for ref in pod["metadata"].get("ownerReferences", [])
            )
        ]
        assert pods, f"No pods retained for current Job '{job_name}'"
        for pod_data in pods:
            errors = get_scc_pinning_errors(pod_data, job_name, expected_scc)
            assert not errors, "\n".join(errors)

    @tier2
    @polarion_id("OCS-8075")
    def test_cronjob_pods_carry_scc_annotation(self, request):
        """
        Verify status-reporter templates and one generated Job per CronJob.

        Requires a configured storage client. Checks requested and admitted
        SCCs on newly generated pods, including completed pods. Generated
        Jobs are deleted by the test finalizer.
        """
        namespace = config.ENV_DATA["cluster_namespace"]
        ocp_cronjob = OCP(kind=constants.CRONJOB, namespace=namespace)
        cronjobs = [
            cj
            for cj in ocp_cronjob.get()["items"]
            if cj["metadata"]["name"].endswith("-status-reporter")
        ]
        if not cronjobs:
            csvs = CSV(namespace=namespace).get()["items"]
            if not any(
                csv["metadata"]["name"].startswith("ocs-client-operator")
                for csv in csvs
            ):
                pytest.skip("Storage client operator is not installed")
            clients = OCP(kind=constants.STORAGECLIENT, namespace=namespace).get()[
                "items"
            ]
            assert (
                not clients
            ), "Storage client exists but has no status-reporter CronJob"
            pytest.skip(
                "No storage client with a status-reporter CronJob is configured"
            )

        ocp_job = OCP(kind=constants.JOB, namespace=namespace)
        for cj in cronjobs:
            cj_name = cj["metadata"]["name"]
            template = cj["spec"]["jobTemplate"]["spec"]["template"]
            required_scc, _ = get_pod_scc_annotations(template)
            assert required_scc == constants.SCC_RESTRICTED_V2, (
                f"CronJob '{cj_name}' template requests '{required_scc}', "
                f"expected '{constants.SCC_RESTRICTED_V2}'"
            )
            job_name = f"scc-status-reporter-{uuid4().hex[:8]}"
            request.addfinalizer(
                partial(
                    ocp_job.exec_oc_cmd,
                    command=f"delete job {job_name} --ignore-not-found=true",
                )
            )
            job = ocp_job.exec_oc_cmd(
                command=f"create job {job_name} --from=cronjob/{cj_name} -o yaml"
            )
            for pods in TimeoutSampler(
                timeout=120,
                sleep=3,
                func=get_pods_having_label,
                label=f"job-name={job_name}",
                namespace=namespace,
            ):
                job_pods = [
                    pod
                    for pod in pods
                    if any(
                        ref.get("kind") == "Job"
                        and ref.get("uid") == job["metadata"]["uid"]
                        for ref in pod["metadata"].get("ownerReferences", [])
                    )
                ]
                if job_pods:
                    for pod_data in job_pods:
                        errors = get_scc_pinning_errors(
                            pod_data, cj_name, constants.SCC_RESTRICTED_V2
                        )
                        assert not errors, "\n".join(errors)
                    break

    @tier1
    @polarion_id("OCS-8076")
    def test_full_cluster_scc_audit(self):
        """
        Check the expected and admitted SCC on every active storage pod.

        Fail when a required component has no pod or a pod has no mapping.
        Active Job pods are checked when present. Triggered and completed
        Job pods need separate tests for full coverage.

        """
        namespace = config.ENV_DATA["cluster_namespace"]
        all_pods = get_active_scc_pods(namespace)

        csv_names = {
            csv["metadata"]["name"]
            for csv in CSV(namespace=namespace).get().get("items", [])
        }
        required_prefixes = {
            prefix
            for csv_prefix, prefixes in REQUIRED_PREFIXES_BY_CSV.items()
            if any(name.startswith(csv_prefix) for name in csv_names)
            for prefix in prefixes
        }

        errors = []
        matched_components = set()
        for pod in all_pods:
            pod_name = pod.get("metadata", {}).get("name", "unknown")
            matches = get_matching_scc_components(pod)
            if not matches:
                errors.append(f"Pod '{pod_name}' has no SCC mapping")
                continue
            if len(matches) > 1:
                errors.append(
                    f"Pod '{pod_name}' matches multiple SCC entries: "
                    f"{[component for component, _ in matches]}"
                )
                continue

            component, expected_scc = matches[0]
            matched_components.add(component)
            errors.extend(get_scc_pinning_errors(pod, component, expected_scc))

        for component in get_required_scc_labels() | required_prefixes:
            if component not in matched_components:
                errors.append(f"Required component '{component}' has no active pod")

        assert not errors, "SCC mapping failed:\n" + "\n".join(errors)
        logger.info("Verified SCC pinning for all %d active pods", len(all_pods))
