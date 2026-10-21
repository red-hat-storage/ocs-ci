import logging

import pytest

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.csv import CSV
from ocs_ci.ocs.resources.pod import get_pods_having_label
from ocs_ci.helpers.scc_helpers import (
    REQUIRED_PREFIXES_BY_CSV,
    SCC_BY_CATEGORY,
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

        Specifically validates the code path where the blackbox-exporter
        reconciler operates without Multus NetworkAttachmentDefinitions.
        On non-Multus clusters the podAnnotations map in the Go reconciler
        is initialized without Multus entries, and the required-scc annotation
        must still be written without error.

        Skips on Multus-enabled clusters since the non-Multus code path is
        not exercised there.

        """
        if config.ENV_DATA.get("multus_create_public_net"):
            pytest.skip(
                "Cluster has Multus enabled. This test targets "
                "the non-Multus reconciler code path."
            )

        namespace = config.ENV_DATA["cluster_namespace"]
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
                f"expected '{constants.SCC_ODF_BLACKBOX}'. "
                f"Possible nil-map panic in reconciler on "
                f"non-Multus cluster."
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
        Verify DaemonSet-managed CSI pods carry the required-scc annotation
        on every worker node.

        Checks CSI nodeplugin DaemonSets (CephFS and RBD) to confirm that
        every pod instance across all nodes has the required-scc annotation,
        not just a single representative pod.

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
                result = verify_pod_scc_pinning(pod_data, expected_scc)
                assert result["required_match"], (
                    f"DaemonSet pod '{result['pod_name']}' ({ds_name}) "
                    f"has {constants.SCC_REQUIRED_ANNOTATION}="
                    f"'{result['required_scc']}', "
                    f"expected '{expected_scc}'"
                )

    @tier2
    @polarion_id("OCS-8075")
    def test_cronjob_pods_carry_scc_annotation(self):
        """
        Verify CronJob-spawned pods carry the required-scc annotation.

        Checks for reclaimspace and key-rotation CronJob resources and
        inspects any pods owned by their child Jobs. Skips if no CronJob
        pods exist at test execution time.

        """
        namespace = config.ENV_DATA["cluster_namespace"]
        ocp_cronjob = OCP(kind=constants.CRONJOB, namespace=namespace)
        cronjobs = ocp_cronjob.get().get("items", [])

        if not cronjobs:
            pytest.skip(f"No CronJobs found in namespace '{namespace}'")

        ocp_job = OCP(kind=constants.JOB, namespace=namespace)
        all_jobs = ocp_job.get().get("items", [])
        checked_count = 0

        for cj in cronjobs:
            cj_name = cj.get("metadata", {}).get("name", "unknown")
            cj_jobs = [
                j
                for j in all_jobs
                if any(
                    ref.get("kind") == "CronJob" and ref.get("name") == cj_name
                    for ref in j.get("metadata", {}).get("ownerReferences", [])
                )
            ]
            for job in cj_jobs:
                job_name = job.get("metadata", {}).get("name", "unknown")
                job_pods = get_pods_having_label(
                    label=f"job-name={job_name}", namespace=namespace
                )
                for pod_data in job_pods:
                    pod_name = pod_data.get("metadata", {}).get("name", "unknown")
                    required_scc, _ = get_pod_scc_annotations(pod_data)
                    assert required_scc, (
                        f"CronJob pod '{pod_name}' "
                        f"(from CronJob '{cj_name}') is missing the "
                        f"{constants.SCC_REQUIRED_ANNOTATION} annotation"
                    )
                    checked_count += 1

        if checked_count == 0:
            pytest.skip("CronJobs exist but no child Job pods are currently " "present")
        logger.info(
            "Verified required-scc on %d CronJob-spawned pod(s)",
            checked_count,
        )

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
