import logging

import pytest

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.csv import CSV
from ocs_ci.ocs.resources.pod import get_pods_having_label
from ocs_ci.helpers.scc_helpers import (
    get_pod_scc_annotations,
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


# Expected values from the RHSTOR-8757 operator changes. Optional components
# are checked when present. Jobs and optional topologies need separate runs.
SCC_BY_CATEGORY = {
    "Rook-Ceph": {
        constants.MON_APP_LABEL: constants.SCC_ROOK_CEPH,
        constants.OSD_APP_LABEL: constants.SCC_ROOK_CEPH,
        constants.MGR_APP_LABEL: constants.SCC_ROOK_CEPH,
        constants.MDS_APP_LABEL: constants.SCC_ROOK_CEPH,
        constants.RGW_APP_LABEL: constants.SCC_ROOK_CEPH,
        constants.CRASHCOLLECTOR_APP_LABEL: constants.SCC_ROOK_CEPH,
        constants.EXPORTER_APP_LABEL: constants.SCC_ROOK_CEPH,
        constants.TOOL_APP_LABEL: constants.SCC_ROOK_CEPH,
        constants.OPERATOR_LABEL: constants.SCC_ROOK_CEPH,
        constants.OSD_PREPARE_APP_LABEL: constants.SCC_ROOK_CEPH,
        f"app={constants.OSD_KEY_ROTATION_POD_NAME}": constants.SCC_ROOK_CEPH,
        constants.RBD_MIRROR_APP_LABEL: constants.SCC_ROOK_CEPH,
        "app=rook-ceph-nfs": constants.SCC_ROOK_CEPH,
    },
    "CSI": {
        constants.CSI_CEPHFSPLUGIN_LABEL_419: constants.SCC_CEPH_CSI_OP,
        constants.CSI_RBDPLUGIN_LABEL_419: constants.SCC_CEPH_CSI_OP,
        constants.CSI_CEPHFSPLUGIN_PROVISIONER_LABEL_419: constants.SCC_CEPH_CSI_OP,
        constants.CSI_RBDPLUGIN_PROVISIONER_LABEL_419: constants.SCC_CEPH_CSI_OP,
        constants.CSI_CEPHFS_ADDON_NODEPLUGIN_LABEL_420: constants.SCC_CEPH_CSI_OP,
        constants.CSI_RBD_ADDON_NODEPLUGIN_LABEL_420: constants.SCC_CEPH_CSI_OP,
        constants.NFS_CSI_NODEPLUGIN_LABEL_419: constants.SCC_CEPH_CSI_OP,
        constants.NFS_CSI_CTRLPLUGIN_LABEL_419: constants.SCC_CEPH_CSI_OP,
    },
    "NooBaa": {
        constants.NOOBAA_CORE_POD_LABEL: constants.SCC_NOOBAA_CORE,
        constants.NOOBAA_DB_LABEL_419_AND_ABOVE: constants.SCC_RESTRICTED_V2,
        constants.NOOBAA_ENDPOINT_POD_LABEL: constants.SCC_NOOBAA_ENDPOINT,
        constants.NOOBAA_OPERATOR_POD_LABEL: constants.SCC_RESTRICTED_V2,
        constants.NOOBAA_CNPG_POD_LABEL: constants.SCC_RESTRICTED_V2,
        constants.NOOBAA_PROMETHEUS_ADAPTER_LABEL: constants.SCC_RESTRICTED_V2,
    },
    "ODF operators": {
        constants.OCS_OPERATOR_LABEL: constants.SCC_RESTRICTED_V2,
        constants.ODF_OPERATOR_CONTROL_MANAGER_LABEL: constants.SCC_RESTRICTED_V2,
        constants.ODF_EXTERNAL_SNAPSHOTTER: constants.SCC_RESTRICTED_V2,
        constants.CEPH_CSI_CONTROLLER_MANAGER_LABEL: constants.SCC_RESTRICTED_V2,
        constants.CSI_ADDONS_CONTROLLER_MANAGER_LABEL: constants.SCC_RESTRICTED_V2,
        constants.OCS_METRICS_EXPORTER: constants.SCC_RESTRICTED_V2,
        constants.ODF_CONSOLE: constants.SCC_RESTRICTED_V2,
        constants.UX_BACKEND_APP_LABEL: constants.SCC_RESTRICTED_V2,
        constants.PROVIDER_SERVER_LABEL: constants.SCC_RESTRICTED_V2,
        "app=vault-agent-rgw": constants.SCC_RESTRICTED_V2,
        constants.PROMETHEUS_OPERATOR_LABEL: constants.SCC_RESTRICTED_V2,
        constants.MANAGED_PROMETHEUS_LABEL: constants.SCC_NONROOT_V2,
        constants.MANAGED_ALERTMANAGER_LABEL: constants.SCC_NONROOT_V2,
        constants.MANAGED_FUSION_PROMETHEUS_LABEL: constants.SCC_NONROOT_V2,
        constants.MANAGED_FUSION_ALERTMANAGER_LABEL: constants.SCC_NONROOT_V2,
        constants.TOKEN_EXCHANGE_AGENT_LABEL: constants.SCC_RESTRICTED_V2,
    },
    "blackbox exporter": {
        constants.BLACKBOX_POD_LABEL_422_AND_ABOVE: constants.SCC_ODF_BLACKBOX,
    },
}

REQUIRED_CEPH_LABELS = {
    constants.MON_APP_LABEL,
    constants.OSD_APP_LABEL,
    constants.MGR_APP_LABEL,
    constants.OPERATOR_LABEL,
    constants.CSI_CEPHFSPLUGIN_LABEL_419,
    constants.CSI_RBDPLUGIN_LABEL_419,
    constants.CSI_CEPHFSPLUGIN_PROVISIONER_LABEL_419,
    constants.CSI_RBDPLUGIN_PROVISIONER_LABEL_419,
    constants.CEPH_CSI_CONTROLLER_MANAGER_LABEL,
}
REQUIRED_LABELS = REQUIRED_CEPH_LABELS | {
    constants.NOOBAA_CORE_POD_LABEL,
    constants.NOOBAA_DB_LABEL_419_AND_ABOVE,
    constants.NOOBAA_ENDPOINT_POD_LABEL,
    constants.NOOBAA_OPERATOR_POD_LABEL,
    constants.OCS_OPERATOR_LABEL,
    constants.ODF_OPERATOR_CONTROL_MANAGER_LABEL,
    constants.ODF_CONSOLE,
}
HOST_NETWORK_LABELS = {constants.OPERATOR_LABEL, constants.OCS_METRICS_EXPORTER}

# Some operator templates do not have a unique label in the current mapping.
SCC_BY_POD_PREFIX = {
    "devicefinder-discovery": constants.PRIVILEGED,
    constants.OCS_CLIENT_OPERATOR_CONTROLLER_MANAGER_PREFIX: constants.SCC_RESTRICTED_V2,
    constants.OCS_CLIENT_OPERATOR_CONSOLE: constants.SCC_RESTRICTED_V2,
    constants.ODF_MULTICLUSTER_ORCHESTRATOR_CONTROLLER_MANAGER: constants.SCC_RESTRICTED_V2,
    "odf-multicluster-console": constants.SCC_RESTRICTED_V2,
    "ocs-extend-cluster": constants.SCC_RESTRICTED_V2,
    "ocs-osd-removal": constants.SCC_ROOK_CEPH,
    "onboarding-validation-keys-generator": constants.SCC_RESTRICTED_V2,
}
SCC_BY_NAME_FRAGMENT = {
    "status-reporter": constants.SCC_RESTRICTED_V2,
    "analyze-resource": constants.SCC_RESTRICTED_V2,
}
REQUIRED_PREFIXES_BY_CSV = {
    "ocs-client-operator": (
        constants.OCS_CLIENT_OPERATOR_CONTROLLER_MANAGER_PREFIX,
        constants.OCS_CLIENT_OPERATOR_CONSOLE,
    ),
    "odf-multicluster-orchestrator": (
        constants.ODF_MULTICLUSTER_ORCHESTRATOR_CONTROLLER_MANAGER,
        "odf-multicluster-console",
    ),
}


def _required_labels() -> set[str]:
    required = REQUIRED_LABELS
    if config.ENV_DATA.get("mcg_only_deployment"):
        required = required - REQUIRED_CEPH_LABELS
    if config.ENV_DATA.get("noobaa_external_pgsql"):
        required = required - {constants.NOOBAA_DB_LABEL_419_AND_ABOVE}
    return required


def _active_pods(namespace: str) -> list[dict]:
    pods = OCP(kind=constants.POD, namespace=namespace).get().get("items", [])
    return [
        pod
        for pod in pods
        if pod.get("status", {}).get("phase") in ("Running", "Pending")
        and not pod.get("metadata", {}).get("deletionTimestamp")
    ]


def _matches_label(pod: dict, selector: str) -> bool:
    key, _, value = selector.partition("=")
    return (pod.get("metadata", {}).get("labels") or {}).get(key) == value


def _expected_scc_for_label(pod: dict, selector: str, default_scc: str) -> str:
    if selector in HOST_NETWORK_LABELS and pod.get("spec", {}).get("hostNetwork"):
        return constants.SCC_HOSTNETWORK_V2
    return default_scc


def _matching_components(pod: dict) -> list[tuple[str, str]]:
    name = pod.get("metadata", {}).get("name", "")
    matches = []
    for components in SCC_BY_CATEGORY.values():
        for selector, expected_scc in components.items():
            if _matches_label(pod, selector):
                matches.append(
                    (selector, _expected_scc_for_label(pod, selector, expected_scc))
                )

    matches.extend(
        (prefix, expected_scc)
        for prefix, expected_scc in SCC_BY_POD_PREFIX.items()
        if name.startswith(prefix)
    )
    matches.extend(
        (fragment, expected_scc)
        for fragment, expected_scc in SCC_BY_NAME_FRAGMENT.items()
        if fragment in name
    )
    if any(
        container.get("name") == "noobaa-agent"
        for container in pod.get("spec", {}).get("containers", [])
    ):
        matches.append(("noobaa-agent", constants.SCC_NOOBAA_AGENT))
    return matches


def _pinning_errors(pod: dict, component: str, expected_scc: str) -> list[str]:
    result = verify_pod_scc_pinning(pod, expected_scc)
    errors = []
    if not result["required_match"]:
        errors.append(
            f"Pod '{result['pod_name']}' ({component}) requests "
            f"'{result['required_scc']}', expected '{expected_scc}'"
        )
    if not result["admission_match"]:
        errors.append(
            f"Pod '{result['pod_name']}' ({component}) was admitted under "
            f"'{result['admission_scc']}', expected '{expected_scc}'"
        )
    return errors


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
        all_pods = _active_pods(namespace)
        checked_count = 0
        errors = []
        required_labels = _required_labels()

        for selector, default_scc in SCC_BY_CATEGORY[category_name].items():
            selected = [pod for pod in all_pods if _matches_label(pod, selector)]
            if not selected and selector in required_labels:
                errors.append(
                    f"Required component with label '{selector}' has no active pod"
                )
            for pod in selected:
                expected_scc = _expected_scc_for_label(pod, selector, default_scc)
                errors.extend(_pinning_errors(pod, selector, expected_scc))
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
        all_pods = _active_pods(namespace)

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
            matches = _matching_components(pod)
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
            errors.extend(_pinning_errors(pod, component, expected_scc))

        for component in _required_labels() | required_prefixes:
            if component not in matched_components:
                errors.append(f"Required component '{component}' has no active pod")

        assert not errors, "SCC mapping failed:\n" + "\n".join(errors)
        logger.info("Verified SCC pinning for all %d active pods", len(all_pods))
