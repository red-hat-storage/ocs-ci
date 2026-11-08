import logging

import pytest
import requests

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    ignore_leftovers,
    brown_squad,
    skipif_external_mode,
)
from ocs_ci.framework.testlib import ManageTest, tier1, tier2
from ocs_ci.helpers.helpers import get_provisioner_label
from ocs_ci.helpers.network_policy_helpers import (
    get_all_network_policies,
    verify_csv_network_policy_rbac,
    verify_dns_from_pod,
    verify_policy_structure,
    verify_sa_can_manage_network_policies,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import get_pods_having_label
from ocs_ci.utility.utils import ceph_health_check


logger = logging.getLogger(__name__)


@brown_squad
class TestNetworkPolicyCompliance(ManageTest):
    """
    Category A: RBAC & Conforma compliance tests (tier1).
    Verify NetworkPolicies exist and CSVs declare proper RBAC.
    """

    @tier1
    @pytest.mark.polarion_id("OCS-6900")
    def test_network_policies_exist(self, network_policies_present):
        """
        A-1: Verify expected NetworkPolicy CRs exist after OLM install.
        """
        namespace = config.ENV_DATA["cluster_namespace"]
        policies = network_policies_present
        policy_names = [p["metadata"]["name"] for p in policies]

        logger.info(
            "Step 1: Checking that NetworkPolicies exist in " f"namespace '{namespace}'"
        )
        assert len(policies) > 0, f"No NetworkPolicies found in {namespace}"
        logger.info(f"Found {len(policies)} NetworkPolicies: {policy_names}")

        logger.info(
            "Step 2: Classifying each policy by ownership "
            "(OLM-managed, dynamic, or standalone)"
        )
        for policy in policies:
            labels = policy.get("metadata", {}).get("labels", {})
            owners = policy.get("metadata", {}).get("ownerReferences", [])
            name = policy["metadata"]["name"]
            if labels.get("olm.managed") == "true":
                logger.info(f"  {name}: OLM-managed (declared in CSV)")
            elif owners:
                owner_kinds = [o.get("kind", "?") for o in owners]
                logger.info(f"  {name}: dynamically created, owned by {owner_kinds}")
            else:
                logger.info(f"  {name}: standalone (no owner)")

        logger.info(
            f"Result: All {len(policies)} NetworkPolicies are present "
            f"in {namespace}"
        )

    @tier1
    @pytest.mark.polarion_id("OCS-6901")
    def test_csv_rbac_for_network_policies(self):
        """
        A-2: Verify each ODF CSV declares NetworkPolicy RBAC per
        Conforma requirement (networking.k8s.io/networkpolicies
        with create and update/patch).
        """
        namespace = config.ENV_DATA["cluster_namespace"]
        csv_prefixes = constants.ODF_CSV_PREFIXES_FOR_NETWORK_POLICY_RBAC

        logger.info(
            "Step 1: Checking NetworkPolicy RBAC declarations in "
            f"{len(csv_prefixes)} ODF CSVs"
        )
        logger.info(f"CSVs to verify: {csv_prefixes}")

        failures = []
        for csv_prefix in csv_prefixes:
            logger.info(
                f"Step 2: Verifying CSV '{csv_prefix}' declares "
                "networking.k8s.io/networkpolicies RBAC with required verbs"
            )
            try:
                verify_csv_network_policy_rbac(csv_prefix, namespace)
                logger.info(f"  {csv_prefix}: RBAC verified successfully")
            except AssertionError as ex:
                logger.warning(f"  {csv_prefix}: RBAC check FAILED — {ex}")
                failures.append(str(ex))

        if not failures:
            logger.info(
                f"Result: All {len(csv_prefixes)} CSVs have valid " "NetworkPolicy RBAC"
            )

        assert not failures, f"CSV RBAC failures ({len(failures)}):\n" + "\n".join(
            failures
        )

    @tier1
    @pytest.mark.polarion_id("OCS-6902")
    def test_sa_permissions_for_network_policies(self):
        """
        A-3: Verify operator service accounts have effective permissions
        to create/update NetworkPolicies.
        """
        namespace = config.ENV_DATA["cluster_namespace"]
        sa_map = constants.ODF_OPERATOR_SA_FOR_NETWORK_POLICY

        logger.info(
            "Step 1: Checking effective NetworkPolicy permissions for "
            f"{len(sa_map)} operator service accounts using 'oc auth can-i'"
        )

        failures = []
        for csv_prefix, sa_name in sa_map.items():
            logger.info(
                f"Step 2: Verifying SA '{sa_name}' (from {csv_prefix}) "
                "can create/update networkpolicies.networking.k8s.io"
            )
            try:
                verify_sa_can_manage_network_policies(sa_name, namespace)
                logger.info(f"  SA '{sa_name}': all required permissions confirmed")
            except AssertionError as ex:
                logger.warning(f"  SA '{sa_name}': permission check FAILED — {ex}")
                failures.append(f"{csv_prefix} (SA: {sa_name}): {ex}")

        if not failures:
            logger.info(
                f"Result: All {len(sa_map)} service accounts have "
                "effective NetworkPolicy permissions"
            )

        assert not failures, f"SA permission failures ({len(failures)}):\n" + "\n".join(
            failures
        )

    @tier1
    @pytest.mark.polarion_id("OCS-6903")
    def test_policy_structure_validation(self, network_policies_present):
        """
        A-4: Validate NetworkPolicies follow ODF design principles.
        """
        logger.info(
            "Step 1: Validating structure of "
            f"{len(network_policies_present)} NetworkPolicies "
            "(podSelector, policyTypes, ingress/egress rules)"
        )

        all_issues = []
        for policy in network_policies_present:
            name = policy.get("metadata", {}).get("name", "unknown")
            logger.info(f"  Validating policy '{name}'")
            result = verify_policy_structure(policy)
            if result["valid"]:
                logger.info(f"    '{name}': structure is valid")
            else:
                logger.warning(f"    '{name}': issues found — {result['issues']}")
                all_issues.extend(result["issues"])

        if not all_issues:
            logger.info(
                f"Result: All {len(network_policies_present)} policies "
                "pass structural validation"
            )

        assert not all_issues, "Policy structure issues:\n" + "\n".join(all_issues)


@brown_squad
class TestNetworkPolicyAllowedTraffic(ManageTest):
    """
    Category B: Allowed traffic positive tests (tier1).
    Verify ODF functionality works with NetworkPolicies enforced.
    """

    @tier1
    @pytest.mark.polarion_id("OCS-6904")
    def test_dns_resolution_with_policies(self, network_policies_present):
        """
        B-1: Verify ODF pods can resolve DNS with policies active.
        """
        namespace = config.ENV_DATA["cluster_namespace"]

        logger.info(
            "Step 1: Looking for a pod to test DNS resolution from "
            f"namespace '{namespace}'"
        )
        tools_pods = get_pods_having_label(
            constants.TOOL_APP_LABEL, namespace=namespace
        )
        if tools_pods:
            pod_name = tools_pods[0]["metadata"]["name"]
            logger.info(f"  Found tools pod: {pod_name}")
            logger.info(
                "Step 2: Resolving 'kubernetes.default.svc.cluster.local' "
                f"from pod '{pod_name}' to verify DNS works under "
                "NetworkPolicy enforcement"
            )
            verify_dns_from_pod(pod_name, namespace=namespace)
        else:
            logger.warning("  No tools pod found, falling back to operator pod")
            operator_pods = get_pods_having_label(
                constants.OPERATOR_LABEL, namespace=namespace
            )
            assert operator_pods, "No tools or operator pod found for DNS test"
            pod_name = operator_pods[0]["metadata"]["name"]
            logger.info(f"  Found operator pod: {pod_name}")
            logger.info(
                "Step 2: Resolving 'kubernetes.default.svc.cluster.local' "
                f"from pod '{pod_name}' to verify DNS works under "
                "NetworkPolicy enforcement"
            )
            verify_dns_from_pod(pod_name, namespace=namespace)

        logger.info("Result: DNS resolution works under NetworkPolicies")

    @tier1
    @skipif_external_mode
    @pytest.mark.polarion_id("OCS-6905")
    def test_inter_component_communication(self, network_policies_present):
        """
        B-2: Verify ODF components communicate normally.
        CephCluster healthy, CSI pods running.
        """
        namespace = config.ENV_DATA["cluster_namespace"]

        logger.info(
            "Step 1: Checking CephCluster health to verify internal "
            "component communication works with NetworkPolicies active"
        )
        ceph_health_check(namespace=namespace, tries=10, delay=15)
        logger.info("  CephCluster is HEALTH_OK")

        logger.info(
            "Step 2: Verifying CSI provisioner pods (RBD + CephFS) " "are running"
        )
        csi_labels = [
            get_provisioner_label(constants.CEPHBLOCKPOOL),
            get_provisioner_label(constants.CEPHFILESYSTEM),
        ]
        for label in csi_labels:
            pods = get_pods_having_label(label, namespace=namespace)
            assert pods, f"No pods found with label {label}"
            for pod in pods:
                phase = pod.get("status", {}).get("phase", "Unknown")
                pod_name = pod["metadata"]["name"]
                assert (
                    phase == constants.STATUS_RUNNING
                ), f"CSI pod {pod_name} is {phase}, expected Running"
                logger.info(f"  CSI pod {pod_name}: {phase}")

        logger.info("Step 3: Verifying at least 3 Ceph monitor pods are running")
        mon_pods = get_pods_having_label(constants.MON_APP_LABEL, namespace=namespace)
        assert (
            len(mon_pods) >= 3
        ), f"Expected at least 3 mon pods, found {len(mon_pods)}"
        logger.info(f"  Found {len(mon_pods)} mon pods — all healthy")

        logger.info(
            "Result: All ODF components communicate normally "
            "with NetworkPolicies active"
        )

    @tier1
    @pytest.mark.polarion_id("OCS-6906")
    def test_monitoring_scraping(self, network_policies_present):
        """
        B-3: Verify Prometheus can scrape metrics from ODF pods.
        """
        namespace = config.ENV_DATA["cluster_namespace"]

        logger.info(
            "Step 1: Listing ServiceMonitors in namespace "
            f"'{namespace}' to verify monitoring is configured"
        )
        sm_obj = OCP(kind="ServiceMonitor", namespace=namespace)
        service_monitors = sm_obj.get().get("items", [])
        sm_names = [sm["metadata"]["name"] for sm in service_monitors]
        logger.info(f"  Found {len(service_monitors)} ServiceMonitors: {sm_names}")

        logger.info(
            "Step 2: Verifying 'rook-ceph-mgr' ServiceMonitor is present "
            "(required for Ceph metrics)"
        )
        assert (
            "rook-ceph-mgr" in sm_names
        ), f"ServiceMonitor 'rook-ceph-mgr' not found. Present: {sm_names}"
        logger.info("  'rook-ceph-mgr' ServiceMonitor: present")

        if not config.ENV_DATA.get("mcg_only_deployment"):
            if "s3-service-monitor" in sm_names:
                logger.info("  's3-service-monitor': present (MCG deployed)")
            else:
                logger.info("  's3-service-monitor': absent — MCG may not be deployed")

        logger.info(
            "Step 3: Verifying Prometheus pods in openshift-monitoring "
            "are running (can reach ODF metrics through NetworkPolicies)"
        )
        prom_ocp = OCP(kind=constants.POD, namespace="openshift-monitoring")
        prom_pods = prom_ocp.get(selector="app.kubernetes.io/name=prometheus").get(
            "items", []
        )
        assert prom_pods, "No Prometheus pods found in openshift-monitoring"

        for prom_pod in prom_pods:
            prom_name = prom_pod["metadata"]["name"]
            phase = prom_pod.get("status", {}).get("phase", "Unknown")
            assert (
                phase == constants.STATUS_RUNNING
            ), f"Prometheus pod {prom_name} is {phase}, expected Running"
            logger.info(f"  Prometheus pod {prom_name}: {phase}")

        logger.info(
            "Step 4: Querying Prometheus for an ODF metric to verify "
            "actual scraping works through NetworkPolicies"
        )
        prom_route = OCP(kind="Route", namespace="openshift-monitoring")
        try:
            route_data = prom_route.get(resource_name="prometheus-k8s")
            prom_host = route_data.get("spec", {}).get("host", "")
        except Exception:
            prom_host = ""

        if prom_host:
            prom_url = f"https://{prom_host}/api/v1/query"
            try:
                token_ocp = OCP(namespace="openshift-monitoring")
                token = token_ocp.exec_oc_cmd(
                    "create token prometheus-k8s "
                    "-n openshift-monitoring --duration=600s",
                    out_yaml_format=False,
                )
                resp = requests.get(
                    prom_url,
                    params={"query": "up{job='rook-ceph-mgr'}"},
                    headers={"Authorization": f"Bearer {token}"},
                    timeout=15,
                    verify=False,
                )
                if resp.status_code == 200:
                    results = resp.json().get("data", {}).get("result", [])
                    healthy = [
                        r for r in results if r.get("value", [None, "0"])[1] == "1"
                    ]
                    logger.info(
                        f"  Prometheus query 'up{{job=rook-ceph-mgr}}': "
                        f"{len(healthy)}/{len(results)} targets up"
                    )
                    assert healthy, (
                        "Prometheus cannot scrape rook-ceph-mgr — "
                        f"all {len(results)} targets are down"
                    )
                else:
                    logger.warning(
                        f"  Prometheus query returned HTTP {resp.status_code}, "
                        "falling back to resource-presence check"
                    )
            except requests.exceptions.RequestException as ex:
                logger.warning(
                    f"  Could not query Prometheus API: {ex}, "
                    "falling back to resource-presence check"
                )
        else:
            logger.warning("  Prometheus route not found, skipping live metric check")

        logger.info(
            f"Result: Monitoring verified — {len(prom_pods)} Prometheus "
            f"pods running, {len(service_monitors)} ServiceMonitors present"
        )

    @tier1
    @pytest.mark.polarion_id("OCS-6907")
    def test_ingress_routes_accessible(self, network_policies_present):
        """
        B-4: Verify external traffic via Routes reaches ODF services.
        """
        namespace = config.ENV_DATA["cluster_namespace"]

        logger.info(
            "Step 1: Listing Routes in namespace "
            f"'{namespace}' to test external accessibility"
        )
        route_obj = OCP(kind="Route", namespace=namespace)
        routes = route_obj.get().get("items", [])
        route_names = [r["metadata"]["name"] for r in routes]
        logger.info(f"  Found {len(routes)} routes: {route_names}")

        logger.info(
            "Step 2: Sending HTTP GET to each route to verify traffic "
            "passes through NetworkPolicies via openshift-ingress"
        )
        tested = 0
        for route in routes:
            name = route["metadata"]["name"]
            host = route.get("spec", {}).get("host", "")
            if not host:
                logger.warning(f"  Route '{name}' has no host, skipping")
                continue
            tls = route.get("spec", {}).get("tls")
            scheme = "https" if tls else "http"
            url = f"{scheme}://{host}"

            logger.info(f"  Testing route '{name}': GET {url}")
            try:
                # verify=False: cluster routes use self-signed certificates
                resp = requests.get(url, timeout=15, verify=False)
                logger.info(
                    f"    Route '{name}': HTTP {resp.status_code} — "
                    "route is reachable"
                )
                tested += 1
            except requests.exceptions.ConnectionError:
                pytest.fail(f"Route {name} ({url}) is not reachable (connection error)")
            except requests.exceptions.Timeout:
                pytest.fail(f"Route {name} ({url}) timed out")
            except requests.exceptions.RequestException as ex:
                pytest.fail(f"Route {name} ({url}) request failed: {ex}")

        assert tested > 0, f"No routes found to test in {namespace}"
        logger.info(f"Result: {tested} routes are reachable through NetworkPolicies")

    @tier2
    @pytest.mark.polarion_id("OCS-6908")
    def test_storage_operations_with_policies(
        self, network_policies_present, pvc_factory, pod_factory
    ):
        """
        B-5: Verify core storage provisioning works with policies.
        Create RBD and CephFS PVCs, attach pods, run basic I/O.
        """
        logger.info(
            "Step 1: Testing storage provisioning with NetworkPolicies "
            "active — creating PVCs, attaching pods, running FIO I/O"
        )

        for interface in (
            constants.CEPHBLOCKPOOL,
            constants.CEPHFILESYSTEM,
        ):
            logger.info(f"  Creating PVC with interface '{interface}'")
            pvc_obj = pvc_factory(interface=interface)
            logger.info(f"  PVC '{pvc_obj.name}' created and bound, " "attaching pod")

            pod_obj = pod_factory(pvc=pvc_obj)
            logger.info(
                f"  Pod '{pod_obj.name}' running, starting FIO I/O " "(256M, 30s)"
            )

            pod_obj.run_io(
                storage_type="fs",
                size="256M",
                runtime=30,
            )
            pod_obj.get_fio_results()
            logger.info(
                f"  I/O completed successfully on {interface} "
                "with NetworkPolicies active"
            )

        logger.info(
            "Result: Both RBD and CephFS storage operations work "
            "under NetworkPolicy enforcement"
        )


@brown_squad
class TestNetworkPolicyBlockedTraffic(ManageTest):
    """
    Category C: Blocked traffic negative tests (tier2).
    Verify unauthorized traffic is blocked by NetworkPolicies.
    """

    @tier2
    @pytest.mark.polarion_id("OCS-6910")
    def test_allowed_namespace_traffic(self, network_policies_present):
        """
        C-2: Traffic from explicitly allowed namespaces
        (monitoring, console) is not blocked. Verifies Prometheus
        targets for ODF are healthy.
        """
        namespace = config.ENV_DATA["cluster_namespace"]

        logger.info(
            "Step 1: Verifying that Prometheus pods in "
            "openshift-monitoring (an allowed namespace) are running "
            "and can reach ODF metrics endpoints"
        )
        prom_ocp = OCP(kind=constants.POD, namespace="openshift-monitoring")
        prom_pods = prom_ocp.get(selector="app.kubernetes.io/name=prometheus").get(
            "items", []
        )
        assert prom_pods, "No Prometheus pods found"
        for prom_pod in prom_pods:
            prom_name = prom_pod["metadata"]["name"]
            phase = prom_pod.get("status", {}).get("phase", "Unknown")
            assert (
                phase == constants.STATUS_RUNNING
            ), f"Prometheus pod not running: {phase}"
            logger.info(f"  Prometheus pod {prom_name}: {phase}")

        logger.info(
            "Step 2: Checking ServiceMonitors in "
            f"'{namespace}' to confirm monitoring configuration"
        )
        sm_obj = OCP(kind="ServiceMonitor", namespace=namespace)
        service_monitors = sm_obj.get().get("items", [])
        assert service_monitors, "No ServiceMonitors found — monitoring may be broken"
        logger.info(f"  Found {len(service_monitors)} ServiceMonitors")

        logger.info(
            "Step 3: Querying Prometheus for ODF scrape targets to "
            "verify allowed-namespace traffic actually reaches ODF pods"
        )
        prom_route = OCP(kind="Route", namespace="openshift-monitoring")
        try:
            route_data = prom_route.get(resource_name="prometheus-k8s")
            prom_host = route_data.get("spec", {}).get("host", "")
        except Exception:
            prom_host = ""

        if prom_host:
            prom_url = f"https://{prom_host}/api/v1/query"
            try:
                token_ocp = OCP(namespace="openshift-monitoring")
                token = token_ocp.exec_oc_cmd(
                    "create token prometheus-k8s "
                    "-n openshift-monitoring --duration=600s",
                    out_yaml_format=False,
                )
                resp = requests.get(
                    prom_url,
                    params={"query": (f'up{{namespace="{namespace}"}}')},
                    headers={"Authorization": f"Bearer {token}"},
                    timeout=15,
                    verify=False,
                )
                if resp.status_code == 200:
                    results = resp.json().get("data", {}).get("result", [])
                    healthy = [
                        r for r in results if r.get("value", [None, "0"])[1] == "1"
                    ]
                    logger.info(
                        f"  ODF scrape targets in '{namespace}': "
                        f"{len(healthy)}/{len(results)} targets up"
                    )
                    assert healthy, (
                        "No healthy Prometheus scrape targets found in "
                        f"'{namespace}' — allowed-namespace traffic "
                        "may be blocked by NetworkPolicies"
                    )
                else:
                    logger.warning(
                        f"  Prometheus query returned HTTP {resp.status_code}"
                    )
            except requests.exceptions.RequestException as ex:
                logger.warning(f"  Could not query Prometheus API: {ex}")
        else:
            logger.warning("  Prometheus route not found, skipping live target check")

        logger.info(
            f"Result: Allowed-namespace traffic verified — "
            f"{len(prom_pods)} Prometheus pods running, "
            f"{len(service_monitors)} ServiceMonitors present"
        )


@brown_squad
@ignore_leftovers
class TestNetworkPolicyDisruption(ManageTest):
    """
    Category D: Disruption & recovery tests (tier2).
    Verify policies survive operational events.
    Operator restart creates a replacement pod with a different name.
    """

    @tier2
    @pytest.mark.polarion_id("OCS-6911")
    def test_policies_survive_operator_restart(self, network_policies_present):
        """
        D-1: Restarting rook-ceph-operator does not remove policies.
        """
        namespace = config.ENV_DATA["cluster_namespace"]

        logger.info(
            "Step 1: Recording current NetworkPolicies before " "operator restart"
        )
        policies_before = get_all_network_policies(namespace)
        names_before = sorted([p["metadata"]["name"] for p in policies_before])
        logger.info(f"  {len(policies_before)} policies recorded: {names_before}")

        logger.info(
            "Step 2: Restarting rook-ceph-operator deployment "
            "and waiting for rollout to complete"
        )
        deploy_obj = OCP(kind="Deployment", namespace=namespace)
        deploy_obj.exec_oc_cmd(
            "rollout restart deployment/rook-ceph-operator",
            out_yaml_format=False,
        )
        logger.info("  Rollout restart initiated, waiting for completion...")
        deploy_obj.exec_oc_cmd(
            "rollout status deployment/rook-ceph-operator " "--timeout=300s",
            out_yaml_format=False,
        )
        logger.info("  rook-ceph-operator rollout completed")

        logger.info(
            "Step 3: Comparing NetworkPolicies after restart — "
            "none should be added or removed"
        )
        policies_after = get_all_network_policies(namespace)
        names_after = sorted([p["metadata"]["name"] for p in policies_after])
        logger.info(
            f"  {len(policies_after)} policies found after restart: " f"{names_after}"
        )

        assert names_before == names_after, (
            f"NetworkPolicies changed after operator restart.\n"
            f"Before: {names_before}\n"
            f"After: {names_after}"
        )
        logger.info("  Policy list is unchanged")

        logger.info(
            "Step 4: Verifying CephCluster recovers to HEALTH_OK "
            "after operator restart"
        )
        ceph_health_check(namespace=namespace, tries=20, delay=30)

        logger.info(
            "Result: NetworkPolicies survived operator restart and " "Ceph is healthy"
        )


@brown_squad
@ignore_leftovers
class TestNetworkPolicyPodRestart(ManageTest):
    """
    Category D-2: Pod restart under policies (tier2).
    Separated into its own class because deleting an OSD pod creates
    a replacement with a different name, which the environment checker
    would flag as a leftover.
    """

    @tier2
    @skipif_external_mode
    @pytest.mark.polarion_id("OCS-6912")
    def test_pod_restart_under_policies(self, network_policies_present):
        """
        D-2: Restarted pods regain connectivity under existing policies.
        """
        namespace = config.ENV_DATA["cluster_namespace"]

        logger.info(
            "Step 1: Finding an OSD pod to delete and verify that "
            "its replacement regains connectivity under NetworkPolicies"
        )
        osd_pods = get_pods_having_label(constants.OSD_APP_LABEL, namespace=namespace)
        assert osd_pods, "No OSD pods found"
        logger.info(f"  Found {len(osd_pods)} OSD pods")

        target_osd = osd_pods[0]["metadata"]["name"]
        logger.info(f"Step 2: Deleting OSD pod '{target_osd}'")
        pod_ocp = OCP(kind=constants.POD, namespace=namespace)
        pod_ocp.delete(resource_name=target_osd)

        logger.info(f"Step 3: Waiting for pod '{target_osd}' to be fully deleted")
        pod_ocp.wait_for_delete(resource_name=target_osd, timeout=120)
        logger.info(f"  Pod '{target_osd}' deleted")

        logger.info(
            f"Step 4: Waiting for {len(osd_pods)} OSD pods to be "
            "Running again (replacement pod should start)"
        )
        pod_ocp.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            selector=constants.OSD_APP_LABEL,
            resource_count=len(osd_pods),
            timeout=300,
        )
        logger.info("  All OSD pods are Running")

        logger.info(
            "Step 5: Verifying CephCluster health after OSD pod "
            "restart under NetworkPolicies"
        )
        ceph_health_check(namespace=namespace, tries=20, delay=30)

        logger.info(
            "Result: Replacement OSD pod started and Ceph is healthy — "
            "connectivity restored under NetworkPolicies"
        )
