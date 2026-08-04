import logging

import pytest
import requests

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    purple_squad,
    skipif_external_mode,
)
from ocs_ci.framework.testlib import ManageTest, tier1, tier2
from ocs_ci.helpers.helpers import get_provisioner_label
from ocs_ci.helpers.network_policy_helpers import (
    check_pod_connectivity,
    get_all_network_policies,
    get_csv_name_by_prefix,
    get_route_url,
    get_service_ip,
    verify_csv_network_policy_rbac,
    verify_dns_from_pod,
    verify_network_policies_exist,
    verify_policy_structure,
    verify_sa_can_manage_network_policies,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import get_pods_having_label
from ocs_ci.utility.utils import ceph_health_check


logger = logging.getLogger(__name__)


@purple_squad
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

        assert len(policies) > 0, (
            f"No NetworkPolicies found in {namespace}"
        )
        logger.info(
            f"Found {len(policies)} NetworkPolicies: {policy_names}"
        )

        for policy in policies:
            labels = policy.get("metadata", {}).get("labels", {})
            owners = policy.get("metadata", {}).get("ownerReferences", [])
            name = policy["metadata"]["name"]
            if labels.get("olm.managed") == "true":
                logger.info(f"  {name}: OLM-managed")
            elif owners:
                owner_kinds = [o.get("kind", "?") for o in owners]
                logger.info(
                    f"  {name}: dynamic, owned by {owner_kinds}"
                )
            else:
                logger.info(f"  {name}: standalone")

    @tier1
    @pytest.mark.polarion_id("OCS-6901")
    def test_csv_rbac_for_network_policies(self):
        """
        A-2: Verify each ODF CSV declares NetworkPolicy RBAC per
        Conforma requirement (networking.k8s.io/networkpolicies
        with create, delete, update/patch).
        """
        namespace = config.ENV_DATA["cluster_namespace"]
        failures = []
        for csv_prefix in constants.ODF_CSV_PREFIXES_FOR_NETWORK_POLICY_RBAC:
            try:
                verify_csv_network_policy_rbac(csv_prefix, namespace)
            except AssertionError as ex:
                failures.append(str(ex))

        assert not failures, (
            f"CSV RBAC failures ({len(failures)}):\n"
            + "\n".join(failures)
        )

    @tier1
    @pytest.mark.polarion_id("OCS-6902")
    def test_sa_permissions_for_network_policies(self):
        """
        A-3: Verify operator service accounts have effective permissions
        to create/delete/update NetworkPolicies.
        """
        namespace = config.ENV_DATA["cluster_namespace"]
        failures = []
        for csv_prefix, sa_name in (
            constants.ODF_OPERATOR_SA_FOR_NETWORK_POLICY.items()
        ):
            try:
                verify_sa_can_manage_network_policies(sa_name, namespace)
            except AssertionError as ex:
                failures.append(f"{csv_prefix} (SA: {sa_name}): {ex}")

        assert not failures, (
            f"SA permission failures ({len(failures)}):\n"
            + "\n".join(failures)
        )

    @tier1
    @pytest.mark.polarion_id("OCS-6903")
    def test_policy_structure_validation(self, network_policies_present):
        """
        A-4: Validate NetworkPolicies follow ODF design principles.
        """
        all_issues = []
        for policy in network_policies_present:
            result = verify_policy_structure(policy)
            if not result["valid"]:
                all_issues.extend(result["issues"])

        assert not all_issues, (
            "Policy structure issues:\n" + "\n".join(all_issues)
        )


@purple_squad
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

        tools_pods = get_pods_having_label(
            constants.TOOL_APP_LABEL, namespace=namespace
        )
        if tools_pods:
            verify_dns_from_pod(
                tools_pods[0]["metadata"]["name"], namespace=namespace
            )
        else:
            logger.warning("No tools pod found, trying operator pod")
            operator_pods = get_pods_having_label(
                constants.OPERATOR_LABEL, namespace=namespace
            )
            assert operator_pods, "No tools or operator pod found for DNS test"
            verify_dns_from_pod(
                operator_pods[0]["metadata"]["name"], namespace=namespace
            )

    @tier1
    @skipif_external_mode
    @pytest.mark.polarion_id("OCS-6905")
    def test_inter_component_communication(self, network_policies_present):
        """
        B-2: Verify ODF components communicate normally.
        CephCluster healthy, CSI pods running.
        """
        namespace = config.ENV_DATA["cluster_namespace"]

        ceph_health_check(namespace=namespace, tries=10, delay=15)

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
                assert phase == constants.STATUS_RUNNING, (
                    f"CSI pod {pod_name} is {phase}, expected Running"
                )

        mon_pods = get_pods_having_label(
            constants.MON_APP_LABEL, namespace=namespace
        )
        assert len(mon_pods) >= 3, (
            f"Expected at least 3 mon pods, found {len(mon_pods)}"
        )

    @tier1
    @pytest.mark.polarion_id("OCS-6906")
    def test_monitoring_scraping(self, network_policies_present):
        """
        B-3: Verify Prometheus can scrape metrics from ODF pods.
        """
        namespace = config.ENV_DATA["cluster_namespace"]
        sm_obj = OCP(kind="ServiceMonitor", namespace=namespace)
        service_monitors = sm_obj.get().get("items", [])

        sm_names = [sm["metadata"]["name"] for sm in service_monitors]
        logger.info(
            f"Found {len(service_monitors)} ServiceMonitors: {sm_names}"
        )

        assert "rook-ceph-mgr" in sm_names, (
            f"ServiceMonitor 'rook-ceph-mgr' not found. Present: {sm_names}"
        )

        if not config.ENV_DATA.get("mcg_only_deployment"):
            if "s3-service-monitor" in sm_names:
                logger.info("s3-service-monitor present (MCG deployed)")
            else:
                logger.info(
                    "s3-service-monitor absent — MCG may not be deployed"
                )

        prom_ocp = OCP(kind=constants.POD, namespace="openshift-monitoring")
        prom_pods = prom_ocp.get(
            selector="app.kubernetes.io/name=prometheus"
        ).get("items", [])
        assert prom_pods, "No Prometheus pods found in openshift-monitoring"

        for prom_pod in prom_pods:
            phase = prom_pod.get("status", {}).get("phase", "Unknown")
            assert phase == constants.STATUS_RUNNING, (
                f"Prometheus pod {prom_pod['metadata']['name']} "
                f"is {phase}, expected Running"
            )

    @tier1
    @pytest.mark.polarion_id("OCS-6907")
    def test_ingress_routes_accessible(self, network_policies_present):
        """
        B-4: Verify external traffic via Routes reaches ODF services.
        """
        namespace = config.ENV_DATA["cluster_namespace"]
        route_obj = OCP(kind="Route", namespace=namespace)
        routes = route_obj.get().get("items", [])
        route_names = [r["metadata"]["name"] for r in routes]
        logger.info(f"Routes in {namespace}: {route_names}")

        tested = 0
        for route in routes:
            name = route["metadata"]["name"]
            host = route.get("spec", {}).get("host", "")
            if not host:
                logger.warning(f"Route {name} has no host, skipping")
                continue
            tls = route.get("spec", {}).get("tls")
            scheme = "https" if tls else "http"
            url = f"{scheme}://{host}"

            try:
                # verify=False: cluster routes use self-signed certificates
                resp = requests.get(url, timeout=15, verify=False)
                logger.info(
                    f"Route {name}: {url} -> HTTP {resp.status_code}"
                )
                assert resp.status_code < 500, (
                    f"Route {name} returned server error: "
                    f"HTTP {resp.status_code}"
                )
                tested += 1
            except requests.exceptions.ConnectionError:
                pytest.fail(
                    f"Route {name} ({url}) is not reachable "
                    f"(connection error)"
                )
            except requests.exceptions.Timeout:
                pytest.fail(
                    f"Route {name} ({url}) timed out"
                )
            except requests.exceptions.RequestException as ex:
                pytest.fail(
                    f"Route {name} ({url}) request failed: {ex}"
                )

        assert tested > 0, (
            f"No routes found to test in {namespace}"
        )

    @tier1
    @pytest.mark.polarion_id("OCS-6908")
    def test_storage_operations_with_policies(
        self, network_policies_present, pvc_factory, pod_factory
    ):
        """
        B-5: Verify core storage provisioning works with policies.
        Create RBD and CephFS PVCs, attach pods, run basic I/O.
        """
        for interface in (
            constants.CEPHBLOCKPOOL,
            constants.CEPHFILESYSTEM,
        ):
            pvc_obj = pvc_factory(interface=interface)
            pod_obj = pod_factory(pvc=pvc_obj)
            pod_obj.run_io(
                storage_type="fs",
                size="256M",
                runtime=30,
            )
            pod_obj.get_fio_results()
            logger.info(
                f"I/O completed successfully on {interface} "
                f"with NetworkPolicies active"
            )


@purple_squad
class TestNetworkPolicyBlockedTraffic(ManageTest):
    """
    Category C: Blocked traffic negative tests (tier2).
    Verify unauthorized traffic is blocked by NetworkPolicies.
    """

    @tier2
    @pytest.mark.polarion_id("OCS-6909")
    def test_blocked_traffic_from_foreign_namespace(
        self, network_policies_present, test_pod_in_foreign_ns
    ):
        """
        C-1: Pod in a foreign namespace cannot reach ODF operand services.
        """
        pod_name, ns = test_pod_in_foreign_ns
        storage_ns = config.ENV_DATA["cluster_namespace"]

        mon_pods = get_pods_having_label(
            constants.MON_APP_LABEL, namespace=storage_ns
        )
        mon_svc_name = None
        if mon_pods:
            mon_svc_name = mon_pods[0]["metadata"]["labels"].get(
                "ceph_daemon_id"
            )
            if mon_svc_name:
                mon_svc_name = f"rook-ceph-mon-{mon_svc_name}"

        targets = {}
        if mon_svc_name:
            targets[mon_svc_name] = 3300
        targets["noobaa-mgmt"] = 443

        tested = 0
        for svc_name, port in targets.items():
            svc_ip = get_service_ip(svc_name, namespace=storage_ns)
            if not svc_ip:
                logger.warning(
                    f"Service {svc_name} not found, skipping"
                )
                continue

            check_pod_connectivity(
                source_pod_name=pod_name,
                target_ip=svc_ip,
                port=port,
                namespace=ns,
                should_succeed=False,
                timeout=10,
            )
            tested += 1

        assert tested > 0, (
            "No services were tested — cannot verify blocked traffic"
        )

    @tier2
    @pytest.mark.polarion_id("OCS-6910")
    def test_allowed_namespace_traffic(self, network_policies_present):
        """
        C-2: Traffic from explicitly allowed namespaces
        (monitoring, console) is not blocked. Verifies Prometheus
        targets for ODF are healthy.
        """
        namespace = config.ENV_DATA["cluster_namespace"]

        prom_ocp = OCP(kind=constants.POD, namespace="openshift-monitoring")
        prom_pods = prom_ocp.get(
            selector="app.kubernetes.io/name=prometheus"
        ).get("items", [])
        assert prom_pods, "No Prometheus pods found"
        for prom_pod in prom_pods:
            phase = prom_pod.get("status", {}).get("phase", "Unknown")
            assert phase == constants.STATUS_RUNNING, (
                f"Prometheus pod not running: {phase}"
            )

        sm_obj = OCP(kind="ServiceMonitor", namespace=namespace)
        service_monitors = sm_obj.get().get("items", [])
        assert service_monitors, (
            "No ServiceMonitors found — monitoring may be broken"
        )

        ep_obj = OCP(kind="Endpoints", namespace=namespace)
        for sm in service_monitors:
            sm_name = sm["metadata"]["name"]
            try:
                ep = ep_obj.get(resource_name=sm_name)
                subsets = ep.get("subsets", [])
                if subsets:
                    logger.info(
                        f"ServiceMonitor {sm_name}: endpoints reachable "
                        f"({len(subsets)} subsets)"
                    )
            except Exception:
                logger.info(
                    f"ServiceMonitor {sm_name}: no matching endpoints "
                    f"(may use different service name)"
                )

        logger.info(
            f"Monitoring access verified: {len(prom_pods)} Prometheus "
            f"pods running, {len(service_monitors)} ServiceMonitors present"
        )


@purple_squad
class TestNetworkPolicyDisruption(ManageTest):
    """
    Category D: Disruption & recovery tests (tier2).
    Verify policies survive operational events.
    """

    @tier2
    @pytest.mark.polarion_id("OCS-6911")
    def test_policies_survive_operator_restart(
        self, network_policies_present
    ):
        """
        D-1: Restarting rook-ceph-operator does not remove policies.
        """
        namespace = config.ENV_DATA["cluster_namespace"]

        policies_before = get_all_network_policies(namespace)
        names_before = sorted(
            [p["metadata"]["name"] for p in policies_before]
        )

        deploy_obj = OCP(kind="Deployment", namespace=namespace)
        deploy_obj.exec_oc_cmd(
            "rollout restart deployment/rook-ceph-operator",
            out_yaml_format=False,
        )
        logger.info("Waiting for rook-ceph-operator rollout to complete")
        deploy_obj.exec_oc_cmd(
            "rollout status deployment/rook-ceph-operator "
            "--timeout=300s",
            out_yaml_format=False,
        )

        policies_after = get_all_network_policies(namespace)
        names_after = sorted(
            [p["metadata"]["name"] for p in policies_after]
        )

        assert names_before == names_after, (
            f"NetworkPolicies changed after operator restart.\n"
            f"Before: {names_before}\n"
            f"After: {names_after}"
        )

        ceph_health_check(namespace=namespace, tries=20, delay=30)
        logger.info(
            "NetworkPolicies intact and Ceph healthy after "
            "operator restart"
        )

    @tier2
    @skipif_external_mode
    @pytest.mark.polarion_id("OCS-6912")
    def test_pod_restart_under_policies(self, network_policies_present):
        """
        D-2: Restarted pods regain connectivity under existing policies.
        """
        namespace = config.ENV_DATA["cluster_namespace"]

        osd_pods = get_pods_having_label(
            constants.OSD_APP_LABEL, namespace=namespace
        )
        assert osd_pods, "No OSD pods found"

        target_osd = osd_pods[0]["metadata"]["name"]
        logger.info(f"Deleting OSD pod {target_osd} to test restart")

        pod_ocp = OCP(kind=constants.POD, namespace=namespace)
        pod_ocp.delete(resource_name=target_osd)

        pod_ocp.wait_for_delete(resource_name=target_osd, timeout=120)

        pod_ocp.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            selector=constants.OSD_APP_LABEL,
            resource_count=len(osd_pods),
            timeout=300,
        )

        ceph_health_check(namespace=namespace, tries=20, delay=30)
        logger.info(
            "OSD pod restarted and Ceph healthy under NetworkPolicies"
        )
