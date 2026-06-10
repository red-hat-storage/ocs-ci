"""Metrics-exporter verification helpers for CephX key rotation."""

import logging

from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import (
    get_ceph_tools_pod,
    get_pod_ip,
    get_pod_logs,
    get_pod_obj,
    get_pods_having_label,
)
from ocs_ci.utility.utils import TimeoutSampler

log = logging.getLogger(__name__)

_PROMETHEUS_SA = "prometheus-k8s"
_PROMETHEUS_NS = "openshift-monitoring"


class CephXMetricsHelper:
    """ocs-metrics-exporter verification for CephX rotations."""

    def get_metrics_exporter_pods(self):
        """Return Running ocs-metrics-exporter pod resource dicts."""
        return get_pods_having_label(
            constants.OCS_METRICS_EXPORTER,
            namespace=self.namespace,
            statuses=[constants.STATUS_RUNNING],
        )

    def assert_metrics_exporter_running(self):
        """Assert a Running ocs-metrics-exporter pod exists."""
        pods = self.get_metrics_exporter_pods()
        if not pods:
            raise UnexpectedBehaviour("No Running ocs-metrics-exporter pods found")
        pod_name = pods[0]["metadata"]["name"]
        log.info(f"ocs-metrics-exporter pod is Running: {pod_name}")
        return pods[0]

    def _get_metrics_exporter_deployment(self):
        """Return the ocs-metrics-exporter Deployment dict."""
        return OCP(
            kind=constants.DEPLOYMENT,
            namespace=self.namespace,
            resource_name="ocs-metrics-exporter",
        ).get()

    def _discover_metrics_exporter_scrape_port(self):
        """
        Resolve the HTTPS port that serves OCS/Ceph /metrics.

        Preference order:
            1. Service port named ``https-main`` (kube-rbac-proxy layout)
            2. Any other Service HTTPS port except ``https-self``
            3. Deployment containerPort named ``https-main``
            4. Fallback constants (8443 modern, then 9443 legacy)
        """
        cached = getattr(self, "_metrics_scrape_port", None)
        if cached:
            return cached

        port = None
        source = "fallback"

        try:
            service = OCP(
                kind=constants.SERVICE,
                namespace=self.namespace,
                resource_name="ocs-metrics-exporter",
            ).get()
            ports = service.get("spec", {}).get("ports", []) or []
            by_name = {
                p.get("name"): p.get("port")
                for p in ports
                if p.get("name") and p.get("port") is not None
            }
            if constants.OCS_METRICS_EXPORTER_HTTPS_MAIN_PORT_NAME in by_name:
                port = by_name[constants.OCS_METRICS_EXPORTER_HTTPS_MAIN_PORT_NAME]
                source = "service/https-main"
            else:
                for name, value in by_name.items():
                    if name == constants.OCS_METRICS_EXPORTER_HTTPS_SELF_PORT_NAME:
                        continue
                    port = value
                    source = f"service/{name}"
                    break
        except Exception as exc:
            log.debug(f"Could not read ocs-metrics-exporter Service ports: {exc}")

        if port is None:
            try:
                deploy = self._get_metrics_exporter_deployment()
                for container in (
                    deploy.get("spec", {})
                    .get("template", {})
                    .get("spec", {})
                    .get("containers", [])
                    or []
                ):
                    for cport in container.get("ports", []) or []:
                        if (
                            cport.get("name")
                            == constants.OCS_METRICS_EXPORTER_HTTPS_MAIN_PORT_NAME
                            and cport.get("containerPort") is not None
                        ):
                            port = cport["containerPort"]
                            source = "deployment/https-main"
                            break
                    if port is not None:
                        break
            except Exception as exc:
                log.debug(
                    f"Could not read ocs-metrics-exporter Deployment ports: {exc}"
                )

        if port is None:
            port = constants.OCS_METRICS_EXPORTER_PORT
            source = "constant/OCS_METRICS_EXPORTER_PORT"

        self._metrics_scrape_port = int(port)
        log.info(
            "ocs-metrics-exporter scrape port=%s (source=%s)",
            self._metrics_scrape_port,
            source,
        )
        return self._metrics_scrape_port

    def _metrics_exporter_local_url(self):
        """HTTPS /metrics URL on the exporter pod loopback interface."""
        port = self._discover_metrics_exporter_scrape_port()
        return (
            f"https://127.0.0.1:{port}" f"{constants.OCS_METRICS_EXPORTER_METRICS_PATH}"
        )

    def _metrics_exporter_requires_auth(self):
        """
        Return True when /metrics is fronted by auth (kube-rbac-proxy or
        legacy ``--secure-serving=true`` on the exporter container).

        Current ODF deployments put kube-rbac-proxy sidecars first in the
        container list; checking only containers[0] misses that layout.
        """
        try:
            deploy = self._get_metrics_exporter_deployment()
            containers = (
                deploy.get("spec", {})
                .get("template", {})
                .get("spec", {})
                .get("containers", [])
                or []
            )
            for container in containers:
                name = container.get("name") or ""
                if name.startswith("kube-rbac-proxy"):
                    return True
                args = container.get("args") or []
                if "--secure-serving=true" in args:
                    return True
            return False
        except Exception as exc:
            log.debug(f"Could not inspect ocs-metrics-exporter deployment: {exc}")
            return False

    # Backward-compatible alias used by older call sites / tests.
    _is_metrics_exporter_secure_serving = _metrics_exporter_requires_auth

    def _get_metrics_bearer_token(self):
        """Create a short-lived bearer token for scraping authenticated /metrics."""
        ocp = OCP(namespace=_PROMETHEUS_NS)
        token = ocp.exec_oc_cmd(
            f"create token {_PROMETHEUS_SA} --duration=600s",
            out_yaml_format=False,
        )
        return token.strip()

    def _curl_metrics_from_pod(
        self, pod_obj, metrics_url, container_name=None, bearer_token=None
    ):
        """Run curl against *metrics_url* inside *pod_obj*."""
        auth_header = ""
        if bearer_token:
            auth_header = f" -H 'Authorization: Bearer {bearer_token}'"
        return pod_obj.exec_cmd_on_pod(
            f"curl -sk --connect-timeout 15{auth_header} {metrics_url}",
            out_yaml_format=False,
            container_name=container_name,
        )

    def _metrics_output_is_valid(self, metrics_output):
        """Return True when *metrics_output* looks like a Prometheus scrape."""
        if not metrics_output or "# TYPE" not in metrics_output:
            return False
        return any(
            prefix in metrics_output
            for prefix in constants.OCS_METRICS_EXPORTER_METRIC_PREFIXES
        )

    def fetch_metrics_exporter_metrics(self, metrics_pod=None):
        """
        Fetch the ocs-metrics-exporter /metrics payload.

        When metrics are behind kube-rbac-proxy (or legacy
        ``--secure-serving=true``), a short-lived ``prometheus-k8s`` bearer
        token is included in the request. The scrape port is discovered from
        the live Service (prefer ``https-main``); ``https-self`` is skipped
        because it only exposes process metrics.

        Prefer curl from the exporter pod itself (works on baremetal where
        toolbox-to-pod-IP routing may fail). Fall back to the toolbox pod
        curling the exporter pod IP.
        """
        metrics_pod = metrics_pod or self.assert_metrics_exporter_running()
        pod_name = metrics_pod["metadata"]["name"]
        scrape_port = self._discover_metrics_exporter_scrape_port()
        local_url = self._metrics_exporter_local_url()

        bearer_token = None
        if self._metrics_exporter_requires_auth():
            log.info(
                "ocs-metrics-exporter /metrics requires auth "
                "(kube-rbac-proxy or secure-serving); "
                "fetching prometheus-k8s bearer token"
            )
            try:
                bearer_token = self._get_metrics_bearer_token()
            except Exception as exc:
                log.warning(f"Failed to create metrics bearer token: {exc}")

        log.info(f"Fetching ocs-metrics-exporter metrics from {pod_name} ({local_url})")
        try:
            local_output = self._curl_metrics_from_pod(
                get_pod_obj(pod_name, namespace=self.namespace),
                local_url,
                container_name=constants.OCS_METRICS_EXPORTER_CONTAINER,
                bearer_token=bearer_token,
            )
            if self._metrics_output_is_valid(local_output):
                return local_output
            log.debug(
                "ocs-metrics-exporter local /metrics response was empty or invalid"
            )
        except Exception as exc:
            log.debug(f"ocs-metrics-exporter local metrics fetch failed: {exc}")

        pod_ip = get_pod_ip(
            OCP(
                kind=constants.POD,
                namespace=self.namespace,
                resource_name=pod_name,
            )
        )
        if not pod_ip:
            raise UnexpectedBehaviour("ocs-metrics-exporter pod IP is not assigned")

        remote_url = (
            f"https://{pod_ip}:{scrape_port}"
            f"{constants.OCS_METRICS_EXPORTER_METRICS_PATH}"
        )
        log.info(f"Fetching ocs-metrics-exporter metrics via toolbox ({remote_url})")
        toolbox = get_ceph_tools_pod()
        return self._curl_metrics_from_pod(
            toolbox, remote_url, bearer_token=bearer_token
        )

    def assert_metrics_exporter_metrics(self, metrics_output=None, metrics_pod=None):
        """
        Assert ocs-metrics-exporter exposes Prometheus metrics from Ceph/OCS.

        Args:
            metrics_output (str): Pre-fetched /metrics payload.
            metrics_pod (dict): Optional exporter pod resource dict.
        """
        metrics_output = metrics_output or self.fetch_metrics_exporter_metrics(
            metrics_pod
        )
        if not metrics_output or "# TYPE" not in metrics_output:
            raise UnexpectedBehaviour(
                "ocs-metrics-exporter /metrics response is empty or invalid"
            )
        if not any(
            prefix in metrics_output
            for prefix in constants.OCS_METRICS_EXPORTER_METRIC_PREFIXES
        ):
            raise UnexpectedBehaviour(
                "ocs-metrics-exporter /metrics missing expected OCS/Ceph metric names"
            )
        log.info("ocs-metrics-exporter metrics export verified successfully")

    def verify_metrics_exporter_no_auth_bad_key(self, metrics_pod=None, tail=500):
        """Assert ocs-metrics-exporter logs do not contain AUTH_BAD_KEY errors."""
        metrics_pod = metrics_pod or self.assert_metrics_exporter_running()
        pod_name = metrics_pod["metadata"]["name"]
        auth_errors = get_pod_logs(
            pod_name=pod_name,
            container=constants.OCS_METRICS_EXPORTER_CONTAINER,
            namespace=self.namespace,
            tail=str(tail),
            grep=constants.AUTH_BAD_KEY_LOG,
            return_empty_string=True,
        )
        if auth_errors and constants.AUTH_BAD_KEY_LOG in auth_errors:
            raise UnexpectedBehaviour(
                f"AUTH_BAD_KEY errors found in ocs-metrics-exporter logs: "
                f"{auth_errors.strip()}"
            )
        log.info("No AUTH_BAD_KEY errors in ocs-metrics-exporter logs")

    def wait_for_metrics_exporter_metrics(
        self, timeout=600, sleep=15, metrics_pod=None
    ):
        """Wait until ocs-metrics-exporter exports valid metrics."""
        log.info(
            f"Waiting for ocs-metrics-exporter metrics export (timeout={timeout}s)"
        )

        def _metrics_ready():
            try:
                self.assert_metrics_exporter_metrics(metrics_pod=metrics_pod)
                self.verify_metrics_exporter_no_auth_bad_key(metrics_pod=metrics_pod)
                return True
            except UnexpectedBehaviour as exc:
                log.debug(f"ocs-metrics-exporter metrics not ready yet: {exc}")
                return False

        for ready in TimeoutSampler(timeout, sleep, _metrics_ready):
            if ready:
                log.info("ocs-metrics-exporter metrics export is healthy")
                return True

        raise UnexpectedBehaviour(
            f"ocs-metrics-exporter did not export metrics within {timeout}s"
        )

    def wait_for_metrics_exporter_after_rotation(
        self, previous_pod_name=None, timeout=900, sleep=15
    ):
        """
        Wait for ocs-metrics-exporter to recover after CephX key rotation.

        The exporter may restart or reload its Ceph keyring; metrics export and
        logs are polled until healthy.
        """
        log.info(
            "Waiting for ocs-metrics-exporter to use rotated CephX key "
            f"(previous pod={previous_pod_name or 'unknown'})"
        )

        def _exporter_ready():
            pods = self.get_metrics_exporter_pods()
            if not pods:
                log.debug("ocs-metrics-exporter pod is not Running yet")
                return False
            pod_name = pods[0]["metadata"]["name"]
            if previous_pod_name and pod_name != previous_pod_name:
                log.info(
                    f"ocs-metrics-exporter pod restarted: "
                    f"{previous_pod_name} -> {pod_name}"
                )
            try:
                self.assert_metrics_exporter_metrics(metrics_pod=pods[0])
                self.verify_metrics_exporter_no_auth_bad_key(pods[0])
                return True
            except UnexpectedBehaviour as exc:
                log.debug(f"ocs-metrics-exporter not ready after rotation: {exc}")
                return False

        for ready in TimeoutSampler(timeout, sleep, _exporter_ready):
            if ready:
                log.info("ocs-metrics-exporter is healthy after CephX key rotation")
                return True

        raise UnexpectedBehaviour(
            f"ocs-metrics-exporter did not recover within {timeout}s after rotation"
        )
