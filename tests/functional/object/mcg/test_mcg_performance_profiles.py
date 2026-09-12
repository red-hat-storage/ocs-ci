import json
import logging
import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    tier1,
    red_squad,
    mcg,
    runs_on_provider,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import get_pods_having_label
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)


def get_pod_resources(pod):
    """
    Get CPU and memory resources from a pod's first container.

    Args:
        pod (dict): Pod resource dict as returned by get_pods_having_label

    Returns:
        dict: Resources with requests and limits
    """
    container = pod["spec"]["containers"][0]
    return container.get("resources", {})


def normalize_cpu(value):
    """
    Normalize a CPU quantity so equivalent values compare equal
    (e.g. "500m" == 0.5, "1" == "1000m").

    Args:
        value: CPU quantity as a string (e.g. "500m", "1") or number

    Returns:
        float or None: Normalized CPU value in cores, or None if value is None
    """
    if value is None:
        return None
    if isinstance(value, str):
        if value.endswith("m"):
            return float(value[:-1]) / 1000
        return float(value)
    return float(value)


def verify_resources(
    actual,
    expected_req_cpu,
    expected_lim_cpu,
    expected_req_mem,
    expected_lim_mem,
    component_name,
):
    """
    Verify that actual resources match expected values.

    Args:
        actual (dict): Actual resource dict from pod
        expected_req_cpu (str): Expected CPU request
        expected_lim_cpu (str): Expected CPU limit
        expected_req_mem (str): Expected memory request
        expected_lim_mem (str): Expected memory limit
        component_name (str): Name of component being verified

    Returns:
        bool: True if all values match, False otherwise
    """
    requests = actual.get("requests", {})
    limits = actual.get("limits", {})

    req_cpu = requests.get("cpu")
    lim_cpu = limits.get("cpu")
    req_mem = requests.get("memory")
    lim_mem = limits.get("memory")

    req_cpu_norm = normalize_cpu(req_cpu)
    lim_cpu_norm = normalize_cpu(lim_cpu)
    exp_req_cpu_norm = normalize_cpu(expected_req_cpu)
    exp_lim_cpu_norm = normalize_cpu(expected_lim_cpu)

    errors = []

    if req_cpu_norm != exp_req_cpu_norm:
        errors.append(
            f"{component_name} CPU request: expected {expected_req_cpu}, got {req_cpu}"
        )

    if lim_cpu_norm != exp_lim_cpu_norm:
        errors.append(
            f"{component_name} CPU limit: expected {expected_lim_cpu}, got {lim_cpu}"
        )

    if req_mem != expected_req_mem:
        errors.append(
            f"{component_name} Memory request: expected {expected_req_mem}, got {req_mem}"
        )

    if lim_mem != expected_lim_mem:
        errors.append(
            f"{component_name} Memory limit: expected {expected_lim_mem}, got {lim_mem}"
        )

    if errors:
        for error in errors:
            logger.error(error)
        return False

    logger.info(f"{component_name} resources verified successfully")
    return True


def resources_match(actual, req_cpu, lim_cpu, req_mem, lim_mem):
    """
    Return whether a pod's resource dict matches the expected requests/limits.

    A quiet, non-logging counterpart to verify_resources, intended for polling
    while the operator recreates pods after a profile change.

    Args:
        actual (dict): Actual resource dict from a pod
        req_cpu (str): Expected CPU request
        lim_cpu (str): Expected CPU limit
        req_mem (str): Expected memory request
        lim_mem (str): Expected memory limit

    Returns:
        bool: True if requests and limits match (CPU compared normalized)
    """
    requests = actual.get("requests", {})
    limits = actual.get("limits", {})
    return (
        normalize_cpu(requests.get("cpu")) == normalize_cpu(req_cpu)
        and normalize_cpu(limits.get("cpu")) == normalize_cpu(lim_cpu)
        and requests.get("memory") == req_mem
        and limits.get("memory") == lim_mem
    )


@tier1
@mcg
@red_squad
@runs_on_provider
class TestMCGPerformanceProfiles:
    """
    Test MCG Performance Profiles (default, mixed-workload, small-objects).

    Verifies that setting performanceProfile on StorageCluster CR correctly
    propagates resource specifications to noobaa-core, noobaa-db, and
    noobaa-endpoint pods.

    Test cases correspond to:
    - CLI-1: Verify "default" profile
    - CLI-2: Verify "mixed-workload" profile
    - CLI-3: Verify "small-objects" profile
    """

    # Profile specifications as per RHSTOR-9144 and NooBaa operator source code
    # (https://github.com/noobaa/noobaa-operator/blob/master/pkg/system/performance_profiles.go)
    PROFILE_SPECS = {
        "default": {
            "core": {
                "req_cpu": "500m",
                "lim_cpu": "1",
                "req_mem": "1Gi",
                "lim_mem": "4Gi",
                "qos": "Burstable",
            },
            "db": {
                "req_cpu": "1",
                "lim_cpu": "1",
                "req_mem": "2Gi",
                "lim_mem": "2Gi",
                "qos": "Guaranteed",
            },
            "endpoint": {
                "req_cpu": "500m",
                "lim_cpu": "2",
                "req_mem": "1Gi",
                "lim_mem": "3Gi",
                "qos": "Burstable",
            },
            "endpoint_count": {"min": 1, "max": 2},
            "db_instances": 2,
            "pv_pool": {"cpu": "400m", "mem": "800Mi"},
        },
        "mixed-workload": {
            "core": {
                "req_cpu": "1",
                "lim_cpu": "2",
                "req_mem": "2Gi",
                "lim_mem": "4Gi",
                "qos": "Burstable",
            },
            "db": {
                "req_cpu": "4",
                "lim_cpu": "4",
                "req_mem": "8Gi",
                "lim_mem": "8Gi",
                "qos": "Guaranteed",
            },
            "endpoint": {
                "req_cpu": "2",
                "lim_cpu": "4",
                "req_mem": "2Gi",
                "lim_mem": "4Gi",
                "qos": "Burstable",
            },
            "endpoint_count": {"min": 2, "max": 4},
            "db_instances": 2,
            "pv_pool": {"cpu": "1", "mem": "2Gi"},
        },
        "small-objects": {
            "core": {
                "req_cpu": "1",
                "lim_cpu": "2",
                "req_mem": "2Gi",
                "lim_mem": "6Gi",
                "qos": "Burstable",
            },
            "db": {
                "req_cpu": "6",
                "lim_cpu": "6",
                "req_mem": "16Gi",
                "lim_mem": "16Gi",
                "qos": "Guaranteed",
            },
            "endpoint": {
                "req_cpu": "1",  # Note: Lower than mixed-workload due to single-process saturation
                "lim_cpu": "4",
                "req_mem": "2Gi",
                "lim_mem": "4Gi",
                "qos": "Burstable",
            },
            "endpoint_count": {"min": 2, "max": 4},
            "db_instances": 2,
            "pv_pool": {"cpu": "1", "mem": "2Gi"},
        },
    }

    @pytest.fixture
    def set_profile(self, request):
        """
        Set the MCG performance profile on the StorageCluster CR, wait for it
        to propagate to the NooBaa CR, and restore the original profile on
        teardown so the cluster is left as it was found.

        Returns:
            str: Profile name that was set
        """
        profile = request.param
        logger.info(f"Setting MCG performance profile to '{profile}'")

        ocp_obj = OCP(
            kind=constants.STORAGECLUSTER,
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name=constants.DEFAULT_CLUSTERNAME,
        )
        noobaa_ocp = OCP(
            kind="NooBaa",
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name="noobaa",
        )

        # Capture the original profile so it can be restored on teardown.
        # A JSON-merge patch removes a key when its value is null, so if the
        # field was unset originally we restore it to None to drop it.
        original_profile = (
            ocp_obj.get()
            .get("spec", {})
            .get("multiCloudGateway", {})
            .get("performanceProfile")
        )

        def finalizer():
            logger.info(
                f"Restoring MCG performance profile to its original value "
                f"'{original_profile}'"
            )
            restore_patch = {
                "spec": {"multiCloudGateway": {"performanceProfile": original_profile}}
            }
            patched = ocp_obj.patch(
                params=json.dumps(restore_patch), format_type="merge"
            )
            # patch() returns False when nothing changed, which is expected when
            # the profile was already at its original value (e.g. restoring a
            # None/unset field), so only warn rather than fail the teardown.
            if not patched:
                logger.warning(
                    "Restore patch reported no change while restoring profile to "
                    f"'{original_profile}'"
                )
            # Wait for the restore to fully reconcile so the cluster is left
            # healthy and the next test's NooBaa health check does not run
            # while NooBaa is still recreating pods.
            self._wait_for_profile_settled(noobaa_ocp, original_profile)

        request.addfinalizer(finalizer)

        # Patch StorageCluster with the profile. OCP.patch returns False when
        # the resource is not patched, so assert on it to fail fast.
        patch = {"spec": {"multiCloudGateway": {"performanceProfile": profile}}}
        assert ocp_obj.patch(
            params=json.dumps(patch), format_type="merge"
        ), f"Failed to patch StorageCluster with performance profile '{profile}'"

        # The operator recreates the NooBaa pods asynchronously after the
        # profile changes, so wait until NooBaa is back to Ready and the core
        # pods have actually been recreated with the new resources before the
        # test reads pod state.
        self._wait_for_profile_settled(noobaa_ocp, profile)
        logger.info(f"Profile '{profile}' successfully applied and reconciled")

        return profile

    def _wait_for_profile_settled(self, noobaa_ocp, profile):
        """
        Wait until a performance-profile change has fully reconciled: NooBaa is
        back to the Ready phase and the running noobaa-core pod has been
        recreated with the target profile's resources.

        The operator recreates the NooBaa pods asynchronously, so both reading
        pod resources and starting the next test must wait for this; otherwise
        the previous profile's pods (or a NooBaa still in the Creating phase)
        are observed. TimeoutSampler swallows transient errors raised while
        pods churn and retries until the timeout.

        Args:
            noobaa_ocp (OCP): OCP handle for the NooBaa CR
            profile (str or None): target profile; None means the default spec
        """
        core_spec = self.PROFILE_SPECS[profile or "default"]["core"]

        def _settled():
            phase = noobaa_ocp.get().get("status", {}).get("phase")
            if phase != constants.STATUS_READY:
                return False
            core_pods = get_pods_having_label(
                label=constants.NOOBAA_CORE_POD_LABEL,
                namespace=config.ENV_DATA["cluster_namespace"],
                statuses=[constants.STATUS_RUNNING],
            )
            return bool(core_pods) and resources_match(
                get_pod_resources(core_pods[0]),
                core_spec["req_cpu"],
                core_spec["lim_cpu"],
                core_spec["req_mem"],
                core_spec["lim_mem"],
            )

        for settled in TimeoutSampler(timeout=900, sleep=20, func=_settled):
            if settled:
                break
            logger.info(
                f"Waiting for NooBaa to settle on profile '{profile}' "
                "(Ready phase and core pods recreated with new resources)"
            )

    def _verify_core(self, spec, profile):
        """
        Verify noobaa-core pod resources and QoS class.
        """
        logger.info("Verifying noobaa-core pod resources")
        core_pods = get_pods_having_label(
            label=constants.NOOBAA_CORE_POD_LABEL,
            namespace=config.ENV_DATA["cluster_namespace"],
            statuses=[constants.STATUS_RUNNING],
        )
        assert core_pods, "No running noobaa-core pods found"

        core_resources = get_pod_resources(core_pods[0])
        assert verify_resources(
            core_resources,
            spec["core"]["req_cpu"],
            spec["core"]["lim_cpu"],
            spec["core"]["req_mem"],
            spec["core"]["lim_mem"],
            "noobaa-core",
        ), f"noobaa-core resources do not match '{profile}' profile"

        # Use the pod-reported QoS class, which Kubernetes computes across all
        # (app and init) containers, rather than deriving it from a single
        # container's resources.
        qos = core_pods[0]["status"]["qosClass"]
        assert (
            qos == spec["core"]["qos"]
        ), f"noobaa-core QoS class: expected {spec['core']['qos']}, got {qos}"
        logger.info(f"noobaa-core QoS class: {qos} ✓")

    def _verify_db(self, spec, profile):
        """
        Verify noobaa-db pod resources and QoS class (per instance) and the
        expected DB instance count.
        """
        logger.info("Verifying noobaa-db pod resources")
        db_pods = get_pods_having_label(
            label=constants.NOOBAA_DB_LABEL_419_AND_ABOVE,
            namespace=config.ENV_DATA["cluster_namespace"],
            statuses=[constants.STATUS_RUNNING],
        )
        assert db_pods, "No running noobaa-db pods found"

        for i, db_pod in enumerate(db_pods):
            db_resources = get_pod_resources(db_pod)
            assert verify_resources(
                db_resources,
                spec["db"]["req_cpu"],
                spec["db"]["lim_cpu"],
                spec["db"]["req_mem"],
                spec["db"]["lim_mem"],
                f"noobaa-db-{i+1}",
            ), f"noobaa-db pod {i+1} resources do not match '{profile}' profile"

            qos = db_pod["status"]["qosClass"]
            assert (
                qos == spec["db"]["qos"]
            ), f"noobaa-db pod {i+1} QoS class: expected {spec['db']['qos']}, got {qos}"
        logger.info(
            f"All {len(db_pods)} noobaa-db pods verified, QoS: {spec['db']['qos']} ✓"
        )

        # Verify DB instances count
        expected_db_instances = spec["db_instances"]
        assert len(db_pods) == expected_db_instances, (
            f"DB instance count: expected {expected_db_instances}, "
            f"got {len(db_pods)}"
        )
        logger.info(f"DB instances: {len(db_pods)} ✓")

    def _verify_endpoints(self, spec, profile):
        """
        Verify noobaa-endpoint pod resources, QoS class, and pod count
        (min/max via HPA or deployment).
        """
        logger.info("Verifying noobaa-endpoint pod resources")
        endpoint_label = (
            f"{constants.NOOBAA_APP_LABEL},{constants.NOOBAA_ENDPOINT_POD_LABEL}"
        )
        expected_min = spec["endpoint_count"]["min"]
        expected_max = spec["endpoint_count"]["max"]

        # A profile change recreates/rescales the endpoint pods, so wait until
        # the running endpoint count settles within the expected range before
        # asserting, to avoid reading terminating or freshly created pods.
        endpoint_pods = []
        for endpoint_pods in TimeoutSampler(
            timeout=300,
            sleep=10,
            func=get_pods_having_label,
            label=endpoint_label,
            namespace=config.ENV_DATA["cluster_namespace"],
            statuses=[constants.STATUS_RUNNING],
        ):
            if expected_min <= len(endpoint_pods) <= expected_max:
                break
            logger.info(
                f"Waiting for running endpoint pod count to reach "
                f"[{expected_min}, {expected_max}], current: {len(endpoint_pods)}"
            )
        assert endpoint_pods, "No running noobaa-endpoint pods found"

        # Verify every endpoint pod, not just the first one, so a pod with
        # stale or incorrect resources cannot slip through.
        for i, endpoint_pod in enumerate(endpoint_pods):
            endpoint_resources = get_pod_resources(endpoint_pod)
            assert verify_resources(
                endpoint_resources,
                spec["endpoint"]["req_cpu"],
                spec["endpoint"]["lim_cpu"],
                spec["endpoint"]["req_mem"],
                spec["endpoint"]["lim_mem"],
                f"noobaa-endpoint-{i+1}",
            ), f"noobaa-endpoint pod {i+1} resources do not match '{profile}' profile"

            qos = endpoint_pod["status"]["qosClass"]
            assert qos == spec["endpoint"]["qos"], (
                f"noobaa-endpoint pod {i+1} QoS class: "
                f"expected {spec['endpoint']['qos']}, got {qos}"
            )
        logger.info(
            f"All {len(endpoint_pods)} noobaa-endpoint pods verified, "
            f"QoS: {spec['endpoint']['qos']} ✓"
        )

        # Verify endpoint pod count (min/max)
        current_count = len(endpoint_pods)
        assert expected_min <= current_count <= expected_max, (
            f"Endpoint pod count {current_count} not within expected range "
            f"[{expected_min}, {expected_max}]"
        )
        logger.info(
            f"Endpoint pod count: {current_count} "
            f"(within range [{expected_min}, {expected_max}]) ✓"
        )

        # Check HPA if it exists
        hpa_ocp = OCP(
            kind="HorizontalPodAutoscaler",
            namespace=config.ENV_DATA["cluster_namespace"],
        )
        hpas = hpa_ocp.get(selector=constants.NOOBAA_ENDPOINT_POD_LABEL).get(
            "items", []
        )
        if hpas:
            hpa = hpas[0]
            hpa_min = hpa["spec"]["minReplicas"]
            hpa_max = hpa["spec"]["maxReplicas"]
            assert (
                hpa_min == expected_min
            ), f"HPA minReplicas: expected {expected_min}, got {hpa_min}"
            assert (
                hpa_max == expected_max
            ), f"HPA maxReplicas: expected {expected_max}, got {hpa_max}"
            logger.info(f"HPA configured: min={hpa_min}, max={hpa_max} ✓")
        else:
            logger.info("No HPA found (static replica count)")

    def _verify_pv_pool(self, spec, profile):
        """
        Verify PV pool agent pod resources (vSphere/on-prem only). Skipped on
        cloud platforms where the default backingstore is not a pv-pool.
        """
        logger.info("Checking for PV pool backingstore")
        bs_ocp = OCP(
            kind="BackingStore", namespace=config.ENV_DATA["cluster_namespace"]
        )

        # Limit the try to the backingstore lookup only, so a real resource
        # mismatch below is not swallowed. A missing default backingstore is a
        # valid skip (e.g. cloud platforms without a pv-pool).
        try:
            default_bs = bs_ocp.get(resource_name="noobaa-default-backing-store")
        except CommandFailed as e:
            # Only a genuine "not found" is a valid skip (e.g. cloud platforms
            # without a pv-pool). Authorization, connectivity, or other API
            # errors must fail the test rather than be silently skipped.
            if "not found" in str(e).lower() or "notfound" in str(e).lower():
                logger.info(f"Default backingstore not found: {e}")
                logger.info("Skipping PV pool verification")
                return
            raise

        bs_type = default_bs.get("spec", {}).get("type")
        if bs_type != "pv-pool":
            logger.info(
                f"Backingstore type is '{bs_type}' (cloud storage), "
                "skipping PV pool verification (N/A for cloud platforms)"
            )
            return

        logger.info("PV pool backingstore detected, verifying agent pod resources")
        pv_pool_pods = get_pods_having_label(
            label=constants.NOOBAA_DEFAULT_BACKINGSTORE_LABEL,
            namespace=config.ENV_DATA["cluster_namespace"],
            statuses=[constants.STATUS_RUNNING],
        )
        assert (
            pv_pool_pods
        ), "PV pool backingstore exists but no running agent pods found"

        pv_resources = get_pod_resources(pv_pool_pods[0])
        # PV pool pods have equal requests and limits
        expected_cpu = spec["pv_pool"]["cpu"]
        expected_mem = spec["pv_pool"]["mem"]
        assert verify_resources(
            pv_resources,
            expected_cpu,
            expected_cpu,  # limits == requests for PV pool
            expected_mem,
            expected_mem,  # limits == requests for PV pool
            "PV pool agent",
        ), f"PV pool agent resources do not match '{profile}' profile"
        logger.info("PV pool agent pod resources verified ✓")

    @pytest.mark.parametrize(
        "set_profile",
        ["default", "mixed-workload", "small-objects"],
        indirect=True,
    )
    @pytest.mark.polarion_id("OCS-6000")  # TODO: Update with actual Polarion ID
    def test_mcg_performance_profile_resources(self, set_profile):
        """
        Verify MCG performance profile resource specifications.

        Test Steps (per profile):
            1. Set spec.multiCloudGateway.performanceProfile on StorageCluster CR
            2. Verify noobaa-core pod resources and QoS class
            3. Verify noobaa-db pod resources, QoS class, and instance count
            4. Verify noobaa-endpoint pod resources, QoS class, and count
            5. Verify PV pool agent pod resources (vSphere/on-prem only)

        Expected Results:
            All resource values match the profile specification from RHSTOR-9144
        """
        profile = set_profile
        spec = self.PROFILE_SPECS[profile]

        logger.info(f"Testing '{profile}' profile resource specifications")

        self._verify_core(spec, profile)
        self._verify_db(spec, profile)
        self._verify_endpoints(spec, profile)
        self._verify_pv_pool(spec, profile)

        logger.info(f"✅ All verifications passed for '{profile}' profile")
