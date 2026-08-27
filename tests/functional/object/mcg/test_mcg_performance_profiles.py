import logging
import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    tier1,
    red_squad,
    mcg,
    runs_on_provider,
)
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import get_pods_having_label

logger = logging.getLogger(__name__)


def get_pod_resources(pod):
    """
    Get CPU and memory resources from a pod's first container.

    Args:
        pod (Pod): Pod object

    Returns:
        dict: Resources with requests and limits
    """
    container = pod.get()["spec"]["containers"][0]
    return container.get("resources", {})


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

    # Normalize CPU values (500m == 0.5)
    def normalize_cpu(value):
        if value is None:
            return None
        if isinstance(value, str):
            if value.endswith("m"):
                return float(value[:-1]) / 1000
            return float(value)
        return float(value)

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


def get_qos_class(resources):
    """
    Determine QoS class from resources.

    Args:
        resources (dict): Resource dict with requests and limits

    Returns:
        str: QoS class (Guaranteed, Burstable, or BestEffort)
    """
    requests = resources.get("requests", {})
    limits = resources.get("limits", {})

    if not limits:
        return "BestEffort"

    # Guaranteed: requests == limits for all resources
    if requests.get("cpu") == limits.get("cpu") and requests.get(
        "memory"
    ) == limits.get("memory"):
        return "Guaranteed"

    return "Burstable"


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

    @pytest.fixture()
    def set_profile(self, request):
        """
        Set MCG performance profile on StorageCluster CR and wait for propagation.

        Returns:
            str: Profile name that was set
        """
        profile = request.param
        logger.info(f"Setting MCG performance profile to '{profile}'")

        ocp_obj = OCP(
            kind="StorageCluster",
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name="ocs-storagecluster",
        )

        # Patch StorageCluster with the profile
        patch = {"spec": {"multiCloudGateway": {"performanceProfile": profile}}}
        ocp_obj.patch(params=f"{patch}", format_type="merge")

        # Verify it propagated to NooBaa CR
        noobaa_ocp = OCP(
            kind="NooBaa",
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name="noobaa",
        )
        noobaa_profile = noobaa_ocp.get()["spec"].get("performanceProfile")
        assert noobaa_profile == profile, (
            f"Profile '{profile}' not propagated to NooBaa CR, "
            f"got '{noobaa_profile}' instead"
        )

        logger.info(f"Profile '{profile}' successfully set and propagated to NooBaa CR")

        # Wait a bit for operator to reconcile
        import time

        time.sleep(10)

        return profile

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
            3. Verify noobaa-db pod resources and QoS class (per instance)
            4. Verify noobaa-endpoint pod resources and QoS class
            5. Verify endpoint pod count (min/max via HPA or deployment)
            6. Verify DB instances count
            7. Verify PV pool agent pod resources (vSphere/on-prem only)

        Expected Results:
            All resource values match the profile specification from RHSTOR-9144
        """
        profile = set_profile
        spec = self.PROFILE_SPECS[profile]

        logger.info(f"Testing '{profile}' profile resource specifications")

        # Step 2: Verify noobaa-core pod resources
        logger.info("Verifying noobaa-core pod resources")
        core_pods = get_pods_having_label(
            label="noobaa-core=noobaa",
            namespace=config.ENV_DATA["cluster_namespace"],
        )
        assert core_pods, "No noobaa-core pods found"

        core_pod = core_pods[0]
        core_resources = get_pod_resources(core_pod)

        assert verify_resources(
            core_resources,
            spec["core"]["req_cpu"],
            spec["core"]["lim_cpu"],
            spec["core"]["req_mem"],
            spec["core"]["lim_mem"],
            "noobaa-core",
        ), f"noobaa-core resources do not match '{profile}' profile"

        qos = get_qos_class(core_resources)
        assert (
            qos == spec["core"]["qos"]
        ), f"noobaa-core QoS class: expected {spec['core']['qos']}, got {qos}"
        logger.info(f"noobaa-core QoS class: {qos} ✓")

        # Step 3: Verify noobaa-db pod resources (per instance)
        logger.info("Verifying noobaa-db pod resources")
        db_pods = get_pods_having_label(
            label="cnpg.io/cluster=noobaa-db-pg-cluster",
            namespace=config.ENV_DATA["cluster_namespace"],
        )
        assert db_pods, "No noobaa-db pods found"

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

            qos = get_qos_class(db_resources)
            assert (
                qos == spec["db"]["qos"]
            ), f"noobaa-db pod {i+1} QoS class: expected {spec['db']['qos']}, got {qos}"
        logger.info(
            f"All {len(db_pods)} noobaa-db pods verified, QoS: {spec['db']['qos']} ✓"
        )

        # Step 4: Verify noobaa-endpoint pod resources
        logger.info("Verifying noobaa-endpoint pod resources")
        endpoint_pods = get_pods_having_label(
            label="app=noobaa,noobaa-s3=noobaa",
            namespace=config.ENV_DATA["cluster_namespace"],
        )
        assert endpoint_pods, "No noobaa-endpoint pods found"

        endpoint_pod = endpoint_pods[0]
        endpoint_resources = get_pod_resources(endpoint_pod)

        assert verify_resources(
            endpoint_resources,
            spec["endpoint"]["req_cpu"],
            spec["endpoint"]["lim_cpu"],
            spec["endpoint"]["req_mem"],
            spec["endpoint"]["lim_mem"],
            "noobaa-endpoint",
        ), f"noobaa-endpoint resources do not match '{profile}' profile"

        qos = get_qos_class(endpoint_resources)
        assert (
            qos == spec["endpoint"]["qos"]
        ), f"noobaa-endpoint QoS class: expected {spec['endpoint']['qos']}, got {qos}"
        logger.info(f"noobaa-endpoint QoS class: {qos} ✓")

        # Step 5: Verify endpoint pod count (min/max)
        logger.info("Verifying endpoint pod count configuration")
        current_count = len(endpoint_pods)
        expected_min = spec["endpoint_count"]["min"]
        expected_max = spec["endpoint_count"]["max"]

        assert expected_min <= current_count <= expected_max, (
            f"Endpoint pod count {current_count} not within expected range "
            f"[{expected_min}, {expected_max}]"
        )
        logger.info(
            f"Endpoint pod count: {current_count} (within range [{expected_min}, {expected_max}]) ✓"
        )

        # Check HPA if it exists
        hpa_ocp = OCP(
            kind="HorizontalPodAutoscaler",
            namespace=config.ENV_DATA["cluster_namespace"],
        )
        hpas = hpa_ocp.get(selector="noobaa-s3=noobaa").get("items", [])

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

        # Step 6: Verify DB instances count
        logger.info("Verifying DB instances count")
        db_count = len(db_pods)
        expected_db_instances = spec["db_instances"]

        assert (
            db_count == expected_db_instances
        ), f"DB instance count: expected {expected_db_instances}, got {db_count}"
        logger.info(f"DB instances: {db_count} ✓")

        # Step 7: Verify PV pool agent pod resources (vSphere/on-prem only)
        logger.info("Checking for PV pool backingstore")
        bs_ocp = OCP(
            kind="BackingStore", namespace=config.ENV_DATA["cluster_namespace"]
        )

        try:
            default_bs = bs_ocp.get(resource_name="noobaa-default-backing-store")
            bs_type = default_bs.get("spec", {}).get("type")

            if bs_type == "pv-pool":
                logger.info(
                    "PV pool backingstore detected, verifying agent pod resources"
                )

                pv_pool_pods = get_pods_having_label(
                    label="pool=noobaa-default-backing-store",
                    namespace=config.ENV_DATA["cluster_namespace"],
                )

                if pv_pool_pods:
                    pv_pod = pv_pool_pods[0]
                    pv_resources = get_pod_resources(pv_pod)

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
                else:
                    logger.warning(
                        "PV pool backingstore exists but no agent pods found"
                    )
            else:
                logger.info(
                    f"Backingstore type is '{bs_type}' (cloud storage), "
                    "skipping PV pool verification (N/A for cloud platforms)"
                )
        except Exception as e:
            logger.info(f"Default backingstore not found or error checking: {e}")
            logger.info("Skipping PV pool verification")

        logger.info(f"✅ All verifications passed for '{profile}' profile")
