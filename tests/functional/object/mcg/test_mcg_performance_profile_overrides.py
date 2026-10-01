import json
import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import (
    tier2,
    red_squad,
    mcg,
    runs_on_provider,
)
from ocs_ci.helpers import mcg_performance_profiles as profiles
from ocs_ci.ocs import constants

logger = logging.getLogger(__name__)

# Profile used as the baseline the overrides have to beat. Its values differ
# from every override below, so an override that silently fails to apply is
# always visible.
BASE_PROFILE = "mixed-workload"

# Explicit per-component resources set under spec.resources on the
# StorageCluster CR. Shaped like a PROFILE_SPECS entry so the same component
# verifiers can be reused.
OVERRIDE_SPEC = {
    "core": profiles._resources(
        "750m", "1500m", "1536Mi", "3Gi", profiles.QOS_BURSTABLE
    ),
    "db": profiles._resources("2", "2", "3Gi", "3Gi", profiles.QOS_GUARANTEED),
    "endpoint": profiles._resources(
        "600m", "1200m", "1200Mi", "2560Mi", profiles.QOS_BURSTABLE
    ),
    "endpoint_count": {"min": 3, "max": 5},
    "db_instances": profiles.PROFILE_SPECS[BASE_PROFILE]["db_instances"],
}

# Resources set on the user-created PV pool backingstore itself, through
# spec.pvPool.resources. Differ from every profile's pvPoolResources.
PV_POOL_OVERRIDE_CPU = "600m"
PV_POOL_OVERRIDE_MEM = "1Gi"

PV_POOL_NUM_VOLUMES = 1

OVERRIDE_KEYS = (
    profiles.SC_RESOURCE_KEY_CORE,
    profiles.SC_RESOURCE_KEY_DB,
    profiles.SC_RESOURCE_KEY_ENDPOINT,
)


def _as_k8s_resources(component_spec):
    """
    Convert a component spec into a Kubernetes ResourceRequirements dict.

    Args:
        component_spec (dict): Entry with req_cpu, lim_cpu, req_mem, lim_mem

    Returns:
        dict: requests/limits dict suitable for a CR patch
    """
    return {
        "requests": {
            "cpu": component_spec["req_cpu"],
            "memory": component_spec["req_mem"],
        },
        "limits": {
            "cpu": component_spec["lim_cpu"],
            "memory": component_spec["lim_mem"],
        },
    }


def _clear_overrides():
    """
    Remove the NooBaa resource overrides and the MCG endpoints section from the
    StorageCluster CR, handing every component back to the active profile.
    """
    profiles.get_storagecluster_ocp().patch(
        params=json.dumps(
            {
                "spec": {
                    "resources": {key: None for key in OVERRIDE_KEYS},
                    "multiCloudGateway": {"endpoints": None},
                }
            }
        ),
        format_type="merge",
    )


@mcg
@red_squad
@runs_on_provider
class TestMCGPerformanceProfileOverrides:
    """
    Verify that explicit resource specifications take precedence over the
    values a performance profile would otherwise apply, and that removing the
    overrides hands control back to the profile.
    """

    @pytest.fixture
    def clear_preexisting_overrides(self, request):
        """
        Snapshot the StorageCluster resource overrides and the MCG endpoints
        section, clear them for the duration of the test, and put them back on
        teardown.

        A pre-existing override would mask the profile baseline the tests
        assert before applying their own.

        Only the three NooBaa keys under spec.resources are touched, because
        that map is shared with the Ceph components and must not be wiped
        wholesale.

        Returns:
            dict: The original spec.resources map
        """
        sc_ocp = profiles.get_storagecluster_ocp()
        spec = sc_ocp.get().get("spec", {})
        original_resources = spec.get("resources") or {}
        original_endpoints = (spec.get("multiCloudGateway") or {}).get("endpoints")
        had_overrides = bool(
            any(original_resources.get(key) for key in OVERRIDE_KEYS)
            or original_endpoints
        )

        def finalizer():
            restored = {
                key: original_resources.get(key) or None for key in OVERRIDE_KEYS
            }
            logger.info(f"Restoring StorageCluster resource overrides: {restored}")
            profiles.get_storagecluster_ocp().patch(
                params=json.dumps({"spec": {"resources": restored}}),
                format_type="merge",
            )
            logger.info(f"Restoring MCG endpoints section: {original_endpoints}")
            profiles.get_storagecluster_ocp().patch(
                params=json.dumps(
                    {"spec": {"multiCloudGateway": {"endpoints": original_endpoints}}}
                ),
                format_type="merge",
            )
            if had_overrides:
                # Putting real overrides back rolls the NooBaa pods once more.
                # This is the last finalizer to run, so wait for that rollout
                # instead of leaving it in flight for the next test module.
                logger.info("Waiting for the restored overrides to settle")
                profiles.verify_noobaa_pods_healthy()

        request.addfinalizer(finalizer)

        if had_overrides:
            logger.info("Clearing pre-existing MCG resource and endpoint overrides")
            _clear_overrides()
        return original_resources

    @pytest.fixture
    def base_profile(self, request, clear_preexisting_overrides):
        """
        Put the cluster on a known profile for the test and restore whatever
        profile it had afterwards.

        Depends on clear_preexisting_overrides so that the profile is applied
        to a cluster with no override left pinning a component's resources -
        otherwise the profile could never settle.

        Returns:
            str: The profile that was set
        """
        original_profile = (
            profiles.get_storagecluster_ocp()
            .get()
            .get("spec", {})
            .get("multiCloudGateway", {})
            .get("performanceProfile")
        )

        def finalizer():
            logger.info(f"Restoring MCG performance profile to '{original_profile}'")
            profiles.set_storagecluster_profile(original_profile)
            profiles.wait_for_profile_settled(original_profile)

        request.addfinalizer(finalizer)

        profiles.apply_profile(BASE_PROFILE)
        return BASE_PROFILE

    @pytest.fixture
    def restore_overrides(self, request, base_profile):
        """
        Drop the overrides the test itself applied.

        Fixture order guarantees LIFO cleanup: this fixture's finalizer runs
        first (removing test overrides), then base_profile's finalizer restores
        the original profile. This order is critical because profile restoration
        cannot settle while an explicit override pins a component's resources.

        After the test-applied overrides are dropped, NooBaa pods will roll
        back to the active profile's resources. The base_profile finalizer
        then waits for that profile to settle before restoring the original
        profile, ensuring clean state between tests.

        The pre-existing overrides (if any) are restored last by
        clear_preexisting_overrides's finalizer.
        """

        def finalizer():
            logger.info("Dropping the resource and endpoint overrides set by the test")
            _clear_overrides()
            # Wait for NooBaa to settle on the active profile after overrides are removed
            profiles.verify_noobaa_pods_healthy()

        request.addfinalizer(finalizer)

    @tier2
    def test_component_resource_overrides(self, restore_overrides, base_profile):
        """
        Verify explicit per-component resources win over the active profile.

        Test Steps:
            1. Set a profile and verify every component follows it
            2. Override noobaa-core resources and verify only noobaa-core
               changed - noobaa-db and noobaa-endpoint still follow the profile
            3. Override noobaa-db resources and verify noobaa-db changed
            4. Override noobaa-endpoint resources and the endpoint
               minCount/maxCount, and verify both the resources and the
               endpoint count/HPA follow the override rather than the profile
            5. Remove all the overrides and verify every component returns to
               the profile values

        Expected Results:
            An explicit resource specification always takes precedence over the
            profile, the override is scoped to the component it names, and
            removing it hands the component back to the profile.
        """
        profile_spec = profiles.PROFILE_SPECS[base_profile]

        logger.info(f"Baseline: every component follows the '{base_profile}' profile")
        profiles.verify_all_components(profile_spec, base_profile, check_pv_pool=False)

        logger.info("Overriding noobaa-core resources")
        profiles.get_storagecluster_ocp().patch(
            params=json.dumps(
                {
                    "spec": {
                        "resources": {
                            profiles.SC_RESOURCE_KEY_CORE: _as_k8s_resources(
                                OVERRIDE_SPEC["core"]
                            )
                        }
                    }
                }
            ),
            format_type="merge",
        )
        profiles.wait_for_pods_with_resources(
            constants.NOOBAA_CORE_POD_LABEL, OVERRIDE_SPEC["core"]
        )
        profiles.verify_core(OVERRIDE_SPEC, "noobaa-core override")
        # The override must not bleed into the components it does not name.
        profiles.verify_db(profile_spec, base_profile)
        profiles.verify_endpoints(profile_spec, base_profile)

        logger.info("Overriding noobaa-db resources")
        profiles.get_storagecluster_ocp().patch(
            params=json.dumps(
                {
                    "spec": {
                        "resources": {
                            profiles.SC_RESOURCE_KEY_DB: _as_k8s_resources(
                                OVERRIDE_SPEC["db"]
                            )
                        }
                    }
                }
            ),
            format_type="merge",
        )
        profiles.verify_db(OVERRIDE_SPEC, "noobaa-db override")
        profiles.verify_core(OVERRIDE_SPEC, "noobaa-core override")

        logger.info("Overriding noobaa-endpoint resources and the endpoint count")
        profiles.get_storagecluster_ocp().patch(
            params=json.dumps(
                {
                    "spec": {
                        "resources": {
                            profiles.SC_RESOURCE_KEY_ENDPOINT: _as_k8s_resources(
                                OVERRIDE_SPEC["endpoint"]
                            )
                        },
                        "multiCloudGateway": {
                            "endpoints": {
                                "minCount": OVERRIDE_SPEC["endpoint_count"]["min"],
                                "maxCount": OVERRIDE_SPEC["endpoint_count"]["max"],
                            }
                        },
                    }
                }
            ),
            format_type="merge",
        )
        profiles.verify_endpoints(OVERRIDE_SPEC, "noobaa-endpoint override")
        profiles.verify_core(OVERRIDE_SPEC, "noobaa-core override")
        profiles.verify_db(OVERRIDE_SPEC, "noobaa-db override")
        profiles.verify_noobaa_pods_healthy()

        logger.info("Removing all overrides, the profile must take over again")
        _clear_overrides()
        profiles.wait_for_profile_settled(base_profile)
        profiles.verify_all_components(profile_spec, base_profile, check_pv_pool=False)
        profiles.verify_noobaa_pods_healthy()

        logger.info(
            "✅ Component resource overrides take precedence over the profile "
            "and removing them restores the profile values"
        )

    @tier2
    def test_pv_pool_volume_resources_override(
        self, restore_overrides, base_profile, backingstore_factory
    ):
        """
        Verify spec.pvPool.resources on a backingstore overrides the profile's
        PV pool agent resources, and that a backingstore without it still gets
        the profile values.

        Both backingstores are created after the profile is set, because the PV
        pool agent resources are stamped into the pod template at pod creation
        time only - an agent pod that predates the profile keeps the values it
        was born with.

        Test Steps:
            1. Set a profile
            2. Create a PV pool backingstore with explicit
               spec.pvPool.resources cpu/memory
            3. Create a PV pool backingstore without any resource override
            4. Verify the first backingstore's agent pods carry the override
               and the second one's carry the profile's pvPoolResources

        Expected Results:
            An explicit spec.pvPool.resources takes precedence over the
            profile's PV pool resources, and it is scoped to the backingstore
            that declares it.
        """
        profile_spec = profiles.PROFILE_SPECS[base_profile]

        overridden_bs = backingstore_factory(
            "OC",
            {
                "pv": [
                    (
                        PV_POOL_NUM_VOLUMES,
                        constants.MIN_PV_BACKINGSTORE_SIZE_IN_GB,
                        constants.DEFAULT_STORAGECLASS_RBD,
                        PV_POOL_OVERRIDE_CPU,
                        PV_POOL_OVERRIDE_MEM,
                        PV_POOL_OVERRIDE_CPU,
                        PV_POOL_OVERRIDE_MEM,
                    )
                ]
            },
        )[0]
        profile_bs = backingstore_factory(
            "OC",
            {
                "pv": [
                    (
                        PV_POOL_NUM_VOLUMES,
                        constants.MIN_PV_BACKINGSTORE_SIZE_IN_GB,
                        constants.DEFAULT_STORAGECLASS_RBD,
                    )
                ]
            },
        )[0]

        logger.info(
            f"Verifying '{overridden_bs.name}' agent pods carry the explicit "
            f"{PV_POOL_OVERRIDE_CPU}/{PV_POOL_OVERRIDE_MEM} override"
        )
        override_spec = {
            "req_cpu": PV_POOL_OVERRIDE_CPU,
            "lim_cpu": PV_POOL_OVERRIDE_CPU,
            "req_mem": PV_POOL_OVERRIDE_MEM,
            "lim_mem": PV_POOL_OVERRIDE_MEM,
        }
        override_agents = profiles.wait_for_pods_with_resources(
            f"pool={overridden_bs.name}",
            override_spec,
            expected_count=PV_POOL_NUM_VOLUMES,
        )
        profiles.verify_pv_pool_agent_resources(
            override_agents,
            PV_POOL_OVERRIDE_CPU,
            PV_POOL_OVERRIDE_MEM,
            "the explicit spec.pvPool.resources override",
        )

        logger.info(
            f"Verifying '{profile_bs.name}' agent pods still carry the "
            f"'{base_profile}' profile PV pool resources"
        )
        pv_pool = profile_spec["pv_pool"]
        profile_agents = profiles.wait_for_pods_with_resources(
            f"pool={profile_bs.name}",
            {
                "req_cpu": pv_pool["cpu"],
                "lim_cpu": pv_pool["cpu"],
                "req_mem": pv_pool["mem"],
                "lim_mem": pv_pool["mem"],
            },
            expected_count=PV_POOL_NUM_VOLUMES,
        )
        profiles.verify_pv_pool_agent_resources(
            profile_agents,
            pv_pool["cpu"],
            pv_pool["mem"],
            f"the '{base_profile}' profile PV pool resources",
        )

        logger.info(
            "✅ spec.pvPool.resources overrides the profile for the "
            "backingstore that declares it and only for that backingstore"
        )
