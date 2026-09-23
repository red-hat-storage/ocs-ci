import logging
import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    tier2,
    tier3,
    red_squad,
    mcg,
    runs_on_provider,
)
from ocs_ci.helpers import mcg_performance_profiles as profiles
from ocs_ci.helpers.helpers import modify_deployment_replica_count
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import get_pods_having_label
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)

OCS_OPERATOR_DEPLOYMENT = "ocs-operator"

# Number of volumes for the user-created PV pool backingstore. Deliberately
# different from the 3 volumes the profile logic gives the default
# backingstore, so a profile leaking into a user-created store is visible.
USER_PV_POOL_NUM_VOLUMES = 4


@mcg
@red_squad
@runs_on_provider
class TestMCGPerformanceProfiles:
    """
    Test MCG Performance Profiles (default, mixed-workload, small-objects).

    Verifies that setting performanceProfile on StorageCluster CR correctly
    propagates resource specifications to noobaa-core, noobaa-db, and
    noobaa-endpoint pods.
    """

    PROFILE_SPECS = profiles.PROFILE_SPECS

    @pytest.fixture
    def restore_profile(self, request):
        """
        Capture the performance profile the cluster had before the test and
        restore it afterwards, so the cluster is always left as it was found
        even if the test fails part-way through a profile switch.
        """
        original_profile = profiles.get_storagecluster_profile()
        logger.info(f"Original MCG performance profile: '{original_profile}'")

        def finalizer():
            logger.info(f"Restoring MCG performance profile to '{original_profile}'")
            profiles.set_storagecluster_profile(original_profile)
            # Wait for the restore to fully reconcile so the cluster is left
            # healthy and the next test's NooBaa health check does not run
            # while NooBaa is still recreating pods.
            profiles.wait_for_profile_settled(original_profile)

        request.addfinalizer(finalizer)
        return original_profile

    @pytest.fixture
    def clear_endpoint_overrides(self, request):
        """
        Remove spec.multiCloudGateway.endpoints for the duration of the test
        and put it back afterwards.

        An explicit endpoints minCount/maxCount takes precedence over the
        profile, so a cluster that ships with one pins the endpoint count and
        makes every profile endpoint-count assertion fail even though the
        profile resolved correctly. Overrides are exercised deliberately in the
        dedicated overrides test module.
        """
        sc_ocp = profiles.get_storagecluster_ocp()
        original_endpoints = (
            sc_ocp.get().get("spec", {}).get("multiCloudGateway", {}).get("endpoints")
        )

        def finalizer():
            if original_endpoints is None:
                return
            logger.info(f"Restoring endpoints override: {original_endpoints}")
            profiles.patch_storagecluster(
                {"spec": {"multiCloudGateway": {"endpoints": original_endpoints}}}
            )

        request.addfinalizer(finalizer)

        if original_endpoints is not None:
            logger.info(
                f"Clearing pre-existing endpoints override {original_endpoints} "
                "so the endpoint count is free to follow the profile"
            )
            profiles.patch_storagecluster(
                {"spec": {"multiCloudGateway": {"endpoints": None}}}
            )
        return original_endpoints

    @pytest.fixture
    def set_profile(self, request, clear_endpoint_overrides, restore_profile):
        """
        Set the MCG performance profile on the StorageCluster CR and wait for
        it to propagate to the NooBaa CR. The restore_profile fixture puts the
        original profile back on teardown, and clear_endpoint_overrides makes
        sure no explicit endpoint count is pinning the profile's own count.

        Returns:
            str: Profile name that was set
        """
        profile = request.param
        profiles.apply_profile(profile)
        return profile

    @pytest.fixture
    def user_pv_pool_backingstore(self, backingstore_factory):
        """
        Create a user-owned PV pool backingstore with an explicit numVolumes
        that differs from the value the profile logic applies to the default
        backingstore.

        Returns:
            str: Name of the created backingstore
        """
        backingstore = backingstore_factory(
            "OC",
            {
                "pv": [
                    (
                        USER_PV_POOL_NUM_VOLUMES,
                        constants.MIN_PV_BACKINGSTORE_SIZE_IN_GB,
                        constants.DEFAULT_STORAGECLASS_RBD,
                    )
                ]
            },
        )[0]
        logger.info(
            f"Created user PV pool backingstore '{backingstore.name}' with "
            f"{USER_PV_POOL_NUM_VOLUMES} volumes"
        )
        return backingstore.name

    def _verify_user_pv_pool_unchanged(self, backingstore_name, context):
        """
        Verify a user-created PV pool backingstore still has the numVolumes it
        was created with, together with the matching number of agent pods and
        PVCs.

        Args:
            backingstore_name (str): BackingStore name
            context (str): Description of the current state, for messages
        """
        backingstore = profiles.get_backingstore(backingstore_name)
        assert backingstore, (
            f"User PV pool backingstore '{backingstore_name}' disappeared "
            f"({context})"
        )

        num_volumes = backingstore["spec"]["pvPool"]["numVolumes"]
        assert num_volumes == USER_PV_POOL_NUM_VOLUMES, (
            f"User backingstore numVolumes changed to {num_volumes} "
            f"({context}), expected {USER_PV_POOL_NUM_VOLUMES} - the profile "
            "must not manage user-created backingstores"
        )

        # A profile switch can reschedule an agent pod (a DB instance growing
        # to 6 CPU evicts it, and the RBD volume needs a moment to detach from
        # the old node), so poll rather than reading a single sample.
        def _all_agents_running():
            return (
                len(profiles.get_pv_pool_agent_pods(backingstore_name))
                == USER_PV_POOL_NUM_VOLUMES
            )

        for running in TimeoutSampler(timeout=600, sleep=15, func=_all_agents_running):
            if running:
                break
            logger.info(
                f"Waiting for {USER_PV_POOL_NUM_VOLUMES} running agent pods of "
                f"'{backingstore_name}' ({context})"
            )

        pvc_ocp = OCP(
            kind=constants.PVC, namespace=config.ENV_DATA["cluster_namespace"]
        )
        pvcs = pvc_ocp.get(selector=f"pool={backingstore_name}").get("items", [])
        assert len(pvcs) == USER_PV_POOL_NUM_VOLUMES, (
            f"User backingstore PVC count is {len(pvcs)} ({context}), expected "
            f"{USER_PV_POOL_NUM_VOLUMES}"
        )
        logger.info(
            f"User PV pool backingstore '{backingstore_name}' still has "
            f"{USER_PV_POOL_NUM_VOLUMES} volumes, agent pods and PVCs "
            f"({context}) ✓"
        )

    @tier2
    @pytest.mark.parametrize(
        "set_profile",
        [
            pytest.param("default", marks=pytest.mark.polarion_id("OCS-8289")),
            pytest.param("mixed-workload", marks=pytest.mark.polarion_id("OCS-8290")),
            pytest.param("small-objects", marks=pytest.mark.polarion_id("OCS-8291")),
        ],
        indirect=True,
    )
    def test_mcg_performance_profile_resources(self, set_profile):
        """
        Verify MCG performance profile resource specifications.

        Test Steps (per profile):
            1. Set spec.multiCloudGateway.performanceProfile on StorageCluster CR
            2. Verify the profile propagated to the NooBaa CR and that
               ocs-operator wrote no hardcoded resource values there
            3. Verify noobaa-core pod resources and QoS class
            4. Verify noobaa-db pod resources, QoS class, and instance count
            5. Verify noobaa-endpoint pod resources, QoS class, and count
            6. Verify PV pool agent pod resources (vSphere/on-prem only)

        Expected Results:
            All resource values match the profile specification from RHSTOR-9144
        """
        profile = set_profile
        spec = self.PROFILE_SPECS[profile]

        logger.info(f"Testing '{profile}' profile resource specifications")

        profiles.verify_profile_propagated(profile)
        profiles.verify_all_components(spec, profile)

        logger.info(f"✅ All verifications passed for '{profile}' profile")

    @tier3
    def test_mcg_profile_switching(
        self, clear_endpoint_overrides, restore_profile, user_pv_pool_backingstore
    ):
        """
        Verify pod CPU/memory and endpoint count update when switching between
        profiles, that a user-created PV pool backingstore is unaffected by the
        switches, and that removing the profile field falls back to default.

        Test Steps:
            1. Create a user PV pool backingstore with a custom numVolumes
            2. Switch to "default" and verify every component matches it
            3. Switch to "mixed-workload" and verify the pods restarted with
               the new CPU/memory and the endpoint count moved to min 2 / max 4
            4. Switch to "small-objects" and verify the new CPU/memory, with
               the endpoint count staying at min 2 / max 4
            5. Delete the performanceProfile field entirely and verify the
               fall back to the default profile values and min 1 / max 2
            6. After every switch, verify no pod is stuck and the user
               backingstore still has its original numVolumes

        Expected Results:
            Pods restart automatically with the correct values for each
            profile, the endpoint count follows the profile, no manual
            intervention is needed, no pod is left stuck, the user-created
            backingstore is never touched, and removing the field falls back to
            the default profile.
        """
        backingstore_name = user_pv_pool_backingstore

        for profile in ("default", "mixed-workload", "small-objects"):
            logger.info(f"Switching MCG performance profile to '{profile}'")
            profiles.apply_profile(profile)
            profiles.verify_profile_propagated(profile)
            profiles.verify_all_components(self.PROFILE_SPECS[profile], profile)
            profiles.verify_noobaa_pods_healthy()
            self._verify_user_pv_pool_unchanged(
                backingstore_name, f"profile '{profile}'"
            )
            logger.info(f"Switch to '{profile}' verified ✓")

        # Removing the field must behave exactly like the default profile.
        # ocs-operator writes "default" onto the NooBaa CR explicitly rather
        # than leaving the field empty.
        logger.info("Removing the performanceProfile field from the StorageCluster CR")
        profiles.apply_profile(None)
        profiles.verify_profile_propagated("default")
        profiles.verify_all_components(self.PROFILE_SPECS["default"], "default")
        profiles.verify_noobaa_pods_healthy()
        self._verify_user_pv_pool_unchanged(backingstore_name, "profile field removed")

        logger.info(
            "✅ Profile switching, fall back to default and user PV pool "
            "isolation all verified"
        )

    @tier2
    @pytest.mark.parametrize(
        "invalid_profile",
        [
            # Not a profile at all
            "high-performance",
            # The enum is case sensitive
            "Default",
            "DEFAULT",
            # Underscore instead of hyphen
            "small_objects",
            # Valid on the NooBaa CR but not on the StorageCluster CR
            "dev-env",
            # The empty string is not in the enum either, so the field cannot
            # be cleared by blanking it - it has to be removed
            "",
        ],
    )
    def test_invalid_profile_value_rejected(self, invalid_profile):
        """
        Verify an invalid performance profile value is rejected by CRD
        validation and does not partially apply.

        Test Steps:
            1. Patch the StorageCluster CR with an invalid profile value
            2. Verify the patch is rejected at admission
            3. Verify the stored profile value is unchanged

        Expected Results:
            Invalid profile values are rejected by CRD validation and the
            StorageCluster CR keeps the value it had.
        """
        profile_before = profiles.get_storagecluster_profile()

        with pytest.raises(CommandFailed) as exc_info:
            profiles.patch_storagecluster(
                {"spec": {"multiCloudGateway": {"performanceProfile": invalid_profile}}}
            )
        message = str(exc_info.value)
        assert "performanceProfile" in message and "Unsupported value" in message, (
            f"Patch with '{invalid_profile}' failed, but not with the expected "
            f"CRD enum validation error: {message}"
        )
        logger.info(f"Profile '{invalid_profile}' rejected: {message}")

        profile_after = profiles.get_storagecluster_profile()
        assert profile_after == profile_before, (
            f"A rejected patch changed the stored profile from "
            f"'{profile_before}' to '{profile_after}'"
        )

    @tier2
    def test_invalid_profile_value_rejected_on_noobaa_cr(self):
        """
        Verify the NooBaa CR enforces its own performanceProfile enum, which is
        wider than the StorageCluster one - it also accepts dev-env and
        mini-env.

        Test Steps:
            1. Patch the NooBaa CR with an invalid profile value
            2. Verify the patch is rejected at admission
            3. Verify the stored profile value is unchanged

        Expected Results:
            Invalid profile values are rejected by CRD validation on the NooBaa
            CR as well.
        """
        profile_before = profiles.get_noobaa_profile()
        noobaa_ocp = profiles.get_noobaa_ocp()

        with pytest.raises(CommandFailed) as exc_info:
            noobaa_ocp.patch(
                params='{"spec":{"performanceProfile":"high-performance"}}',
                format_type="merge",
            )
        message = str(exc_info.value)
        assert "performanceProfile" in message and "Unsupported value" in message, (
            "NooBaa CR patch failed, but not with the expected CRD enum "
            f"validation error: {message}"
        )
        # The NooBaa CRD enum is wider than the StorageCluster one.
        for noobaa_only_profile in profiles.NOOBAA_ONLY_PROFILES:
            assert noobaa_only_profile in message, (
                f"'{noobaa_only_profile}' is missing from the NooBaa CRD enum "
                f"reported in the validation error: {message}"
            )

        assert (
            profiles.get_noobaa_profile() == profile_before
        ), "A rejected patch changed the profile stored on the NooBaa CR"

    @pytest.fixture
    def ocs_operator_replicas(self, request):
        """
        Provide a callable that scales the ocs-operator deployment, and always
        restore the original replica count on teardown.

        Scaling ocs-operator down stops StorageCluster reconciliation for the
        whole cluster, so the restore must happen even when the test fails
        mid-way.

        Returns:
            callable: Takes a replica count and scales ocs-operator to it
        """
        deployment_ocp = OCP(
            kind=constants.DEPLOYMENT,
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name=OCS_OPERATOR_DEPLOYMENT,
        )
        original_replicas = deployment_ocp.get()["spec"]["replicas"]

        def _scale(replicas):
            logger.info(f"Scaling {OCS_OPERATOR_DEPLOYMENT} to {replicas} replicas")
            modify_deployment_replica_count(OCS_OPERATOR_DEPLOYMENT, replicas)

            def _at_replicas():
                pods = get_pods_having_label(
                    label=constants.OCS_OPERATOR_LABEL,
                    namespace=config.ENV_DATA["cluster_namespace"],
                    statuses=[constants.STATUS_RUNNING],
                )
                return len(pods) == replicas

            for scaled in TimeoutSampler(timeout=300, sleep=10, func=_at_replicas):
                if scaled:
                    break
                logger.info(
                    f"Waiting for {OCS_OPERATOR_DEPLOYMENT} to reach "
                    f"{replicas} running pods"
                )

        def finalizer():
            logger.info(
                f"Restoring {OCS_OPERATOR_DEPLOYMENT} to {original_replicas} replicas"
            )
            _scale(original_replicas)

        request.addfinalizer(finalizer)
        return _scale

    @tier3
    def test_profile_set_directly_on_noobaa_cr(
        self, restore_profile, ocs_operator_replicas
    ):
        """
        Verify how a profile set directly on the NooBaa CR behaves, both while
        ocs-operator is running and while it is scaled down.

        Test Steps:
            1. Set "mixed-workload" on the StorageCluster CR and let it settle
            2. Verify the profile propagated to the NooBaa CR and that
               ocs-operator wrote no hardcoded resource values there
            3. With ocs-operator running, set a different profile directly on
               the NooBaa CR and verify it is reconciled back to the
               StorageCluster value - the StorageCluster stays the single
               source of truth for a managed NooBaa CR
            4. Scale ocs-operator down, set "small-objects" directly on the
               NooBaa CR, and verify the NooBaa operator resolves the profile
               resources on its own
            5. Scale ocs-operator back up (fixture teardown)

        Expected Results:
            The profile propagates from StorageCluster to NooBaa CR, no
            hardcoded resources are written while a profile is active, a direct
            edit is reverted while ocs-operator runs, and the NooBaa operator
            resolves profile resources regardless of how the profile was set.
        """
        sc_profile = "mixed-workload"
        direct_profile = "small-objects"

        profiles.apply_profile(sc_profile)
        profiles.verify_profile_propagated(sc_profile)

        # ocs-operator reconciles a direct edit away within seconds.
        logger.info(
            f"Setting '{direct_profile}' directly on the NooBaa CR while "
            "ocs-operator is running, it is expected to be reverted"
        )
        noobaa_ocp = profiles.get_noobaa_ocp()
        noobaa_ocp.patch(
            params=f'{{"spec":{{"performanceProfile":"{direct_profile}"}}}}',
            format_type="merge",
        )

        def _reverted():
            return profiles.get_noobaa_profile() == sc_profile

        for reverted in TimeoutSampler(timeout=180, sleep=10, func=_reverted):
            if reverted:
                break
            logger.info(
                "Waiting for ocs-operator to reconcile the NooBaa CR profile "
                f"back to '{sc_profile}'"
            )
        logger.info(
            f"Direct NooBaa CR edit reverted back to '{sc_profile}' by "
            "ocs-operator ✓"
        )

        # With ocs-operator out of the way the direct edit sticks, and the
        # NooBaa operator has to resolve the profile resources by itself.
        ocs_operator_replicas(0)
        logger.info(f"Setting '{direct_profile}' directly on the NooBaa CR")
        assert noobaa_ocp.patch(
            params=f'{{"spec":{{"performanceProfile":"{direct_profile}"}}}}',
            format_type="merge",
        ), f"Failed to patch the NooBaa CR with profile '{direct_profile}'"

        profiles.wait_for_profile_settled(direct_profile)
        assert profiles.get_noobaa_profile() == direct_profile, (
            "NooBaa CR profile did not stay at the directly set value while "
            "ocs-operator was scaled down"
        )
        spec = self.PROFILE_SPECS[direct_profile]
        profiles.verify_core(spec, direct_profile)
        profiles.verify_db(spec, direct_profile)
        profiles.verify_endpoints(spec, direct_profile)

        logger.info(
            f"✅ NooBaa operator resolved '{direct_profile}' resources from a "
            "profile set directly on the NooBaa CR"
        )
