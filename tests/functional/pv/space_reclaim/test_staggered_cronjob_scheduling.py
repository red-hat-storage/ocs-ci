"""
Tests for staggered CronJob scheduling in csi-addons.

Verifies that the csi-addons controller applies a deterministic, UID-based
offset to ReclaimSpaceCronJob execution times to prevent thundering herd.

Upstream PR: https://github.com/csi-addons/kubernetes-csi-addons/pull/949
"""

import logging
import time
from datetime import datetime, timezone

import pytest

from ocs_ci.framework.pytest_customization.marks import (
    green_squad,
    jira,
    skipif_ocs_version,
)
from ocs_ci.framework.testlib import ManageTest, tier2, tier3
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.csi_addons import (
    get_csi_addons_config_value,
    remove_csi_addons_config_key,
    restart_csi_addons_controller,
    update_csi_addons_config,
)
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)

# --- Test-local constants (no magic numbers) ---
CRONJOB_STAGGER_WINDOW_KEY = "cronjob-stagger-window"
CRONJOB_STAGGER_WINDOW_DEFAULT = "2"
SCHEDULE_PRECEDENCE_KEY = "schedule-precedence"
SCHEDULE_PRECEDENCE_STORAGECLASS = "storageclass"
NUM_PVCS = 3
PVC_SIZE_GIB = 5
SHORT_SCHEDULE = "*/3 * * * *"
CRONJOB_CREATION_TIMEOUT = 180
JOB_CREATION_TIMEOUT = 420
MIN_STAGGER_SPREAD_SECONDS = 5
CUSTOM_STAGGER_WINDOW_HOURS = "1"
INVALID_STAGGER_WINDOW = "abc"
NUM_PVCS_LARGE = 10
SECOND_ROUND_TIMEOUT = 720
OFFSET_TOLERANCE_SECONDS = 10
JOB_IMMEDIATE_THRESHOLD_SECONDS = 120
SUSPEND_WAIT_SECONDS = 210
LARGE_SCALE_JOB_TIMEOUT = 600


@green_squad
@skipif_ocs_version("<4.22")
class TestStaggeredCronjobScheduling(ManageTest):
    """
    Test staggered CronJob scheduling for ReclaimSpace operations.
    """

    @pytest.fixture(autouse=True)
    def setup_and_teardown(self, request):
        """
        Record initial csi-addons-config values, set schedule-precedence
        to 'storageclass' so the controller reads StorageClass annotations,
        and restore original values after the test completes.
        """
        self.original_stagger_window = get_csi_addons_config_value(
            CRONJOB_STAGGER_WINDOW_KEY, default=CRONJOB_STAGGER_WINDOW_DEFAULT
        )
        self.original_precedence = get_csi_addons_config_value(
            SCHEDULE_PRECEDENCE_KEY, default=""
        )
        logger.info(
            "Recorded original values: stagger_window=%s, schedule_precedence=%s",
            self.original_stagger_window,
            self.original_precedence or "(not set)",
        )

        update_csi_addons_config(
            SCHEDULE_PRECEDENCE_KEY, SCHEDULE_PRECEDENCE_STORAGECLASS
        )

        def finalizer():
            logger.info("Restoring stagger window to: %s", self.original_stagger_window)
            update_csi_addons_config(
                CRONJOB_STAGGER_WINDOW_KEY, self.original_stagger_window
            )
            if self.original_precedence:
                logger.info(
                    "Restoring schedule-precedence to: %s", self.original_precedence
                )
                update_csi_addons_config(
                    SCHEDULE_PRECEDENCE_KEY, self.original_precedence
                )
            else:
                logger.info(
                    "Removing schedule-precedence key (was not set before test)"
                )
                remove_csi_addons_config_key(SCHEDULE_PRECEDENCE_KEY)

        request.addfinalizer(finalizer)

    @tier2
    @pytest.mark.polarion_id("OCS-7800")
    def test_staggered_cronjob_scheduling_happy_path(
        self,
        storageclass_factory,
        multi_pvc_factory,
        pod_factory,
    ):
        """
        Verify staggered CronJob scheduling for ReclaimSpace operations.

        Steps:
            1. Verify the default stagger window (2h) in the ConfigMap.
            2. Create a StorageClass with a short-interval ReclaimSpace schedule.
            3. Create RBD PVCs and attach pods.
            4. Wait for ReclaimSpaceCronJob creation for each PVC.
            5. Verify spec.schedule is unchanged (stagger is controller-internal).
            6. Verify CronJobs have distinct UIDs for unique stagger offsets.
            7. Wait for ReclaimSpaceJobs and compare creation timestamps.
            8. Disable stagger (window=0) and verify CronJobs survive.

        """
        # Step 1: Verify default stagger window
        logger.info("Step 1: Verifying default stagger window value")
        current_window = get_csi_addons_config_value(
            CRONJOB_STAGGER_WINDOW_KEY, default=CRONJOB_STAGGER_WINDOW_DEFAULT
        )
        assert current_window == CRONJOB_STAGGER_WINDOW_DEFAULT, (
            f"Expected default stagger window '{CRONJOB_STAGGER_WINDOW_DEFAULT}', "
            f"got '{current_window}'"
        )

        # Step 2: Create StorageClass with short-interval schedule
        logger.info("Step 2: Creating StorageClass with schedule: %s", SHORT_SCHEDULE)
        sc_obj = storageclass_factory(
            interface=constants.CEPHBLOCKPOOL,
            annotations={
                constants.RECLAIMSPACE_SCHEDULE_ANNOTATION: SHORT_SCHEDULE,
            },
        )

        # Step 3: Create PVCs and pods
        logger.info("Step 3: Creating %d RBD PVCs (size=%dGiB)", NUM_PVCS, PVC_SIZE_GIB)
        pvc_objs = multi_pvc_factory(
            size=PVC_SIZE_GIB,
            num_of_pvc=NUM_PVCS,
            storageclass=sc_obj,
            access_modes=[f"{constants.ACCESS_MODE_RWO}-Block"],
            wait_each=True,
        )

        logger.info("Step 3: Creating pods for PVCs")
        for pvc_obj in pvc_objs:
            pod_factory(
                interface=constants.CEPHBLOCKPOOL,
                pvc=pvc_obj,
                status=constants.STATUS_RUNNING,
                raw_block_pv=True,
            )

        # Step 4: Wait for ReclaimSpaceCronJob creation per PVC
        logger.info("Step 4: Waiting for ReclaimSpaceCronJob creation")
        cronjob_map = {}
        for pvc_obj in pvc_objs:
            cronjob_obj = self._wait_for_cronjob(pvc_obj)
            cronjob_map[pvc_obj.name] = cronjob_obj

        # Steps 5-6: Verify spec.schedule unchanged and collect unique UIDs
        logger.info("Steps 5-6: Verifying schedule and collecting CronJob UIDs")
        uids = set()
        for pvc_name, cj_obj in cronjob_map.items():
            cj_data = cj_obj.get()
            actual_schedule = cj_data["spec"]["schedule"]
            assert actual_schedule == SHORT_SCHEDULE, (
                f"CronJob for PVC '{pvc_name}' has schedule '{actual_schedule}', "
                f"expected '{SHORT_SCHEDULE}'"
            )
            uid = cj_data["metadata"]["uid"]
            logger.info(
                "CronJob for PVC '%s': schedule=%s, UID=%s",
                pvc_name,
                actual_schedule,
                uid,
            )
            uids.add(uid)
        assert (
            len(uids) == NUM_PVCS
        ), f"Expected {NUM_PVCS} unique CronJob UIDs, got {len(uids)}: {uids}"

        # Step 7: Wait for ReclaimSpaceJobs and compare timestamps
        logger.info("Step 7: Waiting for ReclaimSpaceJobs to appear")
        pvc_namespace = pvc_objs[0].namespace
        cronjob_names = [cj_obj.resource_name for cj_obj in cronjob_map.values()]

        timestamps = self._collect_job_timestamps(pvc_namespace, cronjob_names)
        self._assert_stagger_spread(timestamps)

        # Step 8: Disable stagger and verify CronJobs survive
        logger.info("Step 8: Disabling stagger (window=0)")
        update_csi_addons_config(CRONJOB_STAGGER_WINDOW_KEY, "0")

        readback = get_csi_addons_config_value(
            CRONJOB_STAGGER_WINDOW_KEY, default=CRONJOB_STAGGER_WINDOW_DEFAULT
        )
        assert (
            readback == "0"
        ), f"Expected stagger window '0' after disable, got '{readback}'"

        logger.info("Step 8: Verifying CronJobs survive stagger disable")
        for pvc_name, cj_obj in cronjob_map.items():
            cj_data = cj_obj.get()
            assert (
                cj_data["spec"]["schedule"] == SHORT_SCHEDULE
            ), f"CronJob for PVC '{pvc_name}' schedule changed after stagger disable"
        logger.info("All CronJobs intact after stagger disable")

    def _wait_for_cronjob(self, pvc_obj) -> OCP:
        """
        Wait for a ReclaimSpaceCronJob to be created for the given PVC.

        Uses the deterministic naming convention: {pvc-name}-reclaimspace.
        ODF 4.22+ no longer annotates PVCs with the CronJob name.

        Args:
            pvc_obj: PVC object to look up the CronJob for.

        Returns:
            OCP: The CronJob OCP object.

        Raises:
            TimeoutExpiredError: If the CronJob is not created within timeout.

        """
        expected_name = f"{pvc_obj.name}-reclaimspace"
        cronjob_ocp = OCP(
            kind=constants.RECLAIMSPACECRONJOB,
            namespace=pvc_obj.namespace,
            resource_name=expected_name,
        )

        found = cronjob_ocp.check_resource_existence(
            should_exist=True, timeout=CRONJOB_CREATION_TIMEOUT
        )
        if not found:
            raise TimeoutExpiredError(
                f"CronJob '{expected_name}' not created within "
                f"{CRONJOB_CREATION_TIMEOUT}s"
            )
        logger.info(
            "CronJob '%s' found for PVC '%s'",
            expected_name,
            pvc_obj.name,
        )
        return cronjob_ocp

    def _get_earliest_job_timestamps(
        self, ocp_jobs: OCP, cronjob_names: list[str]
    ) -> dict[str, datetime] | None:
        """
        Check whether at least one ReclaimSpaceJob exists per CronJob and
        return their earliest creation timestamps.

        Args:
            ocp_jobs (OCP): OCP client for ReclaimSpaceJobs in the target namespace.
            cronjob_names (list[str]): CronJob names to match Jobs against.

        Returns:
            dict[str, datetime] | None: Timestamps if all CronJobs have Jobs, else None.

        """
        all_jobs = ocp_jobs.get().get("items", [])
        timestamps = {}
        for cj_name in cronjob_names:
            prefix = f"{cj_name}-"
            matching = [j for j in all_jobs if j["metadata"]["name"].startswith(prefix)]
            if not matching:
                return None
            earliest = min(matching, key=lambda j: j["metadata"]["creationTimestamp"])
            ts_str = earliest["metadata"]["creationTimestamp"]
            timestamps[cj_name] = datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%SZ")
        return timestamps

    def _collect_job_timestamps(
        self,
        namespace: str,
        cronjob_names: list[str],
        timeout: int = JOB_CREATION_TIMEOUT,
    ) -> dict[str, datetime]:
        """
        Wait for at least one ReclaimSpaceJob per CronJob and return their
        earliest creation timestamps.

        Args:
            namespace (str): Namespace where Jobs are created.
            cronjob_names (list[str]): CronJob names to match Jobs against.
            timeout (int): Maximum wait time in seconds.

        Returns:
            dict[str, datetime]: Mapping of CronJob name to earliest Job creation datetime.

        Raises:
            TimeoutExpiredError: If Jobs don't appear for all CronJobs.

        """
        ocp_jobs = OCP(kind=constants.RECLAIMSPACEJOBS, namespace=namespace)

        for timestamps in TimeoutSampler(
            timeout=timeout,
            sleep=15,
            func=self._get_earliest_job_timestamps,
            ocp_jobs=ocp_jobs,
            cronjob_names=cronjob_names,
        ):
            if timestamps:
                for cj_name, ts in timestamps.items():
                    logger.info("Job timestamp for CronJob '%s': %s", cj_name, ts)
                return timestamps
        raise TimeoutExpiredError(f"Jobs not found for all CronJobs within {timeout}s")

    def _assert_stagger_spread(self, timestamps: dict) -> None:
        """
        Assert that Job creation timestamps are NOT all clustered within
        MIN_STAGGER_SPREAD_SECONDS of each other, confirming stagger is active.

        Args:
            timestamps (dict): Mapping of CronJob name to creation datetime.

        Raises:
            AssertionError: If all timestamps are within the spread threshold.

        """
        ts_values = list(timestamps.values())
        assert (
            len(ts_values) >= 2
        ), f"Need at least 2 timestamps to measure spread, got {len(ts_values)}"
        max_spread = max(
            abs((a - b).total_seconds())
            for i, a in enumerate(ts_values)
            for b in ts_values[i + 1 :]
        )
        logger.info("Maximum timestamp spread: %.1f seconds", max_spread)
        logger.info("Timestamps: %s", timestamps)
        assert max_spread > MIN_STAGGER_SPREAD_SECONDS, (
            f"All Job timestamps within {MIN_STAGGER_SPREAD_SECONDS}s of each other "
            f"(spread={max_spread:.1f}s). Stagger may not be active. "
            f"Timestamps: {timestamps}"
        )

    def _create_pvcs_and_wait_for_cronjobs(
        self,
        storageclass_factory,
        multi_pvc_factory,
        pod_factory,
        num_pvcs: int = NUM_PVCS,
        schedule: str = SHORT_SCHEDULE,
    ) -> tuple[list, dict[str, OCP]]:
        """
        Create StorageClass with ReclaimSpace schedule, PVCs, pods, and
        wait for ReclaimSpaceCronJob creation for each PVC.

        Args:
            storageclass_factory: Fixture for creating StorageClasses.
            multi_pvc_factory: Fixture for creating multiple PVCs.
            pod_factory: Fixture for creating pods.
            num_pvcs (int): Number of PVCs to create.
            schedule (str): Cron schedule for ReclaimSpace.

        Returns:
            tuple[list, dict[str, OCP]]: (pvc_objs, cronjob_map) where
                cronjob_map maps PVC name to CronJob OCP object.

        """
        logger.info("Creating StorageClass with schedule: %s", schedule)
        sc_obj = storageclass_factory(
            interface=constants.CEPHBLOCKPOOL,
            annotations={
                constants.RECLAIMSPACE_SCHEDULE_ANNOTATION: schedule,
            },
        )

        logger.info("Creating %d RBD PVCs (size=%dGiB)", num_pvcs, PVC_SIZE_GIB)
        pvc_objs = multi_pvc_factory(
            size=PVC_SIZE_GIB,
            num_of_pvc=num_pvcs,
            storageclass=sc_obj,
            access_modes=[f"{constants.ACCESS_MODE_RWO}-Block"],
            wait_each=True,
        )

        logger.info("Creating pods for %d PVCs", num_pvcs)
        for pvc_obj in pvc_objs:
            pod_factory(
                interface=constants.CEPHBLOCKPOOL,
                pvc=pvc_obj,
                status=constants.STATUS_RUNNING,
                raw_block_pv=True,
            )

        logger.info("Waiting for ReclaimSpaceCronJob creation")
        cronjob_map = {}
        for pvc_obj in pvc_objs:
            cronjob_obj = self._wait_for_cronjob(pvc_obj)
            cronjob_map[pvc_obj.name] = cronjob_obj

        return pvc_objs, cronjob_map

    def _get_nth_job_timestamps(
        self,
        ocp_jobs: OCP,
        cronjob_names: list[str],
        round_number: int,
    ) -> dict[str, datetime] | None:
        """
        Check whether each CronJob has at least *round_number* Jobs and
        return the Nth Job's creation timestamp.

        Args:
            ocp_jobs (OCP): OCP client for ReclaimSpaceJobs.
            cronjob_names (list[str]): CronJob names to match against.
            round_number (int): Which round to collect (1-based).

        Returns:
            dict[str, datetime] | None: Timestamps if all have enough Jobs, else None.

        """
        all_jobs = ocp_jobs.get().get("items", [])
        timestamps = {}
        for cj_name in cronjob_names:
            prefix = f"{cj_name}-"
            matching = sorted(
                [j for j in all_jobs if j["metadata"]["name"].startswith(prefix)],
                key=lambda j: j["metadata"]["creationTimestamp"],
            )
            if len(matching) < round_number:
                return None
            ts_str = matching[round_number - 1]["metadata"]["creationTimestamp"]
            timestamps[cj_name] = datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%SZ")
        return timestamps

    def _collect_nth_round_job_timestamps(
        self,
        namespace: str,
        cronjob_names: list[str],
        round_number: int,
    ) -> dict[str, datetime]:
        """
        Wait for the Nth round of ReclaimSpaceJobs per CronJob and return
        their creation timestamps.

        Args:
            namespace (str): Namespace where Jobs are created.
            cronjob_names (list[str]): CronJob names to match against.
            round_number (int): Which round to collect (1-based).

        Returns:
            dict[str, datetime]: Mapping of CronJob name to Nth Job timestamp.

        Raises:
            TimeoutExpiredError: If not all CronJobs have enough Jobs in time.

        """
        ocp_jobs = OCP(kind=constants.RECLAIMSPACEJOBS, namespace=namespace)

        for timestamps in TimeoutSampler(
            timeout=SECOND_ROUND_TIMEOUT,
            sleep=15,
            func=self._get_nth_job_timestamps,
            ocp_jobs=ocp_jobs,
            cronjob_names=cronjob_names,
            round_number=round_number,
        ):
            if timestamps:
                for cj_name, ts in timestamps.items():
                    logger.info(
                        "Round %d timestamp for CronJob '%s': %s",
                        round_number,
                        cj_name,
                        ts,
                    )
                return timestamps
        raise TimeoutExpiredError(
            f"Round {round_number} Jobs not found for all CronJobs "
            f"within {SECOND_ROUND_TIMEOUT}s"
        )

    def _calculate_stagger_offsets(
        self,
        timestamps: dict[str, datetime],
        interval_minutes: int,
    ) -> dict[str, float]:
        """
        Calculate the stagger offset for each CronJob by computing how far
        its Job creation time is from the nearest schedule boundary.

        Uses modular arithmetic: offset = seconds_since_epoch % interval.

        Args:
            timestamps (dict[str, datetime]): CronJob name to Job creation time.
            interval_minutes (int): Schedule interval in minutes.

        Returns:
            dict[str, float]: CronJob name to offset in seconds within the interval.

        """
        interval_seconds = interval_minutes * 60
        offsets = {}
        for cj_name, ts in timestamps.items():
            epoch_seconds = int(ts.timestamp())
            offset = epoch_seconds % interval_seconds
            offsets[cj_name] = float(offset)
            logger.info(
                "CronJob '%s': timestamp=%s, offset=%.1fs (within %ds interval)",
                cj_name,
                ts,
                offset,
                interval_seconds,
            )
        return offsets

    def _assert_offsets_match(
        self,
        offsets_round1: dict[str, float],
        offsets_round2: dict[str, float],
        interval_seconds: int,
        tolerance: float = OFFSET_TOLERANCE_SECONDS,
    ) -> None:
        """
        Assert that stagger offsets from two rounds match within tolerance
        for each CronJob, using circular distance to handle wrap-around
        at the interval boundary.

        Args:
            offsets_round1 (dict[str, float]): Offsets from first round.
            offsets_round2 (dict[str, float]): Offsets from second round.
            interval_seconds (int): Schedule interval in seconds for
                circular distance calculation.
            tolerance (float): Maximum allowed difference in seconds.

        Raises:
            AssertionError: If any CronJob's offset differs beyond tolerance,
                or if the two rounds have different CronJob sets.

        """
        assert offsets_round1.keys() == offsets_round2.keys(), (
            f"CronJob sets differ between rounds: "
            f"round1={set(offsets_round1.keys())}, "
            f"round2={set(offsets_round2.keys())}"
        )
        for cj_name in offsets_round1:
            o1 = offsets_round1[cj_name]
            o2 = offsets_round2[cj_name]
            linear_diff = abs(o1 - o2)
            diff = min(linear_diff, interval_seconds - linear_diff)
            logger.info(
                "CronJob '%s': round1_offset=%.1fs, round2_offset=%.1fs, "
                "diff=%.1fs (circular)",
                cj_name,
                o1,
                o2,
                diff,
            )
            assert diff <= tolerance, (
                f"CronJob '{cj_name}' offset changed between rounds: "
                f"{o1:.1f}s vs {o2:.1f}s (diff={diff:.1f}s, tolerance={tolerance}s)"
            )

    def _assert_stagger_within_window(
        self,
        timestamps: dict[str, datetime],
        window_seconds: float,
    ) -> None:
        """
        Assert that the maximum spread between any two Job timestamps
        does not exceed the given stagger window.

        Args:
            timestamps (dict[str, datetime]): CronJob name to creation time.
            window_seconds (float): Maximum allowed spread in seconds.

        Raises:
            AssertionError: If max spread exceeds the window.

        """
        ts_values = list(timestamps.values())
        assert (
            len(ts_values) >= 2
        ), f"Need at least 2 timestamps to measure spread, got {len(ts_values)}"
        max_spread = max(
            abs((a - b).total_seconds())
            for i, a in enumerate(ts_values)
            for b in ts_values[i + 1 :]
        )
        logger.info(
            "Stagger spread: %.1fs (window limit: %.1fs)",
            max_spread,
            window_seconds,
        )
        assert max_spread <= window_seconds, (
            f"Job timestamps spread ({max_spread:.1f}s) exceeds stagger window "
            f"({window_seconds:.1f}s). Timestamps: {timestamps}"
        )

    def suspend_cronjob(self, cronjob_ocp: OCP) -> None:
        """
        Suspend a ReclaimSpaceCronJob by patching spec.suspend=true.

        Uses direct CronJob patching instead of the PVC-annotation-based
        helper, which is not available in ODF 4.22+.

        Args:
            cronjob_ocp (OCP): The CronJob OCP object to suspend.

        """
        patch = '[{"op": "add", "path": "/spec/suspend", "value": true}]'
        cronjob_ocp.patch(params=patch, format_type="json")
        logger.info("Suspended CronJob '%s'", cronjob_ocp.resource_name)

    def resume_cronjob(self, cronjob_ocp: OCP) -> None:
        """
        Resume a suspended ReclaimSpaceCronJob by patching spec.suspend=false.

        Args:
            cronjob_ocp (OCP): The CronJob OCP object to resume.

        """
        patch = '[{"op": "replace", "path": "/spec/suspend", "value": false}]'
        cronjob_ocp.patch(params=patch, format_type="json")
        logger.info("Resumed CronJob '%s'", cronjob_ocp.resource_name)

    @tier2
    @pytest.mark.polarion_id("OCS-7982")
    @jira("DFBUGS-6991")
    def test_invalid_stagger_window_blocks_cronjob_creation(
        self,
        storageclass_factory,
        multi_pvc_factory,
        pod_factory,
    ):
        """
        Verify controller behavior when cronjob-stagger-window contains
        an invalid (non-numeric) value.

        Known bug (DFBUGS-6991): The controller uses strconv.Atoi to
        parse the window value. When parsing fails, the error blocks
        CronJob reconciliation entirely instead of falling back to the
        default window (2h). Once the bug is fixed, this test should be
        updated to assert that CronJobs ARE created with default fallback.

        Steps:
            1. Set cronjob-stagger-window to a non-numeric value.
            2. Create PVCs with a short-interval schedule.
            3. Verify CronJobs are NOT created (blocked by parse error).

        """
        logger.info(
            "Setting invalid stagger window: %s",
            INVALID_STAGGER_WINDOW,
        )
        update_csi_addons_config(
            CRONJOB_STAGGER_WINDOW_KEY,
            INVALID_STAGGER_WINDOW,
        )

        readback = get_csi_addons_config_value(
            CRONJOB_STAGGER_WINDOW_KEY,
            default=CRONJOB_STAGGER_WINDOW_DEFAULT,
        )
        assert (
            readback == INVALID_STAGGER_WINDOW
        ), f"ConfigMap should contain '{INVALID_STAGGER_WINDOW}', got '{readback}'"

        logger.info("Creating StorageClass with schedule: %s", SHORT_SCHEDULE)
        sc_obj = storageclass_factory(
            interface=constants.CEPHBLOCKPOOL,
            annotations={
                constants.RECLAIMSPACE_SCHEDULE_ANNOTATION: SHORT_SCHEDULE,
            },
        )

        logger.info("Creating %d RBD PVCs (size=%dGiB)", NUM_PVCS, PVC_SIZE_GIB)
        pvc_objs = multi_pvc_factory(
            size=PVC_SIZE_GIB,
            num_of_pvc=NUM_PVCS,
            storageclass=sc_obj,
            access_modes=[f"{constants.ACCESS_MODE_RWO}-Block"],
            wait_each=True,
        )

        logger.info("Creating pods for PVCs")
        for pvc_obj in pvc_objs:
            pod_factory(
                interface=constants.CEPHBLOCKPOOL,
                pvc=pvc_obj,
                status=constants.STATUS_RUNNING,
                raw_block_pv=True,
            )

        expected_name = f"{pvc_objs[0].name}-reclaimspace"
        cronjob_ocp = OCP(
            kind=constants.RECLAIMSPACECRONJOB,
            namespace=pvc_objs[0].namespace,
            resource_name=expected_name,
        )
        found = cronjob_ocp.check_resource_existence(
            should_exist=True, timeout=CRONJOB_CREATION_TIMEOUT
        )
        assert not found, (
            f"CronJob '{expected_name}' was created despite invalid "
            f"stagger window '{INVALID_STAGGER_WINDOW}'. If the controller "
            f"now falls back to default, DFBUGS-6991 is fixed — update "
            f"this test to verify fallback behavior."
        )

    @tier2
    @pytest.mark.polarion_id("OCS-7983")
    def test_configmap_update_requires_controller_restart(
        self,
        storageclass_factory,
        multi_pvc_factory,
        pod_factory,
    ):
        """
        Verify that ConfigMap changes only take effect after the controller
        is restarted — the controller caches values at startup.

        Steps:
            1. Create PVCs and wait for staggered Jobs (round 1).
            2. Patch ConfigMap window=0 WITHOUT restarting the controller.
            3. Wait for round 2 Jobs — stagger should STILL be active.
            4. Restart the controller.
            5. Wait for round 3 Jobs — stagger should now be DISABLED.

        """
        pvc_objs, cronjob_map = self._create_pvcs_and_wait_for_cronjobs(
            storageclass_factory,
            multi_pvc_factory,
            pod_factory,
        )
        pvc_namespace = pvc_objs[0].namespace
        cronjob_names = [cj.resource_name for cj in cronjob_map.values()]

        logger.info("Waiting for round 1 Jobs (stagger active)")
        ts_round1 = self._collect_job_timestamps(pvc_namespace, cronjob_names)
        self._assert_stagger_spread(ts_round1)

        logger.info("Patching ConfigMap window=0 WITHOUT controller restart")
        update_csi_addons_config(
            CRONJOB_STAGGER_WINDOW_KEY,
            "0",
            restart=False,
        )
        readback = get_csi_addons_config_value(
            CRONJOB_STAGGER_WINDOW_KEY,
            default=CRONJOB_STAGGER_WINDOW_DEFAULT,
        )
        assert (
            readback == "0"
        ), f"ConfigMap should read '0' after patch, got '{readback}'"

        logger.info("Waiting for round 2 Jobs (controller NOT restarted)")
        ts_round2 = self._collect_nth_round_job_timestamps(
            pvc_namespace,
            cronjob_names,
            round_number=2,
        )
        self._assert_stagger_spread(ts_round2)
        logger.info("Round 2 still shows stagger — old value cached")

        logger.info("Restarting controller to pick up window=0")
        restart_csi_addons_controller()

        logger.info("Waiting for round 3 Jobs (stagger should be disabled)")
        ts_round3 = self._collect_nth_round_job_timestamps(
            pvc_namespace,
            cronjob_names,
            round_number=3,
        )
        ts_values = list(ts_round3.values())
        max_spread = max(
            abs((a - b).total_seconds())
            for i, a in enumerate(ts_values)
            for b in ts_values[i + 1 :]
        )
        logger.info(
            "Round 3 spread: %.1fs (expecting < %ds)",
            max_spread,
            MIN_STAGGER_SPREAD_SECONDS,
        )
        assert max_spread < MIN_STAGGER_SPREAD_SECONDS, (
            f"After restart with window=0, Jobs should fire together but "
            f"spread is {max_spread:.1f}s. Timestamps: {ts_round3}"
        )

    @tier2
    @pytest.mark.polarion_id("OCS-7984")
    def test_short_interval_shrinks_stagger_window(
        self,
        storageclass_factory,
        multi_pvc_factory,
        pod_factory,
    ):
        """
        Verify that when the schedule interval is shorter than the configured
        stagger window, the effective window shrinks to match the interval.

        With */3 schedule and default 2h window: min(3min, 2h) = 3min.
        Jobs must be staggered but within a 3-minute window.

        Steps:
            1. Ensure default stagger window (2h).
            2. Create PVCs with */3 schedule.
            3. Wait for Jobs and verify stagger is active.
            4. Verify all timestamps fall within 3 minutes of each other.

        """
        pvc_objs, cronjob_map = self._create_pvcs_and_wait_for_cronjobs(
            storageclass_factory,
            multi_pvc_factory,
            pod_factory,
        )
        pvc_namespace = pvc_objs[0].namespace
        cronjob_names = [cj.resource_name for cj in cronjob_map.values()]

        timestamps = self._collect_job_timestamps(pvc_namespace, cronjob_names)
        self._assert_stagger_spread(timestamps)
        self._assert_stagger_within_window(timestamps, window_seconds=180)

    @tier2
    @pytest.mark.polarion_id("OCS-7985")
    def test_same_uid_produces_same_offset(
        self,
        storageclass_factory,
        multi_pvc_factory,
        pod_factory,
    ):
        """
        Verify that the stagger offset is deterministic — the same CronJob
        UID always produces the same offset across consecutive Job rounds.

        Steps:
            1. Create 1 PVC with */3 schedule, wait for CronJob.
            2. Wait for round 1 Job, calculate offset.
            3. Wait for round 2 Job, calculate offset.
            4. Assert offsets match within tolerance.

        """
        pvc_objs, cronjob_map = self._create_pvcs_and_wait_for_cronjobs(
            storageclass_factory,
            multi_pvc_factory,
            pod_factory,
            num_pvcs=1,
        )
        pvc_namespace = pvc_objs[0].namespace
        cronjob_names = [cj.resource_name for cj in cronjob_map.values()]

        logger.info("Waiting for round 1 Job")
        ts_round1 = self._collect_job_timestamps(pvc_namespace, cronjob_names)
        offsets_round1 = self._calculate_stagger_offsets(ts_round1, interval_minutes=3)

        logger.info("Waiting for round 2 Job")
        ts_round2 = self._collect_nth_round_job_timestamps(
            pvc_namespace,
            cronjob_names,
            round_number=2,
        )
        offsets_round2 = self._calculate_stagger_offsets(ts_round2, interval_minutes=3)

        self._assert_offsets_match(offsets_round1, offsets_round2, interval_seconds=180)

    @tier3
    @pytest.mark.polarion_id("OCS-7986")
    def test_missed_run_bypasses_stagger(
        self,
        storageclass_factory,
        multi_pvc_factory,
        pod_factory,
    ):
        """
        Verify that a CronJob that missed its scheduled run while suspended
        fires immediately upon resume, without waiting for the stagger offset.

        Steps:
            1. Create 1 PVC with */3 schedule, wait for CronJob + first Job.
            2. Suspend the CronJob.
            3. Wait past the schedule interval so a run is missed.
            4. Resume the CronJob and record the resume timestamp.
            5. Wait for the next Job and assert it was created promptly.

        """
        pvc_objs, cronjob_map = self._create_pvcs_and_wait_for_cronjobs(
            storageclass_factory,
            multi_pvc_factory,
            pod_factory,
            num_pvcs=1,
        )
        pvc_namespace = pvc_objs[0].namespace
        cj_name = list(cronjob_map.keys())[0]
        cj_obj = cronjob_map[cj_name]

        logger.info("Waiting for first Job to confirm scheduling works")
        self._collect_job_timestamps(pvc_namespace, [cj_obj.resource_name])

        logger.info("Suspending CronJob '%s'", cj_obj.resource_name)
        self.suspend_cronjob(cj_obj)

        cj_data = cj_obj.get()
        assert (
            cj_data["spec"].get("suspend") is True
        ), f"CronJob '{cj_obj.resource_name}' should be suspended"

        logger.info(
            "Waiting %ds for at least one scheduled run to be missed",
            SUSPEND_WAIT_SECONDS,
        )
        time.sleep(SUSPEND_WAIT_SECONDS)

        ocp_jobs = OCP(kind=constants.RECLAIMSPACEJOBS, namespace=pvc_namespace)
        jobs_before = ocp_jobs.get().get("items", [])
        jobs_before_count = len(
            [
                j
                for j in jobs_before
                if j["metadata"]["name"].startswith(f"{cj_obj.resource_name}-")
            ]
        )

        logger.info("Resuming CronJob '%s'", cj_obj.resource_name)
        resume_time = datetime.now(timezone.utc).replace(tzinfo=None)
        self.resume_cronjob(cj_obj)

        logger.info("Waiting for post-resume Job")
        for sample in TimeoutSampler(
            timeout=JOB_IMMEDIATE_THRESHOLD_SECONDS,
            sleep=5,
            func=ocp_jobs.get,
        ):
            all_jobs = sample.get("items", [])
            matching = [
                j
                for j in all_jobs
                if j["metadata"]["name"].startswith(f"{cj_obj.resource_name}-")
            ]
            if len(matching) > jobs_before_count:
                newest = max(
                    matching,
                    key=lambda j: j["metadata"]["creationTimestamp"],
                )
                ts_str = newest["metadata"]["creationTimestamp"]
                job_time = datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%SZ")
                delay = (job_time - resume_time).total_seconds()
                logger.info(
                    "Post-resume Job created %.1fs after resume",
                    delay,
                )
                assert delay < JOB_IMMEDIATE_THRESHOLD_SECONDS, (
                    f"Missed-run Job took {delay:.1f}s after resume, "
                    f"expected within {JOB_IMMEDIATE_THRESHOLD_SECONDS}s"
                )
                break

    @tier3
    @pytest.mark.polarion_id("OCS-7987")
    def test_offset_survives_controller_restart(
        self,
        storageclass_factory,
        multi_pvc_factory,
        pod_factory,
    ):
        """
        Verify that stagger offsets remain the same after a controller
        restart, since the offset is derived from the CronJob UID which
        does not change.

        Steps:
            1. Create PVCs, wait for CronJobs and round 1 Jobs.
            2. Calculate stagger offsets for round 1.
            3. Verify CronJob UIDs before restart.
            4. Restart the controller.
            5. Wait for round 3 Jobs (skip round 2 catch-up), calculate offsets.
            6. Assert UIDs unchanged and offsets match.

        """
        pvc_objs, cronjob_map = self._create_pvcs_and_wait_for_cronjobs(
            storageclass_factory,
            multi_pvc_factory,
            pod_factory,
        )
        pvc_namespace = pvc_objs[0].namespace
        cronjob_names = [cj.resource_name for cj in cronjob_map.values()]

        logger.info("Collecting round 1 timestamps")
        ts_round1 = self._collect_job_timestamps(pvc_namespace, cronjob_names)
        offsets_round1 = self._calculate_stagger_offsets(ts_round1, interval_minutes=3)

        uids_before = {}
        for pvc_name, cj_obj in cronjob_map.items():
            uids_before[pvc_name] = cj_obj.get()["metadata"]["uid"]

        logger.info("Restarting CSI Addons controller")
        restart_csi_addons_controller()

        uids_after = {}
        for pvc_name, cj_obj in cronjob_map.items():
            uids_after[pvc_name] = cj_obj.get()["metadata"]["uid"]
        assert uids_before == uids_after, (
            f"CronJob UIDs changed after controller restart: "
            f"before={uids_before}, after={uids_after}"
        )

        logger.info(
            "Collecting round 3 timestamps (post-restart, skipping catch-up round)"
        )
        ts_round3 = self._collect_nth_round_job_timestamps(
            pvc_namespace,
            cronjob_names,
            round_number=3,
        )
        offsets_round3 = self._calculate_stagger_offsets(ts_round3, interval_minutes=3)

        self._assert_offsets_match(offsets_round1, offsets_round3, interval_seconds=180)
        logger.info("Stagger offsets survived controller restart")

    @tier3
    @pytest.mark.polarion_id("OCS-7988")
    def test_custom_stagger_window(
        self,
        storageclass_factory,
        multi_pvc_factory,
        pod_factory,
    ):
        """
        Verify that a custom stagger window value is accepted by the
        controller and stagger remains active.

        Sets window=1 (1 hour) and uses */3 schedule. Since the interval
        (3min) is shorter than the window (1h), the effective stagger is
        min(3min, 1h) = 3min. This test verifies the controller accepts
        the custom value and continues to stagger Jobs.

        Note: Testing window-as-the-limiting-factor requires interval >
        window, which needs @hourly or longer schedules (impractical for
        CI). The controller only accepts integer hours (strconv.Atoi).

        Steps:
            1. Set custom stagger window to 1h.
            2. Create PVCs with */3 schedule.
            3. Wait for Jobs.
            4. Verify stagger is active.

        """
        logger.info(
            "Setting custom stagger window: %sh",
            CUSTOM_STAGGER_WINDOW_HOURS,
        )
        update_csi_addons_config(
            CRONJOB_STAGGER_WINDOW_KEY,
            CUSTOM_STAGGER_WINDOW_HOURS,
        )

        pvc_objs, cronjob_map = self._create_pvcs_and_wait_for_cronjobs(
            storageclass_factory,
            multi_pvc_factory,
            pod_factory,
        )
        pvc_namespace = pvc_objs[0].namespace
        cronjob_names = [cj.resource_name for cj in cronjob_map.values()]

        timestamps = self._collect_job_timestamps(pvc_namespace, cronjob_names)
        self._assert_stagger_spread(timestamps)

    @tier3
    @pytest.mark.polarion_id("OCS-7989")
    def test_many_pvcs_spread_uniformly(
        self,
        storageclass_factory,
        multi_pvc_factory,
        pod_factory,
    ):
        """
        Verify that staggered scheduling prevents thundering herd with
        many PVCs sharing the same schedule.

        Steps:
            1. Create 10 PVCs with */3 schedule.
            2. Wait for 10 CronJobs, verify unique UIDs.
            3. Wait for 10 Jobs.
            4. Verify stagger spread and no timestamp clustering.

        """
        pvc_objs, cronjob_map = self._create_pvcs_and_wait_for_cronjobs(
            storageclass_factory,
            multi_pvc_factory,
            pod_factory,
            num_pvcs=NUM_PVCS_LARGE,
        )

        uids = set()
        for cj_obj in cronjob_map.values():
            uid = cj_obj.get()["metadata"]["uid"]
            uids.add(uid)
        assert len(uids) == NUM_PVCS_LARGE, (
            f"Expected {NUM_PVCS_LARGE} unique CronJob UIDs, "
            f"got {len(uids)}: {uids}"
        )

        pvc_namespace = pvc_objs[0].namespace
        cronjob_names = [cj.resource_name for cj in cronjob_map.values()]

        timestamps = self._collect_job_timestamps(
            pvc_namespace, cronjob_names, timeout=LARGE_SCALE_JOB_TIMEOUT
        )
        self._assert_stagger_spread(timestamps)

        ts_sorted = sorted(timestamps.values())
        clustered = 0
        for i in range(len(ts_sorted) - 1):
            if abs((ts_sorted[i + 1] - ts_sorted[i]).total_seconds()) < 2:
                clustered += 1
        max_clustered = NUM_PVCS_LARGE // 3
        logger.info(
            "Adjacent pairs within 2s: %d (max allowed: %d)",
            clustered,
            max_clustered,
        )
        assert clustered <= max_clustered, (
            f"Too many Jobs clustered together ({clustered} pairs within 2s, "
            f"max {max_clustered}). Thundering herd not prevented. "
            f"Timestamps: {dict(sorted(timestamps.items(), key=lambda x: x[1]))}"
        )
