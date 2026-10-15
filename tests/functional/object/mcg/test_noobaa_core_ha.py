import json
import logging
import time
from datetime import datetime

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    ignore_leftovers,
    mcg,
    post_upgrade,
    red_squad,
    runs_on_provider,
    skipif_ocs_version,
    stretchcluster_required,
    tier2,
    tier4b,
)
from ocs_ci.framework.testlib import MCGTest
from ocs_ci.ocs import constants
from ocs_ci.ocs.node import schedule_nodes, unschedule_nodes, wait_for_nodes_status
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import (
    delete_pods,
    get_active_core_pod,
    get_noobaa_core_pods,
    get_pod_node,
    get_standby_core_pod,
    wait_for_pods_by_label_count,
    wait_for_pods_to_be_running,
)
from ocs_ci.ocs.bucket_utils import (
    copy_objects,
    start_background_object_upload,
    sync_object_directory,
    verify_s3_object_integrity,
    write_random_objects_in_pod,
)
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)

# NooBaa core leader-election lease tunables. leaseDurationSeconds,
# holderIdentity and renewTime live on the Lease object (constants.
# NOOBAA_CORE_LEASE_NAME); renewDeadline and retryPeriod are leader-election
# client settings exposed only through the noobaa-config ConfigMap (read as env
# vars by the core pod), so a change to them requires a core pod restart to take
# effect.
LEASE_DURATION_KEY = "NOOBAA_CORE_LEASE_DURATION"
RENEW_DEADLINE_KEY = "NOOBAA_CORE_RENEW_DEADLINE"
RETRY_PERIOD_KEY = "NOOBAA_CORE_RETRY_PERIOD"


@mcg
@red_squad
@runs_on_provider
@ignore_leftovers
class TestNoobaaCoreHA(MCGTest):
    """
    Test NooBaa core high availability (HA).
    """

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        """
        Ensure NooBaa core HA is re-enabled and any cordoned nodes are
        uncordoned after the test, even if it fails mid-way, so the cluster is
        not left with a single core pod or unschedulable nodes.
        """
        # Per-instance tracker of nodes this test cordoned, uncordoned by the
        # finalizer so a mid-test failure does not leak unschedulable nodes into
        # later tests. Kept per-instance (not a class attribute) so state is not
        # shared across test methods or repeated runs.
        self._cordoned_nodes = []
        # Original lease config captured by a test before it mutates it, so the
        # finalizer can restore it. None means the test never touched it.
        self._original_lease_config = None

        def finalizer():
            # Uncordon, HA restore and lease-config restore are independent
            # recovery steps, each guarded so a failure in one still runs the
            # others.
            if self._cordoned_nodes:
                logger.warning(
                    f"Uncordoning {len(self._cordoned_nodes)} node(s) left "
                    "cordoned during the test"
                )
                try:
                    self._set_nodes_unschedulable(list(self._cordoned_nodes), False)
                except Exception:
                    logger.exception("Failed to uncordon nodes during teardown")

            core_pods = get_noobaa_core_pods()
            if len(core_pods) != 2:
                logger.warning(
                    f"Found {len(core_pods)} NooBaa core pod(s) during teardown, "
                    "re-enabling core HA"
                )
                try:
                    self._set_core_ha(disabled=False)
                    wait_for_pods_by_label_count(
                        constants.NOOBAA_CORE_POD_LABEL, expected_count=2
                    )
                except Exception:
                    logger.exception("Failed to restore core HA during teardown")

            if self._original_lease_config is not None:
                try:
                    if self._get_lease_config() != self._original_lease_config:
                        logger.warning(
                            "Restoring original NooBaa lease configuration "
                            f"{self._original_lease_config}"
                        )
                        self._set_lease_config(**self._original_lease_config)
                        self._rolling_restart_core_pods()
                except Exception:
                    logger.exception(
                        "Failed to restore lease configuration during teardown"
                    )

        request.addfinalizer(finalizer)

    def _set_nodes_unschedulable(self, node_names, unschedulable):
        """
        Cordon or uncordon the given nodes and update the cordon tracker.

        Delegates to the shared ``unschedule_nodes``/``schedule_nodes`` helpers,
        which wait for the nodes to reach the expected scheduling status before
        returning. This avoids racing a follow-up pod deletion against a cordon
        the scheduler has not yet observed.

        When cordoning, only nodes that are currently schedulable are cordoned
        and tracked, so the test never uncordons a node that was already
        unschedulable before it ran.

        Args:
            node_names (list): Node names to cordon/uncordon
            unschedulable (bool): True cordons the nodes, False uncordons them

        """
        if not node_names:
            return
        if unschedulable:
            ocp = OCP(kind="Node")
            to_cordon = [
                node_name
                for node_name in node_names
                if not ocp.get(resource_name=node_name)
                .get("spec", {})
                .get("unschedulable", False)
            ]
            if not to_cordon:
                return
            # Track before cordoning so that if unschedule_nodes raises partway
            # through (e.g. wait_for_nodes_status times out), the finalizer can
            # still uncordon the nodes that were already cordoned.
            for node_name in to_cordon:
                if node_name not in self._cordoned_nodes:
                    self._cordoned_nodes.append(node_name)
            unschedule_nodes(to_cordon)
        else:
            schedule_nodes(node_names)
            for node_name in node_names:
                if node_name in self._cordoned_nodes:
                    self._cordoned_nodes.remove(node_name)

    def _set_core_ha(self, disabled):
        """
        Patch the NooBaa CR to enable/disable core HA.

        Args:
            disabled (bool): True disables core HA (single core pod),
                False enables core HA (two core pods)

        """
        noobaa_obj = OCP(
            kind=constants.NOOBAA_RESOURCE_NAME,
            namespace=config.ENV_DATA["cluster_namespace"],
        )
        params = f'{{"spec":{{"disableCoreHA":{str(disabled).lower()}}}}}'
        logger.info(
            f"{'Disabling' if disabled else 'Enabling'} NooBaa core HA "
            f"via patch: {params}"
        )
        noobaa_obj.patch(
            resource_name=constants.NOOBAA_RESOURCE_NAME,
            params=params,
            format_type="merge",
        )

    def _assert_lease_holder(self, core_pod_names):
        """
        Confirm the NooBaa core leader-election lease exists and its
        holderIdentity matches one of the NooBaa core pods.

        The search is scoped to NooBaa-owned leases so that a missing or
        renamed core lease is caught rather than silently ignored.

        Args:
            core_pod_names (list): Names of the running NooBaa core pods

        """
        namespace = config.ENV_DATA["cluster_namespace"]
        lease_ocp = OCP(kind="Lease", namespace=namespace)
        leases = lease_ocp.get().get("items", [])
        nb_leases = [
            lease
            for lease in leases
            if "noobaa" in lease.get("metadata", {}).get("name", "")
        ]
        assert nb_leases, (
            f"No NooBaa Lease object found in namespace {namespace}; "
            "the core leader-election lease may be missing or renamed"
        )

        holder = None
        for lease in nb_leases:
            holder_identity = lease.get("spec", {}).get("holderIdentity")
            if holder_identity and any(
                holder_identity == name or holder_identity.startswith(f"{name}_")
                for name in core_pod_names
            ):
                holder = holder_identity
                logger.info(
                    f"Lease '{lease['metadata']['name']}' holderIdentity "
                    f"'{holder_identity}' matches a NooBaa core pod"
                )
                break

        assert holder, (
            "No NooBaa Lease holderIdentity matched a running NooBaa core pod. "
            f"Core pods: {core_pod_names}"
        )

    def _validate_core_ha_topology(self):
        """
        Confirm the HA-enabled topology: two NooBaa core pods, running, on
        distinct nodes, with a leader-election lease held by one of them.

        Returns:
            list: The running NooBaa core Pod objects

        """
        core_pods = self._wait_for_core_pods_running(
            expected_count=2,
            assert_msg="Expected 2 NooBaa core pods but the count was not "
            "reached in time",
        )

        node_names = [get_pod_node(pod).name for pod in core_pods]
        logger.info(f"NooBaa core pods are scheduled on nodes: {node_names}")
        assert (
            len(set(node_names)) == 2
        ), f"NooBaa core pods are not running on different nodes: {node_names}"

        self._assert_lease_holder([pod.name for pod in core_pods])
        return core_pods

    def _get_pod_zones(self, pods):
        """
        Get the topology zone for each pod's node.

        Args:
            pods (list): List of Pod objects

        Returns:
            dict: Mapping of pod names to their zones

        """
        pod_zones = {}
        for pod in pods:
            node = get_pod_node(pod)
            labels = node.data.get("metadata", {}).get("labels", {})
            zone = labels.get(constants.ZONE_LABEL, "unknown")
            pod_zones[pod.name] = zone
            logger.info(f"Pod {pod.name} is on node {node.name} in zone {zone}")
        return pod_zones

    def _wait_for_core_pods_running(self, expected_count=2, assert_msg=None):
        """
        Wait for the expected number of NooBaa core pods to exist and be
        Running, then return them.

        Args:
            expected_count (int): Number of core pods to wait for
            assert_msg (str): Message raised if the count is not reached

        Returns:
            list: The running NooBaa core Pod objects

        """
        assert wait_for_pods_by_label_count(
            constants.NOOBAA_CORE_POD_LABEL, expected_count=expected_count
        ), (assert_msg or f"Expected {expected_count} NooBaa core pod(s)")
        core_pods = get_noobaa_core_pods()
        # Re-check the count after fetching: a pod can be deleted between the
        # label-count wait above and this fetch, which would otherwise return
        # fewer pods than expected to later topology assertions.
        assert len(core_pods) == expected_count, (
            assert_msg or f"Expected {expected_count} NooBaa core pod(s)"
        )
        assert wait_for_pods_to_be_running(
            namespace=config.ENV_DATA["cluster_namespace"],
            pod_names=[pod.name for pod in core_pods],
            timeout=300,
        ), "NooBaa core pods did not reach Running state within 300 seconds"
        return core_pods

    def _wait_for_core_pods_replaced(self, original_core_pods, timeout=300):
        """
        Wait until both NooBaa core pods are replaced (new UIDs) and Running.

        Used after a kill that does not wait for deletion (e.g. a simultaneous
        kill with wait=False), so topology validation runs against the recovered
        pods rather than passing on the pre-kill pods that are still terminating.

        Args:
            original_core_pods (list): Core Pod objects present before the kill
            timeout (int): Seconds to wait for both pods to be replaced

        """
        original_uids = {pod.pod_data["metadata"]["uid"] for pod in original_core_pods}
        for current in TimeoutSampler(
            timeout=timeout, sleep=10, func=get_noobaa_core_pods
        ):
            if len(current) != 2:
                continue
            if original_uids & {pod.pod_data["metadata"]["uid"] for pod in current}:
                continue
            if all(pod.status() == constants.STATUS_RUNNING for pod in current):
                break

    def _wait_for_active_core_pod(self, core_pods, namespace, timeout=120):
        """
        Return the active (leader) NooBaa core pod, waiting for the
        leader-election lease to settle on one of the given pods.

        After the leader is deleted the lease can still name the old pod until
        it is renewed, so ``get_active_core_pod`` may briefly return None; this
        polls until the lease resolves to one of ``core_pods``. The returned
        object is an element of ``core_pods`` so callers can compare it by
        identity.

        Args:
            core_pods (list): Current NooBaa core Pod objects
            namespace (str): Namespace holding the leader-election lease
            timeout (int): Seconds to wait for the lease to settle

        Returns:
            Pod: The active NooBaa core pod

        """
        active_pod = None
        for active_pod in TimeoutSampler(
            timeout=timeout,
            sleep=5,
            func=get_active_core_pod,
            core_pods=core_pods,
            namespace=namespace,
        ):
            if active_pod:
                break
        assert (
            active_pod
        ), "Could not identify the active NooBaa core pod before standby selection"
        return active_pod

    def _wait_for_leader_change(
        self, previous_active_name, namespace, timeout=60, start_time=None
    ):
        """
        Wait for the NooBaa core leader-election lease to move off the given pod
        and return the pod that took over along with how long the takeover took.

        Used after the active pod is lost (killed or its node shut down): the
        lease keeps naming the old leader until it stops renewing, so this polls
        ``get_active_core_pod`` until a different pod holds the lease. The
        ``timeout`` caps the allowed takeover window - reaching the return proves
        the takeover happened within it. The new leader is also confirmed Running
        (healthy and able to serve) before returning. For accurate SLA enforcement,
        pass ``start_time`` as the time before the disruption; otherwise timing
        starts when this function is called.

        Args:
            previous_active_name (str): Name of the pod that held leadership
            namespace (str): Namespace holding the leader-election lease
            timeout (int): Maximum seconds to allow for the takeover
            start_time (float): Monotonic time before the disruption (for SLA enforcement)

        Returns:
            tuple: (Pod, float) the new active pod and the takeover time in
                seconds

        """
        start = start_time or time.monotonic()
        new_active = None
        for candidate in TimeoutSampler(
            timeout=timeout,
            sleep=5,
            func=get_active_core_pod,
            namespace=namespace,
        ):
            if candidate and candidate.name != previous_active_name:
                new_active = candidate
                break
        elapsed = time.monotonic() - start
        assert new_active, (
            f"No new NooBaa core leader took over within {timeout}s after "
            f"{previous_active_name} stopped being the leader"
        )
        # TimeoutSampler can yield a result slightly past the deadline; enforce the SLA explicitly.
        assert (
            elapsed <= timeout
        ), f"Leadership transfer took {elapsed:.1f}s, exceeding the {timeout}s deadline"
        # Confirm the new leader is Running (healthy and able to serve) before returning.
        assert wait_for_pods_to_be_running(
            namespace=namespace, pod_names=[new_active.name], timeout=120
        ), f"New active core pod {new_active.name} did not reach Running state after takeover"
        logger.info(
            f"Leadership moved from {previous_active_name} to {new_active.name} "
            f"in {elapsed:.1f}s"
        )
        return new_active, elapsed

    def _assert_upload_in_progress(self, upload_future):
        """
        Fail loudly if a background upload has already finished.

        The failover tests kill a pod "while the upload is in progress"; if the
        object is small enough to upload before the kill, the kill would land
        after the upload and the scenario would pass without ever exercising a
        mid-upload failover. This guards against that silent loss of coverage,
        re-raising a genuine upload failure first if the future already errored.

        Args:
            upload_future (Future): The background upload future

        """
        if upload_future.done():
            upload_future.result()
            raise AssertionError(
                "Background upload finished before the pod was killed, so the "
                "failover was not exercised mid-upload; increase the object size"
            )

    def _upload_through_pod_disruption(
        self, mcg_obj, awscli_pod, bucket_factory, test_directory_setup, disrupt_fn, msg
    ):
        """
        Run a background object upload through a cluster disruption and verify it
        survives intact.

        Starts a background upload, waits for it to begin streaming, invokes
        ``disrupt_fn`` (which kills a core pod and performs any failover checks)
        while the upload is in flight, then confirms the upload completes without
        error, the HA topology recovers, and the downloaded object matches the
        original by checksum.

        Args:
            mcg_obj (MCG): MCG object holding the S3 credentials
            awscli_pod (Pod): Pod running the AWS CLI tools
            bucket_factory: Fixture creating a new bucket
            test_directory_setup: Fixture providing origin/result directories
            disrupt_fn (callable): Called once the upload is streaming; performs
                the pod kill and any takeover assertions
            msg (str): Assertion message on checksum mismatch

        Returns:
            list: The running NooBaa core Pod objects after recovery

        """
        bucket_name = bucket_factory(1)[0].name
        result_dir = test_directory_setup.result_dir
        (
            executor,
            upload_future,
            object_key,
            object_path,
            s3_object_path,
        ) = start_background_object_upload(
            mcg_obj, awscli_pod, bucket_name, test_directory_setup.origin_dir
        )
        try:
            # Let the upload start streaming before disrupting, so it is interrupted mid-flight.
            time.sleep(10)
            self._assert_upload_in_progress(upload_future)
            disrupt_fn()
            upload_future.result(timeout=900)
            logger.info("S3 upload completed without failure during the disruption")
        finally:
            executor.shutdown(wait=True)

        # The HA topology should recover to two running core pods.
        core_pods = self._validate_core_ha_topology()

        # Download via copy_objects (aws s3 cp), not sync: sync treats a single-object key as a prefix.
        copy_objects(awscli_pod, s3_object_path, f"{result_dir}/{object_key}", mcg_obj)
        assert verify_s3_object_integrity(
            original_object_path=object_path,
            result_object_path=f"{result_dir}/{object_key}",
            awscli_pod=awscli_pod,
        ), msg
        return core_pods

    @staticmethod
    def _parse_seconds(value):
        """
        Parse a ``noobaa-config`` duration value (e.g. ``"20s"``) into an int.

        Args:
            value (str): Duration string, with or without the trailing ``s``

        Returns:
            int: The duration in seconds

        """
        return int(str(value).rstrip("s"))

    @staticmethod
    def _parse_timestamp(value):
        """
        Parse an RFC3339 Lease timestamp (e.g. ``renewTime``) into a datetime.

        Args:
            value (str): RFC3339 timestamp, possibly ending in ``Z``

        Returns:
            datetime: The parsed, timezone-aware datetime

        """
        return datetime.fromisoformat(value.replace("Z", "+00:00"))

    @staticmethod
    def _noobaa_config_ocp():
        """
        Return an OCP handle to the noobaa-config ConfigMap.

        Returns:
            OCP: Handle bound to the noobaa-config ConfigMap

        """
        return OCP(
            kind=constants.CONFIGMAP,
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name=constants.NOOBAA_CONFIGMAP,
        )

    def _get_lease_config(self):
        """
        Read the NooBaa core leader-election tunables from the noobaa-config
        ConfigMap.

        Returns:
            dict: ``lease_duration``, ``renew_deadline`` and ``retry_period`` in
                seconds (int)

        """
        data = self._noobaa_config_ocp().get().get("data", {})
        return {
            "lease_duration": self._parse_seconds(data[LEASE_DURATION_KEY]),
            "renew_deadline": self._parse_seconds(data[RENEW_DEADLINE_KEY]),
            "retry_period": self._parse_seconds(data[RETRY_PERIOD_KEY]),
        }

    def _set_lease_config(
        self, lease_duration=None, renew_deadline=None, retry_period=None
    ):
        """
        Patch the NooBaa core leader-election tunables in the noobaa-config
        ConfigMap. Only the provided values are changed. A core pod restart is
        required for the change to take effect.

        Args:
            lease_duration (int): leaseDurationSeconds in seconds
            renew_deadline (int): renewDeadline in seconds
            retry_period (int): retryPeriod in seconds

        """
        updates = {
            LEASE_DURATION_KEY: lease_duration,
            RENEW_DEADLINE_KEY: renew_deadline,
            RETRY_PERIOD_KEY: retry_period,
        }
        patch_data = {key: f"{val}s" for key, val in updates.items() if val is not None}
        if not patch_data:
            return
        self._noobaa_config_ocp().patch(
            resource_name=constants.NOOBAA_CONFIGMAP,
            params=json.dumps({"data": patch_data}),
            format_type="merge",
        )
        logger.info(f"Patched {constants.NOOBAA_CONFIGMAP} lease config: {patch_data}")

    def _get_lease_spec(self):
        """
        Read the NooBaa core Lease object spec.

        Returns:
            dict: ``leaseDurationSeconds``, ``holderIdentity`` and ``renewTime``

        """
        lease = OCP(
            kind="Lease",
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name=constants.NOOBAA_CORE_LEASE_NAME,
        ).get()
        spec = lease.get("spec", {})
        return {
            "leaseDurationSeconds": spec.get("leaseDurationSeconds"),
            "holderIdentity": spec.get("holderIdentity"),
            "renewTime": spec.get("renewTime"),
        }

    def _rolling_restart_core_pods(self):
        """
        Rolling restart of the NooBaa core pods: delete the standby first, wait
        for the full topology to recover, then delete the active (leader) pod.

        Restarting both pods at once would drop HA and the lease leader
        simultaneously, so a config change that needs a restart is rolled out one
        pod at a time.
        """
        namespace = config.ENV_DATA["cluster_namespace"]
        core_pods = get_noobaa_core_pods()
        active_pod = self._wait_for_active_core_pod(core_pods, namespace)
        standby_pod = get_standby_core_pod(core_pods, namespace, active_pod=active_pod)
        assert standby_pod, "Could not identify the standby NooBaa core pod"

        logger.info(f"Rolling restart: deleting standby pod {standby_pod.name} first")
        delete_pods([standby_pod])
        self._wait_for_core_pods_running(
            expected_count=2,
            assert_msg="Standby core pod did not recover after rolling restart",
        )

        # Re-resolve the active pod before deleting it; the standby restart should
        # not move leadership, but confirm against the live lease.
        core_pods = get_noobaa_core_pods()
        active_pod = self._wait_for_active_core_pod(core_pods, namespace)
        logger.info(f"Rolling restart: deleting active pod {active_pod.name}")
        delete_pods([active_pod])
        self._wait_for_core_pods_running(
            expected_count=2,
            assert_msg="Active core pod did not recover after rolling restart",
        )

    def _assert_renew_time_advances(self, samples=3):
        """
        Confirm the Lease ``renewTime`` advances on every poll cycle, proving the
        leader keeps renewing the lease.

        Polls once per ``renewDeadline`` window (long enough for at least one
        renewal) and asserts each reading is strictly newer than the previous.

        Args:
            samples (int): Number of readings to compare

        """
        sleep = self._get_lease_config()["renew_deadline"]
        previous = None
        for sample in range(samples):
            renew_time = self._parse_timestamp(self._get_lease_spec()["renewTime"])
            if previous is not None:
                assert renew_time > previous, (
                    "Lease renewTime did not advance between polls: "
                    f"{previous} -> {renew_time}"
                )
            logger.info(f"renewTime poll {sample + 1}/{samples}: {renew_time}")
            previous = renew_time
            if sample < samples - 1:
                time.sleep(sleep)

    @tier2
    @post_upgrade
    @skipif_ocs_version("<5.0")
    @pytest.mark.polarion_id("OCS-8366")
    def test_noobaa_core_ha_availability(
        self, mcg_obj, awscli_pod, bucket_factory, test_directory_setup
    ):
        """
        Verifies that with HA enabled two NooBaa core pods run on different nodes
        with a leader-election lease, that toggling the ``disableCoreHA`` flag on
        the NooBaa CR scales the core pods down to one and back up to two while
        the HA topology is restored, and that data written before the toggle
        survives the scale-down/up.

        1. Confirm 2 NooBaa core pods run on different nodes with a matching
           leader-election lease.
        2. Create an S3 bucket and upload baseline objects (before the toggle).
        3. Disable the HA flag on the NooBaa CR.
        4. Validate a single core pod remains and is Running.
        5. Re-enable the HA flag on the NooBaa CR.
        6. Validate the HA topology is restored (2 running pods on different
           nodes with a matching lease).
        7. Download the baseline objects and verify their checksums survived.

        """
        # 1. Confirm the initial HA topology
        logger.info("Verifying the initial NooBaa core HA topology")
        self._validate_core_ha_topology()

        # 2. Create bucket and upload baseline objects before disabling HA
        bucket_name = bucket_factory(1)[0].name
        logger.info(f"Created bucket {bucket_name} for object I/O verification")
        origin_dir = test_directory_setup.origin_dir
        result_dir = test_directory_setup.result_dir
        full_object_path = f"s3://{bucket_name}"

        written_objs = write_random_objects_in_pod(awscli_pod, origin_dir, 10, bs="64K")
        sync_object_directory(awscli_pod, origin_dir, full_object_path, mcg_obj)

        try:
            # 3. Disable HA flag from NooBaa CR
            self._set_core_ha(disabled=True)

            # 4. Validate there is only 1 core pod and it is Running
            logger.info("Verifying that a single NooBaa core pod remains")
            self._wait_for_core_pods_running(
                expected_count=1,
                assert_msg="Expected a single NooBaa core pod after disabling HA",
            )
        finally:
            # 5. Re-enable HA flag in NooBaa CR (always restore, even on failure)
            self._set_core_ha(disabled=False)

        # 6. Validate the HA topology is restored after re-enabling
        logger.info("Verifying the NooBaa core HA topology is restored")
        self._validate_core_ha_topology()

        # 7. Download the baseline objects and verify their checksums survived
        sync_object_directory(awscli_pod, full_object_path, result_dir, mcg_obj)
        for obj in written_objs:
            assert verify_s3_object_integrity(
                original_object_path=f"{origin_dir}/{obj}",
                result_object_path=f"{result_dir}/{obj}",
                awscli_pod=awscli_pod,
            ), f"Checksum mismatch for object {obj} after the HA toggle"
        logger.info("Baseline object checksums survived the HA toggle")

    @tier2
    @pytest.mark.polarion_id("OCS-8367")
    def test_noobaa_core_ha_multipart_upload_failover(
        self, mcg_obj, awscli_pod, bucket_factory, test_directory_setup
    ):
        """
        Verifies that an in-flight multipart upload survives an active NooBaa
        core pod failover: the active (leader) core pod is killed mid-upload, the
        standby pod takes over leadership, and the S3 upload retries and
        completes without failure with the downloaded object matching the
        original data.

        1. Confirm 2 NooBaa core pods run on different nodes with a matching
           leader-election lease.
        2. Start uploading a 1GB multipart object with a >2 minute retry window.
        3. Kill the active core pod so the in-flight upload goes into retry mode.
        4. Confirm the standby pod takes over leadership within 60 seconds.
        5. Confirm the S3 multipart upload completes without failure.
        6. Download the object and validate its checksum against the original.

        """
        namespace = config.ENV_DATA["cluster_namespace"]

        # 1. Confirm the initial HA topology (2 core pods on different nodes)
        logger.info("Verifying the initial NooBaa core HA topology")
        core_pods = self._validate_core_ha_topology()

        def kill_active_and_assert_failover():
            # 3. Kill the active (leader) core pod mid-upload to force the upload into retry mode.
            active_pod = self._wait_for_active_core_pod(core_pods, namespace)
            logger.info(f"Killing active NooBaa core pod {active_pod.name} mid-upload")
            start_time = time.monotonic()
            delete_pods([active_pod])
            # 4. The standby pod should take over leadership within 60 seconds.
            self._wait_for_leader_change(
                active_pod.name, namespace, start_time=start_time
            )

        # 2-6. Upload 1GB mid-kill, then verify the upload completes and the object round-trips.
        self._upload_through_pod_disruption(
            mcg_obj,
            awscli_pod,
            bucket_factory,
            test_directory_setup,
            kill_active_and_assert_failover,
            "Checksum mismatch for the multipart object after the core pod failover",
        )
        logger.info("Multipart object checksum validated after core pod failover")

    @tier4b
    @pytest.mark.polarion_id("OCS-8368")
    def test_noobaa_core_ha_failover_resilience(
        self,
        mcg_obj,
        awscli_pod,
        bucket_factory,
        test_directory_setup,
        nodes,
        node_restart_teardown,
    ):
        """
        Verifies NooBaa core HA failover resilience across pod kills, a node
        shutdown and repeated/simultaneous pod kills, always returning to the
        two-pod HA topology with leadership taken over within 60 seconds.

        1. Confirm the initial HA topology.
        2. Kill the active core pod.
        3. Confirm the standby pod takes over leadership within 60 seconds.
        4. Confirm the HA topology recovers with a new standby core pod.
        5. Kill the new standby pod while an S3 upload is in progress and confirm
           the upload completes without failure.
        6. Shut down the node running the active pod.
        7. Confirm the standby pod takes over leadership within 60 seconds.
        8. Recover the node and confirm the cluster converges back to a healthy
           single-active HA topology (a leadership re-election during recovery is
           an accepted, self-healing transient per DFBUGS-10829).
        9. Kill the active pod 5 times, letting both pods recover between kills.
        10. Kill the active and standby pods simultaneously and confirm recovery.

        """
        namespace = config.ENV_DATA["cluster_namespace"]

        # 1. Confirm the initial HA topology
        logger.info("Verifying the initial NooBaa core HA topology")
        core_pods = self._validate_core_ha_topology()

        # 2-3. Kill the active core pod; the standby must take over within 60s
        active_pod = self._wait_for_active_core_pod(core_pods, namespace)
        logger.info(f"Killing active core pod {active_pod.name}")
        start_time = time.monotonic()
        delete_pods([active_pod])
        self._wait_for_leader_change(active_pod.name, namespace, start_time=start_time)

        # 4. The topology must recover with a new standby core pod
        core_pods = self._validate_core_ha_topology()
        standby_pod = get_standby_core_pod(core_pods, namespace)
        assert standby_pod, "Could not identify the standby NooBaa core pod"
        logger.info(f"New standby core pod is {standby_pod.name}")

        # 5. Kill the standby during an upload; it must not disrupt the leader or the S3 I/O.
        def kill_standby():
            logger.info(f"Killing standby core pod {standby_pod.name} during S3 I/O")
            delete_pods([standby_pod])

        core_pods = self._upload_through_pod_disruption(
            mcg_obj,
            awscli_pod,
            bucket_factory,
            test_directory_setup,
            kill_standby,
            "Checksum mismatch for the object uploaded while the standby was killed",
        )

        # 6-7. Shut down the active pod's node; the standby must take over within 60 seconds.
        active_pod = self._wait_for_active_core_pod(core_pods, namespace)
        active_node = get_pod_node(active_pod)
        logger.info(
            f"Shutting down node {active_node.name} hosting active pod "
            f"{active_pod.name}"
        )
        start_time = time.monotonic()
        nodes.stop_nodes(nodes=[active_node])
        try:
            wait_for_nodes_status(
                node_names=[active_node.name], status=constants.NODE_NOT_READY
            )
            self._wait_for_leader_change(
                active_pod.name, namespace, start_time=start_time
            )
        finally:
            # 8. Recover the node (always, even if the assertions above failed).
            logger.info(f"Starting node {active_node.name} back up")
            nodes.start_nodes(nodes=[active_node])
            wait_for_nodes_status(
                node_names=[active_node.name], status=constants.NODE_READY
            )

        # 8. After recovery, assert HA converges to one active pod; switchback is an accepted transient (DFBUGS-10829).
        self._validate_core_ha_topology()
        current_active_pod = self._wait_for_active_core_pod(
            get_noobaa_core_pods(), namespace
        )
        logger.info(
            "HA topology healthy after node recovery; active core pod is "
            f"{current_active_pod.name}"
        )

        # 9. Kill the active pod 5x, asserting HA converges between kills (no leader-change assertion).
        for iteration in range(5):
            core_pods = self._validate_core_ha_topology()
            active_pod = self._wait_for_active_core_pod(core_pods, namespace)
            logger.info(
                f"Kill iteration {iteration + 1}/5: deleting active pod "
                f"{active_pod.name}"
            )
            delete_pods([active_pod])
        self._validate_core_ha_topology()

        # 10. Kill the active and standby pods simultaneously and confirm HA recovers.
        core_pods = self._wait_for_core_pods_running(
            expected_count=2,
            assert_msg="Core pods not running before the simultaneous kill",
        )
        logger.info(
            f"Killing both core pods simultaneously: {[p.name for p in core_pods]}"
        )
        # wait=False fires both deletions back-to-back; wait=True would delete them sequentially.
        delete_pods(core_pods, wait=False)
        # Wait until both pods are actually replaced so validation runs on the recovered pods, not the pre-kill ones.
        self._wait_for_core_pods_replaced(core_pods)
        self._validate_core_ha_topology()
        logger.info("NooBaa core HA recovered after killing both pods simultaneously")

    @tier2
    @stretchcluster_required
    @pytest.mark.polarion_id("OCS-8369")
    def test_noobaa_core_ha_zone_spread(self):
        """
        Verifies NooBaa core HA behavior across zones in a stretch cluster.

        1. Validate 2 NooBaa core pods run on different nodes with zones.
        2. Get the initial zones for each core pod.
        3. Kill core pods and verify they respawn with zone distribution.
        4. Repeat pod killing multiple times to verify zone resilience.
        5. Cordon all nodes in one zone to force pod migration.
        6. Uncordon the zone and confirm no auto re-spread occurs.
        7. Kill the standby core pod and confirm it respins in the cordoned zone.

        """
        namespace = config.ENV_DATA["cluster_namespace"]

        # 1. Validate initial HA topology
        logger.info("Step 1: Validating initial NooBaa core HA topology")
        core_pods = self._validate_core_ha_topology()

        # 2. Get the initial zone mapping and confirm the pods start out spread
        # across exactly two zones; this is the placement each kill cycle below
        # must restore.
        logger.info("Step 2: Getting initial zone mapping for core pods")
        initial_zones = set(self._get_pod_zones(core_pods).values())
        assert len(initial_zones) == 2, (
            f"Expected NooBaa core pods spread across two zones, found "
            f"{initial_zones}"
        )

        # 3-4. Kill pods multiple times and verify zone distribution
        logger.info("Step 3-4: Killing core pods multiple times to verify resilience")
        for iteration in range(3):
            logger.info(f"Iteration {iteration + 1}: Killing NooBaa core pods")
            core_pods = get_noobaa_core_pods()
            logger.info(f"Deleting pods: {[pod.name for pod in core_pods]}")
            delete_pods(core_pods)

            # Wait for pods to respawn and verify zone distribution
            core_pods = self._wait_for_core_pods_running(
                expected_count=2,
                assert_msg=f"Expected 2 NooBaa core pods in iteration "
                f"{iteration + 1}",
            )
            zones = set(self._get_pod_zones(core_pods).values())
            assert zones == initial_zones, (
                f"Iteration {iteration + 1}: expected core pods in "
                f"{initial_zones}, found {zones}"
            )
            logger.info(f"After iteration {iteration + 1}: Pods in zones {zones}")

        # 5. Zone A is the zone currently hosting the active (leader) pod, so
        # cordoning it targets the active pod as the scenario requires. The
        # active pod's zone is resolved here (not earlier) because leadership
        # may have shifted during the kill loop above.
        core_pods = get_noobaa_core_pods()
        active_pod = self._wait_for_active_core_pod(core_pods, namespace)
        pod_zones = self._get_pod_zones(core_pods)
        zone_a = pod_zones[active_pod.name]
        zone_b = next(zone for zone in pod_zones.values() if zone != zone_a)
        logger.info(
            f"Step 5: Active pod {active_pod.name} is in Zone A ({zone_a}); "
            f"cordoning its zone. Zone B is {zone_b}"
        )

        # Cordon all nodes in zone A, then kill the active pod so it respins into
        # zone B. Cordoning only blocks new scheduling; a running pod is not
        # evicted by a cordon, so it must be deleted to force migration.
        all_nodes = OCP(kind="Node").get().get("items", [])
        zone_a_nodes = [
            node["metadata"]["name"]
            for node in all_nodes
            if node.get("metadata", {}).get("labels", {}).get(constants.ZONE_LABEL)
            == zone_a
        ]
        logger.info(f"Nodes in zone A: {zone_a_nodes}")
        self._set_nodes_unschedulable(zone_a_nodes, True)

        logger.info(
            f"Deleting active pod {active_pod.name} to force migration from zone A"
        )
        delete_pods([active_pod])

        # Wait for the full two-pod topology to respin away from zone A before
        # uncordoning, so the deleted leader's replacement is guaranteed to exist.
        logger.info("Waiting for all core pods to leave zone A")
        for sampler in TimeoutSampler(
            timeout=300,
            sleep=10,
            func=self._core_pods_migrated_out_of_zone,
            zone=zone_a,
            namespace=namespace,
        ):
            if sampler:
                break

        # 6. Uncordon zone A and confirm no auto re-spread. Only the nodes this
        # test cordoned are uncordoned, so nodes already unschedulable before the
        # test keep their original state.
        logger.info("Step 6: Uncordoning zone A and confirming no auto re-spread")
        pod_zones_before_uncordon = self._get_pod_zones(get_noobaa_core_pods())
        self._set_nodes_unschedulable(list(self._cordoned_nodes), False)

        # Fixed wait to let a (potential) re-spread start, then assert it did
        # not happen. This proves a negative, so there is no event to wait on.
        time.sleep(30)
        core_pods = get_noobaa_core_pods()
        pod_zones_after_uncordon = self._get_pod_zones(core_pods)
        assert (
            pod_zones_before_uncordon == pod_zones_after_uncordon
        ), "Pods auto re-spread after uncordoning zone A (unexpected behavior)"
        logger.info("Confirmed: No auto re-spread occurred after uncordoning zone A")

        # 7. Kill standby core pod and confirm it respins in zone A
        logger.info("Step 7: Killing standby core pod")
        core_pods = get_noobaa_core_pods()
        active_pod = self._wait_for_active_core_pod(core_pods, namespace)
        standby_pod = get_standby_core_pod(core_pods, namespace, active_pod=active_pod)
        assert standby_pod, "Could not identify the standby NooBaa core pod"

        # After the cordon forced both pods into zone B, the standby must be
        # there; killing it should let it respin back into the recovered zone A.
        standby_zone = self._get_pod_zones([standby_pod]).get(standby_pod.name)
        assert standby_zone == zone_b, (
            f"Standby pod is in {standby_zone}, expected both pods in zone B "
            f"({zone_b}) after the cordon/recover sequence"
        )
        logger.info(f"Deleting standby pod {standby_pod.name}")
        delete_pods([standby_pod])

        # Wait for respawn
        core_pods = self._wait_for_core_pods_running(
            expected_count=2,
            assert_msg="Expected 2 NooBaa core pods after deleting standby",
        )

        # Verify new standby pod is in zone A
        respawned_pod = next(
            (pod for pod in core_pods if pod.name != active_pod.name), None
        )
        respawned_zone = self._get_pod_zones([respawned_pod]).get(respawned_pod.name)
        assert (
            respawned_zone == zone_a
        ), f"Respawned pod is in {respawned_zone}, expected {zone_a}"
        logger.info(f"Confirmed: Respawned standby pod is in zone A ({zone_a})")

    @tier2
    @pytest.mark.polarion_id("OCS-8370")
    def test_noobaa_core_lease_configuration(self):
        """
        Verifies the NooBaa core leader-election lease tunables are configurable
        via the noobaa-config ConfigMap and take effect on the Lease object.

        leaseDurationSeconds, holderIdentity and renewTime live on the Lease
        object; renewDeadline and retryPeriod live only in the ConfigMap. Changes
        to the tunables need a core pod restart, applied as a rolling restart
        (standby first, then active) to preserve HA.

        1. Record leaseDurationSeconds, renewDeadline, retryPeriod,
           holderIdentity and renewTime.
        2. Set leaseDurationSeconds=15 and renewDeadline=6 together, rolling
           restart, and validate leaseDurationSeconds=15 on the Lease.
        3. Verify retryPeriod < renewDeadline < leaseDurationSeconds (3 < 6 < 15).
        4. Validate renewTime updates every poll cycle.
        5. Restore leaseDurationSeconds=20 and renewDeadline=10 (confirming the
           values are configurable in both directions) and validate.

        """
        # 0. Start from a healthy HA topology so the rolling restarts have two
        # pods to work with.
        self._validate_core_ha_topology()

        # 1. Record the initial lease configuration and live Lease state.
        self._original_lease_config = self._get_lease_config()
        lease_spec = self._get_lease_spec()
        logger.info(
            f"Step 1: Initial lease config {self._original_lease_config}; "
            f"holderIdentity={lease_spec['holderIdentity']}, "
            f"renewTime={lease_spec['renewTime']}, "
            f"leaseDurationSeconds={lease_spec['leaseDurationSeconds']}"
        )

        # 2. Set leaseDurationSeconds=15 and renewDeadline=6 together. They must
        # change together: setting 15s alone leaves renewDeadline=10s, and
        # LOST_GRACE must stay < leaseDurationSeconds - renewDeadline. With
        # renewDeadline=10 that window is 15-10=5 < LOST_GRACE (8s) and the core
        # crashes; with renewDeadline=6 it is 15-6=9 > LOST_GRACE.
        logger.info("Step 2: Setting leaseDurationSeconds=15, renewDeadline=6")
        self._set_lease_config(lease_duration=15, renew_deadline=6)
        self._rolling_restart_core_pods()
        self._wait_for_core_pods_running(
            expected_count=2,
            assert_msg="Core pods not Running after rolling restart (15s/6s)",
        )
        updated_config = self._get_lease_config()
        assert (
            updated_config["lease_duration"] == 15
        ), f"Expected leaseDuration 15 in config, found {updated_config}"
        lease_duration = self._get_lease_spec()["leaseDurationSeconds"]
        assert (
            lease_duration == 15
        ), f"Expected leaseDurationSeconds=15 on the Lease, found {lease_duration}"

        # 3. Verify retryPeriod < renewDeadline < leaseDurationSeconds (3 < 6 < 15)
        logger.info("Step 3: Verifying retryPeriod < renewDeadline < leaseDuration")
        assert (
            updated_config["retry_period"]
            < updated_config["renew_deadline"]
            < updated_config["lease_duration"]
        ), (
            "Expected retryPeriod < renewDeadline < leaseDurationSeconds, found "
            f"{updated_config}"
        )

        # 4. Validate renewTime advances on every poll cycle.
        logger.info("Step 4: Validating renewTime advances every poll cycle")
        self._assert_renew_time_advances()

        # 5. Restore leaseDurationSeconds=20 and renewDeadline=10 to confirm the
        # values are configurable in both directions.
        logger.info("Step 5: Restoring leaseDurationSeconds=20, renewDeadline=10")
        self._set_lease_config(lease_duration=20, renew_deadline=10)
        self._rolling_restart_core_pods()
        self._wait_for_core_pods_running(
            expected_count=2,
            assert_msg="Core pods not Running after rolling restart (20s/10s)",
        )
        restored_config = self._get_lease_config()
        assert (
            restored_config["lease_duration"] == 20
            and restored_config["renew_deadline"] == 10
        ), f"Expected leaseDuration=20, renewDeadline=10 in config, found {restored_config}"
        lease_duration = self._get_lease_spec()["leaseDurationSeconds"]
        assert (
            lease_duration == 20
        ), f"Expected leaseDurationSeconds=20 on the Lease, found {lease_duration}"
        logger.info("Confirmed: NooBaa core lease tunables are configurable")

    def _core_pods_migrated_out_of_zone(self, zone, namespace):
        """
        Check whether the full HA topology has migrated out of the given zone.

        Requires exactly two NooBaa core pods, all Running, none in ``zone``,
        and the leader-election lease settled on one of them. Waiting for the
        complete topology (not merely "no pod in the zone") avoids uncordoning
        while the deleted leader's replacement has not yet been recreated - at
        that moment only the surviving standby exists, which would otherwise
        satisfy a simple "no pod in zone" check.

        Args:
            zone (str): The zone the core pods must have left
            namespace (str): Namespace holding the leader-election lease

        Returns:
            bool: True when the migration is complete, False otherwise

        """
        core_pods = get_noobaa_core_pods()
        if len(core_pods) != 2:
            return False
        if not all(pod.status() == constants.STATUS_RUNNING for pod in core_pods):
            return False
        pod_zones = self._get_pod_zones(core_pods)
        if any(pod_zone == zone for pod_zone in pod_zones.values()):
            return False
        return get_active_core_pod(core_pods, namespace) is not None
