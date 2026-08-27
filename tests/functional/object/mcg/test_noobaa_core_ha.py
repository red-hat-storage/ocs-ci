import logging

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    mcg,
    post_upgrade,
    red_squad,
    runs_on_provider,
    skipif_ocs_version,
    tier2,
)
from ocs_ci.framework.testlib import MCGTest
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import (
    Pod,
    get_pod_node,
    get_pods_having_label,
    wait_for_pods_by_label_count,
    wait_for_pods_to_be_running,
)
from ocs_ci.ocs.bucket_utils import (
    sync_object_directory,
    verify_s3_object_integrity,
    write_random_objects_in_pod,
)

logger = logging.getLogger(__name__)


def get_noobaa_core_pods():
    """
    Fetch all NooBaa core pods in the cluster namespace.

    Returns:
        list: List of NooBaa core Pod objects

    """
    core_pods = get_pods_having_label(
        label=constants.NOOBAA_CORE_POD_LABEL,
        namespace=config.ENV_DATA["cluster_namespace"],
    )
    return [Pod(**pod) for pod in core_pods]


@mcg
@red_squad
@runs_on_provider
class TestNoobaaCoreHA(MCGTest):
    """
    Test NooBaa core high availability (HA).
    """

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        """
        Ensure NooBaa core HA is re-enabled after the test, even if it fails
        mid-way, so the cluster is not left with a single core pod.
        """

        def finalizer():
            core_pods = get_noobaa_core_pods()
            if len(core_pods) == 2:
                return
            logger.warning(
                f"Found {len(core_pods)} NooBaa core pod(s) during teardown, "
                "re-enabling core HA"
            )
            self._set_core_ha(disabled=False)
            wait_for_pods_by_label_count(
                constants.NOOBAA_CORE_POD_LABEL, expected_count=2
            )

        request.addfinalizer(finalizer)

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
        assert wait_for_pods_by_label_count(
            constants.NOOBAA_CORE_POD_LABEL, expected_count=2
        ), "Expected 2 NooBaa core pods but the count was not reached in time"
        core_pods = get_noobaa_core_pods()
        wait_for_pods_to_be_running(
            namespace=config.ENV_DATA["cluster_namespace"],
            pod_names=[pod.name for pod in core_pods],
            timeout=300,
        )

        node_names = [get_pod_node(pod).name for pod in core_pods]
        logger.info(f"NooBaa core pods are scheduled on nodes: {node_names}")
        assert (
            len(set(node_names)) == 2
        ), f"NooBaa core pods are not running on different nodes: {node_names}"

        self._assert_lease_holder([pod.name for pod in core_pods])
        return core_pods

    @tier2
    @post_upgrade
    @skipif_ocs_version("<5.0")
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
            assert wait_for_pods_by_label_count(
                constants.NOOBAA_CORE_POD_LABEL, expected_count=1
            ), "Expected a single NooBaa core pod after disabling HA"
            single_core_pod = get_noobaa_core_pods()
            wait_for_pods_to_be_running(
                namespace=config.ENV_DATA["cluster_namespace"],
                pod_names=[pod.name for pod in single_core_pod],
                timeout=300,
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
