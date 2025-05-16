import logging
import pytest

from ocs_ci.framework import config
from ocs_ci.ocs import ocp, constants
from ocs_ci.ocs.node import (
    apply_node_affinity_for_noobaa_pod,
)
from ocs_ci.ocs.resources.pod import (
    wait_for_pods_to_be_running,
)
from ocs_ci.framework.pytest_customization.marks import (
    brown_squad,
    tier1,
    mcg_only_required,
)

log = logging.getLogger(__name__)


@tier1
@mcg_only_required
@brown_squad
class TestNoobaaPodNodeAffinity:
    @pytest.fixture(scope="class", autouse=True)
    def teardown(self, request):
        """
        This teardown will bring storage cluster with default values.
        """

        def finalizer():
            """
            Finalizer will take care of below activities:
            1. Removes nodeaffinity to bring storage cluster with default values.

            """
            resource_name = constants.DEFAULT_CLUSTERNAME
            storagecluster_obj = ocp.OCP(
                resource_name=resource_name,
                namespace=config.ENV_DATA["cluster_namespace"],
                kind=constants.STORAGECLUSTER,
            )
            params = '[{"op": "remove", "path": "/spec/placement/noobaa-standalone"}]'
            storagecluster_obj.patch(params=params, format_type="json")
            log.info("Patched storage cluster  back to the default")
            assert (
                wait_for_pods_to_be_running()
            ), "some of the pods didn't came up running"

        request.addfinalizer(finalizer)

    def test_tolerations_on_standalone_noobaa(self):
        """
        This test verifies whether standalone noobaa toleration is
        gets added in storagecluster or not.
        https://bugzilla.redhat.com/show_bug.cgi?id=2260550#c21
        """

        assert apply_node_affinity_for_noobaa_pod(), "Failed to apply toleration."
