import pytest

from logging import getLogger

from ocs_ci.ocs.resources.pvc import get_all_pvcs
from ocs_ci.ocs.ephernal_storage import EphernalPodFactory
from ocs_ci.ocs.constants import (
    CEPHFS_INTERFACE,
    RBD_INTERFACE,
)
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    tier1,
    brown_squad,
)

log = getLogger(__name__)


@tier1
@brown_squad
class TestEphernalPod:
    @pytest.mark.parametrize(
        argnames=["interface"], argvalues=[[CEPHFS_INTERFACE], [RBD_INTERFACE]]
    )
    def test_ephernal_pod_creation(self, interface) -> None:
        pod_name = None
        storage_type = interface
        ephemeral_pod = EphernalPodFactory.create_ephmeral_pod(pod_name, storage_type)
        log.info(f"Pods Created: {ephemeral_pod}")

        # Test PVC Creation
        log.info("Starting PVC validation")
        pvcs = get_all_pvcs(
            namespace=config.ENV_DATA["cluster_namespace"], selector="test=ephemeral"
        )
        pvc_names = list()
        for pvc in pvcs.get("items"):
            pvc_names.append(pvc.get("metadata").get("name"))
        pvc_prefix_name = ephemeral_pod.get("metadata").get("name")
        found = False
        for pvc_name in pvc_names:
            if pvc_name.startswith(pvc_prefix_name):
                found = True
                break
        assert found, f"PVC attached to pod {pvc_prefix_name} not found"

        # Test Detele pod and make sure pvc is deleted as well
        log.info("Starting pod deletion validation")
        log.info(f"LEN_OF_EPHEMERAL_PODS: {len(ephemeral_pod)}")
        log.info("\n\n\n STARTING POD OBJ")
        # p_name = ephemeral_pod.get("metadata").get("name")

        # delete pod
        # make sure pvc deleted aswell
