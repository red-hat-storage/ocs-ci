import pytest

from logging import getLogger

from ocs_ci.ocs.resources.pvc import get_all_pvcs
from ocs_ci.ocs.ephernal_storage import EphemeralPodFactory
from ocs_ci.ocs.constants import (
    CEPHFS_INTERFACE,
    RBD_INTERFACE,
)
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    tier2,
    brown_squad,
    polarion_id,
    run_on_all_clients_push_missing_configs,
)

log = getLogger(__name__)


@tier2
@brown_squad
@polarion_id("OCS-5792")
class TestEphemeralPod:
    # This a workaround for any cluster other than multiclent provider mode
    @pytest.fixture()
    def cluster_index(self):
        return 0

    @pytest.mark.parametrize(
        argnames=["interface"], argvalues=[[CEPHFS_INTERFACE], [RBD_INTERFACE]]
    )
    @run_on_all_clients_push_missing_configs
    def test_ephemeral_pod_creation(self, interface, cluster_index) -> None:
        pod_name = None
        storage_type = interface
        ephemeral_pod = EphemeralPodFactory.create_ephemeral_pod(pod_name, storage_type)
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
        p_name = ephemeral_pod.get("metadata").get("name")
        log.info(f"P_NAME: {p_name}")
        log.info("Start Deleting ephemeral pods")
        EphemeralPodFactory.delete_ephemeral_pod(
            p_name, config.ENV_DATA["cluster_namespace"]
        )

        # Make sure pvc deleted aswell
        log.info("Starting PVC delete validation")
        pvcs = get_all_pvcs(
            namespace=config.ENV_DATA["cluster_namespace"], selector="test=ephemeral"
        )
        log.info(f"PVCS AT END: {pvcs}")
        assert not pvcs.get("items"), f"PVC {pvc_prefix_name} not deleted"
