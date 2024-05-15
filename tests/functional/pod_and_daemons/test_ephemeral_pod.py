import pytest

from logging import getLogger
from ocs_ci.ocs.resources.pod import get_pvc_name
from ocs_ci.ocs.ephernal_storage import EphernalPodFactory
from ocs_ci.ocs.constants import (
    CEPHFS_INTERFACE,
    RBD_INTERFACE,
)
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
        self.ephemeral_pod = EphernalPodFactory.create_ephmeral_pod(
            pod_name, storage_type
        )

    def test_delete_ephernal_pod(self) -> None:
        get_pvc_name(self.ephemeral_pod)
        # ephemeral_pvcs = list()

        # delete pod
        # make sure pvc deleted aswell
