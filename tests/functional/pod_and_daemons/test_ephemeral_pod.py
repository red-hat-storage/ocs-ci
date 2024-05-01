import pytest

from logging import getLogger

from ocs_ci.ocs.ephernal_storage import EphernalPodFactory
from ocs_ci.ocs.constants import (
    CEPHFS_INTERFACE,
    RBD_INTERFACE,
)

log = getLogger(__name__)


@pytest.mark.parametrize(
    argnames=["interface"], argvalues=[[CEPHFS_INTERFACE], [RBD_INTERFACE]]
)
def test_ephernal_pod_creation(interface):
    pod_name = None
    storage_type = interface
    EphernalPodFactory.create_ephmeral_pod(pod_name, storage_type)
