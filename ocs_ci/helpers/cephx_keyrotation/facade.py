"""Facade composing focused CephX key-rotation helper mixins."""

from ocs_ci.helpers.cephx_keyrotation.auth import CephXAuthHelper
from ocs_ci.helpers.cephx_keyrotation.cluster import CephXClusterHelper
from ocs_ci.helpers.cephx_keyrotation.core import CephXKeyRotationCore
from ocs_ci.helpers.cephx_keyrotation.daemon import CephXDaemonRotation
from ocs_ci.helpers.cephx_keyrotation.io import CephXIOHelper
from ocs_ci.helpers.cephx_keyrotation.metrics import CephXMetricsHelper
from ocs_ci.helpers.cephx_keyrotation.mon import CephXMONHelper
from ocs_ci.helpers.cephx_keyrotation.osd import CephXOSDHelper
from ocs_ci.helpers.cephx_keyrotation.security import CephXSecurityHelper


class CephXKeyRotation(
    CephXKeyRotationCore,
    CephXAuthHelper,
    CephXSecurityHelper,
    CephXClusterHelper,
    CephXDaemonRotation,
    CephXOSDHelper,
    CephXMONHelper,
    CephXMetricsHelper,
    CephXIOHelper,
):
    """
    Rotate CephX keys via StorageCluster
    ``managedResources.cephCluster.security.cephx``.

    Implementation is split across focused mixins; this class preserves the
    historical single-entry API used by tests and background operations.
    """
