"""
Backward-compatible import path for CephX key rotation helpers.

Implementation lives in ``ocs_ci.helpers.cephx_keyrotation`` (focused mixins +
facade). Prefer::

    from ocs_ci.helpers.cephx_keyrotation import CephXKeyRotation
"""

from ocs_ci.helpers.cephx_keyrotation import CephXKeyRotation

__all__ = ["CephXKeyRotation"]
